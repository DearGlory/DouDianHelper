from __future__ import annotations

import asyncio
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Error, Page, TimeoutError, async_playwright


FEIGE_URL = "https://im.jinritemai.com/pc_seller_v2/main/workspace"
DOUDIAN_ORDER_LIST_URL = "https://fxg.jinritemai.com/ffa/morder/order/list"
BOOTSTRAP_STORAGE_STATE = "storage_state.bootstrap.json"
CDP_STARTUP_LOCK = asyncio.Lock()
FEIGE_SEARCH_LOCK = asyncio.Lock()
CDP_NAVIGATION_RETRY_ERRORS = ("ERR_ABORTED", "ERR_CONNECTION_CLOSED", "ERR_NETWORK_CHANGED", "ERR_CONNECTION_RESET")
RISK_CONTROL_ERROR_TOKEN = "RISK_CONTROL_DETECTED"
SOFT_RISK_CONTROL_ERROR_TOKEN = "SOFT_RISK_CONTROL_DETECTED"


@dataclass
class EligibilityResult:
    eligible: bool
    reason: str


def resolve_runtime_browser_mode(browser_cfg: dict) -> str:
    """Return the effective browser mode used at runtime.

    Rules:
    - headless=true  -> always use isolated Playwright launch (msedge channel)
    - headless=false -> respect configured mode (default cdp)
    """
    if bool(browser_cfg.get("headless", False)):
        return "launch"
    return str(browser_cfg.get("mode", "cdp")).strip().lower() or "cdp"


class BrowserWorker:
    LOGIN_REQUIRED_TEXTS = [
        "登录过期",
        "请重新登录",
        "当前登录账号暂无会话权限",
        "无会话权限",
        "账号权限",
    ]
    SOFT_RISK_TEXTS = [
        "网络问题",
        "网络异常",
        "网络错误",
        "请刷新",
        "刷新重试",
        "请稍后重试",
        "加载失败",
        "请求失败",
        "服务异常",
    ]
    DO_NOT_CONTACT_KEYWORDS = [
        "不要联系",
        "勿联系",
        "别联系",
        "请勿联系",
        "不要打电话",
        "勿打电话",
        "不要来电",
        "勿来电",
        "不要发消息",
        "勿发消息",
        "不要私信",
        "勿私信",
    ]

    def __init__(self, config: dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.playwright = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self._is_cdp = False
        self._last_search_ts = 0.0
        self.order_page: Page | None = None

    async def _load_storage_state(self) -> dict[str, Any] | None:
        browser_cfg = self.config.get("browser", {})
        storage_state_path = browser_cfg.get("storage_state_path")
        if browser_cfg.get("use_real_user_profile", False):
            storage_state_path = browser_cfg.get("bootstrap_storage_state_path", BOOTSTRAP_STORAGE_STATE)
        elif browser_cfg.get("mode") == "cdp":
            storage_state_path = browser_cfg.get("bootstrap_storage_state_path", BOOTSTRAP_STORAGE_STATE)
        if not storage_state_path or not Path(storage_state_path).exists():
            return None
        try:
            return json.loads(Path(storage_state_path).read_text(encoding="utf-8"))
        except Exception as exc:
            self.logger.warning("启动 | storage_state | ignored | %s", exc)
            return None

    async def _prepare_cdp_page(self) -> Page:
        if self.context is None:
            raise RuntimeError("browser context 未初始化")
        async with CDP_STARTUP_LOCK:
            page = await self._find_feige_page()
            await self._ensure_logged_in(page)
        return page

    async def _prepare_order_page(self) -> Page:
        if self.context is None:
            raise RuntimeError("browser context 未初始化")
        for p in self.context.pages:
            url = p.url or ""
            if "fxg.jinritemai.com/ffa/morder/order/list" in url:
                return p
        page = await self.context.new_page()
        await page.goto(DOUDIAN_ORDER_LIST_URL, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(self.config.get("default_wait_ms", 3000))
        return page

    async def start(self, num_workers: int = 1) -> None:
        browser_cfg = self.config["browser"]
        self.playwright = await async_playwright().start()
        effective_mode = resolve_runtime_browser_mode(browser_cfg)

        if effective_mode == "cdp":
            self._is_cdp = True
            storage_state = await self._load_storage_state()
            self.browser = await self.playwright.chromium.connect_over_cdp(browser_cfg["cdp_url"])
            if storage_state is not None:
                self.context = await self.browser.new_context(storage_state=storage_state)
            else:
                self.context = await self.browser.new_context()
        else:
            storage_state = await self._load_storage_state()
            self.browser = await self.playwright.chromium.launch(
                headless=browser_cfg.get("headless", False),
                channel="msedge",
            )
            if storage_state is not None:
                self.context = await self.browser.new_context(storage_state=storage_state)
            else:
                self.context = await self.browser.new_context()
        self.logger.info("浏览器已连接 (mode=%s | CDP=%s | headless=%s)", effective_mode, self._is_cdp, browser_cfg.get("headless", False))

        if self._is_cdp:
            self.page = await self._prepare_cdp_page()
            self.order_page = await self._prepare_order_page()
        else:
            self.page = await self._find_feige_page()
            await self._ensure_logged_in(self.page)
            self.order_page = await self._prepare_order_page()
        await self.page.set_viewport_size({"width": 1920, "height": 1080})
        if self.order_page is not None:
            await self.order_page.set_viewport_size({"width": 1920, "height": 1080})
        feige_url = self.page.url
        self.logger.info("工作页已就绪: %s", feige_url)

    async def _save_storage_state(self) -> None:
        browser_cfg = self.config.get("browser", {})
        if self._is_cdp:
            self.logger.info("CDP 模式不保存 storage state，跳过")
            return
        storage_state_path = browser_cfg.get("storage_state_path")
        if not storage_state_path or self.context is None:
            return
        try:
            await self.context.storage_state(path=storage_state_path)
            self.logger.info("已保存 storage state: %s", storage_state_path)
        except Exception as exc:
            self.logger.warning("停止 | storage_state | ignored | %s", exc)

    async def stop(self) -> None:
        if self.context is not None:
            await self._save_storage_state()
            try:
                await self.context.close()
            except Exception as exc:
                self.logger.warning("停止 | context | ignored | %s", exc)
        if self.browser and not self._is_cdp:
            try:
                await self.browser.close()
            except Exception as exc:
                self.logger.warning("停止 | browser | ignored | %s", exc)
        if self.playwright:
            try:
                await self.playwright.stop()
            except Exception as exc:
                self.logger.warning("停止 | playwright | ignored | %s", exc)
        self.page = None
        self.order_page = None
        self.browser = None
        self.context = None
        self.playwright = None
        self._is_cdp = False

    def _sel(self, key: str) -> str:
        val = self.config["selectors"].get(key, "__TODO__")
        if val == "__TODO__":
            raise NotImplementedError(f"请先在 config.json 中配置 selectors.{key}")
        return val

    async def _goto_feige_workspace(self, page: Page, timeout_ms: int = 15_000) -> None:
        max_attempts = 3 if self._is_cdp else 1
        last_error: Exception | None = None
        for attempt in range(max_attempts):
            try:
                await page.goto(FEIGE_URL, wait_until="domcontentloaded", timeout=timeout_ms)
                break
            except TimeoutError:
                self.logger.warning("导航 | feige | timeout | %s", FEIGE_URL)
                break
            except Error as exc:
                last_error = exc
                if not self._is_cdp or not any(token in str(exc) for token in CDP_NAVIGATION_RETRY_ERRORS) or attempt == max_attempts - 1:
                    raise
                self.logger.warning("导航 | feige | retry %s/%s | %s", attempt + 1, max_attempts, exc)
                await page.wait_for_timeout(1000 * (attempt + 1))
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=5_000)
        except TimeoutError:
            self.logger.warning("导航 | feige | domcontentloaded-timeout | %s", FEIGE_URL)
        except Error as exc:
            if last_error is not None and self._is_cdp and any(token in str(exc) for token in CDP_NAVIGATION_RETRY_ERRORS):
                raise last_error
            raise

    async def _find_feige_page(self) -> Page:
        """Find an existing Feige page, or create/navigate one to Feige."""
        keywords = self.config["browser"].get("target_url_keywords", [])
        if self.context is None:
            raise RuntimeError("browser context 未初始化")
        for p in self.context.pages:
            url = p.url or ""
            if any(k in url for k in keywords):
                return p

        page = self.context.pages[0] if self.context.pages else await self.context.new_page()
        self.logger.warning("导航 | feige | missing-page | goto")
        await self._goto_feige_workspace(page)
        await page.wait_for_timeout(self.config.get("default_wait_ms", 3000))
        return page

    async def _ensure_logged_in(self, page: Page) -> None:
        body_text = await page.locator("body").inner_text()
        if any(text in body_text for text in self.LOGIN_REQUIRED_TEXTS):
            browser_cfg = self.config.get("browser", {})
            user_data_dir = browser_cfg.get("user_data_dir", "edge-profile")
            headless = browser_cfg.get("headless", False)
            raise RuntimeError(
                "飞鸽页面当前没有可用登录态。"
                f"请先使用同一个 user_data_dir 登录一次: {user_data_dir}。"
                f"当前 headless={headless}，请先改为 false，运行 start_edge.bat 手动登录飞鸽，"
                "确认进入工作台后关闭浏览器，再切回 headless=true 重试。"
            )

    async def _reset_feige_page(self, page: Page) -> None:
        self.logger.warning("重置 | feige-page | input-missing | %s", page.url)
        try:
            await page.reload(wait_until="domcontentloaded", timeout=30_000)
        except Exception:
            await self._goto_feige_workspace(page, timeout_ms=30_000)
        await self._ensure_logged_in(page)
        await page.wait_for_timeout(self.config.get("default_wait_ms", 3000))
        await self._raise_if_risk_control_detected(page)

    async def _dismiss_blocking_modal(self, page: Page) -> bool:
        modals = page.locator("div[role='dialog'].auxo-modal-wrap")
        try:
            modal_count = await modals.count()
        except Exception:
            return False
        if modal_count == 0:
            return False

        self.logger.warning("页面 | modal | dismiss-attempt")
        button_patterns = ["确定", "知道了", "关闭", "取消", "我知道了", "确认"]
        close_selectors = [
            "button.auxo-modal-close",
            ".auxo-modal-close",
            "[aria-label='关闭']",
            "[aria-label='Close']",
            "button",
            "div[role='button']",
            "span",
        ]

        for index in range(modal_count - 1, -1, -1):
            modal = modals.nth(index)
            try:
                if not await modal.is_visible():
                    continue
            except Exception:
                continue

            for pattern in button_patterns:
                try:
                    button = modal.get_by_text(pattern, exact=True).first
                    if await button.count() == 0:
                        continue
                    await button.click(timeout=2_000)
                    await page.wait_for_timeout(300)
                    if not await modal.is_visible():
                        return True
                except Exception:
                    continue

            for selector in close_selectors:
                button = modal.locator(selector).first
                try:
                    if await button.count() == 0:
                        continue
                    await button.click(timeout=2_000)
                    await page.wait_for_timeout(300)
                    if not await modal.is_visible():
                        return True
                except Exception:
                    continue

        try:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(300)
        except Exception:
            pass

        try:
            remaining = await page.locator("div[role='dialog'].auxo-modal-wrap").filter(has=page.locator(":visible")).count()
            return remaining == 0
        except Exception:
            return False

    async def _detect_risk_control(self, page: Page) -> str | None:
        risk_selectors = [
            "#captcha_container",
            "iframe[src*='captcha']",
            "iframe[id*='captcha']",
            "div[class*='captcha']",
            "div[id*='captcha']",
        ]
        risk_texts = [
            "验证码",
            "滑块验证",
            "请完成验证",
            "安全验证",
            "人机验证",
        ]

        for selector in risk_selectors:
            try:
                locator = page.locator(selector).first
                if await locator.count() == 0:
                    continue
                if await locator.is_visible():
                    text = ""
                    try:
                        text = (await locator.inner_text()).strip()
                    except Exception:
                        text = ""
                    normalized_text = " ".join(text.split())
                    if normalized_text and any(risk_text in normalized_text for risk_text in risk_texts):
                        return normalized_text
                    if selector == "#captcha_container":
                        return selector
                    return text or selector
            except Exception:
                continue

        return None

    async def _detect_soft_risk_control(self, page: Page) -> str | None:
        try:
            body_text = (await page.locator("body").inner_text()).strip()
        except Exception:
            return None
        normalized = " ".join(body_text.split())
        for text in self.SOFT_RISK_TEXTS:
            if text in normalized:
                return text
        return None

    async def _raise_if_risk_control_detected(self, page: Page) -> None:
        detail = await self._detect_risk_control(page)
        if detail is not None:
            raise RuntimeError(
                f"{RISK_CONTROL_ERROR_TOKEN}: 检测到飞鸽风控/验证码拦截，请先在浏览器中完成人机验证或稍后重试。详情: {detail}"
            )
        soft_detail = await self._detect_soft_risk_control(page)
        if soft_detail is not None:
            raise RuntimeError(
                f"{SOFT_RISK_CONTROL_ERROR_TOKEN}: 检测到飞鸽搜索/页面出现软风控或限流提示，请暂停后重试。详情: {soft_detail}"
            )

    async def _ensure_page_ready(self, page: Page) -> None:
        await self._raise_if_risk_control_detected(page)
        for _ in range(3):
            dismissed = await self._dismiss_blocking_modal(page)
            if not dismissed:
                await self._raise_if_risk_control_detected(page)
                return
            await page.wait_for_timeout(300)
            await self._raise_if_risk_control_detected(page)

    async def _focus_search_input(self, page: Page):
        search_input = page.locator(self._sel("feige_search_input")).first
        try:
            await self._ensure_page_ready(page)
            await search_input.wait_for(state="visible", timeout=10_000)
            await search_input.click()
            await self._raise_if_risk_control_detected(page)
            return search_input
        except TimeoutError as exc:
            if "intercepts pointer events" in str(exc):
                await self._raise_if_risk_control_detected(page)
            if "intercepts pointer events" not in str(exc):
                raise
            await self._ensure_page_ready(page)
            await search_input.wait_for(state="visible", timeout=10_000)
            await search_input.click()
            await self._raise_if_risk_control_detected(page)
            return search_input
        except Error as e:
            if "Target page, context or browser has been closed" in str(e):
                raise RuntimeError("BROWSER_SESSION_BROKEN") from e
            raise

    async def _locate_search_contact(self, page: Page, order_id: str):
        sel_dropdown = self._sel("feige_search_dropdown")
        candidates = [
            page.locator(sel_dropdown).get_by_text("来自订单", exact=False).first,
            page.locator(sel_dropdown).get_by_text(order_id, exact=False).first,
            page.get_by_text("来自订单", exact=False).first,
            page.get_by_text(order_id, exact=False).first,
            page.locator("[class*='search']").get_by_text("来自订单", exact=False).first,
        ]
        for candidate in candidates:
            try:
                await candidate.wait_for(state="visible", timeout=1_500)
                return candidate
            except Exception:
                continue
        return None

    async def _clear_search_input(self, search_input) -> None:
        try:
            await search_input.click()
        except Exception:
            pass
        try:
            await search_input.press("Control+A")
            await search_input.press("Backspace")
            return
        except Exception:
            pass
        try:
            await search_input.fill("")
            return
        except Exception:
            pass
        try:
            await search_input.press("Control+A")
            await search_input.press("Delete")
        except Exception:
            pass

    async def _throttle_search(self) -> None:
        min_interval_ms = int(self.config.get("search_min_interval_ms", 2500))
        jitter_ms = int(self.config.get("search_interval_jitter_ms", 700))
        now = asyncio.get_running_loop().time()
        target_interval = (min_interval_ms + random.randint(0, max(0, jitter_ms))) / 1000.0
        elapsed = now - self._last_search_ts
        if self._last_search_ts > 0 and elapsed < target_interval:
            await asyncio.sleep(target_interval - elapsed)
        self._last_search_ts = asyncio.get_running_loop().time()

    async def _human_type_order_id(self, search_input, order_id: str) -> None:
        type_delay_ms = int(self.config.get("search_type_delay_ms", 120))
        type_jitter_ms = int(self.config.get("search_type_jitter_ms", 80))
        try:
            await search_input.click()
        except Exception:
            pass
        for ch in order_id:
            await search_input.type(ch, delay=max(20, type_delay_ms + random.randint(-type_jitter_ms, type_jitter_ms)))

    async def _search_once_in_feige(self, page: Page, order_id: str) -> bool:
        await self._throttle_search()
        search_input = await self._focus_search_input(page)
        input_wait_ms = int(self.config.get("search_after_input_wait_ms", 1000))
        await self._clear_search_input(search_input)
        await page.wait_for_timeout(random.randint(180, 420))
        await self._human_type_order_id(search_input, order_id)
        await self._raise_if_risk_control_detected(page)
        await page.wait_for_timeout(input_wait_ms + random.randint(100, 500))
        contact = await self._locate_search_contact(page, order_id)
        if contact is not None:
            await self._raise_if_risk_control_detected(page)
            await contact.click()
            await self._raise_if_risk_control_detected(page)
            await page.wait_for_timeout(self.config.get("feige_load_wait_ms", 5000))
            await self._raise_if_risk_control_detected(page)
            return True
        try:
            await search_input.press("Enter")
            await page.wait_for_timeout(max(800, input_wait_ms // 2) + random.randint(120, 360))
            await self._raise_if_risk_control_detected(page)
        except Exception:
            pass
        contact = await self._locate_search_contact(page, order_id)
        if contact is not None:
            await self._raise_if_risk_control_detected(page)
            await contact.click()
            await self._raise_if_risk_control_detected(page)
            await page.wait_for_timeout(self.config.get("feige_load_wait_ms", 5000))
            await self._raise_if_risk_control_detected(page)
            return True
        return False

    async def _search_in_feige(self, page: Page, order_id: str) -> bool:
        """Search order ID in Feige search bar, click contact result.

        Returns True after the matching contact conversation is opened.
        """
        retry_backoff_seconds = self.config.get("retry_backoff_seconds", 2)
        search_attempt = 0

        async with FEIGE_SEARCH_LOCK:
            self.logger.info("订单 %s 获取飞鸽搜索锁，开始执行搜索", order_id)
            while True:
                search_attempt += 1
                try:
                    found = await self._search_once_in_feige(page, order_id)
                    if found:
                        return True
                    raise TimeoutError("未命中飞鸽搜索结果")
                except Exception as search_error:
                    if RISK_CONTROL_ERROR_TOKEN in str(search_error) or SOFT_RISK_CONTROL_ERROR_TOKEN in str(search_error):
                        raise
                    if "BROWSER_SESSION_BROKEN" in str(search_error):
                        raise
                    if "Target page, context or browser has been closed" in str(search_error):
                        raise RuntimeError("BROWSER_SESSION_BROKEN") from search_error
                    self.logger.warning(
                        "搜索 | %s | retry %s | %s",
                        order_id,
                        search_attempt,
                        search_error,
                    )
                    try:
                        active_input = page.locator(self._sel("feige_search_input")).first
                        await active_input.press("Escape")
                        await asyncio.sleep(0.2)
                    except Exception:
                        pass
                    await self._reset_feige_page(page)
                    await asyncio.sleep(retry_backoff_seconds)

    async def _resolve_review_icon_button(self, card_root, card_header):
        candidate_locators = [
            card_header.locator("span.i-icon-look-evaluate"),
            card_root.locator("span.i-icon-look-evaluate"),
            card_header.locator("[class*='i-icon-look-evaluate']"),
            card_root.locator("[class*='i-icon-look-evaluate']"),
            card_header.get_by_role("button", name="评", exact=True),
            card_root.get_by_role("button", name="评", exact=True),
            card_header.locator("button").filter(has_text="评"),
            card_root.locator("button").filter(has_text="评"),
            card_header.locator("[role='button']").filter(has_text="评"),
            card_root.locator("[role='button']").filter(has_text="评"),
            card_header.locator("text=评"),
            card_root.locator("text=评"),
        ]
        for locator in candidate_locators:
            try:
                count = await locator.count()
            except Exception:
                continue
            for index in range(count):
                candidate = locator.nth(index)
                try:
                    if not await candidate.is_visible():
                        continue
                    class_name = (await candidate.get_attribute("class") or "").strip().lower()
                    if "i-icon-look-evaluate" in class_name:
                        return candidate
                    text = (await candidate.inner_text()).strip()
                    if text != "评":
                        continue
                    return candidate
                except Exception:
                    continue
        return None

    async def _search_order_in_doudian(self, page: Page, order_id: str) -> str:
        self.logger.info("订单 %s：复用抖店订单页", order_id)
        await page.bring_to_front()
        await page.wait_for_timeout(300)

        input_candidates = [
            page.locator("input[placeholder='请输入']").nth(0),
            page.locator("input[placeholder='请输入']").first,
            page.locator("input.auxo-input").nth(0),
            page.get_by_role("textbox", name="请输入").first,
            page.get_by_role("textbox").nth(0),
        ]
        order_input = None
        last_error = None
        for candidate in input_candidates:
            try:
                if await candidate.count() == 0:
                    continue
                await candidate.wait_for(state="visible", timeout=3_000)
                order_input = candidate
                break
            except Exception as exc:
                last_error = exc
                continue
        if order_input is None:
            raise RuntimeError(f"订单管理页搜索框不可用: {last_error}")

        self.logger.info("订单 %s：输入订单号", order_id)
        await order_input.click(timeout=3_000)
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await order_input.type(order_id, delay=40)

        self.logger.info("订单 %s：提交订单搜索", order_id)
        query_button = page.get_by_role("button", name="查询").first
        try:
            await query_button.click(timeout=3_000)
        except Exception:
            await page.keyboard.press("Enter")

        await page.wait_for_timeout(1800)
        self.logger.info("订单 %s：等待搜索结果", order_id)
        container = page.locator("table, div.index_orderList__axNH7, div.index_latestOrderList__wfoJq, .auxo-table-wrapper").first
        await container.wait_for(state="visible", timeout=10_000)
        return (await container.inner_text()).strip()

    async def _get_doudian_order_snapshot(self, page: Page, order_id: str) -> dict[str, str]:
        await self._search_order_in_doudian(page, order_id)
        await page.wait_for_timeout(800)
        snapshot = await page.evaluate(
            """
            (targetOrderId) => {
              const textOf = (node) => (node && node.innerText ? node.innerText.replace(/\\s+/g, ' ').trim() : '');
              const normalize = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
              const rows = Array.from(document.querySelectorAll('tr, .auxo-table-row, [role="row"]'));

              const result = {
                row_text: '',
                cells_text: '',
                headers_text: '',
                order_status: '',
                after_sale_status: '',
                biz_action_url: '',
                remark: '',
              };

              const buildRowResult = (headerRow, detailRow) => {
                const headerText = normalize(textOf(headerRow));
                const detailText = normalize(textOf(detailRow));
                const detailCells = detailRow
                  ? Array.from(detailRow.querySelectorAll('td, .auxo-table-cell, [role="cell"]')).map((cell) => normalize(textOf(cell))).filter(Boolean)
                  : [];
                result.row_text = [headerText, detailText].filter(Boolean).join(' | ');
                result.cells_text = detailCells.join(' | ');
                if (detailCells.length >= 4) {
                  result.after_sale_status = detailCells[2] || '';
                  result.order_status = detailCells[3] || '';
                }
              };

              for (let i = 0; i < rows.length; i += 1) {
                const row = rows[i];
                const rowText = normalize(textOf(row));
                if (!rowText || !rowText.includes(String(targetOrderId))) continue;
                buildRowResult(row, rows[i + 1] || null);
                break;
              }

              const icons = Array.from(document.querySelectorAll('img.index_imIcon__kySIr[data-kora="飞鸽"]'));
              function getFiber(el) {
                const key = Object.getOwnPropertyNames(el).find(k => k.startsWith('__reactFiber'));
                return key ? el[key] : null;
              }
              for (const img of icons) {
                let fiber = getFiber(img);
                while (fiber) {
                  const p = fiber.memoizedProps;
                  if (p && p.order && p.imOperationAction) {
                    const parent = p.order?.children?.[0]?.parentRecord || {};
                    if (String(parent.shop_order_id || '') === String(targetOrderId)) {
                      result.remark = parent.remark || '';
                      result.biz_action_url = p.imOperationAction?.biz_action_url || parent.action_map?.contactBuyer?.biz_action_url || '';
                      return result;
                    }
                  }
                  fiber = fiber.return;
                }
              }
              return result;
            }
            """,
            order_id,
        )
        if not isinstance(snapshot, dict):
            return {
                "row_text": "",
                "cells_text": "",
                "headers_text": "",
                "order_status": "",
                "after_sale_status": "",
                "biz_action_url": "",
                "remark": "",
            }
        return {
            "row_text": str(snapshot.get("row_text") or "").strip(),
            "cells_text": str(snapshot.get("cells_text") or "").strip(),
            "headers_text": str(snapshot.get("headers_text") or "").strip(),
            "order_status": str(snapshot.get("order_status") or "").strip(),
            "after_sale_status": str(snapshot.get("after_sale_status") or "").strip(),
            "biz_action_url": str(snapshot.get("biz_action_url") or "").strip(),
            "remark": str(snapshot.get("remark") or "").strip(),
        }

    async def _get_doudian_order_row_snapshot(self, page: Page, order_id: str) -> dict[str, str]:
        return await self._get_doudian_order_snapshot(page, order_id)

    async def _precheck_order_from_doudian(self, order_id: str, snapshot: dict[str, str] | None = None) -> EligibilityResult:
        self.logger.info("订单 %s：开始订单管理页预检", order_id)
        page = self.order_page
        if page is None:
            return EligibilityResult(True, "订单页预检查不可用，继续飞鸽流程")
        try:
            await page.bring_to_front()
        except Exception:
            pass
        try:
            snapshot = snapshot or await self._get_doudian_order_snapshot(page, order_id)
        except Exception as exc:
            self.logger.warning("预检 | doudian | degraded | %s", exc)
            return EligibilityResult(True, "订单页预检查失败，继续飞鸽流程")

        order_text = snapshot.get("row_text", "")
        cells_text = snapshot.get("cells_text", "")
        order_status = snapshot.get("order_status", "")
        after_sale_status = snapshot.get("after_sale_status", "")
        combined_text = "\n".join(part for part in (order_text, cells_text) if part)

        for keyword in self.DO_NOT_CONTACT_KEYWORDS:
            if keyword in combined_text:
                return EligibilityResult(False, f"订单管理页命中备注关键词：{keyword}")

        normalized_order_status = order_status.replace(" ", "")
        negative_state_keywords = ["待支付", "待发货", "已关闭", "已取消", "关闭"]
        if normalized_order_status and any(state_keyword in normalized_order_status for state_keyword in negative_state_keywords):
            hit = next(state_keyword for state_keyword in negative_state_keywords if state_keyword in normalized_order_status)
            return EligibilityResult(False, f"订单管理页状态命中排除词：{hit}")

        normalized_after_sale_status = after_sale_status.replace(" ", "")
        negative_after_sale_keywords = ["售后中", "退款中", "退款成功", "退款", "退货", "换货"]
        if normalized_after_sale_status and normalized_after_sale_status not in {"-", "—", "<empty>"}:
            if any(keyword in normalized_after_sale_status for keyword in negative_after_sale_keywords):
                return EligibilityResult(False, f"订单管理页售后状态命中排除词：{after_sale_status}")

        self.logger.info("订单 %s：订单管理页预检通过", order_id)
        return EligibilityResult(True, "订单管理页预检查通过")

    async def _extract_contact_buyer_url_from_doudian(self, order_id: str, snapshot: dict[str, str] | None = None) -> str:
        self.logger.info("订单 %s：提取联系买家直达链接", order_id)
        payload = snapshot or {}
        biz_action_url = str(payload.get("biz_action_url") or "").strip()
        if not biz_action_url:
            raise RuntimeError(f"订单管理页未找到订单 {order_id} 的 biz_action_url")
        return biz_action_url

    async def _goto_feige_conversation_via_url(self, order_id: str, jump_url: str) -> Page:
        self.logger.info("订单 %s：打开飞鸽直达链接", order_id)
        if self.context is None:
            raise RuntimeError("browser context 未初始化")
        page = await self.context.new_page()
        await page.set_viewport_size({"width": 1920, "height": 1080})
        await page.goto(jump_url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(self.config.get("default_wait_ms", 3000))
        await self._ensure_logged_in(page)
        await self._raise_if_risk_control_detected(page)
        self.page = page
        self.logger.info("订单 %s：已直达飞鸽会话", order_id)
        return page

    async def _check_eligibility(self, page: Page, order_id: str) -> EligibilityResult:
        self.logger.info("订单 %s：开始飞鸽页剩余校验", order_id)
        # 订单管理页已完成：备注/已完成/售后状态预检
        # 飞鸽页这里只保留订单管理页做不到的条件，例如评价状态。
        try:
            await page.wait_for_selector(
                "div.ecom-collapse", state="visible", timeout=20_000
            )
        except Exception as exc:
            raise RuntimeError(f"订单卡片未加载: {exc}") from exc

        try:
            card_text = (await page.locator("div.ecom-collapse").first.inner_text()).strip()
        except Exception:
            return EligibilityResult(False, "无法读取订单卡片文字，跳过")

        lines = [l.strip() for l in card_text.split("\n") if l.strip()]
        first_line = lines[0] if lines else ""
        second_line = lines[1] if len(lines) > 1 else ""
        combined_text = "\n".join(lines)
        lottery_keywords = ["抽奖", "开奖", "福袋", "幸运", "中奖"]
        lottery_count = sum(combined_text.count(keyword) for keyword in lottery_keywords)
        if lottery_count > 0:
            return EligibilityResult(False, "订单含抽奖标签")

        card_root = page.locator("div.ecom-collapse").first
        card_header = card_root.locator(":scope > div").first
        review_button = await self._resolve_review_icon_button(card_root, card_header)
        if review_button is None:
            return EligibilityResult(False, "未找到订单信息栏的评价按钮，跳过")

        try:
            is_disabled = await review_button.is_disabled()
        except Exception:
            is_disabled = False

        aria_disabled = ""
        try:
            aria_disabled = (await review_button.get_attribute("aria-disabled") or "").strip().lower()
        except Exception:
            aria_disabled = ""

        disabled_attr = None
        try:
            disabled_attr = await review_button.get_attribute("disabled")
        except Exception:
            disabled_attr = None

        class_name = ""
        try:
            class_name = (await review_button.get_attribute("class") or "").strip().lower()
        except Exception:
            class_name = ""

        icon_disabled_attr = None
        try:
            icon_disabled_attr = await review_button.get_attribute("is_disabled")
        except Exception:
            icon_disabled_attr = None

        if (
            is_disabled
            or aria_disabled == "true"
            or disabled_attr is not None
            or "disabled" in class_name
            or str(icon_disabled_attr).strip().lower() == "false"
        ):
            return EligibilityResult(False, "查看评价按钮可点击，订单已评价")

        return EligibilityResult(True, "查看评价按钮不可点击，订单未评价")

    async def _send_message(self, page: Page, text: str) -> None:
        self.logger.info("发送：准备输入消息")
        sel_input = self._sel("feige_input")
        sel_send = self._sel("feige_send_button")
        try:
            await page.wait_for_selector(sel_input, state="visible", timeout=10_000)
        except Exception as e:
            self.logger.warning("发送 | input | reset-page | %s", e)
            await self._reset_feige_page(page)
            await self._ensure_page_ready(page)
            try:
                await page.wait_for_selector(sel_input, state="visible", timeout=10_000)
            except Exception:
                raise RuntimeError("CHAT_INPUT_NOT_FOUND: 聊天输入框重置后仍未出现，跳过该订单")
        await self._ensure_page_ready(page)
        await page.fill(sel_input, text)
        await self._raise_if_risk_control_detected(page)
        await page.wait_for_timeout(300)
        try:
            await page.click(sel_send)
            await self._raise_if_risk_control_detected(page)
        except TimeoutError as exc:
            if "intercepts pointer events" in str(exc):
                await self._raise_if_risk_control_detected(page)
            if "intercepts pointer events" not in str(exc):
                raise
            await self._ensure_page_ready(page)
            await page.click(sel_send)
            await self._raise_if_risk_control_detected(page)
        await page.wait_for_timeout(self.config.get("post_send_wait_ms", 800))
        await self._raise_if_risk_control_detected(page)

    async def _is_conversation_still_open(self, page: Page) -> bool:
        conversation_locators = [
            page.locator("div.ecom-collapse").first,
            page.locator(self._sel("feige_input")).first,
            page.locator(self._sel("feige_send_button")).first,
        ]
        for locator in conversation_locators:
            try:
                if await locator.count() == 0:
                    continue
                if await locator.is_visible():
                    return True
            except Exception:
                continue
        return False

    async def _is_search_list_ready(self, page: Page) -> bool:
        try:
            search_input = page.locator(self._sel("feige_search_input")).first
            if await search_input.count() == 0 or not await search_input.is_visible():
                return False
        except Exception:
            return False
        return not await self._is_conversation_still_open(page)

    async def _exit_opened_conversation(self, page: Page, order_id: str) -> bool:
        self.logger.info("订单 %s 当前轮处理结束，按 ESC 退出当前会话", order_id)
        for attempt in range(3):
            try:
                await self._raise_if_risk_control_detected(page)
                if await self._is_search_list_ready(page):
                    return True
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(500)
                await self._raise_if_risk_control_detected(page)
                if await self._is_search_list_ready(page):
                    return True
                self.logger.info(
                    "订单 %s 第 %s 次 ESC 后列表态尚未稳定，继续校验/重试",
                    order_id,
                    attempt + 1,
                )
            except Exception as exc:
                if "BROWSER_SESSION_BROKEN" in str(exc):
                    raise
                if "Target page, context or browser has been closed" in str(exc):
                    raise RuntimeError("BROWSER_SESSION_BROKEN") from exc
                self.logger.warning(
                    "会话 | %s | esc-retry %s | %s",
                    order_id,
                    attempt + 1,
                    exc,
                )
            await page.wait_for_timeout(300)
        self.logger.warning("会话 | %s | esc-incomplete", order_id)
        return False

    async def process_order(self, order_id: str, message: str) -> tuple[str, str]:
        page = self.page
        if page is None:
            raise RuntimeError("浏览器工作页未初始化")
        max_retries = self.config.get("max_retries", 2)

        pre_snapshot = await self._get_doudian_order_snapshot(self.order_page, order_id) if self.order_page is not None else {}
        precheck = await self._precheck_order_from_doudian(order_id, pre_snapshot)
        if not precheck.eligible:
            return ("skipped", precheck.reason)

        last_err = None
        for attempt in range(max_retries + 1):
            conversation_page: Page | None = None
            try:
                jump_url = await self._extract_contact_buyer_url_from_doudian(order_id, pre_snapshot)
                conversation_page = await self._goto_feige_conversation_via_url(order_id, jump_url)
                await self._ensure_page_ready(conversation_page)

                eligibility = await self._check_eligibility(conversation_page, order_id)
                if not eligibility.eligible:
                    return ("skipped", eligibility.reason)

                await self._send_message(conversation_page, message)
                exited = await self._exit_opened_conversation(conversation_page, order_id)
                if not exited:
                    self.logger.warning("会话 | %s | esc-exit-incomplete", order_id)
                return ("sent", "message_sent")
            except Exception as e:
                last_err = e
                if RISK_CONTROL_ERROR_TOKEN in str(e) or SOFT_RISK_CONTROL_ERROR_TOKEN in str(e):
                    raise
                if "BROWSER_SESSION_BROKEN" in str(e):
                    raise
                if "Target page, context or browser has been closed" in str(e):
                    raise RuntimeError("BROWSER_SESSION_BROKEN") from e
                if "CHAT_INPUT_NOT_FOUND" in str(e):
                    return ("skipped", "聊天输入框不可用，跳过")
                if attempt < max_retries:
                    wait = self.config.get("retry_backoff_seconds", 2) * (attempt + 1)
                    self.logger.warning("直达 | %s | retry %s/%s | %s", order_id, attempt + 1, max_retries, e)
                    await asyncio.sleep(wait)
                else:
                    raise
            finally:
                if conversation_page is not None:
                    try:
                        await conversation_page.close()
                    except Exception:
                        pass
                try:
                    if self.order_page is not None:
                        await self.order_page.wait_for_timeout(200)
                except Exception:
                    pass

        raise last_err
