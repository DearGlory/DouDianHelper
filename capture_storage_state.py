from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from urllib.request import urlopen

from playwright.async_api import async_playwright

from browser_worker import BOOTSTRAP_STORAGE_STATE, FEIGE_URL, BrowserWorker, resolve_runtime_browser_mode
from launch_edge import find_edge, had_running_edge, kill_edge, launch_edge, relaunch_user_edge, resolve_browser_profile, wait_for_cdp
from logger_utils import setup_logger


def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"配置文件不存在: {p}")
    import json
    import re

    raw = p.read_text(encoding="utf-8")
    stripped = re.sub(r"^\s*//.*$", "", raw, flags=re.MULTILINE)
    return json.loads(stripped)


async def _wait_for_login_confirmation(logger, timeout_seconds: int = 300) -> str:
    logger.info("请在打开的浏览器里完成登录；确认已进入飞鸽工作台后，在此终端按 Enter 继续保存登录态。")
    try:
        command = await asyncio.wait_for(
            asyncio.to_thread(input, "按 Enter 继续保存登录态，输入 q 取消: "),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        raise RuntimeError("等待人工确认超时，请重新运行后在登录完成时按 Enter") from exc
    except (EOFError, RuntimeError):
        logger.warning("当前终端无法读取输入，改为轮询飞鸽登录状态；检测到已登录后将自动继续保存登录态")
        return "__AUTO_CONTINUE__"
    return command.strip().lower()


async def _wait_until_logged_in(checker: BrowserWorker, page, logger, timeout_seconds: int = 300) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        try:
            await checker._ensure_logged_in(page)
            return
        except RuntimeError:
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError("等待登录超时，请确认已在打开的浏览器中完成飞鸽登录")
            logger.info("请在已打开的浏览器中登录飞鸽，登录成功后将自动继续...")
            await page.wait_for_timeout(3000)
            try:
                await page.goto(FEIGE_URL)
                await page.wait_for_load_state("domcontentloaded")
            except Exception:
                pass


def _cdp_is_ready(cdp_url: str) -> bool:
    try:
        with urlopen(f"{cdp_url.rstrip('/')}/json/version", timeout=3) as response:
            return response.status == 200
    except Exception:
        return False


async def _capture_bootstrap_from_existing_cdp_session(config: dict, storage_state_path: Path, logger) -> bool:
    browser_cfg = config.get("browser", {})
    cdp_url = str(browser_cfg.get("cdp_url", "http://127.0.0.1:9222"))
    if not _cdp_is_ready(cdp_url):
        return False

    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(browser_cfg["cdp_url"])
        if not browser.contexts:
            return False

        context = browser.contexts[0]
        checker = BrowserWorker(config, logger)
        page = None
        keywords = browser_cfg.get("target_url_keywords", [])
        for candidate in context.pages:
            url = candidate.url or ""
            if any(keyword in url for keyword in keywords):
                page = candidate
                break

        if page is None:
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto(FEIGE_URL)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(config.get("default_wait_ms", 3000))

        await checker._ensure_logged_in(page)
        await context.storage_state(path=str(storage_state_path))
        logger.info("已从当前 CDP 浏览器会话重新生成 bootstrap storage state: %s", storage_state_path)
        return True
    except Exception as exc:
        logger.info("当前 CDP 浏览器会话无法直接复用登录态，将回退到人工登录预热: %s", exc)
        return False
    finally:
        await playwright.stop()


async def _bootstrap_storage_state_file_is_valid(config: dict, storage_state_path: Path) -> bool:
    if not storage_state_path.exists():
        return False
    browser_cfg = config.get("browser", {})
    if resolve_runtime_browser_mode(browser_cfg) != "cdp":
        return False
    cdp_url = str(browser_cfg.get("cdp_url", "http://127.0.0.1:9222"))
    if not _cdp_is_ready(cdp_url):
        return False
    logger = setup_logger(config.get("log_level", "INFO"), config.get("log_file"))
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(browser_cfg["cdp_url"])
        context = await browser.new_context(storage_state=str(storage_state_path))
        try:
            page = await context.new_page()
            await page.goto(FEIGE_URL)
            await page.wait_for_load_state("domcontentloaded")
            checker = BrowserWorker(config, logger)
            await checker._ensure_logged_in(page)
            return True
        finally:
            await context.close()
    except Exception:
        return False
    finally:
        await playwright.stop()


async def _reuse_existing_headless_session_if_possible(config: dict, storage_state_path: Path) -> bool:
    browser_cfg = config.get("browser", {})
    if not browser_cfg.get("headless", False):
        return False
    if not storage_state_path.exists():
        return False
    if not await _bootstrap_storage_state_file_is_valid(config, storage_state_path):
        return False
    return True


async def ensure_cdp_storage_state(config_path: str, force_refresh: bool = False) -> Path | None:
    """Ensure a valid bootstrap storage state file exists for CDP mode.

    Real user profile mode:
    - Never kill/relaunch Edge automatically.
    - Never switch the user's Edge into headless.
    - Reuse the current CDP session only if it is already available.

    Dedicated automation profile mode:
    - May bootstrap/login/relaunch as needed.
    """
    config = load_config(config_path)
    logger = setup_logger(config.get("log_level", "INFO"), config.get("log_file"))

    browser_cfg = config.get("browser", {})
    use_real_profile = bool(browser_cfg.get("use_real_user_profile", False))
    effective_mode = resolve_runtime_browser_mode(browser_cfg)
    if effective_mode != "cdp" and not use_real_profile:
        return None

    storage_state_path = Path(browser_cfg.get("bootstrap_storage_state_path", BOOTSTRAP_STORAGE_STATE))
    cdp_url = str(browser_cfg.get("cdp_url", "http://127.0.0.1:9222"))
    cdp_port = int(cdp_url.rsplit(":", 1)[-1])
    user_data_dir, profile_directory, use_real_profile = resolve_browser_profile(browser_cfg)
    headless = bool(browser_cfg.get("headless", False))
    edge_path = find_edge()
    user_data_dir.mkdir(parents=True, exist_ok=True)

    if use_real_profile:
        user_edge_was_running = had_running_edge(None)
        if not _cdp_is_ready(cdp_url):
            logger.warning("真实 Edge 未开启 CDP，准备重启为 9222/CDP 模式")
            kill_edge(cdp_port=cdp_port, user_data_dir=user_data_dir, force_all=False)
            launch_edge(edge_path, cdp_port, user_data_dir, profile_directory, False)
            if not wait_for_cdp(cdp_port):
                raise RuntimeError(f"真实 Edge 重启后，CDP 端口 {cdp_port} 未就绪")
            if user_edge_was_running:
                logger.info("真实 Profile 接管完成，重新拉起用户 Edge 以保持伪无感")
                relaunch_user_edge(edge_path, user_data_dir, profile_directory)
        if force_refresh:
            logger.info("已启用 force_refresh，强制重新获取真实 Profile 登录态")
        if not force_refresh and storage_state_path.exists() and await _bootstrap_storage_state_file_is_valid(config, storage_state_path):
            logger.info("复用现有 bootstrap storage state: %s", storage_state_path)
            return storage_state_path
        if await _capture_bootstrap_from_existing_cdp_session(config, storage_state_path, logger):
            return storage_state_path
        raise RuntimeError("真实 Profile 当前无法直接提取有效登录态，请检查飞鸽是否已登录")

    if headless and not force_refresh and storage_state_path.exists():
        if not _cdp_is_ready(cdp_url):
            kill_edge(cdp_port=cdp_port, user_data_dir=user_data_dir, force_all=True)
            launch_edge(edge_path, cdp_port, user_data_dir, profile_directory, True)
            if not wait_for_cdp(cdp_port):
                raise RuntimeError(f"CDP 端口 {cdp_port} 未就绪，无法启动 headless Edge")
        if await _bootstrap_storage_state_file_is_valid(config, storage_state_path):
            logger.info("复用现有 bootstrap storage state: %s", storage_state_path)
            return storage_state_path
        logger.info("bootstrap storage state 已失效，需要重新登录")

    if force_refresh:
        logger.info("已启用 force_refresh，强制重新获取登录态")

    logger.info("准备启动有头浏览器完成登录预热（登录新店铺后按 Enter 保存登录态）")
    kill_edge(cdp_port=cdp_port, user_data_dir=user_data_dir, force_all=True)
    launch_edge(edge_path, cdp_port, user_data_dir, profile_directory, False)
    if not wait_for_cdp(cdp_port):
        raise RuntimeError(f"CDP 端口 {cdp_port} 未就绪，无法执行登录预热")

    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(browser_cfg["cdp_url"])
        context = browser.contexts[0] if browser.contexts else await browser.new_context()

        checker = BrowserWorker(config, logger)
        page = None
        keywords = browser_cfg.get("target_url_keywords", [])
        for candidate in context.pages:
            url = candidate.url or ""
            if any(keyword in url for keyword in keywords):
                page = candidate
                break
        if page is None:
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto(FEIGE_URL)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(config.get("default_wait_ms", 3000))

        confirmation = await _wait_for_login_confirmation(logger)
        if confirmation in {"q", "quit", "exit"}:
            raise RuntimeError("已取消登录态保存，请重新运行后再试")
        await _wait_until_logged_in(checker, page, logger)
        await context.storage_state(path=str(storage_state_path))
        logger.info("bootstrap storage state 已保存: %s", storage_state_path)
        return storage_state_path
    finally:
        await playwright.stop()
        if headless:
            kill_edge(cdp_port=cdp_port, user_data_dir=user_data_dir, force_all=True)
            launch_edge(edge_path, cdp_port, user_data_dir, profile_directory, True)
            if not wait_for_cdp(cdp_port):
                raise RuntimeError(f"重启 headless Edge 后，CDP 端口 {cdp_port} 未就绪")


async def capture_storage_state(config_path: str, force_refresh: bool = False) -> None:
    result = await ensure_cdp_storage_state(config_path, force_refresh=force_refresh)
    if result is None:
        raise RuntimeError("capture_storage_state.py 仅支持 browser.mode=cdp 且 browser.headless=true")



def parse_args():
    parser = argparse.ArgumentParser(description="保存飞鸽登录态到 storage_state.json")
    parser.add_argument("--config", default="config.json", help="配置文件路径")
    parser.add_argument("--force-refresh", action="store_true", help="忽略现有 bootstrap 登录态，强制拉起有头浏览器重新获取")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(capture_storage_state(args.config, force_refresh=args.force_refresh))
