from __future__ import annotations

import argparse
import asyncio
import csv
import ctypes
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from browser_worker import BrowserWorker, LOGIN_STATE_MISSING_ERROR_TOKEN, RISK_CONTROL_ERROR_TOKEN
from capture_storage_state import ensure_cdp_storage_state
from excel_reader import ExcelOrderReader
from launch_edge import kill_edge, resolve_browser_profile
from logger_utils import setup_logger
from pause_state import clear_runtime_state, load_runtime_state, save_runtime_state


QUIT_COMMANDS = {"q", "quit", "exit"}
PROCESS_QUERY_INFORMATION = 0x0400
PROCESS_VM_READ = 0x0010
PROCESS_QUERY_FLAGS = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _disable_console_quickedit() -> None:
    """Disable Windows CMD QuickEdit mode to prevent mouse selection from freezing the process."""
    try:
        kernel32 = ctypes.windll.kernel32
        STD_INPUT_HANDLE = -10
        ENABLE_QUICK_EDIT_MODE = 0x0040
        ENABLE_EXTENDED_FLAGS = 0x0080
        handle = kernel32.GetStdHandle(STD_INPUT_HANDLE)
        if handle == -1:
            return
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return
        new_mode = (mode.value | ENABLE_EXTENDED_FLAGS) & ~ENABLE_QUICK_EDIT_MODE
        kernel32.SetConsoleMode(handle, new_mode)
    except Exception:
        pass


class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    ]




def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"配置文件不存在: {p}\n请先复制 config.example.json 为 config.json 并填写配置"
        )
    import re

    raw = p.read_text(encoding="utf-8")
    stripped = re.sub(r"^\s*//.*$", "", raw, flags=re.MULTILINE)
    return json.loads(stripped)


async def _wait_for_queue_item(
    queue: asyncio.Queue,
    stop_event: asyncio.Event,
    session_broken_event: asyncio.Event,
    risk_control_event: asyncio.Event,
):
    if queue.empty():
        return None
    queue_task = asyncio.create_task(queue.get())
    stop_task = asyncio.create_task(stop_event.wait())
    session_task = asyncio.create_task(session_broken_event.wait())
    risk_task = asyncio.create_task(risk_control_event.wait())
    done, pending = await asyncio.wait(
        {queue_task, stop_task, session_task, risk_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    if stop_task in done or session_task in done or risk_task in done:
        queue_task.cancel()
        await asyncio.gather(queue_task, return_exceptions=True)
        for task in (stop_task, session_task, risk_task):
            if task not in done:
                task.cancel()
        await asyncio.gather(stop_task, session_task, risk_task, return_exceptions=True)
        return None
    stop_task.cancel()
    session_task.cancel()
    risk_task.cancel()
    await asyncio.gather(stop_task, session_task, risk_task, return_exceptions=True)
    return queue_task.result()


async def _watch_for_exit_command(stop_event: asyncio.Event, logger, enabled: bool) -> None:
    if not enabled:
        logger.info("已禁用交互式退出监听，当前进程将按无人值守模式运行")
        return
    stdin = getattr(__import__("sys"), "stdin", None)
    if stdin is None or not getattr(stdin, "isatty", lambda: False)():
        logger.info("标准输入不可用，已禁用交互式退出监听")
        return
    logger.info("交互式退出监听已启用：输入 q / quit / exit 可触发优雅退出")
    while not stop_event.is_set():
        try:
            command = await asyncio.to_thread(input, "")
        except EOFError:
            logger.info("标准输入不可用，已禁用交互式退出监听")
            return
        except KeyboardInterrupt:
            stop_event.set()
            logger.info("收到退出信号，正在优雅退出，等待当前订单处理完成...")
            return
        if command.strip().lower() in QUIT_COMMANDS:
            stop_event.set()
            logger.info("收到退出命令，正在优雅退出，等待当前订单处理完成...")
            return


def _get_current_process_memory_mb() -> float:
    kernel32 = ctypes.windll.kernel32
    psapi = ctypes.windll.psapi
    process_id = os.getpid()
    handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, process_id)
    if not handle:
        return 0.0

    counters = PROCESS_MEMORY_COUNTERS_EX()
    counters.cb = ctypes.sizeof(counters)
    try:
        ok = psapi.GetProcessMemoryInfo(
            handle,
            ctypes.byref(counters),
            counters.cb,
        )
        if not ok:
            return 0.0
        return counters.WorkingSetSize / (1024 * 1024)
    finally:
        kernel32.CloseHandle(handle)


def _read_tasklist_rows() -> list[dict[str, str]]:
    result = subprocess.run(
        ["tasklist", "/FO", "CSV", "/NH", "/FI", "IMAGENAME eq msedge.exe"],
        capture_output=True,
        text=True,
        encoding="gbk",
        errors="replace",
        check=True,
        creationflags=PROCESS_QUERY_FLAGS,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip() and "没有运行的任务" not in line and "No tasks are running" not in line]
    return list(csv.DictReader(lines, fieldnames=["Image Name", "PID", "Session Name", "Session#", "Mem Usage"]))


def _is_cdp_port_listening(port: int) -> bool:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object {{ $_.LocalPort -eq {port} }} | Select-Object -First 1 | ForEach-Object {{ 'LISTENING' }}",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            creationflags=PROCESS_QUERY_FLAGS,
        )
        return "LISTENING" in (result.stdout or "")
    except Exception:
        return False


def _count_edge_processes() -> int:
    try:
        return len(_read_tasklist_rows())
    except Exception:
        return 0


def _parse_tasklist_memory_mb(value: str) -> float:
    digits = "".join(ch for ch in value if ch.isdigit())
    if not digits:
        return 0.0
    return int(digits) / 1024


def _get_edge_process_stats() -> tuple[int, float]:
    try:
        rows = _read_tasklist_rows()
    except Exception:
        return 0, 0.0
    total_mb = sum(_parse_tasklist_memory_mb(row.get("Mem Usage", "0 K")) for row in rows)
    return len(rows), total_mb


def _is_search_input_missing_error(error: Exception) -> bool:
    message = str(error)
    return "input.auxo-input" in message and "Timeout" in message


async def _collect_resource_snapshot(queue: asyncio.Queue | None, stats: dict[str, int], progress: dict[str, int]) -> dict[str, float | int]:
    py_mem_mb = await asyncio.to_thread(_get_current_process_memory_mb)
    edge_count, edge_mem_mb = await asyncio.to_thread(_get_edge_process_stats)
    queue_remaining = queue.qsize() if queue is not None else 0
    return {
        "py_mem_mb": py_mem_mb,
        "edge_count": edge_count,
        "edge_mem_mb": edge_mem_mb,
        "queue_remaining": queue_remaining,
        "processed_total": progress["processed_total"],
        "processed_since_restart": progress["processed_since_restart"],
        "sent": stats["sent"],
        "skipped": stats["skipped"],
        "failed": stats["failed"],
    }


async def _log_resource_snapshot(logger, queue: asyncio.Queue | None, stats: dict[str, int], progress: dict[str, int]) -> dict[str, float | int]:
    snapshot = await _collect_resource_snapshot(queue, stats, progress)
    logger.info(
        "资源: py=%.1fMB | edge=%s/%.1fMB | queue=%s | done=%s | sent=%s skipped=%s failed=%s",
        snapshot["py_mem_mb"],
        snapshot["edge_count"],
        snapshot["edge_mem_mb"],
        snapshot["queue_remaining"],
        snapshot["processed_total"],
        snapshot["sent"],
        snapshot["skipped"],
        snapshot["failed"],
    )
    return snapshot


def _log_restart_delta(logger, before: dict[str, float | int], after: dict[str, float | int]) -> None:
    logger.info(
        "轮转: py %.1f→%.1fMB | edge %s→%s | edge_mem %.1f→%.1fMB | done=%s",
        before["py_mem_mb"],
        after["py_mem_mb"],
        before["edge_count"],
        after["edge_count"],
        before["edge_mem_mb"],
        after["edge_mem_mb"],
        after["processed_total"],
    )


async def _resource_monitor(
    stop_event: asyncio.Event,
    logger,
    queue_ref: dict[str, asyncio.Queue | None],
    stats_ref: dict[str, dict[str, int]],
    progress_ref: dict[str, dict[str, int]],
    interval_seconds: int,
) -> None:
    last_signature: tuple[int, int, int, int] | None = None
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            snapshot = await _collect_resource_snapshot(queue_ref["queue"], stats_ref["value"], progress_ref["value"])
            signature = (
                int(snapshot["queue_remaining"]),
                int(snapshot["processed_total"]),
                int(snapshot["sent"]),
                int(snapshot["failed"]),
            )
            if signature == last_signature:
                continue
            last_signature = signature
            logger.info(
                "资源: py=%.1fMB | edge=%s/%.1fMB | queue=%s | done=%s | sent=%s skipped=%s failed=%s",
                snapshot["py_mem_mb"],
                snapshot["edge_count"],
                snapshot["edge_mem_mb"],
                snapshot["queue_remaining"],
                snapshot["processed_total"],
                snapshot["sent"],
                snapshot["skipped"],
                snapshot["failed"],
            )


async def _order_worker(
    name: str,
    queue: asyncio.Queue,
    worker: BrowserWorker,
    config: dict,
    logger,
    stats: dict[str, int],
    dry_run: bool,
    total: int,
    processed_order_ids: set[str],
    processed_lock: asyncio.Lock,
    progress: dict[str, int],
    stop_event: asyncio.Event,
    session_broken_event: asyncio.Event,
    risk_control_event: asyncio.Event,
    results_by_order_id: dict[str, dict[str, str]],
) -> None:
    while True:
        item = await _wait_for_queue_item(queue, stop_event, session_broken_event, risk_control_event)
        if item is None:
            break
        i, order_id = item

        logger.info("[%s] [%s/%s] %s", name, i, total, order_id)
        should_delete_from_excel = False
        should_advance_progress = False
        try:
            status = "failed"
            reason = "unknown"
            if dry_run:
                logger.info("[%s] %s -> dry-run | skipped", name, order_id)
                stats["skipped"] += 1
                should_delete_from_excel = True
                should_advance_progress = True
                status = "skipped"
                reason = "dry-run"
            else:
                status, reason = await worker.process_order(order_id, config["message_template"])
                if status == "sent":
                    stats["sent"] += 1
                    should_delete_from_excel = True
                    should_advance_progress = True
                elif status == "skipped":
                    stats["skipped"] += 1
                    should_delete_from_excel = True
                    should_advance_progress = True
                else:
                    stats["failed"] += 1
                    should_advance_progress = True
                logger.info("[%s] %s -> %s | %s", name, order_id, status, reason)
        except Exception as e:
            if "BROWSER_SESSION_BROKEN" in str(e):
                session_broken_event.set()
                status = "failed"
                reason = str(e)
                logger.error("[%s] 会话 | %s | broken | %s", name, order_id, e)
            elif RISK_CONTROL_ERROR_TOKEN in str(e):
                risk_control_event.set()
                status = "failed"
                reason = str(e)
                logger.error("[%s] 风控 | %s | paused | %s", name, order_id, e)
            elif LOGIN_STATE_MISSING_ERROR_TOKEN in str(e):
                session_broken_event.set()
                status = "failed"
                reason = str(e)
                logger.error("[%s] 登录态 | %s | missing | %s", name, order_id, e)
            else:
                stats["failed"] += 1
                should_advance_progress = True
                status = "failed"
                reason = str(e)
                logger.error("[%s] 订单 | %s | failed | %s", name, order_id, e)
        finally:
            async with processed_lock:
                results_by_order_id[order_id] = {"status": status, "reason": reason}
                if should_delete_from_excel and not dry_run:
                    processed_order_ids.add(order_id)
                if should_advance_progress:
                    progress["processed_total"] += 1
                    progress["processed_since_restart"] += 1


async def _process_chunk(
    config: dict,
    logger,
    chunk_items: list[tuple[int, str]],
    dry_run: bool,
    num_workers: int,
    total_orders: int,
    processed_order_ids: set[str],
    processed_lock: asyncio.Lock,
    stats: dict[str, int],
    progress: dict[str, int],
    stop_event: asyncio.Event,
    session_broken_event: asyncio.Event,
    risk_control_event: asyncio.Event,
    queue_ref: dict[str, asyncio.Queue | None],
    chunk_index: int,
    total_chunks: int,
    results_by_order_id: dict[str, dict[str, str]],
) -> tuple[bool, bool, bool]:
    queue: asyncio.Queue = asyncio.Queue()
    for item in chunk_items:
        await queue.put(item)
    queue_ref["queue"] = queue

    workers = [BrowserWorker(config, logger) for _ in range(min(num_workers, len(chunk_items)))]

    startup_failed = False
    login_refresh_required = False
    if not dry_run and workers:
        logger.info("浏览器会话 %s/%s 准备启动浏览器，独立工作页=%s", chunk_index, total_chunks, len(workers))
        startup_results = []
        startup_stagger_ms = int(config.get("browser_startup_stagger_ms", 1200))
        for index, worker in enumerate(workers):
            result = await asyncio.gather(worker.start(), return_exceptions=True)
            startup_results.extend(result)
            if index < len(workers) - 1 and startup_stagger_ms > 0:
                await asyncio.sleep(startup_stagger_ms / 1000)
        startup_errors = [result for result in startup_results if isinstance(result, Exception)]
        if startup_errors:
            startup_failed = True
            for error in startup_errors:
                if RISK_CONTROL_ERROR_TOKEN in str(error):
                    risk_control_event.set()
                    logger.error("启动 | chunk %s/%s | paused | %s", chunk_index, total_chunks, error)
                elif LOGIN_STATE_MISSING_ERROR_TOKEN in str(error):
                    session_broken_event.set()
                    login_refresh_required = True
                    logger.error("启动 | chunk %s/%s | login-missing | %s", chunk_index, total_chunks, error)
                elif "BROWSER_SESSION_BROKEN" in str(error):
                    session_broken_event.set()
                    logger.error("启动 | chunk %s/%s | broken | %s", chunk_index, total_chunks, error)
                else:
                    session_broken_event.set()
                    logger.error("启动 | chunk %s/%s | failed | %s", chunk_index, total_chunks, error)
        else:
            logger.info("浏览器会话 %s/%s 浏览器已启动", chunk_index, total_chunks)

    if startup_failed:
        queue_ref["queue"] = None
        if not dry_run and workers:
            logger.info("浏览器会话 %s/%s 启动未完成，准备关闭已启动的浏览器", chunk_index, total_chunks)
            await asyncio.gather(*(worker.stop() for worker in workers), return_exceptions=True)
            logger.info("浏览器会话 %s/%s 已完成启动失败后的浏览器清理", chunk_index, total_chunks)
        return session_broken_event.is_set(), risk_control_event.is_set(), login_refresh_required

    tasks = [
        asyncio.create_task(
            _order_worker(
                f"W{i+1}",
                queue,
                worker,
                config,
                logger,
                stats,
                dry_run,
                total_orders,
                processed_order_ids,
                processed_lock,
                progress,
                stop_event,
                session_broken_event,
                risk_control_event,
                results_by_order_id,
            )
        )
        for i, worker in enumerate(workers)
    ]

    if not tasks:
        queue_ref["queue"] = None
        return session_broken_event.is_set(), risk_control_event.is_set(), login_refresh_required

    try:
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in task_results:
            if isinstance(result, Exception):
                logger.error("worker | task | failed | %s", result)
    finally:
        queue_ref["queue"] = None
        if not dry_run and workers:
            logger.info("浏览器会话 %s/%s 准备关闭浏览器", chunk_index, total_chunks)
            await asyncio.gather(*(worker.stop() for worker in workers), return_exceptions=True)
            logger.info("浏览器会话 %s/%s 浏览器已关闭", chunk_index, total_chunks)
    return session_broken_event.is_set(), risk_control_event.is_set(), login_refresh_required


async def _pause_before_resume(
    pause_seconds: int,
    stop_event: asyncio.Event,
    logger,
) -> bool:
    started_at = datetime.now()
    resume_at = started_at + timedelta(seconds=pause_seconds)
    logger.warning("风控 | all-workers | paused %ss | resume_at=%s", pause_seconds, resume_at.strftime("%Y-%m-%d %H:%M:%S"))
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=pause_seconds)
        logger.info("暂停期间收到退出指令，本次保留恢复进度，等待下次启动继续。")
        return False
    except asyncio.TimeoutError:
        logger.info(
            "风控暂停时间已到，准备自动恢复任务。 | paused_started_at=%s | resumed_at=%s | drift_seconds=%.1f",
            started_at.strftime("%Y-%m-%d %H:%M:%S"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            (datetime.now() - resume_at).total_seconds(),
        )
        return True


def _build_pending_order_ids(
    current_order_ids: list[str],
    processed_order_ids: set[str],
    explicit_pending_order_ids: list[str] | None = None,
) -> list[str]:
    if explicit_pending_order_ids is not None:
        pending_set = set(explicit_pending_order_ids)
        return [order_id for order_id in current_order_ids if order_id in pending_set]
    return [order_id for order_id in current_order_ids if order_id not in processed_order_ids]


def _snapshot_runtime_state(
    config: dict,
    pending_order_ids: list[str],
    processed_order_ids: set[str],
    stats: dict[str, int],
    progress: dict[str, int],
    effective_limit: int | None,
    num_workers: int,
    pause_seconds: int,
) -> dict:
    resume_at = datetime.now() + timedelta(seconds=pause_seconds)
    return {
        "config_path": config.get("config_path", "config.json"),
        "parallel_workers": num_workers,
        "effective_limit": effective_limit,
        "risk_control_pause_seconds": pause_seconds,
        "resume_at": resume_at.isoformat(timespec="seconds"),
        "stats": dict(stats),
        "progress": dict(progress),
        "target_total_orders": progress["target_total_orders"],
        "pending_order_ids": list(pending_order_ids),
        "processed_order_ids": sorted(processed_order_ids),
    }


def _restore_runtime_state(
    runtime_state: dict | None,
    current_order_ids: list[str],
    requested_limit: int | None,
    requested_parallel_workers: int | None,
    prefer_fresh_run: bool = False,
) -> tuple[list[str], set[str], dict[str, int], dict[str, int], int | None, int, bool]:
    effective_limit = requested_limit
    num_workers = max(1, requested_parallel_workers or 1)
    stats = {"sent": 0, "skipped": 0, "failed": 0}
    progress = {"processed_total": 0, "processed_since_restart": 0, "target_total_orders": 0}
    resumed = False
    processed_order_ids: set[str] = set()

    if runtime_state is None or prefer_fresh_run:
        order_ids = current_order_ids[:effective_limit] if effective_limit is not None else list(current_order_ids)
        progress["target_total_orders"] = len(order_ids)
        return order_ids, processed_order_ids, stats, progress, effective_limit, num_workers, resumed

    resumed = True
    saved_limit = runtime_state.get("effective_limit")
    effective_limit = saved_limit if saved_limit is not None else requested_limit
    base_order_ids = current_order_ids[:effective_limit] if effective_limit is not None else list(current_order_ids)
    saved_pending_order_ids = runtime_state.get("pending_order_ids") or []
    pending_set = set(saved_pending_order_ids)
    order_ids = [order_id for order_id in base_order_ids if order_id in pending_set]
    processed_order_ids = set(runtime_state.get("processed_order_ids") or [])
    saved_stats = runtime_state.get("stats") or {}
    saved_progress = runtime_state.get("progress") or {}
    saved_total_target = runtime_state.get("target_total_orders")
    restored_processed_total = int(saved_progress.get("processed_total", 0))
    resumed_target_total = restored_processed_total + len(order_ids)
    if saved_total_target is not None:
        resumed_target_total = int(saved_total_target)
    stats = {
        "sent": int(saved_stats.get("sent", 0)),
        "skipped": int(saved_stats.get("skipped", 0)),
        "failed": int(saved_stats.get("failed", 0)),
    }
    progress = {
        "processed_total": restored_processed_total,
        "processed_since_restart": 0,
        "target_total_orders": resumed_target_total,
    }
    num_workers = max(1, int(runtime_state.get("parallel_workers") or requested_parallel_workers or 1))
    return order_ids, processed_order_ids, stats, progress, effective_limit, num_workers, resumed


def _shutdown_pause_edge_resources(config: dict, logger) -> None:
    browser_cfg = config.get("browser", {})
    cdp_url = str(browser_cfg.get("cdp_url", "http://127.0.0.1:9222"))
    cdp_port = int(cdp_url.rsplit(":", 1)[-1])
    user_data_dir, profile_directory, use_real_profile = resolve_browser_profile(browser_cfg)
    logger.warning(
        "风控暂停：准备释放 Edge 资源 | cdp_port=%s | user_data_dir=%s | profile_directory=%s | use_real_user_profile=%s | browser_mode=%s",
        cdp_port,
        user_data_dir,
        profile_directory,
        use_real_profile,
        browser_cfg.get("mode"),
    )
    kill_edge(cdp_port, user_data_dir, True)


async def run(
    config: dict,
    limit: int | None,
    dry_run: bool,
    parallel_workers: int | None = None,
    force_refresh_login: bool = False,
) -> None:
    logger = setup_logger(config.get("log_level", "INFO"), config.get("log_file"))
    reader = ExcelOrderReader(config["excel_path"], config.get("order_id_aliases"))
    logger.info("Excel路径: %s", reader.excel_path.resolve())
    all_order_ids = reader.read_order_ids()
    if not all_order_ids:
        raise RuntimeError(
            f"Excel 中未读取到任何有效订单号: {reader.excel_path.resolve()}。"
            "请检查是否覆盖到了正确的 Order.xlsx、表头是否为订单号/订单编号、以及文件是否已被清空。"
        )
    selected_order_ids_for_run: list[str] = []
    prefer_fresh_run = limit is not None or parallel_workers is not None
    runtime_state = None if dry_run else load_runtime_state(config)
    order_ids, processed_order_ids, stats, progress, effective_limit, num_workers, resumed = _restore_runtime_state(
        runtime_state,
        all_order_ids,
        limit,
        parallel_workers,
        prefer_fresh_run=prefer_fresh_run,
    )
    selected_order_ids_for_run = list(order_ids)
    restart_every = max(1, int(config.get("browser_restart_every_n_orders", 300)))
    resource_interval = max(5, int(config.get("resource_log_interval_seconds", 60)))
    risk_pause_seconds = int(config.get("risk_control_pause_seconds", 300))

    if resumed:
        logger.warning(
            "检测到未完成的暂停状态，自动恢复任务：待处理=%s | 已发送=%s | 已跳过=%s | 已失败=%s | 已执行=%s | 工作页=%s",
            len(order_ids),
            stats["sent"],
            stats["skipped"],
            stats["failed"],
            progress["processed_total"],
            num_workers,
        )
    elif runtime_state is not None and prefer_fresh_run:
        logger.warning(
            "检测到未完成的暂停状态，但本次显式传入了 limit/parallel-workers，按新任务启动并忽略暂停恢复。"
        )
        if not dry_run:
            clear_runtime_state(config)
            logger.info("已自动清理旧的暂停状态文件。")
        logger.info("启动: 总订单=%s | 本轮=%s | 并行页=%s", len(all_order_ids), len(order_ids), num_workers)
    else:
        logger.info("启动: 总订单=%s | 本轮=%s | 并行页=%s", len(all_order_ids), len(order_ids), num_workers)

    logger.info("控制: 输入 q / quit / exit 可优雅退出")
    logger.info("配置: 资源监控=%ss | 浏览器轮转=%s单 | 风控暂停=%ss", resource_interval, restart_every, risk_pause_seconds)

    processed_lock = asyncio.Lock()
    results_by_order_id: dict[str, dict[str, str]] = {}
    stop_event = asyncio.Event()
    queue_ref: dict[str, asyncio.Queue | None] = {"queue": None}
    stats_ref: dict[str, dict[str, int]] = {"value": stats}
    progress_ref: dict[str, dict[str, int]] = {"value": progress}
    session_broken_event = asyncio.Event()
    risk_control_event = asyncio.Event()

    exit_task = asyncio.create_task(
        _watch_for_exit_command(
            stop_event,
            logger,
            bool(config.get("interactive_exit_listener", True)),
        )
    )
    resource_task = asyncio.create_task(
        _resource_monitor(stop_event, logger, queue_ref, stats_ref, progress_ref, resource_interval)
    )

    try:
        while order_ids and not stop_event.is_set():
            if not dry_run:
                logger.info("准备登录预热")
                bootstrap_state = await ensure_cdp_storage_state(
                    config.get("config_path", "config.json"),
                    force_refresh=force_refresh_login,
                )
                force_refresh_login = False
                if bootstrap_state is not None:
                    logger.info("登录预热完成")

            chunks = [
                list(enumerate(order_ids[start : start + restart_every], progress["processed_total"] + start + 1))
                for start in range(0, len(order_ids), restart_every)
            ]

            pause_requested = False
            for index, chunk_items in enumerate(chunks, 1):
                if stop_event.is_set():
                    break

                progress["processed_since_restart"] = 0
                logger.info("会话 %s/%s: 计划处理 %s 单", index, len(chunks), len(chunk_items))
                before_snapshot = await _collect_resource_snapshot(queue_ref["queue"], stats, progress)
                logger.info(
                    "浏览器会话开始前资源 | python_mem=%.1fMB | edge_processes=%s | edge_mem=%.1fMB | processed_total=%s",
                    before_snapshot["py_mem_mb"],
                    before_snapshot["edge_count"],
                    before_snapshot["edge_mem_mb"],
                    before_snapshot["processed_total"],
                )
                session_broken_event.clear()
                risk_control_event.clear()
                session_broken, risk_detected, login_refresh_required = await _process_chunk(
                    config,
                    logger,
                    chunk_items,
                    dry_run,
                    num_workers,
                    progress["target_total_orders"],
                    processed_order_ids,
                    processed_lock,
                    stats,
                    progress,
                    stop_event,
                    session_broken_event,
                    risk_control_event,
                    queue_ref,
                    index,
                    len(chunks),
                    results_by_order_id,
                )
                after_snapshot = await _log_resource_snapshot(logger, queue_ref["queue"], stats, progress)

                remaining_after_chunk = _build_pending_order_ids(order_ids, processed_order_ids)
                if risk_detected:
                    pause_requested = True
                    if not dry_run:
                        snapshot = _snapshot_runtime_state(
                            config,
                            remaining_after_chunk,
                            processed_order_ids,
                            stats,
                            progress,
                            effective_limit,
                            num_workers,
                            risk_pause_seconds,
                        )
                        state_path = save_runtime_state(config, snapshot)
                        logger.warning("已保存暂停进度: %s", state_path)
                        await asyncio.to_thread(_shutdown_pause_edge_resources, config, logger)
                    break
                if login_refresh_required and not stop_event.is_set():
                    logger.warning("检测到飞鸽登录态缺失，下一轮将强制进入有头登录预热流程。")
                    force_refresh_login = True
                    order_ids = remaining_after_chunk
                    break
                if session_broken and not stop_event.is_set():
                    logger.warning("检测到浏览器会话中断，本批次已提前停止接单，并将立即重连浏览器。")
                    order_ids = remaining_after_chunk
                    break
                if stop_event.is_set():
                    break
                if index < len(chunks):
                    logger.info("会话 %s/%s: 已处理 %s 单，准备轮转浏览器", index, len(chunks), progress["processed_total"])
                    _log_restart_delta(logger, before_snapshot, after_snapshot)

                order_ids = remaining_after_chunk

            if stop_event.is_set():
                break
            if pause_requested:
                should_resume = await _pause_before_resume(risk_pause_seconds, stop_event, logger)
                if not should_resume:
                    break
                runtime_state = None if dry_run else load_runtime_state(config)
                current_order_ids = reader.read_order_ids()
                order_ids, processed_order_ids, stats, progress, effective_limit, num_workers, _ = _restore_runtime_state(
                    runtime_state,
                    current_order_ids,
                    effective_limit,
                    num_workers,
                )
                stats_ref["value"] = stats
                progress_ref["value"] = progress
                logger.warning(
                    "已从暂停状态恢复：待处理=%s | 已发送=%s | 已跳过=%s | 已失败=%s | 已执行=%s | 工作页=%s",
                    len(order_ids),
                    stats["sent"],
                    stats["skipped"],
                    stats["failed"],
                    progress["processed_total"],
                    num_workers,
                )
                continue
            if session_broken_event.is_set() and order_ids and not stop_event.is_set():
                logger.info("会话已重置，继续剩余 %s 单", len(order_ids))
                continue
            break
    except KeyboardInterrupt:
        stop_event.set()
        logger.info("收到 Ctrl+C，正在优雅退出，等待当前订单处理完成...")
    finally:
        stop_event.set()
        exit_task.cancel()
        resource_task.cancel()
        await asyncio.gather(exit_task, resource_task, return_exceptions=True)
        if not dry_run:
            export_review = bool(config.get("export_processed_orders_review", True))
            if export_review:
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                review_dir = Path("logs") / "run_history"
                review_dir.mkdir(parents=True, exist_ok=True)
                review_output = review_dir / f"processed-orders-review-{timestamp}.xlsx"
                review_header, review_rows = reader.fetch_rows_by_order_ids(selected_order_ids_for_run)
                if review_header:
                    reader.export_rows_with_results(str(review_output), review_header, review_rows, results_by_order_id)
                    logger.info("复核文件已生成: %s", review_output)
            if not order_ids:
                clear_runtime_state(config)
            deleted = reader.delete_order_rows_bulk(processed_order_ids)
            logger.info("Excel 已删除 %s 个已处理订单", deleted)
            browser_cfg = config.get("browser", {})
            if browser_cfg.get("mode") == "cdp":
                cdp_url = str(browser_cfg.get("cdp_url", "http://127.0.0.1:9222"))
                cdp_port = int(cdp_url.rsplit(":", 1)[-1])
                user_data_dir, profile_directory, use_real_profile = resolve_browser_profile(browser_cfg)
                if use_real_profile:
                    logger.info(
                        "收尾: 真实 Edge Profile 模式，不主动关闭主 Edge | cdp_port=%s | profile_directory=%s",
                        cdp_port,
                        profile_directory,
                    )
                else:
                    logger.info(
                        "检测到主流程已结束，准备关闭自动化专用 CDP Edge 浏览器进程 | user_data_dir=%s | profile_directory=%s | use_real_user_profile=%s",
                        user_data_dir,
                        profile_directory,
                        use_real_profile,
                    )
                    await asyncio.to_thread(kill_edge, cdp_port, user_data_dir, True)
                    await asyncio.sleep(1)
                    await asyncio.to_thread(kill_edge, cdp_port, user_data_dir, True)
                    await asyncio.sleep(1)
                    if await asyncio.to_thread(_is_cdp_port_listening, cdp_port):
                        logger.warning("检测到 9222/CDP 端口仍在监听，执行第 3 次兜底清理")
                        await asyncio.to_thread(kill_edge, cdp_port, user_data_dir, True)
                        await asyncio.sleep(1)
                    still_listening = await asyncio.to_thread(_is_cdp_port_listening, cdp_port)
                    edge_count_after_cleanup = await asyncio.to_thread(_count_edge_processes)
                    if still_listening:
                        logger.warning(
                            "收尾后检测到 CDP 端口仍在监听，请手动检查残留 Edge 进程 | cdp_port=%s | edge_processes=%s",
                            cdp_port,
                            edge_count_after_cleanup,
                        )
                    else:
                        logger.info(
                            "自动化专用 CDP Edge 浏览器进程已完成清理 | cdp_port=%s | remaining_edge_processes=%s",
                            cdp_port,
                            edge_count_after_cleanup,
                        )
        logger.info("统计: sent=%s | skipped=%s | failed=%s", stats["sent"], stats["skipped"], stats["failed"])
        remaining = reader.read_order_ids()
        logger.info("收尾: Excel剩余=%s", len(remaining))


def parse_args():
    parser = argparse.ArgumentParser(description="抖店批量邀评工具")
    parser.add_argument("--config", default="config.json", help="配置文件路径")
    parser.add_argument("--limit", type=int, default=None, help="最多处理 N 单")
    parser.add_argument("--parallel-workers", type=int, default=None, help="本次运行使用的并行工作页数量")
    parser.add_argument("--dry-run", action="store_true", help="仅模拟，不操作浏览器")
    parser.add_argument("--force-refresh-login", action="store_true", help="忽略现有 bootstrap 登录态，强制拉起有头浏览器重新获取")
    return parser.parse_args()


if __name__ == "__main__":
    _disable_console_quickedit()
    args = parse_args()
    config = load_config(args.config)
    config["config_path"] = args.config
    asyncio.run(run(config, args.limit, args.dry_run, args.parallel_workers, args.force_refresh_login))
