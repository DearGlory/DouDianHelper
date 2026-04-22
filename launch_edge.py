from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen

FEIGE_URL = "https://im.jinritemai.com/pc_seller_v2/main/workspace"
CDP_CHECK_URL_TEMPLATE = "http://127.0.0.1:{port}/json/version"
EDGE_CANDIDATES = [
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
]


def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"配置文件不存在: {p}")
    raw = p.read_text(encoding="utf-8")
    stripped = re.sub(r"^\s*//.*$", "", raw, flags=re.MULTILINE)
    return json.loads(stripped)


def find_edge() -> Path:
    for candidate in EDGE_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Edge not found")


def _default_edge_user_data_dir() -> Path:
    local_appdata = os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        raise RuntimeError("无法获取 LOCALAPPDATA，无法自动定位 Edge 用户目录")
    return Path(local_appdata) / "Microsoft" / "Edge" / "User Data"


def _read_local_state(user_data_dir: Path) -> dict:
    local_state = user_data_dir / "Local State"
    if not local_state.exists():
        return {}
    try:
        return json.loads(local_state.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _auto_detect_profile_directory(user_data_dir: Path) -> str:
    local_state = _read_local_state(user_data_dir)
    info_cache = ((local_state.get("profile") or {}).get("info_cache") or {})
    last_used = str(((local_state.get("profile") or {}).get("last_used") or "")).strip()
    if last_used and (user_data_dir / last_used).exists():
        return last_used
    if "Default" in info_cache and (user_data_dir / "Default").exists():
        return "Default"
    profile_dirs = []
    for child in user_data_dir.iterdir():
        if not child.is_dir():
            continue
        if child.name == "Default" or child.name.startswith("Profile "):
            profile_dirs.append(child)
    if not profile_dirs:
        return "Default"
    profile_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return profile_dirs[0].name


def resolve_browser_profile(browser_cfg: dict) -> tuple[Path, str, bool]:
    use_real_profile = bool(browser_cfg.get("use_real_user_profile", False))
    raw_user_data_dir = str(browser_cfg.get("user_data_dir", "edge-profile")).strip()
    raw_profile_directory = str(browser_cfg.get("profile_directory", "Default")).strip()

    if use_real_profile and (not raw_user_data_dir or raw_user_data_dir.lower() == "auto"):
        user_data_dir = _default_edge_user_data_dir().resolve()
    else:
        user_data_dir = Path(raw_user_data_dir or "edge-profile").resolve()

    if use_real_profile and (not raw_profile_directory or raw_profile_directory.lower() == "auto"):
        profile_directory = _auto_detect_profile_directory(user_data_dir)
    else:
        profile_directory = raw_profile_directory or "Default"

    return user_data_dir, profile_directory, use_real_profile


def _build_kill_command(cdp_port: int | None = None, user_data_dir: Path | None = None, force_all: bool = False) -> str:
    if force_all or user_data_dir is None:
        return "Stop-Process -Name msedge -Force -ErrorAction SilentlyContinue"

    user_data_dir_str = str(user_data_dir).replace("'", "''")
    cdp_arg = f"--remote-debugging-port={cdp_port}" if cdp_port else ""
    cdp_match = f"$cmd -like '*{cdp_arg}*'" if cdp_arg else "$false"
    profile_match = f"$cmd -like '*--user-data-dir={user_data_dir_str}*'"
    return (
        "$targets = Get-CimInstance Win32_Process -Filter \"name = 'msedge.exe'\" -ErrorAction SilentlyContinue | "
        f"Where-Object {{ $cmd = $_.CommandLine; ({cdp_match}) -or ({profile_match}) }}; "
        "foreach ($p in $targets) { try { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue } catch {} }"
    )


def had_running_edge(user_data_dir: Path | None = None) -> bool:
    if user_data_dir is None:
        command = "@(Get-CimInstance Win32_Process -Filter \"name = 'msedge.exe'\" -ErrorAction SilentlyContinue).Count"
    else:
        user_data_dir_str = str(user_data_dir).replace("'", "''")
        command = (
            "$targets = Get-CimInstance Win32_Process -Filter \"name = 'msedge.exe'\" -ErrorAction SilentlyContinue | "
            f"Where-Object {{ $_.CommandLine -like '*--user-data-dir={user_data_dir_str}*' }}; "
            "@($targets).Count"
        )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )
    try:
        return int((result.stdout or "0").strip() or "0") > 0
    except ValueError:
        return False


def kill_edge(cdp_port: int | None = None, user_data_dir: Path | None = None, force_all: bool = False) -> None:
    """Kill Edge processes safely.

    - Workspace/local dedicated profile: can still force-kill all to avoid reuse issues.
    - Real user profile: only kill instances bound to the same CDP port or same user-data-dir.
    """
    command = _build_kill_command(cdp_port=cdp_port, user_data_dir=user_data_dir, force_all=force_all)
    subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            command,
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(4)


def wait_for_cdp(port: int, timeout_seconds: int = 20) -> bool:
    deadline = time.time() + timeout_seconds
    url = CDP_CHECK_URL_TEMPLATE.format(port=port)
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=3) as response:
                if response.status == 200:
                    return True
        except Exception:
            time.sleep(1)
    return False


def launch_edge(edge_path: Path, cdp_port: int, user_data_dir: Path, profile_directory: str, headless: bool) -> None:
    cmd = [
        str(edge_path),
        f"--remote-debugging-port={cdp_port}",
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={profile_directory}",
        "--no-first-run",
        "--disable-background-mode",
        "--disable-backgrounding-occluded-windows",
        "--disable-sync",
        "--window-size=1920,1080",
    ]
    if headless:
        cmd.append("--headless=new")
    cmd.append(FEIGE_URL)

    subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
    )


def relaunch_user_edge(edge_path: Path, user_data_dir: Path, profile_directory: str) -> None:
    edge_path_str = str(edge_path).replace("'", "''")
    user_data_dir_str = str(user_data_dir).replace("'", "''")
    profile_directory_str = str(profile_directory).replace("'", "''")
    command = (
        f"Start-Process -FilePath '{edge_path_str}' "
        f"-ArgumentList '--user-data-dir={user_data_dir_str}','--profile-directory={profile_directory_str}','--restore-last-session','--new-window'"
    )
    subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> int:
    config = load_config("config.json")
    browser = config.get("browser", {})
    cdp_url = str(browser.get("cdp_url", "http://127.0.0.1:9222"))
    cdp_port = int(cdp_url.rsplit(":", 1)[-1])
    user_data_dir, profile_directory, use_real_profile = resolve_browser_profile(browser)
    headless = bool(browser.get("headless", False))

    edge_path = find_edge()
    user_data_dir.mkdir(parents=True, exist_ok=True)

    if headless:
        # headless 模式下由 main.py -> ensure_cdp_storage_state 统一管理 Edge 生命周期
        print("[INFO] Headless mode enabled, Edge lifecycle managed by main.py")
        print("Closing leftover automation Edge ...")
        kill_edge(cdp_port=cdp_port, user_data_dir=user_data_dir, force_all=True)
        return 0

    print("Closing Edge ...")
    kill_edge(cdp_port=cdp_port, user_data_dir=user_data_dir, force_all=True)

    print(f"[INFO] user_data_dir={user_data_dir}")
    print(f"[INFO] profile_directory={profile_directory}")
    print(f"[INFO] use_real_user_profile={use_real_profile}")
    print("Launching Edge ...")

    try:
        launch_edge(edge_path, cdp_port, user_data_dir, profile_directory, headless)
    except subprocess.CalledProcessError:
        print("[ERROR] Failed to launch Edge")
        return 1

    if not wait_for_cdp(cdp_port):
        print(f"[ERROR] CDP port {cdp_port} not responding")
        return 1

    print("CDP OK - Edge is ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
