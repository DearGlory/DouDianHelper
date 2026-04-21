@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

set "VENV_DIR=%~dp0venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "VENV_PIP=%VENV_DIR%\Scripts\pip.exe"
set "OFFLINE_PKG_DIR=%~dp0installer\packages"

echo ============================================
echo   DouDianHelper - Setup
echo ============================================
echo.

:: 1. Check Python
echo [1/5] Checking Python ...
set "PY="

python --version >nul 2>&1
if %errorlevel% equ 0 (
    set "PY=python"
    goto :py_found
)

py -3 --version >nul 2>&1
if %errorlevel% equ 0 (
    set "PY=py -3"
    goto :py_found
)

if exist "%~dp0installer\python-3.13.2-amd64.exe" (
    echo        Installing Python from local package ...
    "%~dp0installer\python-3.13.2-amd64.exe" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0
    if %errorlevel% neq 0 (
        echo [ERROR] Python install failed
        pause
        exit /b 1
    )
    for %%V in (313 312 311 310) do (
        if exist "%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe" (
            set "PY=%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe"
            goto :py_found
        )
    )
    echo [INFO] Python installed but PATH not updated.
    echo        Please close this window and run setup.bat again.
    pause
    exit /b 1
) else (
    echo [ERROR] Python not found and no local installer in installer\ folder.
    pause
    exit /b 1
)

:py_found
for /f "tokens=*" %%v in ('!PY! --version 2^>^&1') do echo        %%v

:: 2. Create or recreate venv
echo.
echo [2/5] Creating virtual environment ...
set "RECREATE_VENV=0"
if exist "%VENV_PY%" (
    "%VENV_PY%" --version >nul 2>&1
    if %errorlevel% neq 0 (
        set "RECREATE_VENV=1"
    ) else (
        "%VENV_PY%" -c "import sys; print(sys.executable)" >nul 2>&1
        if %errorlevel% neq 0 set "RECREATE_VENV=1"
    )
    if "!RECREATE_VENV!"=="1" (
        echo        venv is broken or moved, recreating ...
        rmdir /s /q "%VENV_DIR%"
    ) else (
        echo        venv already exists and works, skipping
        goto :venv_ready
    )
)
!PY! -m venv "%VENV_DIR%"
if %errorlevel% neq 0 (
    echo [ERROR] Failed to create venv
    pause
    exit /b 1
)
echo        venv created
:venv_ready

:: 3. Ensure pip in venv works
echo.
echo [3/5] Checking pip in venv ...
"%VENV_PY%" -m pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo        pip in venv is broken, recreating venv ...
    rmdir /s /q "%VENV_DIR%"
    !PY! -m venv "%VENV_DIR%"
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to recreate venv
        pause
        exit /b 1
    )
    "%VENV_PY%" -m pip --version >nul 2>&1
    if %errorlevel% neq 0 (
        echo [ERROR] pip is unavailable in venv
        pause
        exit /b 1
    )
)
echo        pip ready

:: 4. Install dependencies strictly from offline packages
echo.
echo [4/5] Installing Python dependencies from offline packages ...
if not exist "%OFFLINE_PKG_DIR%" (
    echo [ERROR] Offline package folder not found: %OFFLINE_PKG_DIR%
    pause
    exit /b 1
)
set "PY_TAG="
if exist "%OFFLINE_PKG_DIR%\greenlet-3.3.2-cp313-cp313-win_amd64.whl" if "%PY_TAG%"=="" set "PY_TAG=cp313"
if exist "%OFFLINE_PKG_DIR%\greenlet-3.3.2-cp312-cp312-win_amd64.whl" if "%PY_TAG%"=="" set "PY_TAG=cp312"
if "%PY_TAG%"=="" (
    echo [ERROR] No supported greenlet wheel found in %OFFLINE_PKG_DIR%
    pause
    exit /b 1
)
set "GREENLET_WHL=%OFFLINE_PKG_DIR%\greenlet-3.3.2-%PY_TAG%-%PY_TAG%-win_amd64.whl"
"%VENV_PY%" -c "import sys; from pathlib import Path; pkg_dir = Path(r'%OFFLINE_PKG_DIR%'); wheels = sorted(pkg_dir.glob('*.whl')); missing = []; version = sys.version_info; tag = f'cp{version.major}{version.minor}'; checks = [('playwright-', 'playwright wheel'), ('openpyxl-', 'openpyxl wheel'), ('pyee-', 'pyee wheel'), ('typing_extensions-', 'typing_extensions wheel')]; [missing.append(label) for prefix, label in checks if not any(prefix in wheel.name for wheel in wheels)]; needs_greenlet = any('playwright-' in wheel.name for wheel in wheels); has_greenlet = any(('greenlet-' in wheel.name) and (tag in wheel.name) for wheel in wheels); missing.append(f'greenlet wheel for {tag}') if needs_greenlet and not has_greenlet else None; print('[ERROR] Offline package set is incomplete for this Python version:' + ''.join(f'\n  - {item}' for item in missing)) if missing else None; raise SystemExit(1 if missing else 0)"
if %errorlevel% neq 0 (
    pause
    exit /b 1
)
if not exist "%GREENLET_WHL%" (
    echo [ERROR] Matching greenlet wheel not found for %PY_TAG%: %GREENLET_WHL%
    pause
    exit /b 1
)
"%VENV_PY%" -m pip install --no-index --find-links="%OFFLINE_PKG_DIR%" "%OFFLINE_PKG_DIR%\playwright-1.58.0-py3-none-win_amd64.whl" "%OFFLINE_PKG_DIR%\openpyxl-3.1.5-py2.py3-none-any.whl" "%OFFLINE_PKG_DIR%\pyee-13.0.1-py3-none-any.whl" "%OFFLINE_PKG_DIR%\typing_extensions-4.15.0-py3-none-any.whl" "%GREENLET_WHL%" --quiet
if %errorlevel% neq 0 (
    echo [ERROR] Offline pip install failed
    pause
    exit /b 1
)
"%VENV_PY%" -c "import greenlet, playwright, openpyxl" >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Dependencies were installed, but native modules failed to load.
    echo         Please install Microsoft Visual C++ Redistributable 2015-2022 x64,
    echo         then rerun setup.bat.
    pause
    exit /b 1
)
echo        Python dependencies installed

echo.
echo        Checking Playwright browser binaries ...
"%VENV_PY%" -m playwright install --help >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] playwright command is unavailable in venv
    pause
    exit /b 1
)
echo        Playwright CLI ready

:: 5. Init config
echo.
echo [5/5] Initializing config ...
if not exist "%~dp0config.json" (
    copy "%~dp0config.example.json" "%~dp0config.json" >nul
    echo        config.json created from template, please edit it
) else (
    echo        config.json already exists, skipping
)

echo.
echo ============================================
echo   Setup complete!
echo ============================================
echo.
echo Note:
echo   - This setup now uses offline Python packages only.
echo   - If Chromium browser binaries are also packaged locally, install them separately if needed.
echo.
echo Usage:
echo   1. Double-click start_edge.bat to run the tool
echo   2. Put Order.xlsx in project folder
echo   3. If headless bootstrap asks, login in browser then press Enter in terminal
echo.
pause
