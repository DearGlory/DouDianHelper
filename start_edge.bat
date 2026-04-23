@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
cd /d "%~dp0"

set "LIMIT_ARG="
set "PARALLEL_ARG="
set "LOG_DIR=%cd%\log"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd-HHmmss"') do set "RUN_TS=%%I"
set "LAUNCH_LOG=%LOG_DIR%\launch_edge-%RUN_TS%.log"
set "MAIN_LOG=%LOG_DIR%\main-%RUN_TS%.log"

echo ============================================
echo   Starting Edge with CDP (port 9222)
echo ============================================
echo.

"venv\Scripts\python.exe" --version >nul 2>&1
if errorlevel 1 goto setup_venv
"venv\Scripts\python.exe" -c "import greenlet, playwright, openpyxl" >nul 2>&1
if errorlevel 1 goto setup_venv
goto prompt_runtime_args

:setup_venv
echo.
echo venv missing, broken, or dependencies unavailable, running setup.bat ...
if exist "venv" rmdir /s /q "venv"
call setup.bat
"venv\Scripts\python.exe" --version >nul 2>&1
if errorlevel 1 goto setup_failed
"venv\Scripts\python.exe" -c "import greenlet, playwright, openpyxl" >nul 2>&1
if errorlevel 1 goto setup_failed
goto prompt_runtime_args

:setup_failed
echo [ERROR] setup failed, python or dependencies are still unavailable.
pause
exit /b 1

:prompt_runtime_args
set /p "PARALLEL_WORKERS=parallel_workers (must be a positive integer): "
if not defined PARALLEL_WORKERS goto invalid_parallel
for /f "delims=0123456789" %%A in ("%PARALLEL_WORKERS%") do goto invalid_parallel
if "%PARALLEL_WORKERS%"=="0" goto invalid_parallel
set "PARALLEL_ARG=--parallel-workers %PARALLEL_WORKERS%"

set /p "LIMIT_INPUT=--limit (blank means process all orders): "
if not defined LIMIT_INPUT goto launch_edge
for /f "delims=0123456789" %%A in ("%LIMIT_INPUT%") do goto invalid_limit
if "%LIMIT_INPUT%"=="0" goto invalid_limit
set "LIMIT_ARG=--limit %LIMIT_INPUT%"
goto launch_edge

:invalid_parallel
echo [ERROR] parallel_workers must be a positive integer.
pause
exit /b 1

:invalid_limit
echo [ERROR] --limit must be blank or a positive integer.
pause
exit /b 1

:launch_edge
powershell -NoProfile -Command "$ErrorActionPreference='Continue'; & 'venv\Scripts\python.exe' 'launch_edge.py' 2>&1 | Tee-Object -FilePath '%LAUNCH_LOG%'"
if errorlevel 1 goto cdp_failed
goto run_main

:cdp_failed
echo CDP failed, cannot continue.
pause
exit /b 1

:run_main
echo.
echo ============================================
echo   Running DouDianHelper ...
echo ============================================
echo Type q / quit / exit then press Enter to stop gracefully.
powershell -NoProfile -Command "$ErrorActionPreference='Continue'; & 'venv\Scripts\python.exe' 'main.py' %PARALLEL_ARG% %LIMIT_ARG% 2>&1 | Tee-Object -FilePath '%MAIN_LOG%'"

echo.
echo [INFO] launch log: %LAUNCH_LOG%
echo [INFO] main log: %MAIN_LOG%
pause
