@echo off
setlocal

set PROJECT_DIR=D:\apps_nostock
set SCRIPT_DIR=%~dp0
set LOG_DIR=%PROJECT_DIR%\logs
set LOG_FILE=%LOG_DIR%\scrape_worker.log
set PYTHON_EXE=python
set WORKER_STATUS_DIR=%PROJECT_DIR%

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%PROJECT_DIR%"

echo [%DATE% %TIME%] Starting Python Worker... >> "%LOG_FILE%"
echo ==== START ==== >> "%LOG_FILE%"

rem %PYTHON_EXE% -u -m apps.inventory.scrape_worker_new >> "%LOG_FILE%" 2>&1
%PYTHON_EXE% -u -m apps.inventory.scrape_worker_new

echo ==== END ==== >> "%LOG_FILE%"
echo [%DATE% %TIME%] Python Worker exited. >> "%LOG_FILE%"

endlocal