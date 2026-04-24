@echo off
setlocal

set PROJECT_DIR=D:\apps_nostock
cd /d "%PROJECT_DIR%"

set WORKER_STATUS_DIR=D:\apps_nostock

python -m apps.inventory.watchdog

endlocal