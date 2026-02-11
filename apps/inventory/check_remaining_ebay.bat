@echo off
:loop
echo [%date% %time%] Starting check_remaining_ebay_new...

:: Pythonを実行
python -m check_remaining_ebay_new
set code=%errorlevel%

echo Process exited with code %code%

:: 正常終了 (code 0) なら終了
if %code% == 0 (
    echo SUCCESS: All tasks completed.
    exit /b
)

:: 異常終了なら再起動
echo CRASHED: Restarting in 15 seconds...

:: ゾンビプロセスの掃除 (Windows版)
taskkill /F /IM chrome.exe /T >nul 2>&1
taskkill /F /IM chromedriver.exe /T >nul 2>&1

:: 15秒待機 (timeoutコマンド)
timeout /t 15

goto loop