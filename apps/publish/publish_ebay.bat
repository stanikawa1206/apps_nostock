@echo off
:loop
echo [%date% %time%] Starting publish_ebay...

:: Pythonを実行 (1件でも成功したら exit 0 で戻るようにプログラム側を調整)
python -m publish_ebay
set code=%errorlevel%

echo Process exited with code %code%

:: exit 0 (成功) でも、繰り返し実行したい場合は goto loop に書き換える
if %code% == 0 (
    echo SUCCESS: One cycle completed. Restarting for next items...
    timeout /t 5
    goto loop
)

:: 異常終了なら掃除して再起動
echo CRASHED or TIMEOUT: Cleaning processes and restarting...
taskkill /F /IM chrome.exe /T >nul 2>&1
taskkill /F /IM chromedriver.exe /T >nul 2>&1
timeout /t 15

goto loop