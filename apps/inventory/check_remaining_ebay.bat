@echo off
:loop
echo [%date% %time%] Starting check_remaining_ebay...

:: Pythonを実行
python -m check_remaining_ebay
set code=%errorlevel%

echo Process exited with code %code%

:: 正常終了 (code 0) なら終了
if %code% == 0 (
    echo SUCCESS: All tasks completed.
    exit /b
)

:: 異常終了なら再起動
echo CRASHED or RELOAD: Restarting in 15 seconds...

:: 【修正ポイント】名前で一括削除するのをやめる
:: もしどうしても掃除したい場合は、Python側で処理を完結させるのがベストですが、
:: ここでは「自分のChromeを巻き込まない」ために一旦コメントアウト（無効化）します。
:: taskkill /F /IM chrome.exe /T >nul 2>&1
taskkill /F /IM chromedriver.exe /T >nul 2>&1

:: 15秒待機
timeout /t 15

goto loop