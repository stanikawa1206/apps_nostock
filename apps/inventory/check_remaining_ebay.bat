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

:: chromedriver / Chrome の後片付けは check_remaining_ebay.py が、この実行で起動したプロセスだけを対象に行う
:: (終了時の kill_own_descendants と、監視による自己終了時の kill_own_descendants_and_exit。apps/common/process_watchdog.py)。
:: 2026-11-15 まではここで taskkill /F /IM chromedriver.exe /T を実行しており、同じPCの出品(publish_ebay)や
:: 購入処理の chromedriver まで終了させていた(11/2〜15 の mouse の出品の Renderer異常 7回の原因)。
:: 名前を指定した taskkill は使わないこと。VPS の check_remaining_ebay.sh も自分の実行だけを片付ける方式。

:: 15秒待機
timeout /t 15

goto loop