@echo off
:loop
echo [%date% %time%] Starting publish_ebay...

python -m publish_ebay
set code=%errorlevel%

echo Process exited with code %code%

:: 0 = 成功（まだ続ける）
if %code% == 0 (
    timeout /t 5
    goto loop
)

:: 10 = 枯渇（完全終了）
if %code% == 10 (
    echo NO ITEMS LEFT. Batch finished.
    exit /b 0
)

:: それ以外 = 異常
echo CRASHED: Cleaning processes...
taskkill /F /IM chromedriver.exe /T >nul 2>&1
timeout /t 15
goto loop