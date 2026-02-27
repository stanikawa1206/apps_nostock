@echo off
setlocal

:: =========================
:: 設定（あなたの環境に合わせて調整済み）
:: =========================
set PROJECT_DIR=D:\apps_nostock
:: ログはプロジェクト内の logs フォルダに保存します
set LOG_FILE=%PROJECT_DIR%\logs\scrape_worker.log
set PYTHON_EXE=python

:: ログフォルダがなければ作成
if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"

:: Dドライブへ移動してからプロジェクトフォルダへ移動
d:
cd "%PROJECT_DIR%"

echo [%DATE% %TIME%] Worker Loop Started.
echo 実行状況は %LOG_FILE% で確認できます。

:loop
echo [%DATE% %TIME%] Starting Python Worker... >> "%LOG_FILE%"

:: Python実行
%PYTHON_EXE% -m apps.inventory.scrape_worker >> "%LOG_FILE%" 2>&1

echo [%DATE% %TIME%] Worker exited. Refreshing memory... >> "%LOG_FILE%"

:: ゾンビプロセス対策（Google Chromeを強制終了）
taskkill /F /IM chrome.exe /T >nul 2>&1
taskkill /F /IM chromedriver.exe /T >nul 2>&1

:: 5秒待機してから再起動
timeout /t 5 /nobreak >nul

goto loop