# apps/inventory/cleanup_vendor_item_media.ps1
#
# trx.vendor_item の不要な description/description_en/image_url2〜20 を
# NULL化する日次クリーンアップ処理(apps/inventory/cleanup_vendor_item_media.py)の
# 起動ラッパー。Windowsタスクスケジューラ「VendorItemMediaCleanup」(毎日04:00)から
# 呼び出される。apps/tests/scripts/daily_check.ps1 と同じログ・タイムアウト方式。

param([switch]$Interactive)

# --- UTF-8 固定 ---
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($true)
$env:PYTHONUTF8       = "1"
$env:PYTHONIOENCODING = "utf-8"

# --- パス設定（Root を基準に） ---
$Root    = 'D:\apps_nostock'
$LogDir  = Join-Path $Root 'logs'
$Python  = 'C:\Users\stani\AppData\Local\Programs\Python\Python312\python.exe'
$Module  = 'apps.inventory.cleanup_vendor_item_media'

# --- 事前準備 ---
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
Set-Location $Root
$ts   = Get-Date -Format 'yyyyMMdd_HHmmss'
$log  = Join-Path $LogDir "cleanup_vendor_item_media_$ts.log"
$logO = Join-Path $LogDir "cleanup_vendor_item_media_$ts.out.log"
$logE = Join-Path $LogDir "cleanup_vendor_item_media_$ts.err.log"

"[START] $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $log -Encoding utf8
"Python : $Python`nModule : $Module`nLog    : $log" | Add-Content -Path $log -Encoding utf8

# --- 実行（パッケージ実行がポイント） ---
$ErrorActionPreference = 'Stop'

if ($Interactive) {
  # 同一コンソールで実行 → Ctrl+Cが届く / ログがリアルタイムで見える
  & $Python -u -m $Module 2>&1 | Tee-Object -FilePath $log -Append
  $exitCode = $LASTEXITCODE
}
else {
  # タスク実行用
  $proc = Start-Process -FilePath $Python `
    -ArgumentList @('-u', '-m', $Module) `
    -NoNewWindow `
    -RedirectStandardOutput $logO `
    -RedirectStandardError  $logE `
    -PassThru

  # 初回は既存データが大量に対象になるため長めのタイムアウト（2時間）
  $timeoutSec = 2 * 60 * 60
  $done = Wait-Process -Id $proc.Id -Timeout $timeoutSec -ErrorAction SilentlyContinue
  if (-not $done) {
    Add-Content -Path $log -Value "`r`n[TIMEOUT] exceeded ${timeoutSec}s. Killing process..." -Encoding utf8
    Stop-Process -Id $proc.Id -Force
    $exitCode = 124
  } else {
    $exitCode = $proc.ExitCode
  }

  # 終了後にログ統合
  Add-Content -Path $log -Value "`r`n[STDOUT] ------------------------------" -Encoding utf8
  if (Test-Path $logO) { (Get-Content $logO -Raw -Encoding utf8) | Add-Content -Path $log -Encoding utf8 }
  Add-Content -Path $log -Value "`r`n[STDERR] ------------------------------" -Encoding utf8
  if (Test-Path $logE) { (Get-Content $logE -Raw -Encoding utf8) | Add-Content -Path $log -Encoding utf8 }
  Add-Content -Path $log -Value "`r`n[EXITCODE] $exitCode" -Encoding utf8

  Remove-Item -Force -ErrorAction SilentlyContinue $logO, $logE
}

exit $exitCode
