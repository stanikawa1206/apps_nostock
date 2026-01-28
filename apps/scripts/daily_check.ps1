param([switch]$Interactive)

# --- UTF-8 固定 ---
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($true)
$env:PYTHONUTF8       = "1"
$env:PYTHONIOENCODING = "utf-8"

# --- パス設定（Root を基準に） ---
$Root    = 'D:\apps_nostock'                      # ← ルート
$LogDir  = Join-Path $Root 'logs'
$Python  = 'C:\Users\stani\AppData\Local\Programs\Python\Python313\python.exe'
$Module  = 'apps.inventory.daily_check'           # ← ここだけ変えれば別モジュールも実行可

# --- 事前準備 ---
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
Set-Location $Root
$ts   = Get-Date -Format 'yyyyMMdd_HHmmss'
$log  = Join-Path $LogDir "daily_check_$ts.log"
$logO = Join-Path $LogDir "daily_check_$ts.out.log"
$logE = Join-Path $LogDir "daily_check_$ts.err.log"

"[START] $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File -FilePath $log -Encoding utf8
"Python : $Python`nModule : $Module`nLog    : $log" | Add-Content -Path $log -Encoding utf8

# --- 実行（パッケージ実行がポイント） ---
$ErrorActionPreference = 'Stop'

if ($Interactive) {
  # ✅ 同一コンソールで実行 → Ctrl+Cがpythonに届く / ログがリアルタイムで見える
  & $Python -u -m $Module 2>&1 | Tee-Object -FilePath $log -Append
  $exitCode = $LASTEXITCODE
}
else {
  # ✅ タスク実行用（現状維持）: ただし -u を付けてバッファ対策
  $proc = Start-Process -FilePath $Python `
    -ArgumentList @('-u','-m',$Module) `
    -NoNewWindow `
    -RedirectStandardOutput $logO `
    -RedirectStandardError  $logE `
    -PassThru

  # ✅ ハング検知タイムアウト（例：2時間）※必要なら調整
  $timeoutSec = 2 * 60 * 60
  $done = Wait-Process -Id $proc.Id -Timeout $timeoutSec -ErrorAction SilentlyContinue
  if (-not $done) {
    Add-Content -Path $log -Value "`r`n[TIMEOUT] exceeded ${timeoutSec}s. Killing process..." -Encoding utf8
    Stop-Process -Id $proc.Id -Force
    $exitCode = 124
  } else {
    $exitCode = $proc.ExitCode
  }

  # 終了後にログ統合（今のまま）
  Add-Content -Path $log -Value "`r`n[STDOUT] ------------------------------" -Encoding utf8
  if (Test-Path $logO) { (Get-Content $logO -Raw -Encoding utf8) | Add-Content -Path $log -Encoding utf8 }
  Add-Content -Path $log -Value "`r`n[STDERR] ------------------------------" -Encoding utf8
  if (Test-Path $logE) { (Get-Content $logE -Raw -Encoding utf8) | Add-Content -Path $log -Encoding utf8 }
  Add-Content -Path $log -Value "`r`n[EXITCODE] $exitCode" -Encoding utf8

  Remove-Item -Force -ErrorAction SilentlyContinue $logO, $logE
}

exit $exitCode
