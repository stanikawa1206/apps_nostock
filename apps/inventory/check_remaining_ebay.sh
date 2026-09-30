#!/bin/bash

# check_remaining_ebay を1回ずつ
#   ・専用の一時ディレクトリ（TMPDIR）
#   ・専用のセッション（setsid）
# で起動し、終了後はその実行が起動したプロセスと一時ファイルだけを片付ける。
# 同じVPSで動く publish_ebay / scrape_worker の Chrome・chromedriver や、
# /tmp の他の中身（tmux のソケット等）は巻き込まない。
# （以前の pkill -9 -f chrome / pkill -9 -f chromedriver / find /tmp -mindepth 1 -delete は、
#   同じVPS上の他プロセスのブラウザと /tmp 全体を壊していた）

child=""
run_tmp=""

# この実行で起動したプロセスと一時ディレクトリだけを片付ける
cleanup_run() {
  [ -n "$child" ] || return 0
  # この実行のセッションに属するプロセス（python・chromedriver・Selenium の Chrome）
  pkill -KILL -s "$child" 2>/dev/null
  # Playwright の Chromium は Node が別セッションで起動するため、
  # この実行の TMPDIR をコマンドラインに含むプロセス（--user-data-dir がこの配下）で特定する
  pkill -KILL -f -- "$run_tmp" 2>/dev/null
  rm -rf -- "$run_tmp"
  child=""
}

# setsid で端末から切り離しているため、ウィンドウを閉じた時や Ctrl+C の時も
# この実行を片付けてから終了する
trap 'cleanup_run; exit 130' INT TERM HUP

# 無限ループ
while true; do
  echo "=== [$(date '+%Y-%m-%d %H:%M:%S')] start check_remaining_ebay ==="

  run_tmp=$(mktemp -d /tmp/check_remaining_ebay.XXXXXX)

  # Pythonを実行（バックグラウンドのジョブはプロセスグループのリーダーではないため、
  # setsid は fork せずにそのまま新しいセッションのリーダーになる = セッションID は $!）
  TMPDIR="$run_tmp" setsid python3 -m check_remaining_ebay &
  child=$!
  wait "$child"
  code=$?

  echo "=== exited with code=$code ==="

  cleanup_run

  # 正常終了（code=0）なら、すべての処理が終わったと判断してループを抜ける
  if [ $code -eq 0 ]; then
    echo "SUCCESS: All inventory checks completed normally."
    break
  fi

  # 異常終了（code=1以上）なら、15秒待機して再開
  echo "CRASHED: Cleaned up this run's processes and restarting in 15 seconds..."
  sleep 15
done
