#!/bin/bash

# 無限ループ
while true; do
  echo "=== [$(date '+%Y-%m-%d %H:%M:%S')] start check_remaining_ebay ==="
  
  # Pythonを実行
  python3 -m check_remaining_ebay
  code=$?

  echo "=== exited with code=$code ==="

  # 正常終了（code=0）なら、すべての処理が終わったと判断してループを抜ける
  if [ $code -eq 0 ]; then
    echo "SUCCESS: All inventory checks completed normally."
    break
  fi

  # 異常終了（code=1以上）なら、15秒待機して再開
  echo "CRASHED: Cleanup processes and restarting in 15 seconds..."
  
  # 念のため、残っているブラウザプロセスを掃除（これが安定の秘訣です）
  pkill -9 -f chrome
  pkill -9 -f chromedriver
  
  sleep 15
done