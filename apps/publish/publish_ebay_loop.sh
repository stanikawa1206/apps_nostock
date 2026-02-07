#!/bin/bash

while true; do
  echo "=== start publish_ebay_new ==="
  python3 -m publish_ebay_new
  code=$?

  echo "=== exited with code=$code ==="

  # 正常終了ならループを抜ける
  if [ $code -eq 0 ]; then
    echo "normal exit"
    break
  fi

  echo "restart after 15 sec..."
  sleep 15
done
