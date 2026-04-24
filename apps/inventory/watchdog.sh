#!/bin/bash

PROJECT_DIR="/opt/apps_nostock"
PYTHON_CMD="python3 -u -m apps.inventory.scrape_worker_new"
TIMEOUT_SEC=600
CHECK_INTERVAL=10
TARGET_WORKERS=2

cd "$PROJECT_DIR" || exit 1

while true; do
    now_epoch=$(date +%s)

    # -------------------------
    # ① 死んだ/止まったworker処理
    # -------------------------
    for file in "$PROJECT_DIR"/worker_status_*.txt; do
        [ -e "$file" ] || continue

        pid=$(basename "$file")
        pid=${pid#worker_status_}
        pid=${pid%.txt}

        file_epoch=$(stat -c %Y "$file" 2>/dev/null)
        [ -z "$file_epoch" ] && continue

        diff_sec=$((now_epoch - file_epoch))

        if ! kill -0 "$pid" 2>/dev/null || [ "$diff_sec" -ge "$TIMEOUT_SEC" ]; then
            echo "[$(date '+%F %T')] kill pid=$pid (timeout or dead)"

            kill -9 "$pid" 2>/dev/null || true
            rm -f "$file"
        fi
    done

    # -------------------------
    # ② worker数を維持
    # -------------------------
    running=$(ls "$PROJECT_DIR"/worker_status_*.txt 2>/dev/null | wc -l)

    if [ "$running" -lt "$TARGET_WORKERS" ]; then
        need=$((TARGET_WORKERS - running))
        echo "[$(date '+%F %T')] spawn running=$running need=$need"

        for ((i=0; i<need; i++)); do
            nohup $PYTHON_CMD >/dev/null 2>&1 &
            sleep 1
        done
    fi

    sleep "$CHECK_INTERVAL"
done