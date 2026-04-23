#!/bin/bash

PROJECT_DIR="/opt/apps_nostock"
PYTHON_CMD="python3 -u -m apps.inventory.scrape_worker_new"
TIMEOUT_SEC=600
CHECK_INTERVAL=10

cd "$PROJECT_DIR" || exit 1

while true; do
    now_epoch=$(date +%s)

    for file in "$PROJECT_DIR"/worker_status_*.txt; do
        [ -e "$file" ] || continue

        pid=$(basename "$file")
        pid=${pid#worker_status_}
        pid=${pid%.txt}

        if ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$file"
            nohup $PYTHON_CMD >/dev/null 2>&1 &
            continue
        fi

        file_epoch=$(stat -c %Y "$file" 2>/dev/null)
        if [ -z "$file_epoch" ]; then
            continue
        fi

        diff_sec=$((now_epoch - file_epoch))

        if [ "$diff_sec" -ge "$TIMEOUT_SEC" ]; then
            job_id=$(grep '^job_id=' "$file" | head -n 1 | cut -d= -f2)
            page=$(grep '^page=' "$file" | head -n 1 | cut -d= -f2)

            echo "[$(date '+%F %T')] timeout pid=$pid job_id=$job_id page=$page"

            kill -9 "$pid" 2>/dev/null || true
            rm -f "$file"

            nohup $PYTHON_CMD >/dev/null 2>&1 &
        fi
    done

    sleep "$CHECK_INTERVAL"
done