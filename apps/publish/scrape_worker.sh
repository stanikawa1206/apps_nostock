#!/bin/bash

SESSION_NAME="scrape_worker"
PROJECT_DIR="/opt/apps_nostock"
LOG_FILE="/var/log/scrape_worker.log"

echo "Starting worker in tmux session: $SESSION_NAME"

# 既存セッションがあるか確認
tmux has-session -t $SESSION_NAME 2>/dev/null

if [ $? != 0 ]; then
    echo "Creating new tmux session..."
    tmux new-session -d -s $SESSION_NAME
else
    echo "Session already exists."
fi

# セッション内でコマンド実行
tmux send-keys -t $SESSION_NAME "cd $PROJECT_DIR" C-m
tmux send-keys -t $SESSION_NAME "python3 -m apps.inventory.scrape_worker >> $LOG_FILE 2>&1" C-m

echo "Worker started."
echo "Attach with: tmux attach -t $SESSION_NAME"