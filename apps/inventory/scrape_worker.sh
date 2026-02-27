#!/bin/bash

SESSION_NAME="scrape_worker"
# プロジェクトのルートディレクトリ（appsの上の階層）を指定
PROJECT_DIR="/opt/apps_nostock"
LOG_FILE="$PROJECT_DIR/worker.log"

echo "Starting worker loop in tmux session: $SESSION_NAME"

# 既存セッションを削除してクリーンにする
tmux kill-session -t $SESSION_NAME 2>/dev/null
echo "Creating new tmux session..."
tmux new-session -d -s $SESSION_NAME

# --- 実行コマンド ---
# 1. 確実に PROJECT_DIR に移動
# 2. その場所で python3 -m を実行
COMMAND="cd $PROJECT_DIR && while true; do 
    echo \"[\$(date)] --- Starting Python Worker ---\" | tee -a $LOG_FILE;
    python3 -m apps.inventory.scrape_worker 2>&1 | tee -a $LOG_FILE;
    echo \"[\$(date)] --- Worker exited. Restarting in 3s ---\" | tee -a $LOG_FILE;
    sleep 3;
done"

# セッション内でコマンド実行
tmux send-keys -t $SESSION_NAME "$COMMAND" C-m

echo "Worker loop started."
echo "Check progress with: tmux attach -t $SESSION_NAME"