#!/bin/bash

SESSION_NAME="scrape_worker"
PROJECT_DIR="/opt/apps_nostock"
LOG_FILE="/var/log/scrape_worker.log"

echo "Starting worker loop in tmux session: $SESSION_NAME"

# 既存セッションがあるか確認
tmux has-session -t $SESSION_NAME 2>/dev/null

if [ $? != 0 ]; then
    echo "Creating new tmux session..."
    tmux new-session -d -s $SESSION_NAME
else
    echo "Session already exists."
fi

# --- 修正ポイント：while true ループで実行 ---
# 1. cd でディレクトリ移動
# 2. whileループ内でpythonを実行
# 3. pythonが終了(exit)しても、すぐまた実行される
# 4. 念のため実行の合間に 3秒待機してゾンビプロセスを落ち着かせる
COMMAND="cd $PROJECT_DIR && while true; do 
    echo \"[\$(date)] Starting Python Worker...\" >> $LOG_FILE;
    python3 -m apps.inventory.scrape_worker >> $LOG_FILE 2>&1;
    echo \"[\$(date)] Worker exited. Refreshing memory and restarting in 3s...\" >> $LOG_FILE;
    sleep 3;
done"

# セッション内でコマンド実行
tmux send-keys -t $SESSION_NAME "$COMMAND" C-m

echo "Worker loop started."
echo "Attach with: tmux attach -t $SESSION_NAME"