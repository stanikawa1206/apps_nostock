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

# --- 修正ポイント：画面表示(tee)を追加 ---
# 1. cd でディレクトリ移動
# 2. whileループ内でpythonを実行
# 3. 2>&1 | tee -a で画面表示とログ追記を同時に行う
# 4. python終了後に3秒待機
COMMAND="cd $PROJECT_DIR && while true; do 
    echo \"[\$(date)] Starting Python Worker...\" | tee -a $LOG_FILE;
    python3 -m apps.inventory.scrape_worker 2>&1 | tee -a $LOG_FILE;
    echo \"[\$(date)] Worker exited. Refreshing memory and restarting in 3s...\" | tee -a $LOG_FILE;
    sleep 3;
done"

# セッション内でコマンド実行
tmux send-keys -t $SESSION_NAME "$COMMAND" C-m

echo "Worker loop started."
echo "Attach with: tmux attach -t $SESSION_NAME"