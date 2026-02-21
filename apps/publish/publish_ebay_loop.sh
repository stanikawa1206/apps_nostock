#!/bin/bash

SESSION="pub"

# tmux が無い場合はエラー
if ! command -v tmux &> /dev/null; then
    echo "tmux not installed."
    exit 1
fi

# セッションが既に存在するか確認
tmux has-session -t $SESSION 2>/dev/null

if [ $? != 0 ]; then
    echo "create new tmux session: $SESSION"

    tmux new-session -d -s $SESSION "
        while true; do
            echo '=== start publish_ebay ==='
            python3 -m publish_ebay
            code=\$?
            echo \"=== exited with code=\$code ===\"

            if [ \$code -eq 0 ]; then
                echo 'normal exit'
                break
            fi

            echo 'ERROR DETECTED! Press Enter to restart, or Ctrl+C to stop.'
            read dummy
            
            echo 'restart after 15 sec...'
            sleep 15
        done
    "
else
    echo "tmux session already exists: $SESSION"
fi

# セッションにアタッチ
tmux attach -t $SESSION
