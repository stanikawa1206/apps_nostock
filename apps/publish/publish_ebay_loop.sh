#!/bin/bash

SESSION="pub"

# 一時ファイルは publish_ebay を1回起動するごとに専用の TMPDIR（/tmp/publish_ebay.XXXXXX）へ
# 作らせ、そのプロセスの終了後にそのディレクトリだけを削除する（下のループ内）。
# 以前の find /tmp -mindepth 1 -maxdepth 1 -mmin +2880 ... -exec rm -rf は、
# 2日以上更新の無い /tmp 直下を無条件に消すため、長時間動いている他プロセスの
# ブラウザプロファイルや systemd の PrivateTmp 等、使用中の一時領域まで消し得た。

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
            run_tmp=\$(mktemp -d /tmp/publish_ebay.XXXXXX)
            TMPDIR=\"\$run_tmp\" python3 -m publish_ebay
            code=\$?
            rm -rf -- \"\$run_tmp\"
            echo \"=== exited with code=\$code ===\"

            if [ \$code -eq 0 ] || [ \$code -eq 10 ]; then
                echo 'normal exit'
                break
            fi

            echo 'restart after 15 sec...'
            sleep 15
        done
    "
else
    echo "tmux session already exists: $SESSION"
fi

# セッションにアタッチ
tmux attach -t $SESSION
