#!/bin/bash

SESSION="pub"

# 一時ファイルは publish_ebay を1回起動するごとに専用の TMPDIR（/tmp/publish_ebay.XXXXXX）へ
# 作らせ、そのプロセスの終了後にそのディレクトリだけを削除する（下のループ内）。
# 以前の find /tmp -mindepth 1 -maxdepth 1 -mmin +2880 ... -exec rm -rf は、
# 2日以上更新の無い /tmp 直下を無条件に消すため、長時間動いている他プロセスの
# ブラウザプロファイルや systemd の PrivateTmp 等、使用中の一時領域まで消し得た。

# 画面出力は tmux のスクロールバックにしか残らず、セッション終了と同時に消えるため、
# publish_ebay 自身に画面出力を日別ファイルへも書かせる（publish_ebay.py の PUBLISH_EBAY_LOG_PATH）。
# 起動・終了コード・再起動はこのループが同じファイルに書く。保存期間は LOG_KEEP_DAYS 日。
APP_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_DIR="$APP_ROOT/logs/publish_ebay"
LOG_KEEP_DAYS=30
# 監視による自己終了の記録（publish_ebay.py の EXIT_REPORT_PATH）は1ファイルに追記されるため、
# 一定サイズを超えたら1世代だけ残して切り替える
EXIT_REPORT="$APP_ROOT/logs/publish_ebay_exit.log"
EXIT_REPORT_MAX_BYTES=$((5 * 1024 * 1024))

mkdir -p "$LOG_DIR"
find "$LOG_DIR" -maxdepth 1 -type f -name 'publish_ebay_*.log' -mtime +$LOG_KEEP_DAYS -delete
if [ -f "$EXIT_REPORT" ] && [ "$(stat -c %s "$EXIT_REPORT")" -gt $EXIT_REPORT_MAX_BYTES ]; then
    mv -f "$EXIT_REPORT" "$EXIT_REPORT.1"
fi

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
            log_file=\"$LOG_DIR/publish_ebay_\$(date +%Y%m%d).log\"
            echo \"=== \$(date '+%F %T') host=\$(hostname) start publish_ebay ===\" | tee -a \"\$log_file\"
            run_tmp=\$(mktemp -d /tmp/publish_ebay.XXXXXX)
            PUBLISH_EBAY_LOG_PATH=\"\$log_file\" TMPDIR=\"\$run_tmp\" python3 -m publish_ebay
            code=\$?
            rm -rf -- \"\$run_tmp\"
            echo \"=== \$(date '+%F %T') host=\$(hostname) exited with code=\$code ===\" | tee -a \"\$log_file\"

            if [ \$code -eq 0 ] || [ \$code -eq 10 ]; then
                echo 'normal exit' | tee -a \"\$log_file\"
                break
            fi

            echo 'restart after 15 sec...' | tee -a \"\$log_file\"
            sleep 15
        done
    "
else
    echo "tmux session already exists: $SESSION"
fi

# --no-attach: 起動確認だけしてアタッチせずに終了する（publish_manager.py から起動する場合）。
# 出品ループは tmux 内で動き続け、SSH を切っても止まらない。実行中は tmux attach -t pub、
# 終了後は logs/publish_ebay/publish_ebay_YYYYMMDD.log で確認する。
if [ "$1" = "--no-attach" ]; then
    if tmux has-session -t $SESSION 2>/dev/null; then
        echo "tmux session running: $SESSION"
        exit 0
    fi
    echo "tmux session not found: $SESSION"
    exit 1
fi

# セッションにアタッチ
tmux attach -t $SESSION
