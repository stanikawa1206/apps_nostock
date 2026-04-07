# publish_runner.py
# -*- coding: utf-8 -*-

import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime

# ======================
# パス設定
# ======================
PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import send_mail, get_sql_server_connection

BASE_DIR = PROJECT_ROOT
PYTHON = sys.executable

PUBLISH_SCRIPT = BASE_DIR / "apps" / "publish" / "publish_ebay.py"

# ======================
# VPS一覧（そのまま流用）
# ======================
VPS_LIST = [
    "162.43.42.135",
    "162.43.15.160",
    "162.43.29.154",
    "210.131.209.103",
    "162.43.39.209",
    "85.131.251.127",
    "210.131.209.232",
]

# ======================
# ローカル実行
# ======================
def run_local():
    print("🟢 LOCAL publish 開始")

    result = subprocess.run(
        [PYTHON, str(PUBLISH_SCRIPT)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    print(result.stdout)
    print(result.stderr)

    return result.returncode


# ======================
# VPS実行
# ======================
def run_vps():
    print("🟢 VPS publish 開始")

    for ip in VPS_LIST:
        cmd = (
            f'start "{ip}" '
            f'ssh -tt root@{ip} '
            '"cd /opt/apps_nostock && '
            'git pull && '
            'cd /opt/apps_nostock/apps/publish && '
            'python3 publish_ebay.py"'
        )

        subprocess.Popen(cmd, shell=True)

    print("🚀 VPS全台で publish 起動完了")


# ======================
# メイン
# ======================
def main():
    conn = get_sql_server_connection()

    start = datetime.now()

    try:
        # ① VPS起動（並列）
        run_vps()

        # ② ローカル起動
        code = run_local()

        end = datetime.now()

        subject = "✅ publish_ebay 実行完了" if code == 0 else "❌ publish_ebay エラー"

        body = (
            f"開始: {start}\n"
            f"終了: {end}\n"
            f"処理時間: {end - start}\n"
            f"returncode: {code}\n"
        )

        send_mail(subject, body)

    finally:
        conn.close()


if __name__ == "__main__":
    main()