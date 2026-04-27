# D:\apps_nostock\apps\inventory\watchdog.py

import os
import sys
import time
import glob
import subprocess
from datetime import datetime
from apps.common.utils import get_sql_server_connection
import platform
from pathlib import Path
import psutil

# =========================
# 設定
# =========================

# worker_status_*.txt を置く場所（＝監視対象）
# Windowsなら D:/apps_nostock
# Linuxなら /opt/apps_nostock を環境変数で上書き可能
if os.name == "nt":
    default_dir = Path("D:/apps_nostock/logs")
else:
    default_dir = Path("/opt/apps_nostock")

BASE_DIR = os.environ.get("WORKER_STATUS_DIR", default_dir)

# 「この秒数更新がなければ死んだ」とみなす
# → workerが止まってもすぐ再起動しないための猶予
TIMEOUT_SEC = 600  # 10分

# 常に維持したいworker数
TARGET_WORKERS = 2

# ログフォルダ（watchdog自体のprint用）
LOG_DIR = BASE_DIR
os.makedirs(LOG_DIR, exist_ok=True)


# =========================
# worker起動管理
# =========================
def count_process():
    cnt = 0
    for p in psutil.process_iter(['cmdline']):
        try:
            cmdline = p.info['cmdline']
            if not cmdline:
                continue

            # 完全一致判定
            if (
                len(cmdline) >= 3 and
                cmdline[-2] == "-m" and
                cmdline[-1] == "apps.inventory.scrape_worker_new"
            ):
                cnt += 1

        except:
            pass

    return cnt

def ensure_worker():
    """
    現在のworker数（＝statusファイル数）を見て、
    足りなければ起動する
    """

    running = count_process()
    need = TARGET_WORKERS - running

    print(f"[CHECK] running={running} need={need}")

    if running < TARGET_WORKERS:
        print(f"[SPAWN] running={running} need={need}")

        for _ in range(need):
            log_path = os.path.join(LOG_DIR, f"worker_{int(time.time())}.log")
            f = open(log_path, "a", encoding="utf-8")

            subprocess.Popen(
                [sys.executable, "-u", "-m", "apps.inventory.scrape_worker_new"],
                stdout=f,
                stderr=f
            )

            time.sleep(1)


# =========================
# 停止workerの検知と回収
# =========================
def process_file(path):
    """
    1つの worker_status ファイルを処理

    やっていること：
    1. 最終更新時間を確認
    2. TIMEOUT超えてたら「死んだ」と判断
    3. DBを pending2 に戻す
    4. プロセスをkill
    5. statusファイル削除
    """
    print(f"[CHECK FILE] {path}", flush=True)

    try:
        # 最終更新時刻
        mtime = os.path.getmtime(path)

        # 今との差分（秒）
        age = time.time() - mtime

        # まだ生きてると判断（更新されてる）
        if age < TIMEOUT_SEC:
            return

        # =========================
        # statusファイル読み込み
        # =========================
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()

        # key=value を dict化
        data = {}
        for line in lines:
            if "=" in line:
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()

        pid = data.get("pid")
        job_id = data.get("job_id")
        page = data.get("page")

        print(f"[DETECT] pid={pid} job={job_id} page={page}")

        # =========================
        # DB更新（jobをやり直し状態に戻す）
        # =========================
        if job_id and page:
            job_id = int(job_id)
            page = int(page)

            conn = get_sql_server_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE trx.scrape_job
                SET current_page = ?, 
                    status = 'pending2',      -- 再実行待ち
                    worker_name = NULL,
                    locked_at = NULL
                WHERE job_id = ?
            """, page, job_id)

            conn.commit()
            conn.close()

            print(f"[DB] job {job_id} -> pending2 (page={page})")

        # =========================
        # プロセスkill
        # =========================
        if pid:
            if platform.system() == "Windows":
                os.system(f"taskkill /F /PID {pid} >nul 2>&1")
            else:
                os.system(f"kill -9 {pid} 2>/dev/null")

        # =========================
        # statusファイル削除
        # =========================
        if job_id and page:
            os.remove(path)

        print(f"[CLEAN] {path}")

    except Exception as e:
        print(f"[ERROR] {e}")


# =========================
# メインループ
# =========================
def main():
    print("[WATCHDOG START]")

    while True:
        # 現在の全worker_statusファイル取得
        files = glob.glob(os.path.join(BASE_DIR, "worker_status_*.txt"))
        print(f"[LOOP] worker_files={len(files)}", flush=True)

        # 1つずつチェック（死んでるか確認）
        for path in files:
            process_file(path)

        # 足りないworkerを起動
        ensure_worker()

        # 次のチェックまで待機
        time.sleep(10)


# =========================
# 実行エントリ
# =========================
if __name__ == "__main__":
    main()