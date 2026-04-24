# D:\apps_nostock\apps\inventory\watchdog.py

import os
import time
import glob
import pyodbc
import subprocess
from datetime import datetime
from apps.common.utils import get_sql_server_connection
import platform

# =========================
# 設定
# =========================
BASE_DIR = os.environ.get("WORKER_STATUS_DIR", "D:/apps_nostock")
TIMEOUT_SEC = 600 # 600sec=10miin
TARGET_WORKERS = 1
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def ensure_worker():
    files = glob.glob(os.path.join(BASE_DIR, "worker_status_*.txt"))
    running = len(files)

    if running < TARGET_WORKERS:
        need = TARGET_WORKERS - running
        print(f"[SPAWN] running={running} need={need}")

        for _ in range(need):
            subprocess.Popen(
                ["python", "-u", "-m", "apps.inventory.scrape_worker_new"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            time.sleep(1)

def process_file(path):
    try:
        mtime = os.path.getmtime(path)
        age = time.time() - mtime # mtime:ファイルの更新時刻　age:そこからの経過

        if age < TIMEOUT_SEC:
            return

        # ファイル読む
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()

        data = {}
        for line in lines:
            if "=" in line:
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()

        pid = data.get("pid")
        job_id = data.get("job_id")
        page = data.get("page")

        print(f"[DETECT] pid={pid} job={job_id} page={page}")

        # DB更新（pending2）
        if job_id and page:
            job_id = int(job_id)
            page = int(page)
            
            conn = get_sql_server_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE trx.scrape_job
                SET current_page = ?, status = 'pending2', worker_name = NULL, locked_at = NULL
                WHERE job_id = ?
            """, int(page), int(job_id))

            conn.commit()
            conn.close()

            print(f"[DB] job {job_id} -> pending2 (page={page})")

        # プロセスkill
        if pid:
            if platform.system() == "Windows":
                os.system(f"taskkill /F /PID {pid} >nul 2>&1")
            else:
                os.system(f"kill -9 {pid} 2>/dev/null")
        
        # ファイル削除
        if job_id and page:
            os.remove(path)
        print(f"[CLEAN] {path}")

    except Exception as e:
        print(f"[ERROR] {e}")

def main():
    print("[WATCHDOG START]")

    while True:
        files = glob.glob(os.path.join(BASE_DIR, "worker_status_*.txt"))

        for path in files:
            process_file(path)
        ensure_worker()  

        time.sleep(10)

if __name__ == "__main__":
    main()