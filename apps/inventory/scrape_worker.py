# scrape_worker.py
# 第一弾：fetch_active_ebay の scrape 本体を worker に持たせる

import os
import json
import time
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

import traceback
from datetime import datetime
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse

import pyodbc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from apps.common.utils import get_sql_server_connection
from apps.adapters.mercari_search import make_search_url
from apps.adapters.mercari_scraper import (
    build_driver,
    safe_quit,
    scroll_until_stagnant_collect_items,
    scroll_until_stagnant_collect_shops,
)

# =========================
# 設定
# =========================
WORKER_NAME = os.environ.get("WORKER_NAME", "pc-main")
POLL_SEC = 2
N = 1   # ★ 今回は 1 job ずつ

# =========================
# SQL
# =========================
SQL_PICK_JOBS = f"""
;WITH cte AS (
    SELECT TOP ({N}) *
    FROM trx.scrape_job WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE status = 'pending'
    ORDER BY created_at, job_id
)
UPDATE cte
SET
    status = 'running',
    worker_name = ?,
    locked_at = SYSDATETIME(),
    error_message = NULL
OUTPUT
    inserted.job_id,
    inserted.job_kind,
    inserted.job_payload;
"""

SQL_MARK_DONE = """
UPDATE trx.scrape_job
SET
    status = 'done',
    finished_at = SYSDATETIME()
WHERE job_id = ?;
"""

SQL_MARK_ERROR = """
UPDATE trx.scrape_job
SET
    status = 'error',
    finished_at = SYSDATETIME(),
    error_message = ?
WHERE job_id = ?;
"""

# =========================
# util
# =========================
def add_or_replace_query(url: str, **params) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = str(v)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def page_url(base_url: str, idx_zero_based: int) -> str:
    return base_url if idx_zero_based == 0 else add_or_replace_query(
        base_url, page_token=f"v1:{idx_zero_based}"
    )

# ============================================================
# ★ fetch_active_ebay scrape 本体（1 preset 分）
# ============================================================
def run_fetch_active_ebay(payload: dict):
    """
    payload 例:
    {
      "preset": "...",
      "vendor_name": "...",
      "mode": "...",
      "brand_id": 123,
      "category_id": 456,
      "low_usd_target": 50,
      "high_usd_target": 300
    }
    """

    preset = payload["preset"]
    vendor_name = payload["vendor_name"]

    print(f"[SCRAPE START] preset={preset} vendor={vendor_name}", flush=True)

    conn = None
    driver = None
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        conn = get_sql_server_connection()
        driver = build_driver()

        base_url = make_search_url(
            vendor_name=vendor_name,
            brand_id=payload["brand_id"],
            category_id=payload["category_id"],
            status="on_sale",
            mode=payload["mode"],
            low_usd_target=payload["low_usd_target"],
            high_usd_target=payload["high_usd_target"],
        )

        print(f"🔍 {base_url}", flush=True)

        page_idx = 0
        while True:
            url = page_url(base_url, page_idx)
            print(f"[PAGE] {page_idx+1} {url}", flush=True)

            driver.get(url)
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            print("[DEBUG] before get items")
            if vendor_name == "メルカリshops":
                items = scroll_until_stagnant_collect_shops(driver, pause=0.6)
            else:
                items = scroll_until_stagnant_collect_items(driver, pause=0.6)

            print(
                f"[PAGE {page_idx+1}] "
                f"items={len(items)} "
                f"sample={items[:2]}",
                flush=True
            )


            if not items:
                break

            # ★ 第一弾では DB upsert / price 処理はしない
            #   → scrape が安定したら戻す

            page_idx += 1
            time.sleep(1)

    finally:
        if driver:
            safe_quit(driver)
        if conn:
            conn.close()

    print(f"[SCRAPE END] preset={preset}", flush=True)

# =========================
# Worker main loop
# =========================
def main():
    print(f"[WORKER START] {WORKER_NAME}", flush=True)

    conn = get_sql_server_connection()
    conn.autocommit = False

    while True:
        try:
            cur = conn.cursor()
            cur.execute(SQL_PICK_JOBS, WORKER_NAME)
            jobs = cur.fetchall()
            conn.commit()
        except Exception:
            conn.rollback()
            traceback.print_exc()
            time.sleep(POLL_SEC)
            continue

        if not jobs:
            time.sleep(POLL_SEC)
            continue

        for job_id, job_kind, job_payload in jobs:
            print(f"[JOB START] id={job_id} kind={job_kind}", flush=True)
            try:
                payload = json.loads(job_payload)

                if job_kind == "fetch_active_ebay":
                    run_fetch_active_ebay(payload)
                else:
                    raise ValueError(f"unknown job_kind: {job_kind}")

                cur.execute(SQL_MARK_DONE, job_id)
                conn.commit()
                print(f"[JOB DONE] id={job_id}", flush=True)

            except Exception:
                err = traceback.format_exc()
                print(err, flush=True)
                cur.execute(SQL_MARK_ERROR, err[-4000:], job_id)
                conn.commit()

            if os.environ.get("ONESHOT") == "1": 
                return

if __name__ == "__main__":
    main()
