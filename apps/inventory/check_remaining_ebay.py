# filename: stock_checker.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import random
import re
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from selenium import webdriver  # 型注釈用
from playwright.sync_api import sync_playwright

# ===== プロジェクトルートを sys.path に追加（最初にやる）=====
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ===== その後で apps を import =====
from apps.adapters.mercari_item_status import (
    Status,
    detect_status_from_mercari,
    detect_status_from_mercari_shops,
    handle_listing_delete,
    handle_listing_price_update,
)
from apps.adapters.mercari_scraper import build_driver
from apps.common.utils import get_sql_server_connection, compute_start_price_usd

# ===== UTF-8 出力の強制（絵文字/日本語の安全化） =====
if os.name == "nt" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
        sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
    except Exception:
        pass

# ===== 軽量ロガー & セーフクローズ =====
from datetime import datetime
def log_ctx(msg: str) -> None:
    """時刻付きログ（標準出力）。エンコード例外は潰す。"""
    try:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")
    except Exception:
        try:
            print(str(msg))
        except Exception:
            pass

def safe_quit(driver: Optional[webdriver.Chrome]) -> None:
    """Selenium driver を安全終了"""
    if driver is None:
        return
    try:
        driver.quit()
    except Exception:
        pass

# ===== パス設定 & インポート =====
from apps.common.utils import get_sql_server_connection, compute_start_price_usd
from apps.adapters.mercari_scraper import build_driver

# ===================== 設定 =====================
TEST_MODE =False
TEST_URLS = ["m29108294683"]

HEADLESS = True

RATE = {
    "detail": (2.5, 5.0),
    "cooldown_every": 60,
    "cooldown_sleep": (45, 90),
    "retry_waits": [1.0, 2.0, 4.0],
}


# ===================== ユーティリティ =====================
def human_sleep(a: float, b: float):
    time.sleep(random.uniform(a, b))

def get_status(page, driver: webdriver.Chrome, url: str) -> tuple[Status, Optional[int]]:
    """
    page (Playwright) と driver (Selenium) 両方を受け取れるように拡張
    """
    host_path = re.sub(r"^https?://", "", url)
    
    # --- 1. メルカリ（通常）の場合：Playwrightのpageを使って高速APIキャプチャ ---
    if "mercari.com" in host_path and "/shops/product/" not in host_path:
        # 修正：pageオブジェクトを渡す
        return detect_status_from_mercari(page, url)
    
    # --- 2. メルカリShopsの場合：従来通り Selenium を使用する ---
    driver.get(url)
    if "/shops/product/" in host_path:
        return detect_status_from_mercari_shops(driver)
    
    return "判定不可", None

# ===================== Price Sync I/O（vendor_item） =====================
def get_vendor_item_price(conn, vendor_name: str, sku: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT price
              FROM [trx].[vendor_item]
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (vendor_name, sku))
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

def update_vendor_item_price_and_status(conn, vendor_name: str, sku: str,
                                        price_jpy: Optional[int], status: str) -> None:
    """price_jpy が None のときは status だけ更新"""
    if price_jpy is None:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE [trx].[vendor_item]
                   SET status = ?, last_checked_at = SYSDATETIME()
                 WHERE vendor_name = ? AND vendor_item_id = ?
            """, (status, vendor_name, sku))
        conn.commit()
        return

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE [trx].[vendor_item]
               SET status = ?, price = ?, last_checked_at = SYSDATETIME()
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (status, price_jpy, vendor_name, sku))
    conn.commit()

def _is_transient_inventory_error(resp: dict | None) -> bool:
    if not resp or resp.get("success"):
        return False
    raw = resp.get("raw") or {}
    errors = ((raw.get("putOffer") or {}).get("errors") or []) or raw.get("errors") or []
    msgs = " ".join(str(e.get("message","")) for e in errors if isinstance(e, dict)).lower()
    codes = {int(e.get("errorId")) for e in errors if isinstance(e, dict) and str(e.get("errorId","")).isdigit()}
    return (25001 in codes) or ("internal error" in msgs)

def build_mercari_url(vendor_name: str, sku: str) -> str:
    if vendor_name == "メルカリshops":
        return f"https://jp.mercari.com/shops/product/{sku}"
    return f"https://jp.mercari.com/item/{sku}"



def exists_remaining_target(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT TOP (1) 1
            FROM trx.listings AS l
            INNER JOIN trx.vendor_item AS v
                ON v.vendor_name = l.vendor_name
               AND v.vendor_item_id = l.vendor_item_id
            WHERE l.is_deleted = 0
              AND v.vendor_name IN (N'メルカリ', N'メルカリshops')
              AND v.remaining_check_at IS NULL -- 完了していない
              AND (
                  v.remaining_check_lock IS NULL -- 未着手
                  OR v.remaining_check_lock < DATEADD(MINUTE, -15, SYSDATETIME()) -- 15分以上放置
              )
        """)
        return cur.fetchone() is not None

def pull_one_remaining_target(conn, worker_name: str):
    sql = """
        ;WITH cte AS (
            SELECT TOP (1)
                v.vendor_name,
                v.vendor_item_id,
                l.account,
                l.listing_id,
                v.preset,
                p.mode,
                p.low_usd_target,
                p.high_usd_target
            FROM trx.vendor_item AS v WITH (UPDLOCK, ROWLOCK, READPAST)
            INNER JOIN trx.listings AS l
                ON l.vendor_name    = v.vendor_name
               AND l.vendor_item_id = v.vendor_item_id
            INNER JOIN mst.v_presets AS p
                ON p.preset = v.preset
            WHERE
                l.is_deleted = 0
                AND v.vendor_name IN (N'メルカリ', N'メルカリshops')
                AND (v.status IS NULL OR LTRIM(RTRIM(v.status)) = N'')                               
                AND v.remaining_check_at IS NULL -- 完了していない
                AND (
                    v.remaining_check_lock IS NULL -- 未着手
                    OR v.remaining_check_lock < DATEADD(MINUTE, -5, SYSDATETIME()) -- 15分以上放置
                )
            ORDER BY v.remaining_check_lock ASC -- 古いロックから優先
        )
        UPDATE v
        SET
            remaining_check_by = ?,
            remaining_check_lock = SYSDATETIME() -- lockに開始時刻を入れる
        OUTPUT
            inserted.vendor_name,
            inserted.vendor_item_id,
            cte.account,
            cte.listing_id,
            cte.preset,
            cte.mode,
            cte.low_usd_target,
            cte.high_usd_target
        FROM trx.vendor_item AS v
        INNER JOIN cte
            ON v.vendor_name     = cte.vendor_name
           AND v.vendor_item_id  = cte.vendor_item_id;
    """
    cur = conn.cursor()
    cur.execute(sql, (worker_name,))
    row = cur.fetchone()
    conn.commit()

    if not row:
        return None

    return {
        "vendor_name": row[0],
        "vendor_item_id": row[1],
        "account": row[2],
        "listing_id": row[3],
        "preset": row[4],
        "mode": row[5],
        "low_usd_target": float(row[6]),
        "high_usd_target": float(row[7]),
    }

def count_total_remaining(conn):
    """DB全体で、まだ処理が必要な件数が何件あるかを確認する"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM trx.listings AS l
            INNER JOIN trx.vendor_item AS v
                ON v.vendor_name = l.vendor_name
               AND v.vendor_item_id = l.vendor_item_id
            WHERE l.is_deleted = 0
              AND v.vendor_name IN (N'メルカリ', N'メルカリshops')
              AND (v.status IS NULL OR LTRIM(RTRIM(v.status)) = N'')
              AND v.remaining_check_at IS NULL
        """)
        return cur.fetchone()[0]

def run_remaining_worker(worker_name: str):
    print(f"ver BATCH_MODE start (Worker: {worker_name})")

    # --- バッチ設定 ---
    MAX_PER_RUN = 100  # 100件処理したら一度終了してShellに再起動させる
    processed_count = 0

    pull_conn = None
    work_conn = None
    driver = None

    try:
        # Selenium (Shops用) と Playwright (通常用) 両方を準備
        driver = build_driver()
        pull_conn = get_sql_server_connection()
        work_conn = get_sql_server_connection()

        with sync_playwright() as p:
            # Playwrightブラウザを1回だけ起動
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            # 画像やCSSを遮断（バッチ内全ページに適用）
            page.route("**/*", lambda route: 
                route.abort() if route.request.resource_type in ["image", "media", "font", "stylesheet"] 
                else route.continue_()
            )

            while processed_count < MAX_PER_RUN:
                row = pull_one_remaining_target(pull_conn, worker_name)

                if not row:
                    total_left = count_total_remaining(pull_conn)
                    if total_left > 0:
                        print(f"[RETRY] DB says {total_left} items left. Lock conflict? Wait 10s...")
                        time.sleep(10)
                        continue 
                    else:
                        print("[INFO] DB is empty. All tasks finished normally.")
                        sys.exit(0) # 完全に仕事がない時だけ exit 0 (Shellのループが止まる)

                print(f"\n[INFO] batch processing {processed_count + 1}/{MAX_PER_RUN} ...")
                
                # 実処理を実行（page と driver の両方を渡す）
                process_status_and_sync(work_conn, page, driver, row, worker_name)
                
                processed_count += 1
                # サーバー負荷軽減のための短い待機
                time.sleep(random.uniform(1.0, 2.0))

            # MAX_PER_RUN に達したら exit 1 で終了
            print(f"[INFO] Reached {MAX_PER_RUN} items. Restarting for memory refresh...")
            sys.exit(1) # Shellスクリプトがこれを検知して pkill & 再起動する

    finally:
        safe_quit(driver)
        if pull_conn: pull_conn.close()
        if work_conn: work_conn.close()


def process_status_and_sync(
    conn,
    page,   # Playwright用に追加
    driver, # Selenium用
    row: dict,
    worker_name: str,
):
    vendor_name = row["vendor_name"]
    sku = row["vendor_item_id"]
    url = build_mercari_url(vendor_name, sku)
   
    # =====================
    # 1. Mercari 状態取得
    # =====================
    status, price_jpy = get_status(page, driver, url)
    print(f"[STATUS] {url} -> {status} (price_jpy={price_jpy})")
    
    # =====================
    # ★ 判定不可は即終了（確定しない）
    # =====================
    if status == "判定不可":
        print(f"[INFO] 判定不可のためスキップ sku={sku}")
        return  # remaining_check_at を入れない

    # =====================
    # 2. vendor_item 現在価格取得
    # =====================
    old_price = get_vendor_item_price(conn, vendor_name, sku)

    # =====================
    # 3. 販売中 & 価格取得成功
    # =====================
    if status == "販売中" and price_jpy is not None:

        # ---- 価格変更あり ----
        if old_price is None or old_price != price_jpy:
            new_price_usd = compute_start_price_usd(
                price_jpy,
                row["mode"],
                row["low_usd_target"],
                row["high_usd_target"],
            )

            # =====================
            # 3-A. レンジ外 → eBay削除
            # =====================
            if new_price_usd is None:
                print(
                    f"[PRICE] {sku}: {old_price} -> {price_jpy} JPY / レンジ外 → eBay終了",
                    flush=True,
                )

                handle_listing_delete(
                    conn,
                    sku,
                    vendor_name,
                )

            # =====================
            # 3-B. レンジ内 → eBay価格改定
            # =====================
            else:
                handle_listing_price_update(
                    conn,
                    sku,
                    vendor_name,
                    new_price_usd,
                )


        # ---- vendor_item 更新（価格あり）----
        update_vendor_item_price_and_status(
            conn,
            vendor_name,
            sku,
            price_jpy,
            status,
        )

    # =====================
    # 4. 販売中でない
    # =====================
    else:
        update_vendor_item_price_and_status(
            conn,
            vendor_name,
            sku,
            None,
            status,
        )


    # =====================
    # 5. 終了系ステータス → eBay削除
    # =====================
    if status in {"削除", "オークション", "売り切れ", "公開停止"}:
        print(f"[FINAL_DELETE] sku={sku} status={status}", flush=True)

        handle_listing_delete(
            conn,
            sku,
            vendor_name,
        )

    # =====================
    # ★ 6. remaining 確定 (関数の最後に配置)
    # =====================
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trx.vendor_item
            SET remaining_check_at = SYSDATETIME() -- ここで完了時刻を打つ
            WHERE vendor_name = ? AND vendor_item_id = ?
        """, (row["vendor_name"], row["vendor_item_id"]))

    conn.commit()

import socket

def get_processing_by():
    return os.environ.get("WORKER_NAME", socket.gethostname())

if __name__ == "__main__":
    worker_name = get_processing_by()
    run_remaining_worker(worker_name)
