# filename: stock_checker.py
# -*- coding: utf-8 -*-

from __future__ import annotations
import json, re, time, random, argparse, sys, traceback, os
from typing import Literal, Optional, List, Dict

from selenium import webdriver  # 型注釈用

import sys
from pathlib import Path

# ===== プロジェクトルートを sys.path に追加（最初にやる）=====
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ===== その後で apps を import =====
from apps.adapters.mercari_item_status import (
    Status,
    detect_status_from_mercari,
    detect_status_from_mercari_shops,
)

# ===== ここで send_mail を import（重要） =====
from apps.common.utils import send_mail

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
from apps.adapters.ebay_api import delete_item_from_ebay, update_ebay_price
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

# ===================== DB I/O =====================
def is_account_excluded_for_sku(conn, vendor_item_id: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT a.is_excluded
            FROM trx.listings l
            JOIN mst.ebay_accounts a
              ON l.account = a.account
            WHERE l.vendor_item_id = ?
        """, (vendor_item_id,))
        rows = cur.fetchall()
        return any(r[0] for r in rows)
    finally:
        cur.close()


def load_mercari_targets_from_db(limit: Optional[int] = None) -> List[Dict[str, str]]:
    """
    listings と vendor_item を (vendor_name, vendor_item_id) でJOIN。
    vendor_item.status がブランク（NULL or 空文字）のものだけ対象。
    対象: メルカリshops / メルカリ 両方
    返却: {url, sku(=vendor_item_id), account, ebay_item_id(=listing_id), vendor_name}
    """
    sql = """
        SELECT
            l.listing_id,
            l.account,
            l.vendor_item_id,
            l.vendor_name
        FROM [trx].[listings] AS l
        INNER JOIN [trx].[vendor_item] AS v
            ON v.vendor_name    = l.vendor_name
           AND v.vendor_item_id = l.vendor_item_id
        WHERE l.vendor_name IN (N'メルカリshops', N'メルカリ')
          AND (v.status IS NULL OR LTRIM(RTRIM(v.status)) = N'')
        ORDER BY l.start_time DESC
    """

    conn = get_sql_server_connection()

    try:
        out: List[Dict[str, str]] = []
        with conn.cursor() as cur:
            cur.execute(sql)
            for row in cur:
                ebay_item_id   = str(row[0]).strip()
                account        = str(row[1]).strip()
                vendor_item_id = str(row[2]).strip()
                vendor_name    = str(row[3]).strip()

                # URL生成を vendor_name に応じて分岐
                if vendor_name == "メルカリshops":
                    url = f"https://jp.mercari.com/shops/product/{vendor_item_id}"
                else:
                    url = f"https://jp.mercari.com/item/{vendor_item_id}"

                out.append({
                    "url": url,
                    "sku": vendor_item_id,
                    "account": account,
                    "ebay_item_id": ebay_item_id,
                    "vendor_name": vendor_name,
                })
                if limit and len(out) >= limit:
                    break
        return out
    finally:
        conn.close()

def delete_ebay_listing_record(conn, ebay_item_id: str, account: str, vendor_name: str) -> None:
    """
    eBay 出品を論理削除する
    - trx.listings の record は削除しない
    - is_deleted / deleted_at を更新
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trx.listings
               SET is_deleted = 1,
                   deleted_at = SYSDATETIME()
             WHERE listing_id = ?
               AND account = ?
               AND vendor_name = ?
               AND is_deleted = 0
        """, (ebay_item_id, account, vendor_name))
    conn.commit()



def update_vendor_item_status(conn, vendor_name: str, sku: str, status: str) -> None:
    """ vendor_item の status を更新（sku=vendor_item_id） """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE [trx].[vendor_item]
               SET status = ?, last_checked_at = SYSDATETIME()
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (status, vendor_name, sku))
    conn.commit()

def get_status(driver: webdriver.Chrome, url: str) -> tuple[Status, Optional[int]]:
    driver.get(url)
    host_path = re.sub(r"^https?://", "", url)
    if "/shops/product/" in host_path:
        return detect_status_from_mercari_shops(driver)
    if "mercari.com" in host_path:
        return detect_status_from_mercari(driver)
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

def run_remaining_worker(worker_name: str):
    driver = None
    conn = None

    N = 5  # ★ 最大処理件数

    try:
        driver = build_driver()
        conn = get_sql_server_connection()

        for i in range(N):
            row = pull_one_remaining_target(conn, worker_name)
            if not row:
                print("[INFO] remaining queue empty")
                break

            print(f"\n[INFO] remaining processing {i+1}/{N} "
                  f"vendor={row['vendor_name']} sku={row['vendor_item_id']}")

            process_status_and_sync(
                conn,
                driver,
                row["vendor_name"],
                row["vendor_item_id"],
                worker_name,
            )

            time.sleep(random.uniform(2.0, 5.0))

        print("[INFO] remaining worker finished")

    finally:
        safe_quit(driver)
        if conn:
            conn.close()


def process_status_and_sync(
    conn,
    driver,
    vendor_name: str,
    sku: str,
    worker_name: str,
):
    """
    remaining チェック 1件分の実処理（確定版）

    方針:
    - 判定不可は一切確定しない（次回再処理）
    - 判定できた場合のみ vendor_item / eBay / listings を同期
    - 最後に remaining_check_at を確定
    """

    url = build_mercari_url(vendor_name, sku)

    # =====================
    # 1. Mercari 状態取得
    # =====================
    status, price_jpy = get_status(driver, url)
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
                "GA",
                450,
                1000,
            )

            # =====================
            # 3-A. レンジ外 → eBay削除
            # =====================
            if new_price_usd is None:
                print(f"[PRICE] {sku}: {old_price} -> {price_jpy} JPY / レンジ外 → eBay終了")

                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT listing_id, account
                          FROM trx.listings
                         WHERE vendor_item_id = ?
                           AND deleted_at IS NULL
                    """, (sku,))
                    rows = cur.fetchall()

                for ebay_item_id, account in rows:
                    if is_account_excluded_for_sku(conn, sku):
                        print(f"[SKIP DELETE] excluded account sku={sku}")
                        continue

                    print(f"[DEBUG][DELETE] account={account} listing_id={ebay_item_id}")
                    res = delete_item_from_ebay(account, ebay_item_id)

                    ok = bool(res.get("success")) or res.get("note") in {
                        "already_deleted",
                        "already_ended",
                    }

                    if ok:
                        delete_ebay_listing_record(conn, ebay_item_id, account, vendor_name)

                    else:
                        print(f"[WARN] eBay削除失敗 listingId={ebay_item_id} resp={res}")

            # =====================
            # 3-B. レンジ内 → eBay価格改定
            # =====================
            else:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT listing_id, account
                          FROM trx.listings
                         WHERE vendor_item_id = ?
                           AND deleted_at IS NULL
                    """, (sku,))
                    rows = cur.fetchall()

                for ebay_item_id, account in rows:
                    if is_account_excluded_for_sku(conn, sku):
                        print(f"[SKIP UPDATE] excluded account sku={sku}")
                        continue

                    print(f"[DEBUG][UPDATE] account={account} listing_id={ebay_item_id}")
                    resp = update_ebay_price(
                        account,
                        ebay_item_id,
                        new_price_usd,
                        sku=sku,
                        debug=True,
                    )

                    if not resp or not resp.get("success"):
                        print(f"[WARN] eBay価格更新失敗 listingId={ebay_item_id} resp={resp}")

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
        print(f"[DEBUG][FINAL_DELETE_CHECK] sku={sku} status={status}")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT listing_id, account
                  FROM trx.listings
                 WHERE vendor_item_id = ?
                   AND deleted_at IS NULL
            """, (sku,))
            rows = cur.fetchall()

            # === rows の中身を確認する print ===
            print(f"[DEBUG_DB] SKU:{sku} の検索結果 (deleted_at IS NULL):")
            if not rows:
                print(f"  -> ヒットなし（レコードが存在しないか、既に deleted_at に値が入っています）")
            else:
                for r in rows:
                    print(f"  -> listing_id: {r[0]}, account: {r[1]}")
            # ===============================



        for ebay_item_id, account in rows:
            if is_account_excluded_for_sku(conn, sku):
                print(f"[SKIP DELETE] excluded account sku={sku}")
                continue

            print(f"[DEBUG][CALL delete_item_from_ebay] account={account} listing_id={ebay_item_id}")
            res = delete_item_from_ebay(account, ebay_item_id)

            ok = bool(res.get("success")) or res.get("note") in {
                "already_deleted",
                "already_ended",
            }

            if ok:
                delete_ebay_listing_record(conn, ebay_item_id, account, vendor_name)
            else:
                print(f"[WARN] eBay削除失敗 listingId={ebay_item_id} resp={res}")

    # =====================
    # ★ 6. remaining 確定
    # =====================
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trx.vendor_item
               SET remaining_check_at = SYSDATETIME(),
                   remaining_check_by = ?
             WHERE vendor_item_id = ?
        """, (worker_name,  sku))

    conn.commit()

def pull_one_remaining_target(conn, worker_name: str):
    """
    出品中（is_deleted=false）かつ
    vendor_item.status 未確定 のものを 1件だけ確保
    """
    with conn.cursor() as cur:
        cur.execute("""
            ;WITH cte AS (
                SELECT TOP (1)
                    l.listing_id,
                    l.account,
                    l.vendor_name,
                    l.vendor_item_id
                FROM trx.listings AS l WITH (UPDLOCK, READPAST, ROWLOCK)
                INNER JOIN trx.vendor_item AS v
                    ON v.vendor_name    = l.vendor_name
                   AND v.vendor_item_id = l.vendor_item_id
                WHERE l.is_deleted = 0
                  AND l.vendor_name IN (N'メルカリ', N'メルカリshops')
                  AND (v.status IS NULL OR LTRIM(RTRIM(v.status)) = N'')
                  AND v.remaining_check_at IS NULL
                ORDER BY l.start_time DESC
            )
            UPDATE v
               SET remaining_check_by = ?,
                   remaining_check_at = SYSDATETIME()
            OUTPUT
                inserted.vendor_name,
                inserted.vendor_item_id,
                cte.account,
                cte.listing_id
            FROM trx.vendor_item AS v
            INNER JOIN cte
              ON v.vendor_name    = cte.vendor_name
             AND v.vendor_item_id = cte.vendor_item_id;
        """, (worker_name,))

        row = cur.fetchone()

    if not row:
        return None

    return {
        "vendor_name": row[0],
        "vendor_item_id": row[1],
        "account": row[2],
        "listing_id": row[3],
    }



import socket

def get_processing_by():
    return os.environ.get("WORKER_NAME", socket.gethostname())

if __name__ == "__main__":
    worker_name = get_processing_by()
    run_remaining_worker(worker_name)
