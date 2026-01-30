# =========================
# Standard library
# =========================
import os
import sys
import json
import time
import random
import traceback
import socket
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from datetime import datetime, timezone, timedelta

# =========================
# Path setup
# =========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

# =========================
# Third-party
# =========================
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# Local application modules
# =========================
from apps.common.utils import (
    get_sql_server_connection,
    compute_start_price_usd,
)
from apps.adapters.mercari_search import make_search_url
from apps.adapters.mercari_scraper import (
    build_driver,
    safe_quit,
    scroll_until_stagnant_collect_items,
    scroll_until_stagnant_collect_shops,
)
from apps.adapters.ebay_api import (
    delete_item_from_ebay,
    update_ebay_price,
)

# =========================
# 設定
# =========================
def get_worker_name() -> str:
    # 明示指定があれば最優先
    name = os.environ.get("WORKER_NAME")
    if name:
        return name

    # なければ OS の hostname
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-worker"

WORKER_NAME = get_worker_name()


JST = timezone(timedelta(hours=9))
def now_jst():
    return datetime.now(JST)



POLL_SEC = 2
N = 1   # ★ 今回は 1 job ずつ

NO_RESULT_TEXT = "出品された商品がありません"
SIMULATE = (os.environ.get("SIMULATE") == "1")  # 本番は未設定/0

# --- debug toggles (temporary) ---
EXIT_AFTER_PRICE_UPDATE = False
EXIT_AFTER_DELETE = False

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
    locked_at = ?,
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
    finished_at = ?
WHERE job_id = ?;
"""

SQL_MARK_ERROR = """
UPDATE trx.scrape_job
SET
    status = 'error',
    finished_at = ?,
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

def reset_vendor_item_status_for_active_skus(conn):
    """listings に存在する SKU の vendor_item.status を NULL クリア（vendor_nameも一致させる）"""
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE vi
               SET vi.[status] = NULL
            FROM [trx].[vendor_item] AS vi
            INNER JOIN [trx].[listings] AS l
                ON vi.[vendor_name] = l.[vendor_name]
               AND vi.[vendor_item_id] = l.[vendor_item_id]
        """)
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
    print("[INIT] status cleared on vendor_item joined with listings", flush=True)

def has_no_results_banner(driver) -> bool:
    try:
        txt = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        return NO_RESULT_TEXT in txt
    except Exception:
        return False

# =========================
# listings / vendor_item helpers
# =========================
def get_listing_core_by_sku(
    conn,
    vendor_item_id: str,
    vendor_name: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    cur = conn.cursor()
    try:
        if vendor_name:
            cur.execute("""
                SELECT listing_id, account, vendor_name
                  FROM [trx].[listings]
                 WHERE vendor_item_id = ?
                   AND vendor_name = ?
            """, (vendor_item_id, vendor_name))
        else:
            cur.execute("""
                SELECT listing_id, account, vendor_name
                  FROM [trx].[listings]
                 WHERE vendor_item_id = ?
            """, (vendor_item_id,))
        row = cur.fetchone()
        if row:
            return tuple(str(r).strip() if r is not None else None for r in row)  # type: ignore
        return (None, None, None)
    finally:
        try:
            cur.close()
        except Exception:
            pass

def delete_listing_by_itemid(conn, ebay_item_id: str, account: str, vendor_name: str):
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM [trx].[listings]
             WHERE listing_id = ? AND account = ? AND vendor_name = ?
        """, (ebay_item_id, account, vendor_name))
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass

def get_vendor_item_prices_batch(conn, vendor_name: str, vendor_item_ids: List[str]) -> Dict[str, Optional[int]]:
    if not vendor_item_ids:
        return {}

    placeholders = ",".join("?" for _ in vendor_item_ids)
    sql = f"""
        SELECT vendor_item_id, price
        FROM [trx].[vendor_item]
        WHERE vendor_name = ? AND vendor_item_id IN ({placeholders})
    """
    params = [vendor_name] + vendor_item_ids
    out: Dict[str, Optional[int]] = {}

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        for vid, price in cur.fetchall():
            out[str(vid)] = int(price) if price is not None else None
    finally:
        try:
            cur.close()
        except Exception:
            pass

    for v in vendor_item_ids:
        out.setdefault(v, None)
    return out

# =========================
# eBay side-effects
# =========================
def _is_transient_inventory_error(resp: Dict[str, Any]) -> bool:
    if not resp or resp.get("success"):
        return False
    raw = resp.get("raw") or {}
    errors = ((raw.get("putOffer") or {}).get("errors") or []) or raw.get("errors") or []
    msgs = " ".join(str(e.get("message","")) for e in errors if isinstance(e, dict)).lower()
    codes = {int(e.get("errorId")) for e in errors if isinstance(e, dict) and str(e.get("errorId","")).isdigit()}
    return (25001 in codes) or ("internal error" in msgs)

def handle_price_change_side_effects(
    conn,
    sku: str,
    vendor_name: str,
    old_price: int,
    new_price_jpy: int,
    *,
    mode: str,
    low_usd_target: float,
    high_usd_target: float,
    simulate: bool,
):
    ebay_item_id, account, listing_vendor = get_listing_core_by_sku(conn, sku, vendor_name=vendor_name)

    # eBay出品がないSKUは副作用なし
    if not ebay_item_id:
        return

    usd = compute_start_price_usd(new_price_jpy, mode, low_usd_target, high_usd_target)

    if usd is None:
        print(
            f"[PRICE] {sku}: {old_price} -> {new_price_jpy} JPY / 目標外(usd=None) mode={mode} {low_usd_target}-{high_usd_target}",
            flush=True
        )
        if simulate:
            print(f"[SIMULATE DELETE] sku={sku} item_id={ebay_item_id}", flush=True)
            return

        res = delete_item_from_ebay(account, ebay_item_id)
        ok = bool(res.get("success")) or res.get("note") in {"already_deleted", "already_ended"}
        if ok:
            delete_listing_by_itemid(conn, ebay_item_id, account, listing_vendor or vendor_name)
            if EXIT_AFTER_DELETE:
                sys.exit(0)
        else:
            print(f"[WARN] eBay削除失敗 itemId={ebay_item_id} resp={res}", flush=True)
        return

    print(
        f"【価格変更】 {sku}: {old_price} -> {new_price_jpy} JPY / USD {usd}  mode={mode} {low_usd_target}-{high_usd_target}",
        flush=True
    )

    if simulate:
        print(f"[SIMULATE UPDATE] sku={sku} item_id={ebay_item_id} USD={usd}", flush=True)
        return

    did_update_ebay = False
    resp: Optional[Dict[str, Any]] = None
    for wait in [0, 2, 6, 15]:
        if wait:
            time.sleep(wait)
        resp = update_ebay_price(account, ebay_item_id, usd, sku=sku, debug=True)
        if resp and resp.get("success"):
            did_update_ebay = True
            break
        if not _is_transient_inventory_error(resp or {}):
            break

    if not did_update_ebay:
        print(f"[WARN] eBay価格更新失敗 resp={resp}", flush=True)

    if EXIT_AFTER_PRICE_UPDATE:
        sys.exit(0)

# =========================
# vendor_item UPSERT
# =========================
def upsert_vendor_items(conn, rows: List[Dict[str, Any]], now) -> int:
    if not rows:
        return 0

    sql = """
MERGE [trx].[vendor_item] AS T
USING (SELECT ? AS vendor_name, ? AS vendor_item_id) AS S
ON (T.[vendor_name] = S.vendor_name AND T.[vendor_item_id] = S.vendor_item_id)
WHEN MATCHED THEN
  UPDATE SET
    T.[status]          = ?,
    T.[preset]          = ?,
    T.[title_jp]        = ?,
    T.[vendor_page]     = ?,
    T.[last_checked_at] = ?,
    T.[prev_price] = CASE
                       WHEN (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL)
                             OR (T.[price] IS NOT NULL AND ? IS NULL))
                         THEN T.[price]
                       ELSE T.[prev_price]
                     END,
    T.[price] = COALESCE(?, T.[price]),
    T.[出品状況] = CASE
                     WHEN ISNULL(T.[出品状況], N'') = N'古い更新'
                      AND (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL)
                           OR (T.[price] IS NOT NULL AND ? IS NULL))
                       THEN NULL
                     ELSE T.[出品状況]
                   END,
    T.[出品状況詳細] = CASE
                         WHEN ISNULL(T.[出品状況], N'') = N'古い更新'
                          AND (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL)
                               OR (T.[price] IS NOT NULL AND ? IS NULL))
                           THEN NULL
                         ELSE T.[出品状況詳細]
                       END,
    T.[last_ng_at] = CASE
                       WHEN ISNULL(T.[出品状況], N'') = N'古い更新'
                        AND (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL)
                             OR (T.[price] IS NOT NULL AND ? IS NULL))
                         THEN NULL
                       ELSE T.[last_ng_at]
                     END
WHEN NOT MATCHED THEN
  INSERT (
      [vendor_name], [vendor_item_id], [status], [preset], [title_jp],
      [vendor_page], [created_at], [last_checked_at],
      [price], [prev_price]
  )
  VALUES (
      ?, ?, ?, ?, ?,
      ?, ?, ?,
      ?, NULL
  );
"""

    cur = conn.cursor()
    try:
        for r in rows:
            params = (
                # USING
                r["vendor_name"], r["vendor_item_id"],

                # UPDATE
                r["status"], r["preset"], r["title_jp"], r["vendor_page"],
                now,  # ← ★ last_checked_at

                # prev_price 判定
                r["price"], r["price"], r["price"],
                r["price"],

                # 出品状況クリア
                r["price"], r["price"], r["price"],
                r["price"], r["price"], r["price"],
                r["price"], r["price"], r["price"],

                # INSERT
                r["vendor_name"], r["vendor_item_id"],
                r["status"], r["preset"], r["title_jp"],
                r["vendor_page"],
                now,  # ← ★ created_at
                now,  # ← ★ last_checked_at
                r["price"],
            )
            cur.execute(sql, params)

        conn.commit()
        return len(rows)
    finally:
        try:
            cur.close()
        except Exception:
            pass


# ============================================================
# fetch_active_ebay scrape 本体（1 preset 分）
# ============================================================
def run_fetch_active_ebay(payload: dict):
    preset = payload["preset"]
    vendor_name = payload["vendor_name"]
    mode = payload["mode"]
    low_usd_target = float(payload["low_usd_target"])
    high_usd_target = float(payload["high_usd_target"])

    print(f"[SCRAPE START] preset={preset} vendor={vendor_name} mode={mode}", flush=True)

    conn = None
    driver = None

    try:
        conn = get_sql_server_connection()
        driver = build_driver()

        base_url = make_search_url(
            vendor_name=vendor_name,
            brand_id=payload["brand_id"],
            category_id=payload["category_id"],
            status="on_sale",
            mode=mode,
            low_usd_target=low_usd_target,
            high_usd_target=high_usd_target,
        )
        print(f"🔍 {base_url}", flush=True)

        page_idx = 0
        while True:
            page_start = time.time()

            url = page_url(base_url, page_idx)
            print(f"[PAGE] {page_idx+1} {url}", flush=True)

            driver.get(url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            if has_no_results_banner(driver):
                break

            if vendor_name == "メルカリshops":
                items = scroll_until_stagnant_collect_shops(driver, pause=0.6)   # [(id,title,price),...]
            else:
                items = scroll_until_stagnant_collect_items(driver, pause=0.6)

            print(f"[PAGE {page_idx+1}] items={len(items)} sample={items[:2]}", flush=True)
            if not items:
                break

            # 旧価格をまとめて取得
            item_ids = [iid for iid, _, _ in items]
            old_price_map = get_vendor_item_prices_batch(conn, vendor_name, item_ids)

            # 価格変更の副作用
            cnt_skip = cnt_changed = cnt_unchanged = 0
            for iid, title, price in items:
                if price is None:
                    cnt_skip += 1
                    continue

                old_price = old_price_map.get(iid)
                if old_price is not None and old_price != price:
                    cnt_changed += 1
                    handle_price_change_side_effects(
                        conn,
                        iid,
                        vendor_name,
                        old_price,
                        price,
                        mode=mode,
                        low_usd_target=low_usd_target,
                        high_usd_target=high_usd_target,
                        simulate=SIMULATE,
                    )
                else:
                    cnt_unchanged += 1

            # UPSERT
            rows = [{
                "vendor_name": vendor_name,
                "vendor_item_id": iid,
                "status": "販売中",
                "preset": preset,
                "title_jp": title,
                "vendor_page": page_idx,
                "price": price,
            } for iid, title, price in items]

            now = now_jst()
            upsert_vendor_items(conn, rows, now)
            print(
                f"[PAGE {page_idx+1} RESULT] upserted={len(rows)} "
                f"skip={cnt_skip} changed={cnt_changed} unchanged={cnt_unchanged}",
                flush=True
            )

            # 次ページへ
            elapsed = time.time() - page_start
            TARGET = 35.0

            # NOTE: ここで 35秒に揃える設計なら、末尾の time.sleep(1) は通常不要。
            #       “最低でも+1秒”を意図してるならこのままでOK。
            if elapsed < TARGET:
                time.sleep((TARGET - elapsed) + random.uniform(0.0, 3.0))

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

    # job pick用の接続（長寿命）
    conn = get_sql_server_connection()
    conn.autocommit = False

    # INITは “別接続で1回だけ” が安全（長寿命connを汚さない）
    if os.environ.get("DO_INIT_CLEAR_STATUS", "1") == "1":
        init_conn = None
        try:
            init_conn = get_sql_server_connection()
            reset_vendor_item_status_for_active_skus(init_conn)
        except Exception:
            traceback.print_exc()
            raise
        finally:
            try:
                if init_conn:
                    init_conn.close()
            except Exception:
                pass

    while True:
        cur = None
        try:
            cur = conn.cursor()
            now = now_jst()
            cur.execute(SQL_PICK_JOBS, WORKER_NAME, now)
            jobs = cur.fetchall()
            conn.commit()
        except Exception:
            conn.rollback()
            traceback.print_exc()
            time.sleep(POLL_SEC)
            continue
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass

        if not jobs:
            time.sleep(POLL_SEC)
            continue

        for job_id, job_kind, job_payload in jobs:
            print(f"[JOB START] id={job_id} kind={job_kind}", flush=True)
            cur2 = None
            try:
                payload = json.loads(job_payload)

                if job_kind == "fetch_active_ebay":
                    run_fetch_active_ebay(payload)
                else:
                    raise ValueError(f"unknown job_kind: {job_kind}")

                cur2 = conn.cursor()
                now = now_jst()
                cur2.execute(SQL_MARK_DONE, now, job_id)
                conn.commit()
                print(f"[JOB DONE] id={job_id}", flush=True)

            except Exception:
                err = traceback.format_exc()
                print(err, flush=True)
                try:
                    cur2 = conn.cursor()
                    now = now_jst()
                    cur2.execute(SQL_MARK_ERROR, now, err[-4000:], job_id)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    traceback.print_exc()

            finally:
                try:
                    if cur2:
                        cur2.close()
                except Exception:
                    pass

            if os.environ.get("ONESHOT") == "1":
                return

if __name__ == "__main__":
    main()
