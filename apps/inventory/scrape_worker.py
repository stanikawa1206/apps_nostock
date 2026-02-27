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
import pyodbc
import urllib3

from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from datetime import datetime, timezone, timedelta
from apps.adapters.mercari_item_status import handle_listing_delete,handle_listing_price_update
# =========================
# Path setup
# =========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# =========================
# Third-party
# =========================
from selenium.common.exceptions import TimeoutException, WebDriverException

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# Local application modules
# =========================
from apps.common.utils import (
    get_sql_server_connection,
    compute_start_price_usd,
    build_driver,
)
from apps.adapters.mercari_search import make_search_url
from apps.adapters.mercari_scraper import (
    safe_quit,
    scroll_until_stagnant_collect_items,
    scroll_until_stagnant_collect_shops,
)
from apps.adapters.ebay_api import update_ebay_price

# =========================
# 設定
# =========================
def get_worker_name() -> str:
    name = os.environ.get("WORKER_NAME")
    if name:
        return name
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-worker"

WORKER_NAME = get_worker_name()

JST = timezone(timedelta(hours=9))
def now_jst():
    return datetime.now(JST).replace(tzinfo=None)

POLL_SEC = 2
N = 1   # ★ 1 job ずつ

NO_RESULT_TEXT = "出品された商品がありません"
SIMULATE = (os.environ.get("SIMULATE") == "1")  # 本番は未設定/0

# --- debug toggles (temporary) ---
EXIT_AFTER_PRICE_UPDATE = False
EXIT_AFTER_DELETE = False

# ★ 対策(8): renderer timeout のページリトライ回数
MAX_RENDER_RETRY_PER_PAGE = 2

# ★ 対策(10): swap警告を出すか（Linuxのみ）
CHECK_SWAP = (os.environ.get("CHECK_SWAP", "1") == "1")

MAX_PAGES = 2
PAUSE = 0.6
SIMULATE_DELETE = False


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
    finished_at = ?,
    fetched_pages = ?,
    fetched_items = ?
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
# 対策(10): swapチェック（Linuxのみ）
# =========================
def warn_if_no_swap():
    if os.name != "posix":
        return
    try:
        # /proc/swaps が空なら swapなし
        with open("/proc/swaps", "r", encoding="utf-8") as f:
            lines = f.read().strip().splitlines()
        if len(lines) <= 1:
            print("[WARN] swap が有効ではありません（VPSでrenderer timeoutが出やすい）", flush=True)
        else:
            print("[INFO] swap 有効", flush=True)
    except Exception:
        # 読めない環境もあるので黙る
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
) -> None:

    cursor = conn.cursor()

    # ─────────────────────────────
    # ① クリア前の状態取得
    # ─────────────────────────────
    select_sql = """
    SELECT 出品状況, 出品状況詳細, last_updated_str, last_ng_at
    FROM trx.vendor_item
    WHERE vendor_name = ?
    AND vendor_item_id = ?
    """
    cursor.execute(select_sql, vendor_name, sku)
    row = cursor.fetchone()

    if row is None:
        raise RuntimeError(f"vendor_item not found: {vendor_name} / {sku}")

    before_status, before_detail, before_updated, before_ng = row

    # ─────────────────────────────
    # ② 「○ヶ月前 / ○か月前 / 半年以上前」のときだけクリア
    # ─────────────────────────────
    if before_updated is not None:
        s = str(before_updated)

        if ("ヶ月前" in s) or ("か月前" in s) or ("半年以上前" in s):

            # 変更前状態を表示（デバッグ用）
            print(
                f"[CLEAR BEFORE] sku={sku} "
                f"出品状況={before_status} "
                f"出品状況詳細={before_detail} "
                f"last_updated_str={before_updated} "
                f"last_ng_at={before_ng}",
                flush=True,
            )

            update_sql = """
            UPDATE trx.vendor_item
            SET
                出品状況 = NULL,
                出品状況詳細 = NULL,
                last_updated_str = NULL,
                last_ng_at = NULL
            WHERE vendor_name = ?
            AND vendor_item_id = ?
            """
            cursor.execute(update_sql, vendor_name, sku)
            conn.commit()

            print(f"[CLEAR DONE] sku={sku}", flush=True)


    # ─────────────────────────────
    # ③ USDレンジ計算
    # ─────────────────────────────
    usd = compute_start_price_usd(
        new_price_jpy,
        mode,
        low_usd_target,
        high_usd_target,
    )

    # ─────────────────────────────
    # ④ 範囲外 → 削除
    # ─────────────────────────────
    if usd is None:
        print(
            f"[PRICE OUT] {sku}: {old_price} -> {new_price_jpy} JPY",
            flush=True,
        )
        handle_listing_delete(conn, sku, vendor_name)
        return

    # ─────────────────────────────
    # ⑤ 範囲内 → 価格更新
    # ─────────────────────────────
    print(
        f"[PRICE CHANGE] {sku}: "
        f"{old_price} -> {new_price_jpy} JPY / USD={usd} "
        f"(last_updated_str={before_updated})",
        flush=True,
    )


    handle_listing_price_update(conn, sku, vendor_name, usd)

    if EXIT_AFTER_PRICE_UPDATE:
        sys.exit(0)




# =========================
# vendor_item UPSERT
# =========================
def upsert_vendor_items(conn, rows: List[Dict[str, Any]], now) -> int:
    print(f"[UPSERT] begin rows={len(rows)} now={now}", flush=True)
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
                       WHEN (T.[price] <> ? OR
                            (T.[price] IS NULL AND ? IS NOT NULL) OR
                            (T.[price] IS NOT NULL AND ? IS NULL))
                         THEN T.[price]
                       ELSE T.[prev_price]
                     END,
    T.[price] = COALESCE(?, T.[price])

WHEN NOT MATCHED THEN
  INSERT (
      [vendor_name],
      [vendor_item_id],
      [status],
      [preset],
      [title_jp],
      [vendor_page],
      [created_at],
      [last_checked_at],
      [price],
      [prev_price]
  )
  VALUES (
      ?, ?, ?, ?, ?,
      ?, ?, ?,
      ?, NULL
  );
"""
    cursor = conn.cursor()

    for r in rows:
        params = (
            r["vendor_name"],
            r["vendor_item_id"],

            r["status"],
            r["preset"],
            r["title_jp"],
            r["vendor_page"],
            now,
            r["price"], r["price"], r["price"],
            r["price"],

            # insert
            r["vendor_name"],
            r["vendor_item_id"],
            r["status"],
            r["preset"],
            r["title_jp"],
            r["vendor_page"],
            now,
            now,
            r["price"],
        )

        cursor.execute(sql, params)

    conn.commit()
    print(f"[UPSERT] done rows={len(rows)}", flush=True)
    return len(rows)


# =========================
# 対策(8): renderer timeout判定
# =========================
def is_renderer_timeout(e: BaseException) -> bool:
    s = str(e).lower()
    return (
        "timed out receiving message from renderer" in s
        or "disconnected: unable to receive message from renderer" in s
        or "renderer" in s and "timeout" in s
    )

# ===== URLヘルパ =====
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
    # 1ページ目＝そのまま、それ以降は page_token=v1:{n}
    return base_url if idx_zero_based == 0 else add_or_replace_query(base_url, page_token=f"v1:{idx_zero_based}")


def has_no_results_banner(driver) -> bool:
    try:
        txt = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        return "出品された商品がありません" in txt
    except Exception:
        return False


# ===== DB I/O =====
def upsert_vendor_item(conn: pyodbc.Connection, vendor_name: str, item_id: str, title: str, page_num: int, preset: str, now_str: str):
    with conn.cursor() as cur:
        # 存在確認
        cur.execute("""
            SELECT COUNT(*)
              FROM [trx].[vendor_item]
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (vendor_name, item_id))
        exists = cur.fetchone()[0] > 0

        if not exists:
            cur.execute("""
                INSERT INTO [trx].[vendor_item]
                    (vendor_name, vendor_item_id, title_jp, created_at, last_checked_at,
                     vendor_page, status, preset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (vendor_name, item_id, title, now_str, now_str, page_num, "売り切れ", preset))
        else:
            cur.execute("""
                UPDATE [trx].[vendor_item]
                   SET last_checked_at = ?,
                       vendor_page     = ?,
                       status          = ?,
                       preset          = ?
                 WHERE vendor_name = ? AND vendor_item_id = ?
            """, (now_str, page_num, "売り切れ", preset, vendor_name, item_id))
    conn.commit()

def is_account_excluded(conn, account: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT is_excluded
              FROM mst.ebay_accounts
             WHERE account = ?
        """, (account,))
        row = cur.fetchone()
        return bool(row and row[0])
    finally:
        cur.close()


def run_fetch_sold_ebay(payload: dict) -> Tuple[int, int]:
    preset_name = payload["preset"]
    vendor_name = payload["vendor_name"]
    brand_id    = payload["brand_id"]
    category_id = payload["category_id"]
    mode        = payload.get("mode", "DDP")
    low_usd_target  = payload["low_usd_target"]
    high_usd_target = payload["high_usd_target"]

    print(
        f"[SCRAPE START][SOLD] preset='{preset_name}' "
        f"vendor='{vendor_name}' mode='{mode}'",
        flush=True
    )

    conn = None
    fetched_pages = 0
    fetched_items = 0

    try:
        conn = get_sql_server_connection()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        base_url = make_search_url(
            vendor_name=vendor_name,
            brand_id=brand_id,
            category_id=category_id,
            status="sold_out|trading",
            mode=mode,
            low_usd_target=low_usd_target,
            high_usd_target=high_usd_target,
        )
        print(f"🔍 Base URL: {base_url}", flush=True)

        page_idx = 0
        seen_ids: set[str] = set()

        while page_idx < MAX_PAGES:
            target_url = page_url(base_url, page_idx)
            print(f"[PAGE {page_idx+1}] GET {target_url}", flush=True)

            # 1ページごとにドライバーを生成（メモリリーク対策）
            driver = build_driver()
            try:
                # ページ読み込み
                driver.get(target_url)
                
                # body要素が出るまで待機
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # 「結果なし」バナーのチェック
                if has_no_results_banner(driver):
                    print(f"[PAGE {page_idx+1}] no-results banner detected. stopping.", flush=True)
                    break

                # スクロールとアイテム回収
                if vendor_name == "メルカリshops":
                    items = scroll_until_stagnant_collect_shops(driver, PAUSE)
                else:
                    items = scroll_until_stagnant_collect_items(driver, PAUSE)

                if not items:
                    print(f"[PAGE {page_idx+1}] No items found on page. stopping.", flush=True)
                    break

                print(f"[PAGE {page_idx+1}] Scraped items count: {len(items)}", flush=True)
                fetched_pages += 1

                # 重複排除と新規リスト作成
                new_items_to_process = []
                for iid, title, price in items:
                    iid = (iid or "").strip()
                    if not iid or iid in seen_ids:
                        continue
                    seen_ids.add(iid)
                    new_items_to_process.append((iid, (title or "").strip()))

                if not new_items_to_process:
                    print(f"[PAGE {page_idx+1}] All items on this page were already seen. stopping.", flush=True)
                    break

                # DB更新処理
                for iid, title in new_items_to_process:
                    fetched_items += 1
                    
                    # 個別UPSERT
                    upsert_vendor_item(
                        conn,
                        vendor_name,
                        iid,
                        title,
                        page_idx + 1,
                        preset_name,
                        now_str,
                    )

                    # eBay在庫削除連携
                    handle_listing_delete(conn, iid, vendor_name)

            except (TimeoutException, WebDriverException) as e:
                print(f"[PAGE ERROR] {type(e).__name__} on page {page_idx+1}: {e}", flush=True)
                # ページエラーが起きても、次のページを試すかループを抜けるかは状況次第。
                # 基本的には1度エラーが出ると安定しないため break が安全。
                break 

            finally:
                # 1ページごとに確実にブラウザを閉じる
                safe_quit(driver)

            page_idx += 1
            # サーバー負荷と検知回避のためのインターバル
            time.sleep(PAUSE + random.uniform(0.5, 1.5))

        print(
            f"[SCRAPE END][SOLD] preset='{preset_name}' "
            f"total_pages={fetched_pages} total_items={fetched_items}",
            flush=True
        )
        return fetched_pages, fetched_items

    except Exception:
        print(f"[FATAL ERROR] in run_fetch_sold_ebay:\n{traceback.format_exc()}", flush=True)
        return fetched_pages, fetched_items

    finally:
        if conn:
            try:
                conn.close()
                print("[INFO] Database connection closed.", flush=True)
            except Exception:
                pass
# ============================================================
# fetch_active_ebay scrape 本体（1 preset 分）
# ============================================================
def run_fetch_active_ebay(payload: dict) -> Tuple[int, int]:
    print(f"[ENV] host={socket.gethostname()} pid={os.getpid()} SIMULATE={SIMULATE}", flush=True)

    preset = payload["preset"]
    vendor_name = payload["vendor_name"]
    mode = payload["mode"]
    low_usd_target = float(payload["low_usd_target"])
    high_usd_target = float(payload["high_usd_target"])

    print(f"[SCRAPE START] preset={preset} vendor={vendor_name} mode={mode}", flush=True)

    conn = None
    driver = None
    total_items = 0

    try:
        conn = get_sql_server_connection()

        # (7) 1 job = 1 driver
        # driver = build_driver() #　ここをやめる

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
            driver = build_driver() # ここで毎回作る
            try:
                url = page_url(base_url, page_idx)
                print(f"[PAGE] {page_idx+1} {url}", flush=True)

                # (8) renderer timeout は即捨てて作り直してリトライ
                for attempt in range(1, MAX_RENDER_RETRY_PER_PAGE + 1):
                    try:
                        print(f"[C] driver.get start page={page_idx+1} attempt={attempt}", flush=True)
                        driver.get(url)
                        print(f"[C] driver.get done page={page_idx+1}", flush=True)

                        print("[D] wait body start", flush=True)
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        print("[D] wait body done", flush=True)
                        break  # 成功
                    except (TimeoutException, WebDriverException) as e:
                        if is_renderer_timeout(e):
                            print(f"[RENDERER TIMEOUT] page={page_idx+1} attempt={attempt} -> rebuild driver", flush=True)
                            try:
                                safe_quit(driver)
                            except Exception:
                                pass
                            driver = build_driver()
                            if attempt >= MAX_RENDER_RETRY_PER_PAGE:
                                raise
                            continue
                        raise  # renderer以外はそのまま上へ

                if has_no_results_banner(driver):
                    break

                print("[E] scroll start", flush=True)
                if vendor_name == "メルカリshops":
                    items = scroll_until_stagnant_collect_shops(driver, pause=0.6)
                else:
                    items = scroll_until_stagnant_collect_items(driver, pause=0.6)
                print(f"[E] scroll done items={len(items)}", flush=True)

                total_items += len(items)
                print(f"[PAGE {page_idx+1}] items={len(items)} sample={items[:2]}", flush=True)
                if not items:
                    break

                item_ids = [iid for iid, _, _ in items]
                print(f"[F] old_price select start n={len(item_ids)}", flush=True)
                old_price_map = get_vendor_item_prices_batch(conn, vendor_name, item_ids)
                print(f"[F] old_price select done got={len(old_price_map)}", flush=True)

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
                        )
                    else:
                        cnt_unchanged += 1

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
                print(f"[G] upsert start rows={len(rows)} now={now}", flush=True)
                upsert_vendor_items(conn, rows, now)
                print("[G] upsert done", flush=True)

                print(
                    f"[PAGE {page_idx+1} RESULT] upserted={len(rows)} "
                    f"skip={cnt_skip} changed={cnt_changed} unchanged={cnt_unchanged}",
                    flush=True
                )

                elapsed = time.time() - page_start
                TARGET = 35.0
                if elapsed < TARGET:
                    time.sleep((TARGET - elapsed) + random.uniform(0.0, 3.0))

            finally:
                safe_quit(driver)   # ★ 必ず終了させる               

            page_idx += 1
            time.sleep(1)

    finally:
        if driver:
            try:
                safe_quit(driver)
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    print(f"[SCRAPE END] preset={preset}", flush=True)
    return page_idx, total_items


# =========================
# Worker main loop
# =========================
def main():
    print(f"[WORKER START] {WORKER_NAME}", flush=True)
    print("PATH=", os.environ.get("PATH"), flush=True)
    print("which chrome:", os.system("which google-chrome"), flush=True)
    print("which chromium:", os.system("which chromium"), flush=True)

    if CHECK_SWAP:
        warn_if_no_swap()

    # job pick用の接続（長寿命）
    conn = get_sql_server_connection()
    conn.autocommit = False
 
    while True:
        cur = None
        try:
            cur = conn.cursor()
            now = now_jst()
            cur.execute(SQL_PICK_JOBS, WORKER_NAME, now)
            jobs = cur.fetchall()
            conn.commit()
            print(f"[PICK] fetched jobs={len(jobs)} committed", flush=True)
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
                print(f"[JOB PAYLOAD PARSED] keys={list(payload.keys())}", flush=True)

                if job_kind == "fetch_active_ebay":
                    fetched_pages, fetched_items = run_fetch_active_ebay(payload)
                elif job_kind == "fetch_sold_ebay":
                    fetched_pages, fetched_items = run_fetch_sold_ebay(payload)                
                else:
                    raise ValueError(f"unknown job_kind: {job_kind}")

                cur2 = conn.cursor()
                now = now_jst()
                cur2.execute(SQL_MARK_DONE, now, fetched_pages, fetched_items, job_id)
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
