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

from playwright.sync_api import sync_playwright
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from datetime import datetime, timezone, timedelta
from apps.adapters.mercari_item_status import handle_listing_delete,handle_listing_price_update,fetch_json_core


# =========================
# Local application modules
# =========================
from apps.common.utils import (
    get_sql_server_connection,
    compute_start_price_usd,
)
from apps.adapters.mercari_search import make_search_url

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
WORKER_PID = os.getpid()
STATUS_DIR = os.environ.get("WORKER_STATUS_DIR", os.getcwd())
STATUS_FILE = os.path.join(STATUS_DIR, f"worker_status_{WORKER_PID}.txt")

JST = timezone(timedelta(hours=9))
def now_jst():
    return datetime.now(JST).replace(tzinfo=None)

POLL_SEC = 2
N = 1   # ★ 1 job ずつ

SIMULATE = (os.environ.get("SIMULATE") == "1")  # 本番は未設定/0

# --- debug toggles (temporary) ---
EXIT_AFTER_PRICE_UPDATE = False
EXIT_AFTER_DELETE = False

# ★ 対策(10): swap警告を出すか（Linuxのみ）
CHECK_SWAP = (os.environ.get("CHECK_SWAP", "1") == "1")

MAX_PAGES = 2
PAUSE = 0.6
SIMULATE_DELETE = False

# browser寿命 この回数のjob処理したらrowserを作り直す
BROWSER_RESTART_EVERY = 25
# page寿命
PAGE_RESTART_EVERY = 10

FETCH_TIMEOUT_MS = 30000

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

    # ─────────────────────────────
    # ① クリア前の状態取得
    # ─────────────────────────────
    try:
        with conn.cursor() as cursor:
            select_sql = """
            SELECT 出品状況, 出品状況詳細, last_updated_str, last_ng_at
            FROM trx.vendor_item
            WHERE vendor_name = ?
            AND vendor_item_id = ?
            """
            cursor.execute(select_sql, vendor_name, sku)
            row = cursor.fetchone()
    finally:
        try:
            cursor.close()
        except Exception:
            pass

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
            with conn.cursor() as cursor:
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
        handle_listing_delete(conn, sku, vendor_name,"価格範囲外",)
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
MERGE [trx].[vendor_item] WITH (HOLDLOCK) AS T
USING (SELECT ? AS vendor_name, ? AS vendor_item_id) AS S
ON (T.[vendor_name] = S.vendor_name AND T.[vendor_item_id] = S.vendor_item_id)

WHEN MATCHED THEN
  UPDATE SET
    T.[status]          = ?,
    T.[preset]          = ?,
    T.[title_jp]        = ?,
    T.[last_checked_at] = ?,
    T.[prev_price] = CASE
                       WHEN (T.[price] <> ? OR
                            (T.[price] IS NULL AND ? IS NOT NULL) OR
                            (T.[price] IS NOT NULL AND ? IS NULL))
                         THEN T.[price]
                       ELSE T.[prev_price]
                     END,
    T.[price] = COALESCE(?, T.[price]),
    T.[vendor_created_at] = ?,
    T.[vendor_updated_at] = ?,
    T.[vendor_page] = COALESCE(?, T.[vendor_page])
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
      [prev_price],
      [vendor_created_at],
      [vendor_updated_at]
  )
  VALUES (
      ?, ?, ?, ?, ?,
      ?, ?, ?, ?, NULL,
      ?, ?
  );
"""

    MAX_RETRY = 3

    for attempt in range(1, MAX_RETRY + 1):

        cursor = conn.cursor()
        BATCH_SIZE = 25
        count = 0

        try:
            for r in rows:
                params = (
                    r["vendor_name"],
                    r["vendor_item_id"],

                    # UPDATE
                    r["status"],
                    r["preset"],
                    r["title_jp"],
                    now,
                    r["price"], r["price"], r["price"],
                    r["price"],
                    r["vendor_created_at"],
                    r["vendor_updated_at"],
                    r.get("vendor_page"),  
                    # INSERT
                    r["vendor_name"],
                    r["vendor_item_id"],
                    r["status"],
                    r["preset"],
                    r["title_jp"],
                    r.get("vendor_page"),  
                    now,
                    now,
                    r["price"],
                    r["vendor_created_at"],
                    r["vendor_updated_at"],
                )

                cursor.execute(sql, params)
                count += 1

                if count % BATCH_SIZE == 0:
                    conn.commit()

            conn.commit()
            print(f"[UPSERT] done rows={len(rows)}", flush=True)
            return len(rows)

        except pyodbc.Error as e:
            if "1205" in str(e):
                print(f"[DEADLOCK RETRY] attempt={attempt}", flush=True)
                conn.rollback()
                time.sleep(0.5 * attempt)
                continue
            else:
                conn.rollback()
                raise
        finally:
            try:
                cursor.close()
            except Exception:
                pass


    raise RuntimeError("upsert_vendor_items failed after retries")

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

def release_job(conn, job_id):
    cur = conn.cursor()
    try:
        sql = """
        UPDATE trx.scrape_job
        SET
            status = 'pending',
            worker_name = NULL,
            locked_at = NULL,
            finished_at = NULL,
            error_message = NULL
        WHERE job_id = ?
        """
        cur.execute(sql, job_id)
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass

def fetch_page_json_bk(page, url, conn, job_id):
    print("************* evaluate版2 *************", flush=True)

    page.add_init_script("""
    (function() {
        const origOpen = XMLHttpRequest.prototype.open;
        const origSend = XMLHttpRequest.prototype.send;

        XMLHttpRequest.prototype.open = function(method, url) {
            this._url = url;
            return origOpen.apply(this, arguments);
        };

        XMLHttpRequest.prototype.send = function() {
            this.addEventListener('load', function() {
                try {
                    if (this._url && this._url.includes('entities:search')) {
                        const json = JSON.parse(this.responseText);
                        window.__SEARCH_DATA__ = json;
                    }
                } catch (e) {}
            });
            return origSend.apply(this, arguments);
        };
    })();
    """)

    try:
        page.goto(url, wait_until="commit", timeout=FETCH_TIMEOUT_MS)
    except Exception:
        raise RuntimeError("BROWSER_BROKEN")
    page.wait_for_timeout(1000)


    data = None
    for _ in range(20):
        data = page.evaluate("() => window.__SEARCH_DATA__")
        if data:
            break
        page.wait_for_timeout(500)

    if not data:
        raise RuntimeError("FETCH_TIMEOUT")

    return data

def fetch_page_json_new(page, url, conn, job_id):

    json_data = fetch_json_core(
        page,
        url,
        lambda r: "entities:search" in r.url
    )

    return json_data

def fetch_page_json(page, url, conn, job_id):

    for attempt in range(2):
        try:
            print(f"A: expect_response start {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            # 1. 応答を待機する「入れ物」だけ用意
            with page.expect_response(
                lambda r: r.request.method == "POST" and "entities:search" in r.url,
                timeout=FETCH_TIMEOUT_MS
            ) as resp_info:
                page.goto(url, wait_until="domcontentloaded", timeout=FETCH_TIMEOUT_MS)

                # 2. withを抜けてからレスポンスオブジェクトを取り出す　→　これはやめる
                # ↓をwithの中にいれる
                resp = resp_info.value

                if resp.status != 200:
                    raise RuntimeError(f"Mercari API status={resp.status}")
                                
                print(f"B: before json() (Fetch Start) {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                # json_data = resp.json(timeout=10000) 
                json_data = resp.json()
                print(f"C: after json() {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # 3. body() ではなく json() を直接使い、かつ変数を分離
                # json_data = resp.json() 
                print(f"D: after body {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                return json_data
 
        except Exception as e:
            print(f"D: exception {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {type(e)} {e}")
            if attempt == 1:
                raise RuntimeError("BROWSER_BROKEN")
        

def extract_items_from_json(json_data):

    rows = []

    items = json_data.get("items", [])

    for item in items:

        item_id = item.get("id")
        title = item.get("name")
        price = item.get("price")
        seller = item.get("sellerId")

        created = item.get("created")
        updated = item.get("updated")

        if created is not None:
            created = datetime.fromtimestamp(int(created), JST)

        if updated is not None:
            updated = datetime.fromtimestamp(int(updated), JST)

        if price is not None:
            price = int(price)

        rows.append((item_id, title, price, seller, created, updated))

    return rows


def run_fetch_sold_ebay(page, payload: dict, job_id: int) -> Tuple[int, int]:

    preset_name = payload["preset"]
    vendor_name = payload["vendor_name"]
    brand_id = payload["brand_id"]
    category_id = payload["category_id"]
    mode = payload.get("mode", "DDP")
    low_usd_target = payload["low_usd_target"]
    high_usd_target = payload["high_usd_target"]

    print(
        f"[SCRAPE START][SOLD] preset='{preset_name}' "
        f"vendor='{vendor_name}' mode='{mode}'",
        flush=True
    )

    fetched_pages = 0
    fetched_items = 0

    base_url = make_search_url(
        vendor_name=vendor_name,
        brand_id=brand_id,
        category_id=category_id,
        status="sold_out|trading",
        mode=mode,
        low_usd_target=low_usd_target,
        high_usd_target=high_usd_target,
    )

    print(f"[URL] {base_url}", flush=True)

    page_idx = 0
    seen_ids: set[str] = set()
    conn = get_sql_server_connection()

    try:
        while True:

            if page_idx >= MAX_PAGES:
                print(f"[STOP] reached MAX_PAGES={MAX_PAGES}", flush=True)
                break

            target_url = page_url(base_url, page_idx)
            print(f"[PAGE {page_idx+1}] GET {target_url}", flush=True)

            # --- ここからページ単位の try ---
            try:
                json_data = fetch_page_json(page, target_url, conn, job_id)
                items = extract_items_from_json(json_data)
                print(f"[PAGE {page_idx+1}] scraped={len(items)}", flush=True)
                fetched_pages += 1

                if not items:
                    break

                rows = []

                for iid, title, price, seller, created, updated in items:
                    iid = (iid or "").strip()
                    if not iid or iid in seen_ids:
                        continue
                    seen_ids.add(iid)
                    fetched_items += 1

                    rows.append({
                        "vendor_name": vendor_name,
                        "vendor_item_id": iid,
                        "status": "売り切れ",
                        "preset": preset_name,
                        "title_jp": title,
                        "price": None,
                        "vendor_created_at": created,
                        "vendor_updated_at": updated,
                    })
                    handle_listing_delete(conn, iid, vendor_name,"売り切れ")

                if rows:
                    upsert_vendor_items(conn, rows, now_jst())

            except RuntimeError as e:
                if "FETCH_TIMEOUT" in str(e) or "BROWSER_BROKEN" in str(e):
                    raise
                print(f"[WARN] page error page={page_idx+1}: {e}", flush=True)
            except Exception as e:
                print(f"[WARN] page error page={page_idx+1}: {e}", flush=True)

            # ★ 3. ここでの conn.close() は削除（finallyブロックは外側に移動）

            page_idx += 1
            time.sleep(1)

    finally:
        # ★ 4. 全ページ終わったら（またはエラー時に）ここで確実に閉じる
        if conn:
            try:
                conn.close()
                print(f"[DB] sold_ebay connection closed.", flush=True)
            except Exception:
                pass

    print(
        f"[SCRAPE END][SOLD] preset='{preset_name}' "
        f"pages={fetched_pages} items={fetched_items}",
        flush=True
    )

    return fetched_pages, fetched_items


# ============================================================
# fetch_active_ebay scrape 本体（1 preset 分）
# ============================================================
def run_fetch_active_ebay(page, payload: dict, job_id: int) -> Tuple[int, int]:
    print(f"[ENV] host={socket.gethostname()} pid={os.getpid()} SIMULATE={SIMULATE}", flush=True)

    preset = payload["preset"]
    vendor_name = payload["vendor_name"]
    mode = payload["mode"]
    low_usd_target = float(payload["low_usd_target"])
    high_usd_target = float(payload["high_usd_target"])

    print(f"[SCRAPE START] preset={preset} vendor={vendor_name} mode={mode}", flush=True)

    total_items = 0

    base_url = make_search_url(
        vendor_name=vendor_name,
        brand_id=payload["brand_id"],
        category_id=payload["category_id"],
        status="on_sale",
        mode=mode,
        low_usd_target=low_usd_target,
        high_usd_target=high_usd_target,
    )

    print(f"[URL] {base_url}", flush=True)

    page_idx = 0
    conn = get_sql_server_connection()

    try:
        while True:
            page_start = time.time()
            url = page_url(base_url, page_idx)
            print(f"[PAGE] {page_idx+1} {url}", flush=True)

            json_data = fetch_page_json(page, url, conn, job_id)
            items = extract_items_from_json(json_data)

            print(f"[PAGE {page_idx+1}] items={len(items)} sample={items[:2]}", flush=True)

            if not items:
                break

            total_items += len(items)

            item_ids = [iid for iid, _, _, _, _, _ in items]
            print(f"[F] old_price select start n={len(item_ids)}", flush=True)
            old_price_map = get_vendor_item_prices_batch(conn, vendor_name, item_ids)
            print(f"[F] old_price select done got={len(old_price_map)}", flush=True)

            cnt_skip = 0
            cnt_changed = 0
            cnt_unchanged = 0

            for iid, title, price, seller, created, updated in items:
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
                "vendor_page": page_idx+1,
                "price": price,
                "vendor_created_at": created,
                "vendor_updated_at": updated,
            } for iid, title, price, seller, created, updated in items]

            now = now_jst()
            print(f"[G] upsert start rows={len(rows)} now={now}", flush=True)
            upsert_vendor_items(conn, rows, now)
            print("[G] upsert done", flush=True)

            print(
                f"[PAGE {page_idx+1} RESULT] "
                f"upserted={len(rows)} skip={cnt_skip} changed={cnt_changed} unchanged={cnt_unchanged}",
                flush=True
            )

            elapsed = time.time() - page_start
            TARGET = 8.0
            if elapsed < TARGET:
                time.sleep((TARGET - elapsed) + random.uniform(0.0, 5.0))
            
            page_idx += 1
            time.sleep(1)

    finally:
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

    if CHECK_SWAP:
        warn_if_no_swap()

    # Playwright は worker lifetime で1回だけ起動
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-dev-shm-usage",  # 共有メモリ不足によるクラッシュを防止
                "--disable-gpu",            # 不要なGPUプロセスの排除
                "--no-sandbox"              # VPS環境での安定性向上
            ]
        )

        page = browser.new_page()
        page.set_default_timeout(10000)  
        browser_job_count = 0
        page_job_count = 0

        page.on(
            "request",
            lambda r: "entities:search" in r.url and print("REQ:", r.url, flush=True)
        )

        page.on(
            "response",
            lambda r: "entities:search" in r.url and print("RES:", r.url, flush=True)
        )

        while True:
            conn = get_sql_server_connection()
            conn.autocommit = False
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
                if cur:
                    try:
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
                        fetched_pages, fetched_items = run_fetch_active_ebay(page, payload, job_id)
                    elif job_kind == "fetch_sold_ebay":
                        fetched_pages, fetched_items = run_fetch_sold_ebay(page, payload, job_id)
                    else:
                        raise ValueError(f"unknown job_kind: {job_kind}")

                    cur2 = conn.cursor()
                    now = now_jst()
                    cur2.execute(SQL_MARK_DONE, now, fetched_pages, fetched_items, job_id)
                    conn.commit()
                    print(f"[JOB DONE] id={job_id}", flush=True)

                except RuntimeError as e:
                    if "FETCH_TIMEOUT" in str(e) or "BROWSER_BROKEN" in str(e):
                        print(f"[BROWSER RESTART: {e}]", flush=True)

                        try:
                            page.close()
                        except Exception:
                            pass
                        try:
                            browser.close()
                        except Exception:
                            pass

                        browser = p.chromium.launch(headless=True)
                        page = browser.new_page()
                        page.set_default_timeout(10000)

                        page.on("request", lambda r: "entities:search" in r.url and print("REQ:", r.url, flush=True))
                        page.on("response", lambda r: "entities:search" in r.url and print("RES:", r.url, flush=True))

                        release_job(conn, job_id)
                        continue

                    raise

                except Exception:
                    err = traceback.format_exc()
                    print(err.encode('utf-8', errors='ignore').decode('utf-8'), flush=True)
                    try:
                        cur2 = conn.cursor()
                        now = now_jst()
                        cur2.execute(SQL_MARK_ERROR, now, err[-4000:], job_id)
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        traceback.print_exc()

                finally:
                    if cur2:
                        try:
                            cur2.close()
                        except Exception:
                            pass 

                browser_job_count += 1
                page_job_count += 1            

                if page_job_count >= PAGE_RESTART_EVERY:

                    print("[PAGE RESTART]", flush=True)

                    try:
                        page.close()
                    except Exception:
                        pass

                    page = browser.new_page()
                    page.set_default_timeout(10000)
                    page.on(
                        "request",
                        lambda r: "entities:search" in r.url and print("REQ:", r.url, flush=True)
                    )

                    page.on(
                        "response",
                        lambda r: "entities:search" in r.url and print("RES:", r.url, flush=True)
                    )

                    page_job_count = 0

                if browser_job_count >= BROWSER_RESTART_EVERY:

                    print("[BROWSER RESTART]", flush=True)

                    try:
                        page.close()
                    except Exception:
                        pass

                    try:
                        browser.close()
                    except Exception:
                        pass

                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.set_default_timeout(10000)
                    page.on(
                        "request",
                        lambda r: "entities:search" in r.url and print("REQ:", r.url, flush=True)
                    )

                    page.on(
                        "response",
                        lambda r: "entities:search" in r.url and print("RES:", r.url, flush=True)
                    )

                    browser_job_count = 0
                    page_job_count = 0

                if os.environ.get("ONESHOT") == "1":
                    return

if __name__ == "__main__":
    main()
