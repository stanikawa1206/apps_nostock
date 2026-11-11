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
import glob

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
from apps.common.utils import compute_cost_range_jpy_from_usd_range
from apps.adapters import yahoo_fleamarket as yahoo
from apps.adapters.rakuma_search import (
    rakuma_page_url,
    fetch_rakuma_search_html,
    extract_items_from_rakuma_html,
)
from apps.adapters.rakuma_item_status import is_rakuma_update_too_old

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

BASE_DIR = os.environ.get("WORKER_STATUS_DIR", os.getcwd())

# ログディレクトリ定義＆作成
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ログ出力（PID単位）＋ 画面にも同時出力（tee）
class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            s.write(data)

    def flush(self):
        for s in self._streams:
            s.flush()

# Windows(mouse)では元の stdout/stderr が cp932 のため、絵文字などを print すると UnicodeEncodeError で
# ワーカーが落ちていた（2026-10-21 send_mail の「📧」で4回）。表現できない文字は置換して出力を続ける。
sys.stdout.reconfigure(errors="replace")
sys.stderr.reconfigure(errors="replace")

log_path = os.path.join(LOG_DIR, f"worker_{WORKER_PID}.log")
_log_file = open(log_path, "a", encoding="utf-8", buffering=1)
sys.stdout = _Tee(sys.stdout, _log_file)
sys.stderr = _Tee(sys.stderr, _log_file)

# ステータスファイル
STATUS_FILE = os.path.join(BASE_DIR, f"worker_status_{WORKER_PID}.txt")

# ブラウザ応答待ちファイル（メルカリ検索の1回の取得の間だけ存在する）。
# resp.json() などブラウザへの呼び出しが戻らなくなると、このワーカー自身は時間切れを判定できない。
# watchdog がこのファイルの開始時刻から一定時間（watchdog.BROWSER_HANG_SEC）経っても消えないのを見て、
# このワーカーが起動したブラウザだけを終了させる。止まっていた呼び出しはエラーで戻り、
# 既存の BROWSER_BROKEN（ブラウザ再作成・ジョブを pending へ戻す）に進む。
BROWSER_WAIT_FILE = os.path.join(BASE_DIR, f"worker_browserwait_{WORKER_PID}.txt")

# ログ削除（3日）
LOG_RETENTION_SEC = 3 * 24 * 60 * 60
for file in glob.glob(os.path.join(LOG_DIR, "worker_*.log")):
    if time.time() - os.path.getmtime(file) > LOG_RETENTION_SEC:
        os.remove(file)


def write_status(job_id, page):
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        f.write(f"pid={WORKER_PID}\n")
        f.write(f"job_id={job_id}\n")
        f.write(f"page={page}\n")
        f.write(f"updated_at={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


def begin_browser_wait(job_id):
    with open(BROWSER_WAIT_FILE, "w", encoding="utf-8") as f:
        f.write(f"pid={WORKER_PID}\n")
        f.write(f"job_id={job_id}\n")
        f.write(f"started={time.time():.3f}\n")


def end_browser_wait():
    try:
        os.remove(BROWSER_WAIT_FILE)
    except OSError:
        pass

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

SOLD_MAX_PAGES = 2

# =========================
# メルカリ検索の取得方式（2026-11-04 VPS 210.131.209.232 で試験導入）
# =========================
# browser: 検索ページをブラウザで開き、ページが呼ぶ entities:search の応答を受け取る（従来）
# api    : entities:search を直接呼ぶ（apps.adapters.mercari_search_api）。ブラウザは起動しない
# ホストごとの .env の SEARCH_FETCH_MODE で切り替える（未設定は browser）。反映はワーカーの起動時。
SEARCH_FETCH_MODE = os.environ.get("SEARCH_FETCH_MODE", "browser").strip().lower()
# api 方式で 401・403・429 を受けたら置く。このファイルがある間、このホストのワーカーはジョブを取らない
# （アクセス制限の可能性があるため、方式を問わずメルカリへのアクセスを止める）。人が確認して消すまで残る。
SEARCH_API_BLOCKED_FILE = os.path.join(BASE_DIR, "search_api_blocked.txt")
# api 方式の失敗（通信エラー・5xx・400・応答の形の違いなど）が1ワーカーで SEARCH_API_FALLBACK_AFTER 回
# 続いたら置く。このファイルがある間、このホストのワーカーは従来のブラウザ方式で取得する。
SEARCH_API_FALLBACK_FILE = os.path.join(BASE_DIR, "search_api_fallback.txt")
SEARCH_API_FALLBACK_AFTER = 5
_search_api_fail_streak = 0
# ブラウザ方式に戻っている間も、ファイルの更新時刻から SEARCH_API_PROBE_INTERVAL_SEC 経つごとに
# 1ページだけ api 方式で取得を試し、成功したらファイルを消して api 方式に戻す
# （2026-11-07 mouse は約8分の DNS 障害で戻り、人が消すまで2日間ブラウザ方式のままだった）。
# 試す前にファイルへ1行追記して更新時刻を進めるので、同じホストの他のワーカーは次の間隔まで試さない。
SEARCH_API_PROBE_INTERVAL_SEC = 1800


def use_search_api() -> bool:
    return SEARCH_FETCH_MODE == "api" and not os.path.exists(SEARCH_API_FALLBACK_FILE)


def _write_flag_file(path, reason):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} pid={WORKER_PID} {reason}\n")
    except OSError as e:
        print(f"[SEARCH_API][FLAG WRITE ERROR] {path}: {e}", flush=True)
PAUSE = 0.6
SIMULATE_DELETE = False

# browser寿命 この回数のjob処理したらrowserを作り直す
BROWSER_RESTART_EVERY = 25
# page寿命
PAGE_RESTART_EVERY = 10

FETCH_TIMEOUT_MS = 30000

# entities:search の応答本文の受信完了を待つ上限（"B: before json()" からの秒数）。
# resp.json() は受信完了イベント(loadingFinished/loadingFailed)を時間制限なしで待つため、
# それが届かないと永久に戻らない（各VPSで3日に十数回発生）。正常時の B→C は p99=1秒・最大21秒。
BODY_FINISH_TIMEOUT_SEC = 30
BODY_FINISH_POLL_MS = 100

# =========================
# SQL
# =========================
SQL_PICK_JOBS = f"""
UPDATE trx.scrape_job
SET
    status = 'running',
    worker_name = ?,
    locked_at = ?,
    error_message = NULL
OUTPUT
    inserted.job_id,
    inserted.job_kind,
    inserted.job_payload,
    inserted.current_page
WHERE job_id = (
    SELECT TOP (1) job_id
    FROM trx.scrape_job WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE status IN ('pending','pending2')
    ORDER BY created_at, job_id
);
"""

SQL_MARK_DONE = """
UPDATE trx.scrape_job
SET
    status = 'done',
    finished_at = ?,
    fetched_pages = ?,
    fetched_items = ?,
    current_page = NULL
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


def get_vendor_item_dates_batch(conn, vendor_name: str, vendor_item_ids: List[str]) -> Dict[str, Tuple[Optional[datetime], Optional[datetime]]]:
    """
    既存の vendor_created_at / vendor_updated_at を取得する。
    ラクマの検索一覧にはこれらの日時情報が含まれないため、
    upsert_vendor_items() のMERGE（UPDATE時にCOALESCEせず直接上書きする仕様）で
    既存値がNULLに潰されないよう、再スクレイプ時は既存値をそのまま渡し戻す。
    """
    if not vendor_item_ids:
        return {}

    placeholders = ",".join("?" for _ in vendor_item_ids)
    sql = f"""
        SELECT vendor_item_id, vendor_created_at, vendor_updated_at
        FROM [trx].[vendor_item]
        WHERE vendor_name = ? AND vendor_item_id IN ({placeholders})
    """
    params = [vendor_name] + vendor_item_ids
    out: Dict[str, Tuple[Optional[datetime], Optional[datetime]]] = {}

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        for vid, created, updated in cur.fetchall():
            out[str(vid)] = (created, updated)
    finally:
        try:
            cur.close()
        except Exception:
            pass

    for v in vendor_item_ids:
        out.setdefault(v, (None, None))
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
    # ② 古い更新の判定でクリア
    # メルカリ: last_updated_str に「○ヶ月前 / ○か月前 / 半年以上前」を含むかの文字列一致。
    # ラクマ: 表記も閾値もメルカリと異なる（1ヶ月前まではOK、2ヶ月前以上・年単位がNG）ため、
    #         同じ文字列一致を流用せず、既存の is_rakuma_update_too_old() 判定をそのまま使う。
    # ─────────────────────────────
    if before_updated is not None:
        s = str(before_updated)

        if vendor_name == "ラクマ":
            is_stale = bool(is_rakuma_update_too_old(s))
        else:
            is_stale = ("ヶ月前" in s) or ("か月前" in s) or ("半年以上前" in s)

        if is_stale:

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
    T.[vendor_page] = COALESCE(?, T.[vendor_page]),
    T.[item_condition_id] = COALESCE(?, T.[item_condition_id])
WHEN NOT MATCHED THEN
  INSERT (
      [vendor_name],
      [vendor_item_id],
      [status],
      [preset],
      [title_jp],
      [vendor_page],
      [item_condition_id], 
      [created_at],
      [last_checked_at],
      [price],
      [prev_price],
      [vendor_created_at],
      [vendor_updated_at]
  )
  VALUES (
      ?, ?, ?, ?, ?,?,
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
                    r.get("item_condition_id"),   #
                    # INSERT
                    r["vendor_name"],
                    r["vendor_item_id"],
                    r["status"],
                    r["preset"],
                    r["title_jp"],
                    r.get("vendor_page"),
                    r.get("item_condition_id"),    
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

def fetch_page_json_api(url, job_id):
    """
    api 方式の1ページ取得。失敗はすべて RuntimeError("FETCH_TIMEOUT: ...") にして呼び出し元へ送出する。
    これはブラウザ方式の取得失敗と同じ扱いで、ジョブは pending へ戻り（release_job）、
    そのページの結果を「0件」「売り切れなし」として使うことはない。
    """
    global _search_api_fail_streak
    from apps.adapters.mercari_search_api import fetch_search_json, SearchApiBlocked, SearchApiError

    begin_browser_wait(job_id)  # 呼び出しが戻らない場合の watchdog の記録用（api 方式ではブラウザは無い）
    try:
        json_data = fetch_search_json(url)
    except SearchApiBlocked as e:
        _write_flag_file(SEARCH_API_BLOCKED_FILE, f"job={job_id} {e}")
        print(f"[SEARCH_API][BLOCKED] {e} → {SEARCH_API_BLOCKED_FILE} を置き、このホストはジョブを取らない", flush=True)
        raise RuntimeError(f"FETCH_TIMEOUT: SEARCH_API_BLOCKED {e}")
    except SearchApiError as e:
        _search_api_fail_streak += 1
        print(f"[SEARCH_API][ERROR] streak={_search_api_fail_streak} {e}", flush=True)
        if _search_api_fail_streak >= SEARCH_API_FALLBACK_AFTER:
            _write_flag_file(SEARCH_API_FALLBACK_FILE, f"job={job_id} 連続{_search_api_fail_streak}回失敗 {e}")
            print(f"[SEARCH_API][FALLBACK] {SEARCH_API_FALLBACK_FILE} を置き、ブラウザ方式に戻す", flush=True)
        raise RuntimeError(f"FETCH_TIMEOUT: SEARCH_API_ERROR {e}")
    finally:
        end_browser_wait()

    _search_api_fail_streak = 0
    return json_data


def _search_api_probe_due() -> bool:
    try:
        return time.time() - os.path.getmtime(SEARCH_API_FALLBACK_FILE) >= SEARCH_API_PROBE_INTERVAL_SEC
    except FileNotFoundError:
        return False


def try_return_to_search_api(url, job_id):
    """
    ブラウザ方式に戻っているホストで、api 方式の1ページ取得を試す。
    成功したら search_api_fallback.txt を消して取得結果を返す（以後このホストは api 方式）。
    失敗したら None を返し、呼び出し元はそのままブラウザ方式で取得する。
    401・403・429 は fetch_page_json_api と同じく search_api_blocked.txt を置いて例外を送出する。
    """
    _write_flag_file(SEARCH_API_FALLBACK_FILE, f"job={job_id} api方式への復帰を確認")
    try:
        json_data = fetch_page_json_api(url, job_id)
    except RuntimeError as e:
        if "SEARCH_API_BLOCKED" in str(e):
            raise
        print(f"[SEARCH_API][RETURN_NG] api方式はまだ使えないためブラウザ方式を続ける: {e}", flush=True)
        return None
    try:
        os.remove(SEARCH_API_FALLBACK_FILE)
    except FileNotFoundError:
        pass  # 同じ時刻に復帰を確認した他のワーカーが先に消した
    except OSError as e:
        # mouse では他プロセスが開いているファイルの削除が WinError 32 になる（worker_status で実績あり）。
        # 取得結果は使い、ファイルは次の確認時刻に消し直す
        print(f"[SEARCH_API][RETURN] {SEARCH_API_FALLBACK_FILE} を消せない（次回の確認で再試行）: {e}", flush=True)
        return json_data
    print(f"[SEARCH_API][RETURN] api方式で取得できたため {SEARCH_API_FALLBACK_FILE} を消し、api方式に戻す", flush=True)
    return json_data


def fetch_page_json(page, url, conn, job_id):
    if use_search_api():
        return fetch_page_json_api(url, job_id)
    if SEARCH_FETCH_MODE == "api" and _search_api_probe_due():
        json_data = try_return_to_search_api(url, job_id)
        if json_data is not None:
            return json_data
    if page is None:
        # api 方式で動いていたワーカーが、途中でブラウザ方式に戻った（search_api_fallback.txt）。
        # ブラウザを起動させるため BROWSER_BROKEN として扱う（ジョブは pending へ戻る）
        raise RuntimeError("BROWSER_BROKEN: ブラウザ未起動（api 方式からブラウザ方式へ切り替え）")

    # 受信完了（成功・失敗とも）したリクエストを記録し、完了を確認してから resp.json() を呼ぶ
    finished_requests = set()
    def on_request_done(request):  # page.on は組み込みメソッド(set.add)を受け付けない
        finished_requests.add(request)
    page.on("requestfinished", on_request_done)
    page.on("requestfailed", on_request_done)
    try:
        return _fetch_page_json_attempts(page, url, finished_requests, job_id)
    finally:
        page.remove_listener("requestfinished", on_request_done)
        page.remove_listener("requestfailed", on_request_done)

def _fetch_page_json_attempts(page, url, finished_requests, job_id=None):

    for attempt in range(2):
        # 1回の取得（最長: 応答待ち30秒＋本文の受信完了待ち30秒＋json）の間だけ応答待ちファイルを置く
        begin_browser_wait(job_id)
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
                deadline = time.time() + BODY_FINISH_TIMEOUT_SEC
                while resp.request not in finished_requests:
                    if time.time() > deadline:
                        raise RuntimeError(f"BODY_TIMEOUT: 応答本文が{BODY_FINISH_TIMEOUT_SEC}秒以内に受信完了しない")
                    page.wait_for_timeout(BODY_FINISH_POLL_MS)
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
        finally:
            end_browser_wait()


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

        item_condition_id = item.get("itemConditionId")
        if item_condition_id is not None:
            item_condition_id = int(item_condition_id)

        rows.append((item_id, title, price, seller, created, updated, item_condition_id))

    return rows


def search_has_next_page(json_data) -> bool:
    """
    entities:search の応答に次のページがあるか。
    meta.nextPageToken が空文字のときだけ False（最後のページ）。
    meta・キーが無い、または文字列でない値（None など）の応答は判断できないので True とし、
    従来どおり次のページを取得して0件で終わる。
    取得の失敗は呼び出し元で例外になるため、ここに来るのは正常な応答だけ。
    """
    meta = json_data.get("meta") if isinstance(json_data, dict) else None
    if not isinstance(meta, dict) or "nextPageToken" not in meta:
        print("[NEXT_PAGE] meta.nextPageToken が無い応答 → 次のページを取得して確認する", flush=True)
        return True
    token = meta["nextPageToken"]
    if not isinstance(token, str):
        print(f"[NEXT_PAGE] nextPageToken が文字列でない({token!r}) → 次のページを取得して確認する", flush=True)
        return True
    return token != ""


def run_fetch_sold_ebay(page, start_page, payload: dict, job_id: int) -> Tuple[int, int]:

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

    page_idx = start_page if start_page is not None else 0
    seen_ids: set[str] = set()
    conn = get_sql_server_connection()

    try:
        while True:
            write_status(job_id, page_idx + 1)
            if page_idx >= SOLD_MAX_PAGES:
                print(f"[STOP] reached SOLD_MAX_PAGES={SOLD_MAX_PAGES}", flush=True)
                break

            target_url = page_url(base_url, page_idx)
            print(f"[PAGE {page_idx+1}] GET {target_url}", flush=True)

            # --- ここからページ単位の try ---
            is_last_page = False
            try:
                json_data = fetch_page_json(page, target_url, conn, job_id)
                items = extract_items_from_json(json_data)
                print(f"[PAGE {page_idx+1}] scraped={len(items)}", flush=True)
                fetched_pages += 1

                if not items:
                    break

                # 次のページが無い（nextPageToken が空）なら、このページの処理後に終了する
                is_last_page = not search_has_next_page(json_data)

                rows = []

                for iid, title, price, seller, created, updated, item_condition_id in items:
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

            if is_last_page:
                print(f"[STOP] nextPageToken なし（最終ページ={page_idx}）", flush=True)
                break

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
def run_fetch_active_ebay(page, start_page, payload: dict, job_id: int) -> Tuple[int, int]:
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

    page_idx = start_page if start_page is not None else 0
    conn = get_sql_server_connection()

    try:
        while True:
            write_status(job_id, page_idx + 1)
            page_start = time.time()
            url = page_url(base_url, page_idx)
            print(f"[PAGE] {page_idx+1} {url}", flush=True)

            json_data = fetch_page_json(page, url, conn, job_id)
            items = extract_items_from_json(json_data)

            print(f"[PAGE {page_idx+1}] items={len(items)}", flush=True)

            if not items:
                break

            # 次のページが無い（nextPageToken が空）なら、このページの処理後に終了する
            is_last_page = not search_has_next_page(json_data)

            total_items += len(items)

            item_ids = [iid for iid, _, _, _, _, _, _ in items]
            print(f"[F] old_price select start n={len(item_ids)}", flush=True)
            old_price_map = get_vendor_item_prices_batch(conn, vendor_name, item_ids)
            print(f"[F] old_price select done got={len(old_price_map)}", flush=True)

            cnt_skip = 0
            cnt_changed = 0
            cnt_unchanged = 0
            price_retry = {}

            for iid, title, price, seller, created, updated, item_condition_id in items:
                if price is None:
                    cnt_skip += 1
                    continue

                old_price = old_price_map.get(iid)
                if old_price is not None and old_price != price:
                    cnt_changed += 1
                    try:
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
                    except RuntimeError as e:
                        # eBay価格更新失敗。vendor_item.price を旧価格のまま残し、次回の取得で再試行する
                        # （以前はジョブごと異常終了し、以降のページが処理されなかった）。
                        print(f"[PRICE_RETRY_PENDING] sku={iid} {old_price} -> {price} JPY: {e}", flush=True)
                        price_retry[iid] = old_price
                else:
                    cnt_unchanged += 1

            rows = [{
                "vendor_name": vendor_name,
                "vendor_item_id": iid,
                "status": "販売中",
                "preset": preset,
                "title_jp": title,
                "vendor_page": page_idx+1,
                "price": price_retry.get(iid, price),
                "vendor_created_at": created,
                "vendor_updated_at": updated,
                "item_condition_id": item_condition_id,
            } for iid, title, price, seller, created, updated, item_condition_id in items]

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

            if is_last_page:
                print(f"[STOP] nextPageToken なし（最終ページ={page_idx}）", flush=True)
                break

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    print(f"[SCRAPE END] preset={preset}", flush=True)
    return page_idx, total_items




# ============================================================
# fetch_active_rakuma scrape 本体（1 preset 分）
# Playwright/Selenium不使用: 検索結果ページをHTTP GETするのみ。
# 商品詳細ページへはアクセスしない（購入申請あり/なし判定もしない）。
# ============================================================
def run_fetch_active_rakuma(payload: dict, job_id: int) -> Tuple[int, int]:
    print(f"[ENV] host={socket.gethostname()} pid={os.getpid()} SIMULATE={SIMULATE}", flush=True)

    preset = payload["preset"]
    vendor_name = payload["vendor_name"]
    mode = payload["mode"]
    low_usd_target = float(payload["low_usd_target"])
    high_usd_target = float(payload["high_usd_target"])

    print(f"[SCRAPE START][RAKUMA] preset={preset} vendor={vendor_name} mode={mode}", flush=True)

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
            write_status(job_id, page_idx + 1)
            page_start = time.time()
            url = rakuma_page_url(base_url, page_idx)
            print(f"[PAGE] {page_idx+1} {url}", flush=True)

            html = fetch_rakuma_search_html(url)
            items = extract_items_from_rakuma_html(html) if html is not None else []

            print(f"[PAGE {page_idx+1}] items={len(items)}", flush=True)

            if not items:
                break

            total_items += len(items)

            item_ids = [it["vendor_item_id"] for it in items]
            existing_dates = get_vendor_item_dates_batch(conn, vendor_name, item_ids)

            # 価格変化の検知 → 既存メルカリと同じ handle_price_change_side_effects() を流用。
            # last_updated_str に「ヶ月前」等が入っている（=前回heavy時点で古い更新扱いだった）
            # 商品の価格が変わっていれば、出品者が商品を動かしたとみなして
            # 出品状況/出品状況詳細/last_updated_str/last_ng_at をクリアし、
            # 次回の候補抽出・heavy_check_detailで再評価できる状態に戻す。
            old_price_map = get_vendor_item_prices_batch(conn, vendor_name, item_ids)
            cnt_changed = 0
            cnt_unchanged = 0
            price_retry = {}
            for it in items:
                iid = it["vendor_item_id"]
                price = it["price"]
                if price is None:
                    continue
                old_price = old_price_map.get(iid)
                if old_price is not None and old_price != price:
                    cnt_changed += 1
                    try:
                        handle_price_change_side_effects(
                            conn, iid, vendor_name, old_price, price,
                            mode=mode, low_usd_target=low_usd_target, high_usd_target=high_usd_target,
                        )
                    except RuntimeError as e:
                        # eBay価格更新失敗。vendor_item.price を旧価格のまま残し、次回の取得で再試行する。
                        print(f"[PRICE_RETRY_PENDING] sku={iid} {old_price} -> {price} JPY: {e}", flush=True)
                        price_retry[iid] = old_price
                else:
                    cnt_unchanged += 1
            print(f"[PAGE {page_idx+1}] price changed={cnt_changed} unchanged={cnt_unchanged}", flush=True)

            rows = []
            for it in items:
                iid = it["vendor_item_id"]
                existing_created, existing_updated = existing_dates.get(iid, (None, None))
                rows.append({
                    "vendor_name": vendor_name,
                    "vendor_item_id": iid,
                    "status": "販売中",
                    "preset": preset,
                    "title_jp": it["title_jp"],
                    "vendor_page": page_idx + 1,
                    "price": price_retry.get(iid, it["price"]),
                    # 検索一覧からは取得不可。既存行があれば既存値を保持し、
                    # 新規行はNULLのまま（publish時の詳細取得で埋まる想定）。
                    "vendor_created_at": existing_created,
                    "vendor_updated_at": existing_updated,
                    "item_condition_id": None,
                })

            now = now_jst()
            print(f"[G] upsert start rows={len(rows)} now={now}", flush=True)
            upsert_vendor_items(conn, rows, now)
            print("[G] upsert done", flush=True)

            print(f"[PAGE {page_idx+1} RESULT] upserted={len(rows)}", flush=True)

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

    print(f"[SCRAPE END][RAKUMA] preset={preset}", flush=True)
    return page_idx, total_items


# ============================================================
# fetch_sold_rakuma scrape 本体（1 preset 分）
# Playwright/Selenium不使用: 検索結果ページをHTTP GETするのみ。
# 商品詳細ページへはアクセスしない。
# ============================================================
def run_fetch_sold_rakuma(payload: dict, job_id: int) -> Tuple[int, int]:
    preset_name = payload["preset"]
    vendor_name = payload["vendor_name"]
    brand_id = payload["brand_id"]
    category_id = payload["category_id"]
    mode = payload.get("mode", "DDP")
    low_usd_target = payload["low_usd_target"]
    high_usd_target = payload["high_usd_target"]

    print(
        f"[SCRAPE START][SOLD][RAKUMA] preset='{preset_name}' "
        f"vendor='{vendor_name}' mode='{mode}'",
        flush=True
    )

    fetched_pages = 0
    fetched_items = 0

    base_url = make_search_url(
        vendor_name=vendor_name,
        brand_id=brand_id,
        category_id=category_id,
        status="sold",
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
            write_status(job_id, page_idx + 1)
            if page_idx >= SOLD_MAX_PAGES:
                print(f"[STOP] reached SOLD_MAX_PAGES={SOLD_MAX_PAGES}", flush=True)
                break

            target_url = rakuma_page_url(base_url, page_idx)
            print(f"[PAGE {page_idx+1}] GET {target_url}", flush=True)

            try:
                html = fetch_rakuma_search_html(target_url)
                items = extract_items_from_rakuma_html(html) if html is not None else []
                print(f"[PAGE {page_idx+1}] scraped={len(items)}", flush=True)
                fetched_pages += 1

                if not items:
                    break

                new_ids = []
                for it in items:
                    iid = (it["vendor_item_id"] or "").strip()
                    if not iid or iid in seen_ids:
                        continue
                    seen_ids.add(iid)
                    new_ids.append((iid, it))

                fetched_items += len(new_ids)

                existing_dates = get_vendor_item_dates_batch(
                    conn, vendor_name, [iid for iid, _ in new_ids]
                )

                rows = []
                for iid, it in new_ids:
                    existing_created, existing_updated = existing_dates.get(iid, (None, None))
                    rows.append({
                        "vendor_name": vendor_name,
                        "vendor_item_id": iid,
                        "status": "売り切れ",
                        "preset": preset_name,
                        "title_jp": it["title_jp"],
                        "price": None,
                        "vendor_created_at": existing_created,
                        "vendor_updated_at": existing_updated,
                    })
                    handle_listing_delete(conn, iid, vendor_name, "売り切れ")

                if rows:
                    upsert_vendor_items(conn, rows, now_jst())

            except Exception as e:
                print(f"[WARN] page error page={page_idx+1}: {e}", flush=True)

            page_idx += 1
            time.sleep(1)

    finally:
        if conn:
            try:
                conn.close()
                print(f"[DB] sold_rakuma connection closed.", flush=True)
            except Exception:
                pass

    print(
        f"[SCRAPE END][SOLD][RAKUMA] preset='{preset_name}' "
        f"pages={fetched_pages} items={fetched_items}",
        flush=True
    )

    return fetched_pages, fetched_items


# ============================================================
# Yahoo!フリマ（vendor_name='ＰａｙＰａｙフリマ'）1 preset 分
# Playwright/Selenium不使用: 検索APIをHTTP GETするのみ（apps/adapters/yahoo_fleamarket.py）。
# 検索の brandIds は子ブランドも返すため、保存する preset は商品のブランドから決める
# （resolve_item_preset。親ブランドの検索で見つかった Grand Seiko は グランドセイコーの preset で保存する）。
# preset の決まらない商品（FURLA配下のALBA など）は保存しない。
# ============================================================
YAHOO_PAGE_TARGET_SEC = 8.0


def load_yahoo_preset_map(conn) -> Dict[Tuple[int, int], str]:
    """{(Yahoo!フリマのブランドID, カテゴリーID): preset}。mst.presets_lookup の ＰａｙＰａｙフリマ 分。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT brand_id, category_id, preset FROM mst.presets_lookup WITH (NOLOCK) WHERE vendor_name = ?",
            yahoo.VENDOR_NAME,
        )
        return {(int(b), int(c)): p for b, c, p in cur.fetchall()}


def _yahoo_resolve_page(items, category_id: int, preset_map, job_preset: str):
    """商品ごとに preset を決める。(保存する商品と preset の組の一覧, 付け替え件数, 保存しない件数)"""
    kept = []
    moved = 0
    skipped = 0
    for it in items:
        preset, reason = yahoo.resolve_item_preset(it["brand_id"], it["title_jp"], category_id, preset_map)
        if preset is None:
            skipped += 1
            print(f"[YAHOO SKIP] {it['vendor_item_id']} {reason} brand={it['brand_name']} title={(it['title_jp'] or '')[:40]}", flush=True)
            continue
        if preset != job_preset:
            moved += 1
            print(f"[YAHOO PRESET] {it['vendor_item_id']} {job_preset} → {preset} ({reason}) brand={it['brand_name']}", flush=True)
        kept.append((it, preset))
    return kept, moved, skipped


def run_fetch_active_yahoo(payload: dict, job_id: int) -> Tuple[int, int]:
    print(f"[ENV] host={socket.gethostname()} pid={os.getpid()} SIMULATE={SIMULATE}", flush=True)

    preset = payload["preset"]
    vendor_name = payload["vendor_name"]
    mode = payload["mode"]
    low_usd_target = float(payload["low_usd_target"])
    high_usd_target = float(payload["high_usd_target"])
    category_id = int(payload["category_id"])
    min_cost, max_cost = compute_cost_range_jpy_from_usd_range(
        mode=mode, low_usd_target=low_usd_target, high_usd_target=high_usd_target,
    )

    print(f"[SCRAPE START][YAHOO] preset={preset} vendor={vendor_name} mode={mode} price={min_cost}〜{max_cost}", flush=True)

    total_items = 0
    page_idx = 0
    conn = get_sql_server_connection()

    try:
        preset_map = load_yahoo_preset_map(conn)
        while True:
            offset = page_idx * yahoo.SEARCH_PAGE_SIZE
            if offset >= yahoo.SEARCH_OFFSET_LIMIT:
                print(f"[STOP] offset上限 {yahoo.SEARCH_OFFSET_LIMIT}", flush=True)
                break
            write_status(job_id, page_idx + 1)
            page_start = time.time()
            params = yahoo.search_params(
                brand_id=payload["brand_id"], category_id=category_id, status="open",
                min_price=min_cost, max_price=max_cost, offset=offset,
            )
            items, total = yahoo.fetch_search_page(params)
            print(f"[PAGE {page_idx+1}] offset={offset} items={len(items)} total={total}", flush=True)

            if not items:
                break

            total_items += len(items)
            kept, moved, skipped = _yahoo_resolve_page(items, category_id, preset_map, preset)

            item_ids = [it["vendor_item_id"] for it, _ in kept]
            existing_dates = get_vendor_item_dates_batch(conn, vendor_name, item_ids)

            # 価格変化の検知（ラクマと同じく handle_price_change_side_effects を流用）
            old_price_map = get_vendor_item_prices_batch(conn, vendor_name, item_ids)
            cnt_changed = 0
            price_retry = {}
            for it, _ in kept:
                iid = it["vendor_item_id"]
                price = it["price"]
                old_price = old_price_map.get(iid)
                if price is not None and old_price is not None and old_price != price:
                    cnt_changed += 1
                    try:
                        handle_price_change_side_effects(
                            conn, iid, vendor_name, old_price, price,
                            mode=mode, low_usd_target=low_usd_target, high_usd_target=high_usd_target,
                        )
                    except RuntimeError as e:
                        print(f"[PRICE_RETRY_PENDING] sku={iid} {old_price} -> {price} JPY: {e}", flush=True)
                        price_retry[iid] = old_price

            rows = []
            for it, item_preset in kept:
                iid = it["vendor_item_id"]
                existing_created, existing_updated = existing_dates.get(iid, (None, None))
                rows.append({
                    "vendor_name": vendor_name,
                    "vendor_item_id": iid,
                    "status": "販売中",
                    "preset": item_preset,
                    "title_jp": it["title_jp"],
                    "vendor_page": page_idx + 1,
                    "price": price_retry.get(iid, it["price"]),
                    # 検索結果に更新日時は無い。既存行の値を保持し、新規行は出品時の詳細取得で埋める（ラクマと同じ）
                    "vendor_created_at": existing_created,
                    "vendor_updated_at": existing_updated,
                    "item_condition_id": it["item_condition_id"],
                })

            now = now_jst()
            upsert_vendor_items(conn, rows, now)
            print(
                f"[PAGE {page_idx+1} RESULT] upserted={len(rows)} preset付け替え={moved} 保存しない={skipped} "
                f"price changed={cnt_changed}",
                flush=True,
            )

            page_idx += 1
            if len(items) < yahoo.SEARCH_PAGE_SIZE:
                break

            elapsed = time.time() - page_start
            if elapsed < YAHOO_PAGE_TARGET_SEC:
                time.sleep((YAHOO_PAGE_TARGET_SEC - elapsed) + random.uniform(0.0, 5.0))

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    print(f"[SCRAPE END][YAHOO] preset={preset}", flush=True)
    return page_idx, total_items


def run_fetch_sold_yahoo(payload: dict, job_id: int) -> Tuple[int, int]:
    preset_name = payload["preset"]
    vendor_name = payload["vendor_name"]
    mode = payload.get("mode", "DDP")
    category_id = int(payload["category_id"])
    min_cost, max_cost = compute_cost_range_jpy_from_usd_range(
        mode=mode,
        low_usd_target=float(payload["low_usd_target"]),
        high_usd_target=float(payload["high_usd_target"]),
    )

    print(f"[SCRAPE START][SOLD][YAHOO] preset='{preset_name}' vendor='{vendor_name}' mode='{mode}'", flush=True)

    fetched_pages = 0
    fetched_items = 0
    conn = get_sql_server_connection()

    try:
        preset_map = load_yahoo_preset_map(conn)
        for page_idx in range(SOLD_MAX_PAGES):
            write_status(job_id, page_idx + 1)
            params = yahoo.search_params(
                brand_id=payload["brand_id"], category_id=category_id, status="sold",
                min_price=min_cost, max_price=max_cost, offset=page_idx * yahoo.SEARCH_PAGE_SIZE,
            )
            # 取得の失敗はジョブの失敗としてそのまま送出する（error はサイクル終了時に通知される）
            items, total = yahoo.fetch_search_page(params)
            fetched_pages += 1
            print(f"[PAGE {page_idx+1}] scraped={len(items)} total={total}", flush=True)
            if not items:
                break

            kept, _, _ = _yahoo_resolve_page(items, category_id, preset_map, preset_name)
            fetched_items += len(kept)

            existing_dates = get_vendor_item_dates_batch(conn, vendor_name, [it["vendor_item_id"] for it, _ in kept])
            rows = []
            for it, item_preset in kept:
                iid = it["vendor_item_id"]
                existing_created, existing_updated = existing_dates.get(iid, (None, None))
                rows.append({
                    "vendor_name": vendor_name,
                    "vendor_item_id": iid,
                    "status": "売り切れ",
                    "preset": item_preset,
                    "title_jp": it["title_jp"],
                    "price": None,
                    "vendor_created_at": existing_created,
                    "vendor_updated_at": existing_updated,
                })
                handle_listing_delete(conn, iid, vendor_name, "売り切れ")
            if rows:
                upsert_vendor_items(conn, rows, now_jst())

            if len(items) < yahoo.SEARCH_PAGE_SIZE:
                break
            time.sleep(1)

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    print(f"[SCRAPE END][SOLD][YAHOO] preset='{preset_name}' pages={fetched_pages} items={fetched_items}", flush=True)
    return fetched_pages, fetched_items


# メルカリ処理（ブラウザ・メルカリ検索API）へ渡してよい仕入先。これ以外の未知の仕入先はジョブを error にする
MERCARI_VENDOR_NAMES = ("メルカリ", "メルカリshops")


# =========================
# ブラウザ
# =========================
# 初回の起動も、25ジョブごと・異常時の作り直しも、同じ設定で起動する
BROWSER_LAUNCH_ARGS = [
    "--disable-dev-shm-usage",  # 共有メモリ不足によるクラッシュを防止
    "--disable-gpu",            # 不要なGPUプロセスの排除
    "--no-sandbox",             # VPS環境での安定性向上
]


def launch_browser(p):
    return p.chromium.launch(headless=True, args=BROWSER_LAUNCH_ARGS)


def open_browser_if_needed(p):
    """ブラウザ方式ならブラウザと検索ページを開く。api 方式では起動しない（None, None）。"""
    if use_search_api():
        return None, None
    browser = launch_browser(p)
    return browser, open_search_page(browser)


def open_search_page(browser):
    page = browser.new_page()
    page.set_default_timeout(10000)
    page.on("request", lambda r: "entities:search" in r.url and print("REQ:", r.url, flush=True))
    page.on("response", lambda r: "entities:search" in r.url and print("RES:", r.url, flush=True))
    return page


# =========================
# Worker main loop
# =========================
def main():
    print(f"[WORKER START] {WORKER_NAME}", flush=True)

    if CHECK_SWAP:
        warn_if_no_swap()

    print(f"[SEARCH MODE] {SEARCH_FETCH_MODE} (api使用={use_search_api()})", flush=True)

    # Playwright は worker lifetime で1回だけ起動
    with sync_playwright() as p:
        browser, page = open_browser_if_needed(p)
        browser_job_count = 0
        page_job_count = 0
        last_blocked_log = 0.0

        while True:
            # api 方式でアクセス制限の可能性（401・403・429）を受けた後は、人が確認するまでジョブを取らない
            if os.path.exists(SEARCH_API_BLOCKED_FILE):
                if time.time() - last_blocked_log >= 600:
                    print(f"[SEARCH_API][BLOCKED] {SEARCH_API_BLOCKED_FILE} があるためジョブを取らずに待機", flush=True)
                    last_blocked_log = time.time()
                time.sleep(60)
                continue

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

            for job_id, job_kind, job_payload, current_page in jobs:
                print(f"[JOB START] id={job_id} kind={job_kind}", flush=True)
                cur2 = None

                try:
                    payload = json.loads(job_payload)
                    start_page = current_page

                    job_vendor = payload.get("vendor_name")
                    if job_kind in ("fetch_active_ebay", "fetch_sold_ebay") and job_vendor not in (
                        "ラクマ", yahoo.VENDOR_NAME, *MERCARI_VENDOR_NAMES
                    ):
                        raise ValueError(f"未対応の仕入先: {job_vendor!r}（メルカリ処理には流さない）")

                    if job_kind == "fetch_active_ebay":
                        if job_vendor == "ラクマ":
                            fetched_pages, fetched_items = run_fetch_active_rakuma(payload, job_id)
                        elif job_vendor == yahoo.VENDOR_NAME:
                            fetched_pages, fetched_items = run_fetch_active_yahoo(payload, job_id)
                        else:
                            fetched_pages, fetched_items = run_fetch_active_ebay(page, start_page, payload, job_id)
                    elif job_kind == "fetch_sold_ebay":
                        if job_vendor == "ラクマ":
                            fetched_pages, fetched_items = run_fetch_sold_rakuma(payload, job_id)
                        elif job_vendor == yahoo.VENDOR_NAME:
                            fetched_pages, fetched_items = run_fetch_sold_yahoo(payload, job_id)
                        else:
                            fetched_pages, fetched_items = run_fetch_sold_ebay(page, start_page, payload, job_id)
                    else:
                        raise ValueError(f"unknown job_kind: {job_kind}")

                    cur2 = conn.cursor()
                    now = now_jst()
                    cur2.execute(SQL_MARK_DONE, now, fetched_pages, fetched_items, job_id)
                    conn.commit()
                    if os.path.exists(STATUS_FILE):
                        os.remove(STATUS_FILE)
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

                        browser, page = open_browser_if_needed(p)

                        # statusファイルは release_job() 成功後に消す。先に消すと、release_job() が
                        # DB接続断(08S01)で失敗してワーカーが落ちた際、ジョブが running のまま
                        # watchdog の監視対象からも外れて取り残される（2026-10-06〜10-16 に5件発生）。
                        release_job(conn, job_id)

                        if os.path.exists(STATUS_FILE):
                            os.remove(STATUS_FILE)
                        continue

                    raise

                except Exception:
                    err = traceback.format_exc()
                    print(err.encode("utf-8", errors="ignore").decode("utf-8"), flush=True)
                    try:
                        cur2 = conn.cursor()
                        now = now_jst()
                        cur2.execute(SQL_MARK_ERROR, now, err[-4000:], job_id)
                        conn.commit()
                        if os.path.exists(STATUS_FILE):
                            os.remove(STATUS_FILE)
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

                    page = open_search_page(browser) if browser is not None else None

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

                    browser, page = open_browser_if_needed(p)

                    browser_job_count = 0
                    page_job_count = 0

                if os.environ.get("ONESHOT") == "1":
                    return

if __name__ == "__main__":
    main()
