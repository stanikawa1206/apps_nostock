# -*- coding: utf-8 -*-
"""
trim_ebay_listings_30d_all.py

目的:
- trx.listings から「出品後30日以上」の listing を抽出し、eBayで終了（削除）する。
- eBay 成功分だけ SQL Server の trx.listings からも DELETE して整合性を維持。
- 518/429 レート制限に対して GlobalRateLimiter / GlobalCircuitBreaker / defer で保護する。
- 最後に "✅ 全体合計: XX 件削除" を必ず出力する（inventory_ebay_manager 側が拾う想定）。

前提:
- trx.listings は全て「出品中」であり start_time も必ず存在する（ユーザー前提）。
"""

import sys
from pathlib import Path
import time
import random
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection

from apps.adapters.ebay_api import (
    delete_items_from_ebay_batch,
)

# ===== 設定 =====

MAX_WORKERS      = 2        # 並列アカウント数
BATCH_SIZE       = 10       # EndItems 上限
BASE_SLEEP_SEC   = 0.60     # 通常の微小スリープ
BACKOFF_BASE_SEC = 60       # 518/429 での初期待機
BACKOFF_CAP_SEC  = 300      # 最大バックオフ 5分
API_CONCURRENCY  = 1        # API同時実行上限（全体）

DEFER_WINDOW_SEC = 1800     # 518/429発生 item_id は30分触らない

# 30日以上（固定）
DAYS_THRESHOLD = 30

# ===== グローバル制御 =====

_api_sem = threading.Semaphore(API_CONCURRENCY)


class GlobalRateLimiter:
    def __init__(self):
        self.lock = threading.Lock()
        self.backoff_until = 0.0
        self.backoff_sec   = BACKOFF_BASE_SEC

    def before_call(self):
        with self.lock:
            now = time.time()
            if now < self.backoff_until:
                time.sleep(self.backoff_until - now)
            time.sleep(BASE_SLEEP_SEC + random.uniform(0.0, 0.05))

    def on_518(self):
        with self.lock:
            now = time.time()
            self.backoff_until = max(self.backoff_until, now + self.backoff_sec)
            self.backoff_sec = min(self.backoff_sec * 2, BACKOFF_CAP_SEC)

    def on_success(self):
        with self.lock:
            self.backoff_sec = max(BACKOFF_BASE_SEC, self.backoff_sec * 0.75)


RATE_LIMITER = GlobalRateLimiter()


class GlobalCircuitBreaker:
    """
    短時間の 518 バーストで全体を一時停止するサーキットブレーカー。
    例: 5秒以内に4回 518 が出たらトリップ → 最大15分まで指数的に停止時間を延ばす。
    """
    def __init__(self, window_sec=5, burst_threshold=4, halt_sec=120):
        self.window_sec       = window_sec
        self.burst_threshold  = burst_threshold
        self.default_halt_sec = halt_sec
        self._hits            = deque()
        self._lock            = threading.Lock()
        self.halt_until       = 0.0
        self.trip_count       = 0

    def note_518(self):
        with self._lock:
            now = time.time()
            self._hits.append(now)
            while self._hits and (now - self._hits[0]) > self.window_sec:
                self._hits.popleft()

            if len(self._hits) >= self.burst_threshold:
                self.trip_count += 1
                dyn_halt = min(
                    self.default_halt_sec * (2 ** (self.trip_count - 1)),
                    900,
                )
                self.halt_until = max(self.halt_until, now + dyn_halt)
                return True
            return False

    def should_halt(self):
        now = time.time()
        if now >= self.halt_until and self.trip_count > 0:
            self.trip_count = max(0, self.trip_count - 1)
        return now < self.halt_until


CIRCUIT = GlobalCircuitBreaker()

# ===== defer（一定時間触らない item_id 管理） =====

_defer_until = {}           # item_id -> epoch
_defer_lock  = threading.Lock()


def _fmt(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def mark_defer(item_ids, sec=DEFER_WINDOW_SEC):
    until = time.time() + sec
    with _defer_lock:
        for iid in item_ids:
            _defer_until[str(iid)] = until
    if item_ids:
        print(f"⏸ defer: {len(item_ids)}件 → {_fmt(until)} 以後に再試行")


def is_deferred(iid) -> bool:
    with _defer_lock:
        t = _defer_until.get(str(iid), 0.0)
    return time.time() < t


# ===== SQL 操作 =====

SQL_SELECT_CANDIDATES_30D_ALL = f"""
SELECT
    [account],
    [listing_id]
FROM [trx].[listings]
WHERE DATEDIFF(day, CONVERT(date, [start_time]), CONVERT(date, GETDATE())) >= {DAYS_THRESHOLD}
ORDER BY [account], [start_time] ASC;
"""


def fetch_delete_candidates_30d_all():
    """
    trx.listings から「30日以上」の (account, listing_id) を全部取る。
    defer中の listing_id は除外する。
    """
    with get_sql_server_connection() as conn:
        cur = conn.cursor()
        cur.execute(SQL_SELECT_CANDIDATES_30D_ALL)
        rows = cur.fetchall()

    items = []
    for r in rows:
        acc = str(r[0])
        iid = str(r[1])
        if is_deferred(iid):
            continue
        items.append((acc, iid))
    return items


def delete_rows_from_sql(account: str, item_ids):
    """
    eBay 側で正常終了した listing_id のみ、
    trx.listings から DELETE する。
    """
    if not item_ids:
        return 0

    deleted = 0
    with get_sql_server_connection() as conn:
        cur = conn.cursor()
        for iid in item_ids:
            cur.execute("""
                DELETE FROM [trx].[listings]
                WHERE [account] = ? AND [listing_id] = ?
            """, account, iid)
            if cur.rowcount:
                deleted += cur.rowcount
        conn.commit()
    return deleted


# ===== eBay 呼び出し =====

def run_enditems_batch(account: str, batch_ids):
    """
    1バッチ（最大 BATCH_SIZE 件）分の EndItems を実行する。
    defer 中の item_id は事前に除外し、残りがなければ何もしない。
    """
    batch_ids = [iid for iid in batch_ids if not is_deferred(iid)]
    if not batch_ids:
        return {"ok_ids": [], "ng_ids": [], "rate_limited": False}

    RATE_LIMITER.before_call()
    with _api_sem:
        result = delete_items_from_ebay_batch(account, batch_ids)

    if not isinstance(result, dict):
        print(f"⚠️ {account}: 予期しない返却: {type(result)} -> {str(result)[:200]}")
        return {"ok_ids": [], "ng_ids": batch_ids, "rate_limited": False}

    res_list = result.get("results") or []
    ok_ids, ng_ids, rl_ids = [], [], []

    for r in res_list:
        iid = str(r.get("item_id"))
        if r.get("success"):
            ok_ids.append(iid)
        else:
            code = str(r.get("error_code") or "")
            if code in ("518", "429"):
                rl_ids.append(iid)
            else:
                ng_ids.append(iid)

    if rl_ids:
        mark_defer(rl_ids)
        RATE_LIMITER.on_518()
        if CIRCUIT.note_518():
            print("🧯 518バースト検出。全体を一時停止（短時間）…")

    rate_limited = bool(rl_ids)

    if ok_ids or ng_ids:
        RATE_LIMITER.on_success()

    return {
        "ok_ids": ok_ids,
        "ng_ids": ng_ids + rl_ids,
        "rate_limited": rate_limited,
    }


def delete_items_from_ebay_and_sql(account: str, item_ids):
    """
    1アカウント分:
    - 与えられた item_ids を BATCH_SIZE 件ずつ EndItems
    - 成功分だけ SQL DELETE
    - 518/429 が出たら「このアカウントの処理はいったん終了」
    """
    item_ids = [str(i) for i in item_ids if not is_deferred(i)]
    if not item_ids:
        print(f"🧾 {account}: 今回削除できる候補がありません（defer 中のみ or 対象なし）")
        return account, 0, False

    print(f"▶ {account}: {len(item_ids)}件 削除開始（BATCH_SIZE={BATCH_SIZE}）")

    deleted_total = 0
    idx = 0
    while idx < len(item_ids):
        if CIRCUIT.should_halt():
            remain = int(CIRCUIT.halt_until - time.time())
            if remain > 0:
                print(
                    f"⏸ {account}: レート保護のため {remain}s 停止中…"
                    f"（再開 {time.strftime('%H:%M:%S', time.localtime(CIRCUIT.halt_until))}）"
                )
                time.sleep(min(remain, 5))
            continue

        batch = item_ids[idx: idx + BATCH_SIZE]
        res = run_enditems_batch(account, batch)

        if res["rate_limited"]:
            print(f"⏹ {account}: レート上限のため、このアカウントの処理を一旦終了します。")
            return account, deleted_total, True

        if res["ok_ids"]:
            print(f"✅ {account}: eBay削除成功 listing_id:")
            for iid in res["ok_ids"]:
                print(f"    ✔ {iid}")

            n = delete_rows_from_sql(account, res["ok_ids"])
            print(f"✅ {account}: SQL削除 {n}件 完了")

            deleted_total += len(res["ok_ids"])

        if res["ng_ids"]:
            print(f"🚫 {account}: 失敗/保留 {len(res['ng_ids'])}件（例: {res['ng_ids'][:2]}…）")

        idx += BATCH_SIZE

    print(f"🧾 {account}: 合計 {deleted_total} 件削除完了")
    return account, deleted_total, False


# ===== メイン =====

def main():
    # 1) 30日以上の候補を全部取得（accountも一緒に）
    pairs = fetch_delete_candidates_30d_all()
    if not pairs:
        print("✅ 全体合計: 0 件削除")
        return

    # 2) account ごとにグループ化
    by_account = defaultdict(list)
    for acc, iid in pairs:
        by_account[acc].append(iid)

    print("🎯 30日以上：全消し 削除計画（account: 件数）")
    for acc, ids in by_account.items():
        print(f" - {acc}: {len(ids)} 件")

    workers = min(MAX_WORKERS, max(1, len(by_account)))
    total_deleted = 0
    limited_accounts = set()

    # 3) account 単位で並列（API同時実行は _api_sem で全体1に絞ってる）
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(delete_items_from_ebay_and_sql, acc, ids): acc
            for acc, ids in by_account.items()
        }
        for fut in as_completed(futures):
            acc = futures[fut]
            try:
                a, cnt, limited = fut.result()
                total_deleted += cnt
                if limited:
                    limited_accounts.add(a)
            except Exception as e:
                print(f"❌ {acc} の処理で例外: {e}")

    if limited_accounts:
        print(f"🛑 レート上限/スパイク発生: {', '.join(limited_accounts)}（再実行で続行）")

    # inventory_ebay_manager が拾う想定の行
    print(f"✅ 全体合計: {total_deleted} 件削除")


if __name__ == "__main__":
    main()
