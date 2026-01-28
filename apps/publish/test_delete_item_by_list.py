# -*- coding: utf-8 -*-
"""
trim_ebay_listings_manual_list.py

目的:
- 与えられた (account, listing_id) のリストだけを対象に
  eBay出品を終了（取消）し、成功分だけ trx.listings からも DELETE する。
- まずは N=1 で先頭1件だけテストし、OKなら N を増やして本番にする。
- 518/429 レート制限に対して GlobalRateLimiter / GlobalCircuitBreaker / defer で保護。
- 最後に "✅ 全体合計: XX 件削除" を必ず出力（inventory_ebay_manager 側が拾う想定）。
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
from apps.adapters.ebay_api import delete_items_from_ebay_batch

# ===== テスト設定 =====
N = 100                 # ★まずは1件。OKなら増やす
MAX_WORKERS      = 2
BATCH_SIZE       = 10
BASE_SLEEP_SEC   = 0.60
BACKOFF_BASE_SEC = 60
BACKOFF_CAP_SEC  = 300
API_CONCURRENCY  = 1
DEFER_WINDOW_SEC = 1800

# ===== 対象リスト（提示された順序を保持）=====
TARGET_PAIRS_ALL = [
    ("谷川④", "136958420404"),
    ("谷川④", "136958400298"),
    ("谷川③", "227160681874"),
    ("谷川③", "227160678122"),
    ("谷川③", "227160641087"),
    ("谷川④", "136917130072"),
    ("谷川④", "136917004474"),
    ("谷川④", "136917003603"),
    ("谷川③", "227159301976"),
    ("谷川③", "227159301749"),
    ("谷川③", "227159300999"),
    ("谷川③", "227159291432"),
    ("谷川③", "227159272807"),
    ("谷川③", "227159226639"),
    ("谷川③", "227156367078"),
    ("谷川③", "227153718524"),
    ("川島", "236554828509"),
    ("BUZZ", "358040101914"),
    ("BUZZ", "358040097047"),
    ("川島", "236538205140"),
    ("川島", "236538202234"),
    ("川島", "236538199507"),
    ("川島", "236538192133"),
    ("川島", "236538183152"),
    ("川島", "236538170497"),
    ("川島", "236538161179"),
    ("川島", "236538160866"),
    ("川島", "236538124467"),
    ("川島", "236538119209"),
    ("川島", "236538111860"),
    ("川島", "236538110392"),
    ("川島", "236538109754"),
    ("川島", "236538098478"),
    ("BUZZ", "358037233915"),
    ("BUZZ", "358037207717"),
    ("BUZZ", "358037192479"),
    ("BUZZ", "358037180959"),
    ("BUZZ", "358037178073"),
    ("BUZZ", "358037156023"),
    ("BUZZ", "358037137568"),
]

# ★テスト対象（先頭からN件）
TARGET_PAIRS = TARGET_PAIRS_ALL[: max(0, int(N))]

# ===== グローバル制御 =====
_api_sem = threading.Semaphore(API_CONCURRENCY)


class GlobalRateLimiter:
    def __init__(self):
        self.lock = threading.Lock()
        self.backoff_until = 0.0
        self.backoff_sec = BACKOFF_BASE_SEC

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
    例: 5秒以内に4回 518 が出たらトリップ → 最大15分まで指数的に停止
    """
    def __init__(self, window_sec=5, burst_threshold=4, halt_sec=120):
        self.window_sec = window_sec
        self.burst_threshold = burst_threshold
        self.default_halt_sec = halt_sec
        self._hits = deque()
        self._lock = threading.Lock()
        self.halt_until = 0.0
        self.trip_count = 0

    def note_518(self):
        with self._lock:
            now = time.time()
            self._hits.append(now)
            while self._hits and (now - self._hits[0]) > self.window_sec:
                self._hits.popleft()

            if len(self._hits) >= self.burst_threshold:
                self.trip_count += 1
                dyn_halt = min(self.default_halt_sec * (2 ** (self.trip_count - 1)), 900)
                self.halt_until = max(self.halt_until, now + dyn_halt)
                return True
            return False

    def should_halt(self):
        now = time.time()
        if now >= self.halt_until and self.trip_count > 0:
            self.trip_count = max(0, self.trip_count - 1)
        return now < self.halt_until


CIRCUIT = GlobalCircuitBreaker()

# ===== defer =====
_defer_until = {}
_defer_lock = threading.Lock()


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


# ===== SQL操作 =====
def delete_rows_from_sql(account: str, item_ids):
    """
    eBay 側で正常終了した listing_id のみ trx.listings から DELETE
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


# ===== eBay呼び出し =====
def run_enditems_batch(account: str, batch_ids):
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
    item_ids = [str(i) for i in item_ids if not is_deferred(i)]
    if not item_ids:
        print(f"🧾 {account}: 今回削除できる候補がありません（defer中のみ）")
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
    if not TARGET_PAIRS:
        print("✅ 全体合計: 0 件削除")
        return

    # account ごとにグループ化（ただし対象は先頭N件）
    by_account = defaultdict(list)
    for acc, iid in TARGET_PAIRS:
        by_account[str(acc)].append(str(iid))

    print(f"🧪 手動リスト削除テスト: N={len(TARGET_PAIRS)}（先頭から）")
    print("🎯 削除計画（account: 件数）")
    for acc, ids in by_account.items():
        print(f" - {acc}: {len(ids)} 件")

    workers = min(MAX_WORKERS, max(1, len(by_account)))
    total_deleted = 0
    limited_accounts = set()

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

    print(f"✅ 全体合計: {total_deleted} 件削除")


if __name__ == "__main__":
    main()
