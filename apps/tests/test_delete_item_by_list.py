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
    ("BUZZ", "m59722205200"),
    ("BUZZ", "2JJ6Szgd2yGqZiYFL3Ebhp"),
    ("BUZZ", "m45459538483"),
    ("BUZZ", "m54993878199"),
    ("BUZZ", "m69992078935"),
    ("BUZZ", "m89064091879"),
    ("BUZZ", "m85171964841"),
    ("BUZZ", "m23833899969"),
    ("BUZZ", "m58943249117"),
    ("BUZZ", "m28167599794"),
    ("BUZZ", "m75503480883"),
    ("BUZZ", "m91215913404"),
    ("BUZZ", "m15729190120"),
    ("BUZZ", "m17865994757"),
    ("BUZZ", "m24336840109"),
    ("BUZZ", "m25917484359"),

    ("貴文", "m30078465550"),
    ("貴文", "m33876153072"),
    ("貴文", "m33998293517"),
    ("貴文", "m38307154122"),
    ("貴文", "m38909028323"),
    ("貴文", "m20444952720"),
    ("貴文", "m20479452643"),
    ("貴文", "2JNiA9rAg6ZNoZf4XfGHE5"),
    ("貴文", "2JNKbKENsheizFwQ3am3gu"),
    ("貴文", "m12300566416"),
    ("貴文", "m47741563589"),
    ("貴文", "m54208383172"),
    ("貴文", "m71001578512"),
    ("貴文", "m83196205644"),
    ("貴文", "m85673557498"),
    ("貴文", "m86294600022"),
    ("貴文", "2JMuP6tGEzA8NWVNSdnKwR"),
    ("貴文", "2JNwCMouDXYDUCNUmW8Jv7"),
    ("貴文", "2JNNNq5PGUQEwXeX5zqbEV"),
    ("貴文", "2JNofd8tBAUEuGnWkwWzge"),
    ("貴文", "2JNofdEsBwy87z3ms2GsF3"),
    ("貴文", "2JNokqmgrcmbaXU3xGQX6v"),
    ("貴文", "2JNp7P92qB2omBKFm4HxA4"),
    ("貴文", "2JNr3in8bGN62YVmKjYMzE"),
    ("貴文", "2JNrSjQWQnJkn4ykMdzChZ"),
    ("貴文", "2JNthUSRj5AfgBE7Uwwoc9"),
    ("貴文", "2JNVLfajLqmqapgkdZo8x4"),
    ("貴文", "2JNBxWYLC86cZ83YVSHwDp"),
    ("貴文", "2JNcxrG3DjjS6TdDHhkLfo"),
    ("貴文", "2JNEaGJJVJekQfCxN6ENat"),
    ("貴文", "2JNfizrzBdghtq9hec6uQv"),
    ("貴文", "2JNfv58ytPa8fJTzJvR8vd"),
    ("貴文", "2JNfwKjSJyRUfnESyUgmEb"),
    ("貴文", "2JNg39oXvBhq5qZfwV2Nzj"),
    ("貴文", "2JNb7eGXYdA3cipsuo3DrG"),
    ("貴文", "2JNKkz2yfvLFQMYoyhFfKJ"),
    ("貴文", "2JNm7gFPmtSRXkaiSicSGf"),
    ("貴文", "2JNmCU2RmjpAWic5oNYxE7"),
    ("貴文", "2JMgVHdWs8gJezpHgJWR9r"),
    ("貴文", "m34970297473"),
    ("貴文", "m65738319454"),
    ("貴文", "m31755395331"),
    ("貴文", "m50036765713"),
    ("貴文", "m11724872872"),
    ("貴文", "m62713725307"),
    ("貴文", "m19319499705"),
    ("貴文", "m14076941054"),
    ("貴文", "m59963540962"),
    ("貴文", "2JNw7nE6yAvHNG6dCdgpSR"),
    ("貴文", "m60395431461"),
    ("貴文", "m76882537073"),
    ("貴文", "m90563147680"),
    ("貴文", "m97335397983"),
    ("貴文", "m98474558858"),
    ("貴文", "m46654396426"),
    ("貴文", "2JN4NubBytmPah85t4gqHG"),
    ("貴文", "2JNp2EtQJBTy5oZ9eaUtSK"),
    ("貴文", "2JNrUoLJK5xWEHwviSdmGR"),
    ("貴文", "2JNrVXv5vrUwGcdpVNqdFe"),
    ("貴文", "2JNrWhrmnwoj6AhXg5jNNJ"),
    ("貴文", "m60865333539"),
    ("貴文", "m63557527306"),

    ("貴文②", "m13320741730"),
    ("貴文②", "m13378901326"),
    ("貴文②", "2JML3ujjKTbPktba2tvrtA"),
    ("貴文②", "m71651592732"),
    ("貴文②", "m15976351479"),
    ("貴文②", "m59947329488"),
    ("貴文②", "m94917673924"),
    ("貴文②", "m21559749059"),
    ("貴文②", "m11352333397"),
    ("貴文②", "m13649049712"),
    ("貴文②", "m15770325821"),
    ("貴文②", "m17197930826"),
    ("貴文②", "m23304031793"),
    ("貴文②", "m23720625465"),
    ("貴文②", "m25542421462"),
    ("貴文②", "m26589070009"),
    ("貴文②", "m30017141687"),
    ("貴文②", "m30618786300"),
    ("貴文②", "m45264895117"),
    ("貴文②", "m45393776200"),
    ("貴文②", "m53910390656"),
    ("貴文②", "m55713692768"),
    ("貴文②", "m60338918947"),
    ("貴文②", "m65434702522"),
    ("貴文②", "m65482621077"),
    ("貴文②", "m67202087887"),
    ("貴文②", "m69172651394"),
    ("貴文②", "m72335835950"),
    ("貴文②", "m75922254302"),
    ("貴文②", "m77813770983"),
    ("貴文②", "m85489075023"),
    ("貴文②", "m86485581142"),
    ("貴文②", "m88232429756"),
    ("貴文②", "m89088286314"),
    ("貴文②", "m89524763383"),
    ("貴文②", "m95633534028"),
    ("貴文②", "m97326672254"),
    ("貴文②", "m13956727192"),
    ("貴文②", "m41542388241"),
    ("貴文②", "m98376094191"),
    ("貴文②", "m56970445176"),
    ("貴文②", "m15485322704"),
    ("貴文②", "m78601568183"),

    ("川島", "m70067586538"),
    ("川島", "m80142253911"),
    ("川島", "m22065688614"),
    ("川島", "m82497359172"),
    ("川島", "2JKrfGqtAZRmT9YVs4dEHi"),
    ("川島", "m10985034379"),

    ("谷川②", "2JNY677jRTDbsKUERantjH"),
    ("谷川②", "2JNVRwZsp8wcFP6HF5XtVd"),
    ("谷川②", "2JNaSuVYZyBoz6qkCdghJh"),
    ("谷川②", "m88075041811"),
    ("谷川②", "m73319292521"),
    ("谷川②", "m73658833118"),
    ("谷川②", "m74177118694"),
    ("谷川②", "m56091260545"),
    ("谷川②", "m41368389238"),
    ("谷川②", "m45816999313"),
    ("谷川②", "m47456933179"),
    ("谷川②", "m13236284931"),
    ("谷川②", "m13735180267"),
    ("谷川②", "m17153482012"),
    ("谷川②", "m88420969424"),
    ("谷川②", "m66391174585"),

    ("谷川③", "2JNGav2Ki9jjF5zgPo55GQ"),
    ("谷川③", "m99644921308"),
    ("谷川③", "2JN42JQDBz5xAYN5JRVf6e"),
    ("谷川③", "m56205243698"),
    ("谷川③", "m48589590425"),
    ("谷川③", "2JNKe5qoFBDoPRPYCcV2U5"),
    ("谷川③", "m39071937840"),
    ("谷川③", "m50406454788"),
    ("谷川③", "m51110226080"),
    ("谷川③", "m53032307157"),
    ("谷川③", "2JNBCEabGqpk32Txpk9Yqw"),
    ("谷川③", "2JNMwTFCsewzHSymjzAtA8"),
    ("谷川③", "2JNHjGeMMrdjHvNc88aZyg"),

    ("谷川④", "m28318836402"),
    ("谷川④", "m98663667061"),
    ("谷川④", "m31045060675"),
    ("谷川④", "2JN7NkkGLprfZXKK3EFAqP"),
    ("谷川④", "2JMP5Me2CBTfAmXGTuJAxR"),
    ("谷川④", "m31604558830"),
    ("谷川④", "m27459237472"),
    ("谷川④", "m35995650082"),
    ("谷川④", "m99371877862"),
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
                UPDATE [trx].[listings]
                SET 
                    [is_deleted] = 1,
                    [deleted_at] = SYSDATETIME(),
                    [delete_reason] = N'特別'
                WHERE 
                    [account] = ? 
                    AND [listing_id] = ?                        
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
