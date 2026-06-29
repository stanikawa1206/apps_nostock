# -*- coding: utf-8 -*-
"""
trim_ebay_listings_manual_list.py

仕様:
- TARGET_SKUS_RAW に SKU を貼るだけ
- trx.listings から account, listing_id を逆引き
  (eBay の EndItems API には listing_id（eBay側の実ItemID）を渡す。
   vendor_item_id は出品者側SKUであり eBay の ItemID ではないため、
   削除APIにはそのまま渡さない)
- eBay削除成功後:
    1. trx.listings.is_deleted = 1   (listing_id で更新)
    2. trx.vendor_item.出品不可flg = 1  (vendor_item_id で更新)

前提:
- vendor_item_id は trx.listings 上で1件のみ
- trx.vendor_item も1件のみ
"""

import sys
from pathlib import Path
import time
import random
import threading
import re
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import delete_items_from_ebay_batch

# =========================================================
# 入力
# =========================================================

TARGET_SKUS_RAW = """
m25328293536
m68870266797
"""

TARGET_SKUS = [
    line.strip()
    for line in TARGET_SKUS_RAW.splitlines()
    if line.strip()
]

# =========================================================
# 設定
# =========================================================

N = 700

MAX_WORKERS = 2
BATCH_SIZE = 10

BASE_SLEEP_SEC = 0.60

BACKOFF_BASE_SEC = 60
BACKOFF_CAP_SEC = 300

API_CONCURRENCY = 1

DEFER_WINDOW_SEC = 1800

# =========================================================
# グローバル制御
# =========================================================

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

            self.backoff_until = max(
                self.backoff_until,
                now + self.backoff_sec
            )

            self.backoff_sec = min(
                self.backoff_sec * 2,
                BACKOFF_CAP_SEC
            )

    def on_success(self):

        with self.lock:

            self.backoff_sec = max(
                BACKOFF_BASE_SEC,
                self.backoff_sec * 0.75
            )


RATE_LIMITER = GlobalRateLimiter()


class GlobalCircuitBreaker:

    def __init__(
        self,
        window_sec=5,
        burst_threshold=4,
        halt_sec=120
    ):
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

            while self._hits and (
                now - self._hits[0]
            ) > self.window_sec:

                self._hits.popleft()

            if len(self._hits) >= self.burst_threshold:

                self.trip_count += 1

                dyn_halt = min(
                    self.default_halt_sec * (
                        2 ** (self.trip_count - 1)
                    ),
                    900
                )

                self.halt_until = max(
                    self.halt_until,
                    now + dyn_halt
                )

                return True

            return False

    def should_halt(self):

        now = time.time()

        if now >= self.halt_until and self.trip_count > 0:
            self.trip_count = max(0, self.trip_count - 1)

        return now < self.halt_until


CIRCUIT = GlobalCircuitBreaker()

# =========================================================
# defer
# =========================================================

_defer_until = {}
_defer_lock = threading.Lock()


def _fmt(ts: float) -> str:

    return time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(ts)
    )


def mark_defer(item_ids, sec=DEFER_WINDOW_SEC):

    until = time.time() + sec

    with _defer_lock:

        for iid in item_ids:
            _defer_until[str(iid)] = until

    if item_ids:

        print(
            f"⏸ defer: {len(item_ids)}件 → "
            f"{_fmt(until)} 以後に再試行"
        )


def is_deferred(iid) -> bool:

    with _defer_lock:
        t = _defer_until.get(str(iid), 0.0)

    return time.time() < t

# =========================================================
# DB
# =========================================================


def get_target_pairs():

    """
    TARGET_SKUS を元に
    trx.listings から
    account,
    listing_id,   (eBay側の実ItemID。EndItems APIに渡すのはこちら)
    vendor_item_id  (出品者側SKU。DB更新キーとしてのみ使う)
    を取得
    """

    if not TARGET_SKUS:
        return []

    pairs = []

    with get_sql_server_connection() as conn:

        cur = conn.cursor()

        for sku in TARGET_SKUS[:N]:

            cur.execute("""
                SELECT
                    account,
                    listing_id,
                    vendor_item_id
                FROM trx.listings
                WHERE vendor_item_id = ?
                AND is_deleted = 0
            """, sku)

            row = cur.fetchone()

            if not row:

                print(f"⚠ listings未存在: {sku}")
                continue

            account = str(row[0])
            listing_id = str(row[1])
            vendor_item_id = str(row[2])

            pairs.append(
                (account, listing_id, vendor_item_id)
            )

    return pairs


def delete_rows_from_sql(account: str, pairs):

    """
    pairs: [(listing_id, vendor_item_id), ...]
    eBay側で削除済みと判定された listing_id に対して、
    trx.listings は listing_id で、trx.vendor_item は vendor_item_id で更新する。
    """

    if not pairs:
        return 0

    deleted = 0

    with get_sql_server_connection() as conn:

        cur = conn.cursor()

        for listing_id, vendor_item_id in pairs:

            # =================================================
            # trx.listings
            # =================================================

            cur.execute("""
                UPDATE trx.listings
                SET
                    is_deleted = 1,
                    deleted_at = SYSDATETIME(),
                    delete_reason = N'特別'
                WHERE account = ?
                AND listing_id = ?
                AND is_deleted = 0
            """, account, listing_id)

            deleted += cur.rowcount

            # =================================================
            # trx.vendor_item
            # =================================================

            cur.execute("""
                UPDATE trx.vendor_item
                SET 出品不可flg = 1
                WHERE vendor_item_id = ?
            """, vendor_item_id)

        conn.commit()

    return deleted

# =========================================================
# eBay
# =========================================================


def run_enditems_batch(account: str, batch_ids):

    batch_ids = [
        iid
        for iid in batch_ids
        if not is_deferred(iid)
    ]

    if not batch_ids:
        return {
            "ok_ids": [],
            "ng_ids": [],
            "rate_limited": False
        }

    print(
        f"📤 {account}: "
        f"eBayへ渡すID(listing_id)={batch_ids}"
    )

    RATE_LIMITER.before_call()

    with _api_sem:

        result = delete_items_from_ebay_batch(
            account,
            batch_ids
        )

    if not isinstance(result, dict):

        print(
            f"⚠️ {account}: 予期しない返却: "
            f"{type(result)}"
        )

        return {
            "ok_ids": [],
            "ng_ids": batch_ids,
            "rate_limited": False
        }

    res_list = result.get("results") or []

    ok_ids = []
    ng_ids = []
    rl_ids = []

    for r in res_list:

        iid = str(r.get("item_id"))

        if r.get("success"):

            ok_ids.append(iid)

        else:

            code = str(r.get("error_code") or "")
            message = str(r.get("error_message") or "")

            print(
                f"DEBUG: "
                f"listing_id={iid}, "
                f"code={code}, "
                f"message={message}"
            )

            # 注意: code "37" は eBay公式定義では
            # "Input data is invalid"（入力データ不正）であり、
            # 「既に終了済み」ではない。
            # 以前はここで成功扱いしていたため、誤った listing_id
            # (=SKUを渡してしまうバグ) を送っても DB だけ
            # is_deleted=1 になり、eBay側の出品は残るという
            # 不整合が発生していた。成功扱いにしない。
            if code in ("518", "429"):

                rl_ids.append(iid)

            else:

                ng_ids.append(iid)

    if rl_ids:

        mark_defer(rl_ids)

        RATE_LIMITER.on_518()

        if CIRCUIT.note_518():

            print(
                "🧯 518バースト検出"
            )

    rate_limited = bool(rl_ids)

    if ok_ids or ng_ids:
        RATE_LIMITER.on_success()

    return {
        "ok_ids": ok_ids,
        "ng_ids": ng_ids + rl_ids,
        "rate_limited": rate_limited,
    }


def delete_items_from_ebay_and_sql(
    account: str,
    items
):

    """
    items: [(listing_id, vendor_item_id), ...]
    eBayへ渡すのは listing_id。vendor_item_id は
    trx.vendor_item 更新のために listing_id と紐付けて保持する。
    """

    sku_by_listing_id = {
        str(listing_id): str(vendor_item_id)
        for listing_id, vendor_item_id in items
    }

    item_ids = [
        listing_id
        for listing_id in sku_by_listing_id.keys()
        if not is_deferred(listing_id)
    ]

    if not item_ids:

        print(
            f"🧾 {account}: "
            f"defer中のみ"
        )

        return account, 0, False

    print(
        f"▶ {account}: "
        f"{len(item_ids)}件 削除開始 "
        f"(listing_id={item_ids})"
    )

    deleted_total = 0

    idx = 0

    while idx < len(item_ids):

        if CIRCUIT.should_halt():

            remain = int(
                CIRCUIT.halt_until - time.time()
            )

            if remain > 0:

                print(
                    f"⏸ {account}: "
                    f"{remain}s 停止中"
                )

                time.sleep(min(remain, 5))

            continue

        batch = item_ids[
            idx: idx + BATCH_SIZE
        ]

        res = run_enditems_batch(
            account,
            batch
        )

        if res["rate_limited"]:

            print(
                f"⏹ {account}: "
                f"レート上限"
            )

            return account, deleted_total, True

        if res["ok_ids"]:

            print(
                f"✅ {account}: "
                f"eBay削除成功"
            )

            for iid in res["ok_ids"]:

                print(
                    f"    ✔ listing_id={iid} "
                    f"(sku={sku_by_listing_id.get(iid, '?')})"
                )

            ok_pairs = [
                (iid, sku_by_listing_id.get(iid))
                for iid in res["ok_ids"]
            ]

            n = delete_rows_from_sql(
                account,
                ok_pairs
            )

            print(
                f"✅ {account}: "
                f"SQL更新 {n}件"
            )

            deleted_total += len(res["ok_ids"])

        if res["ng_ids"]:

            print(
                f"🚫 {account}: "
                f"{len(res['ng_ids'])}件失敗"
            )

        idx += BATCH_SIZE

    print(
        f"🧾 {account}: "
        f"合計 {deleted_total} 件"
    )

    return account, deleted_total, False

# =========================================================
# main
# =========================================================


def main():

    target_pairs = get_target_pairs()

    if not target_pairs:

        print("✅ 全体合計: 0 件削除")
        return

    by_account = defaultdict(list)

    for acc, listing_id, vendor_item_id in target_pairs:

        by_account[str(acc)].append(
            (str(listing_id), str(vendor_item_id))
        )

    print(
        f"🧪 手動削除"
    )

    print(
        "🎯 削除計画"
    )

    for acc, ids in by_account.items():

        print(
            f" - {acc}: "
            f"{len(ids)}件"
        )

    workers = min(
        MAX_WORKERS,
        max(1, len(by_account))
    )

    total_deleted = 0

    limited_accounts = set()

    with ThreadPoolExecutor(
        max_workers=workers
    ) as ex:

        futures = {

            ex.submit(
                delete_items_from_ebay_and_sql,
                acc,
                ids
            ): acc

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

                print(
                    f"❌ {acc}: {e}"
                )

    if limited_accounts:

        print(
            f"🛑 レート上限:"
            f"{','.join(limited_accounts)}"
        )

    print(
        f"✅ 全体合計: "
        f"{total_deleted} 件削除"
    )


if __name__ == "__main__":
    main()