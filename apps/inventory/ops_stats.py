# -*- coding: utf-8 -*-
"""
ops_stats.py — 在庫管理の実績集計（trx.inventory_ops_stats）と異常明細（trx.inventory_ops_issue）の記録（2026-11-19）。

  - count(): 処理1件ごとに呼ぶ。プロセスのメモリで (種類, 仕入先, アカウント, 結果, 原因) ごとに数え、
    FLUSH_EVERY_SEC ごと（と終了時）にまとめて INSERT する。全商品の明細は書かない。
  - issue(): 異常（出品終了の失敗・判定不可など）の明細を、その場で1行 MERGE する（未解決の間は同じ行を更新）。
  - note_cause() / note_retry(): 判定不可の原因・retry の有無を、判定の途中から count() に渡す。

記録の失敗（表が無い・DB 接続失敗など）は1行ログに出すだけで、在庫管理・価格更新・出品終了の処理は止めない。
環境変数 OPS_STATS_DISABLED=1 の時は DB に書かない（apps/tests/__init__.py が設定する。テストで本番DBに書かないため）。
時刻は DB サーバーの SYSDATETIME() を10分単位に切り捨てて使う（台ごとの時計の差に依存しない）。
"""
from __future__ import annotations

import atexit
import os
import time
from collections import Counter
from typing import Optional

from apps.adapters.rakuma_outage import worker_host
from apps.common.utils import get_sql_server_connection

FLUSH_EVERY_SEC = 300
BUCKET_SQL = "DATEADD(MINUTE, DATEDIFF(MINUTE, 0, SYSDATETIME()) / 10 * 10, 0)"

_counts: Counter = Counter()          # key -> [items, attempts, retried] は3つの Counter で持つ
_attempts: Counter = Counter()
_retried: Counter = Counter()
_last_flush = time.time()
_pending_cause: Optional[str] = None
_pending_retries = 0


def note_cause(cause: str) -> None:
    """判定不可などの原因を、次の count() に渡す（同じ処理の中で判定より後に count() を呼ぶこと）。"""
    global _pending_cause
    _pending_cause = cause


def note_retry() -> None:
    """仕入先・eBay への retry を1回数え、次の count() に渡す。"""
    global _pending_retries
    _pending_retries += 1


def take_pending() -> tuple[Optional[str], int]:
    global _pending_cause, _pending_retries
    cause, retries = _pending_cause, _pending_retries
    _pending_cause, _pending_retries = None, 0
    return cause, retries


def count(kind: str, vendor_name: Optional[str], result: str, cause: Optional[str] = None,
          account: Optional[str] = None, attempts: int = 1, retried: int = 0) -> None:
    key = (kind, vendor_name, account, result, (cause or None) and str(cause)[:100])
    _counts[key] += 1
    _attempts[key] += attempts
    _retried[key] += retried
    if time.time() - _last_flush >= FLUSH_EVERY_SEC:
        flush()


def flush() -> None:
    """数えた分を INSERT して数え直す。失敗したらログに出して捨てる（在庫管理は止めない）。"""
    global _last_flush
    _last_flush = time.time()
    if not _counts or os.environ.get("OPS_STATS_DISABLED") == "1":
        return
    rows = [(worker_host(), k[0], k[1], k[2], k[3], k[4], n, _attempts[k], _retried[k]) for k, n in _counts.items()]
    _counts.clear()
    _attempts.clear()
    _retried.clear()
    try:
        conn = get_sql_server_connection()
        try:
            cur = conn.cursor()
            cur.fast_executemany = True
            cur.executemany(
                f"INSERT INTO trx.inventory_ops_stats (bucket_at, host, kind, vendor_name, account, result, cause, "
                f"items, attempts, retried) VALUES ({BUCKET_SQL}, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"[OPS_STATS] 集計の記録に失敗（処理は続ける） rows={len(rows)} {type(e).__name__}: {e}", flush=True)


atexit.register(flush)


ISSUE_MERGE_SQL = """
MERGE trx.inventory_ops_issue WITH (HOLDLOCK) AS t
USING (SELECT ? AS issue_kind, ? AS vendor_name, ? AS vendor_item_id) AS s
   ON t.issue_kind = s.issue_kind AND t.vendor_name = s.vendor_name
  AND t.vendor_item_id = s.vendor_item_id AND t.resolved_at IS NULL
WHEN MATCHED THEN UPDATE SET
    account = COALESCE(?, t.account), listing_id = COALESCE(?, t.listing_id), cause = ?,
    last_at = SYSDATETIME(), occurrences = t.occurrences + 1
WHEN NOT MATCHED THEN INSERT
    (issue_kind, vendor_name, vendor_item_id, account, listing_id, cause, first_at, last_at, occurrences)
    VALUES (s.issue_kind, s.vendor_name, s.vendor_item_id, ?, ?, ?, SYSDATETIME(), SYSDATETIME(), 1);
"""


def issue(issue_kind: str, vendor_name: str, vendor_item_id: str, cause: str,
          account: Optional[str] = None, listing_id: Optional[str] = None) -> None:
    """異常の明細を1行 MERGE する（未解決なら last_at・回数・原因を更新）。失敗してもログだけ。"""
    if os.environ.get("OPS_STATS_DISABLED") == "1":
        return
    cause = str(cause or "")[:400]
    lid = str(listing_id) if listing_id is not None else None
    try:
        conn = get_sql_server_connection()
        try:
            conn.cursor().execute(ISSUE_MERGE_SQL, (issue_kind, vendor_name, vendor_item_id,
                                                    account, lid, cause, account, lid, cause))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"[OPS_STATS] 異常明細の記録に失敗（処理は続ける） {issue_kind} {vendor_item_id} {type(e).__name__}: {e}",
              flush=True)
