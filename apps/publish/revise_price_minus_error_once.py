# -*- coding: utf-8 -*-
"""
delete_listings_plus_error_once.py

R_誤差 > 0（本来より安く出ている）を対象に、
eBay出品を終了(EndItem)し、trx.listings からも該当行を削除する。

- テスト用分岐は全部撤去
- 実行前に、同じ条件で削除対象件数（アカウント別）を必ず表示
"""

import sys
import time
from pathlib import Path
from typing import Any, Dict

# ===== パス設定 =====
sys.path.extend([r"D:\apps_nostock"])

from apps.common.utils import get_sql_server_connection  # noqa: E402

# ===== ebay_api.py をファイルパスから直読み（import失敗を回避）=====
def load_ebay_api_module():
    import importlib.util

    here = Path(__file__).resolve()
    root = here.parents[2]  # ...\apps_nostock
    ebay_api_path = root / "apps" / "adapters" / "ebay_api.py"

    if not ebay_api_path.exists():
        raise FileNotFoundError(f"ebay_api.py not found: {ebay_api_path}")

    spec = importlib.util.spec_from_file_location("ebay_api", str(ebay_api_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to create module spec for ebay_api")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ebay_api = load_ebay_api_module()

# ★ WHERE条件を共通化（(1)と(2)をズラさないため）
# WHERE_CLAUSE = "WHERE [R_誤差] > 0 AND LTRIM(RTRIM([account])) IN (N'谷川③', N'川島')"
WHERE_CLAUSE = "WHERE [R_誤差] > 0"

# ===== Rate limit 対策（必要最小限）=====
BASE_SLEEP_SEC = 0.6          # 通常時：1件ごとの待ち（まずは0.6秒）
RETRY_MAX_518 = 5             # 518だけ最大リトライ回数
BACKOFF_518_START = 10        # 518発生時：最初は10秒待つ
BACKOFF_518_MAX = 180         # 518発生時：最大180秒まで

def print_delete_counts_by_account(conn) -> None:
    """
    削除対象件数を、実際の削除条件と同じWHEREで集計して表示。
    """
    sql = f"""
    SELECT
        LTRIM(RTRIM([account])) AS account,
        COUNT(*) AS cnt
    FROM [nostock].[dbo].[vw_price_revision_target]
    {WHERE_CLAUSE}
    GROUP BY LTRIM(RTRIM([account]))
    ORDER BY cnt DESC, account ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    total = sum(int(r[1]) for r in rows)

    print("=== DELETE TARGET COUNTS ===")
    print("VIEW : [nostock].[dbo].[vw_price_revision_target]")
    print(f"COND : {WHERE_CLAUSE.strip()}")
    print("----------------------------------------")
    if not rows:
        print("0件（削除対象なし）")
    else:
        for acc, cnt in rows:
            print(f"account : {acc:<10}  count : {int(cnt)}")
    print("----------------------------------------")
    print(f"TOTAL  : {total}")
    print("========================================")
    print()

def delete_listing_from_db(conn, listing_id: str) -> int:
    """
    trx.listings から該当行を削除（件数を返す）
    """
    sql = "DELETE FROM [trx].[listings] WHERE listing_id = ?"
    cur = conn.cursor()
    cur.execute(sql, (str(listing_id),))
    affected = cur.rowcount or 0
    conn.commit()
    return affected

def _extract_error(res: Any) -> tuple[str, str]:
    code = ""
    msg = ""
    if isinstance(res, dict):
        code = str(res.get("error_code") or res.get("codes") or "").strip()
        msg = str(res.get("error") or res.get("message") or "").strip()
    return code, msg

def _is_518(code: str) -> bool:
    # "518" / 518 / "Error 518" みたいなのを雑に吸う（ここだけ最小限）
    return "518" == code or code.endswith(":518") or " 518" in code or "518" in code

def end_item_with_retry_518(account: str, listing_id: str) -> dict:
    """
    518 のときだけ待ってリトライする。
    518以外は1発で返す（今の運用方針を維持）。
    """
    backoff = BACKOFF_518_START

    for attempt in range(1, RETRY_MAX_518 + 1):
        res = ebay_api.delete_item_from_ebay(account, listing_id)

        if isinstance(res, dict) and res.get("success"):
            return res

        code, msg = _extract_error(res)

        if _is_518(code):
            # 518だけ：待って再試行
            wait_sec = min(backoff, BACKOFF_518_MAX)
            print(f"    [518] rate limit hit. attempt={attempt}/{RETRY_MAX_518} wait={wait_sec}s")
            time.sleep(wait_sec)
            backoff = min(backoff * 2, BACKOFF_518_MAX)
            continue

        # 518以外：即返す
        return res

    # 518でリトライし尽くした：最後の結果を返すため、もう一回取らずに res を返したいが
    # ループ内で最後のresが保持されている想定。安全にするなら再実行ではなくここで返す。
    # ※res が未定義になるケースを避けるため、ここでは失敗dictを返す。
    return {"success": False, "error_code": "518", "message": "rate limit: retry exhausted"}

def process_one(conn, account: str, idx: int, total: int, d: dict) -> bool:
    listing_id = str(d["listing_id"]).strip()

    # 通常時も少し間隔を空ける（バースト抑制）
    time.sleep(BASE_SLEEP_SEC)

    # 518だけリトライするEndItem
    res = end_item_with_retry_518(account, listing_id)

    if not isinstance(res, dict) or not res.get("success"):
        code, msg = _extract_error(res)
        print(f"[{account} {idx}/{total}] listing_id={listing_id} ❌ DELETE FAILED code={code} msg={msg}")
        return False

    delete_listing_from_db(conn, listing_id)
    print(f"[{account} {idx}/{total}] listing_id={listing_id} ✅ deleted")
    return True

def fetch_targets_grouped_by_account(conn) -> dict[str, list[dict]]:
    sql = f"""
    SELECT
        LTRIM(RTRIM([account])) AS account,
        [listing_id]
    FROM [nostock].[dbo].[vw_price_revision_target]
    {WHERE_CLAUSE}
    ORDER BY LTRIM(RTRIM([account])) ASC, [R_誤差] DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    grouped: dict[str, list[dict]] = {}
    for d in rows:
        acc = d["account"]
        grouped.setdefault(acc, []).append(d)
    return grouped

def main() -> None:
    conn = None
    try:
        conn = get_sql_server_connection()

        print_delete_counts_by_account(conn)

        grouped = fetch_targets_grouped_by_account(conn)
        total_all = sum(len(v) for v in grouped.values())

        if total_all == 0:
            print("削除対象なし")
            return

        print(f"=== DELETE START : total = {total_all} ===")

        for account, items in grouped.items():
            total = len(items)
            for idx, d in enumerate(items, start=1):
                ok = process_one(conn, account, idx, total, d)
                if not ok:
                    print(f"[STOP] account={account}")
                    break

        print("=== DELETE DONE ===")

    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
