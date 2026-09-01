# -*- coding: utf-8 -*-
"""
check_listings_consistency.py

trx.listings（is_deleted = false のみ）と ext.ebay_active_download の
整合性をアカウント別に集計・表示するだけのツール。

DB件数 / db_only は (account, vendor_item_id) の比較。
  ・trx.listings.vendor_item_id      … いわゆる SKU
  ・ext.ebay_active_download.vendor_item_id … 同上（eBayから取得したSKU）

download_only は本番の dbo.vw_download_only と同じロジックで判定する
（ジョインキーが vendor_item_id ではなく (account, listing_id) である点、
および LEN(vendor_item_id) >= 6 の短SKU除外フィルタがある点に注意。
vendor_item_id ベースで単純に集合差分を取ると、この2点の違いにより
vw_download_only より多く数えてしまう）。

mst.ebay_accounts.is_excluded = 1 のアカウント（運用対象外）は集計から除外する。

【重要】このスクリプトは集計・表示のみを行う。
INSERT/UPDATE/DELETE・論理削除など、データの更新は一切行わない。
"""
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # D:/apps_nostock
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_EXCLUDED_ACCOUNTS_SUBQUERY = """
    SELECT account FROM mst.ebay_accounts WHERE is_excluded = 1
"""

SQL_DB_SIDE = f"""
    SELECT account, vendor_item_id, listing_id
    FROM trx.listings
    WHERE is_deleted = 0
      AND account NOT IN ({_EXCLUDED_ACCOUNTS_SUBQUERY})
"""

SQL_DOWNLOAD_SIDE = f"""
    SELECT account, vendor_item_id, listing_id
    FROM ext.ebay_active_download
    WHERE account NOT IN ({_EXCLUDED_ACCOUNTS_SUBQUERY})
"""

# vw_download_only が download_only 判定の対象にする最小SKU長
# （これ未満の vendor_item_id は不完全なSKUとして除外される）
_MIN_SKU_LEN_FOR_DOWNLOAD_ONLY = 6


def _display_width(s: str) -> int:
    """全角文字を2、半角文字を1として表示幅を計算する（テーブル整列用）。"""
    width = 0
    for ch in s:
        width += 2 if unicodedata.east_asian_width(ch) in ("W", "F", "A") else 1
    return width


def _pad(s: str, width: int) -> str:
    """表示幅ベースで右側にスペースを詰めて左寄せする。"""
    return s + " " * max(0, width - _display_width(s))


def fetch_rows(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def build_account_stats(db_rows, download_rows):
    """
    account別に (DB件数, Download件数, db_only件数, download_only件数) を計算する。

    DB件数 / Download件数 は各テーブルの生の行数（重複SKUがあってもそのまま数える）。

    db_only は (account, vendor_item_id) の集合差分で数える
    （重複行があっても1件として扱う）。

    download_only は dbo.vw_download_only と同じロジックで判定する:
      ・ジョインキーは (account, vendor_item_id) ではなく (account, listing_id)
      ・trx.listings 側に is_deleted=0 の一致行が無い ext.ebay_active_download の行を対象とする
      ・vendor_item_id の長さが _MIN_SKU_LEN_FOR_DOWNLOAD_ONLY 未満の行は対象外
    （vw_download_only は行単位でフィルタするだけで重複排除しないため、
    ここでも ext.ebay_active_download の生の行を1件ずつ判定する）。
    """
    db_raw_count = defaultdict(int)
    download_raw_count = defaultdict(int)
    db_keys = defaultdict(set)          # (account) -> {vendor_item_id, ...}  db_only用
    download_keys = defaultdict(set)    # (account) -> {vendor_item_id, ...}  db_only用
    db_active_listing_ids = set()       # {(account, listing_id), ...}  download_only用
    download_only_count = defaultdict(int)

    for account, vendor_item_id, listing_id in db_rows:
        db_raw_count[account] += 1
        db_keys[account].add(vendor_item_id)
        db_active_listing_ids.add((account, listing_id))

    for account, vendor_item_id, listing_id in download_rows:
        download_raw_count[account] += 1
        download_keys[account].add(vendor_item_id)

        sku_len = len(vendor_item_id) if vendor_item_id is not None else 0
        if sku_len >= _MIN_SKU_LEN_FOR_DOWNLOAD_ONLY:
            if (account, listing_id) not in db_active_listing_ids:
                download_only_count[account] += 1

    accounts = sorted(set(db_raw_count) | set(download_raw_count))

    stats = []
    for account in accounts:
        d_keys = db_keys[account]
        w_keys = download_keys[account]

        stats.append({
            "account": account,
            "db_count": db_raw_count[account],
            "download_count": download_raw_count[account],
            "db_only": len(d_keys - w_keys),
            "download_only": download_only_count[account],
        })

    return stats


def print_report(stats):
    header = (
        f"{_pad('Account', 12)} {'DB':>6} {'Download':>9} "
        f"{'db_only':>9} {'download_only':>13}"
    )
    sep = "-" * _display_width(header)

    print(header)
    print(sep)

    total_db = total_download = total_db_only = total_download_only = 0

    for row in stats:
        print(
            f"{_pad(row['account'], 12)} "
            f"{row['db_count']:>6} "
            f"{row['download_count']:>9} "
            f"{row['db_only']:>9} "
            f"{row['download_only']:>13}"
        )
        total_db += row["db_count"]
        total_download += row["download_count"]
        total_db_only += row["db_only"]
        total_download_only += row["download_only"]

    print(sep)
    print(
        f"{_pad('合計', 12)} "
        f"{total_db:>6} "
        f"{total_download:>9} "
        f"{total_db_only:>9} "
        f"{total_download_only:>13}"
    )


def main():
    conn = get_sql_server_connection()
    try:
        db_rows = fetch_rows(conn, SQL_DB_SIDE)
        download_rows = fetch_rows(conn, SQL_DOWNLOAD_SIDE)
    finally:
        conn.close()

    stats = build_account_stats(db_rows, download_rows)
    print_report(stats)


if __name__ == "__main__":
    main()
