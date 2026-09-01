# -*- coding: utf-8 -*-
"""
repair_listings_consistency.py

trx.listings と ext.ebay_active_download の不整合を修復する。

【対象】
  1. dbo.vw_db_only       … trx.listings にはあるが eBay(ext.ebay_active_download)には
                             存在しない出品 → is_deleted = 1 に更新する。
  2. dbo.vw_download_only … eBay(ext.ebay_active_download)にはあるが trx.listings には
                             存在しない出品 → trx.listings に反映する。
                             ただし listing_id は trx.listings の主キーのため、
                             同じ listing_id が is_deleted=1 で既に存在するケースが
                             ありうる（過去に「売り切れ」等で削除扱いにされたが、
                             実際は eBay 上でまだ出品中だったもの）。
                             このケースは INSERT ではなく UPDATE で is_deleted=0 に
                             戻す（＝新規INSERTは listing_id が本当に存在しない行のみ）。

【安全策】
  ・デフォルトは Dry Run（更新予定件数の表示のみ、DB更新なし）。
  ・実際に更新するには --apply オプションが必須。
  ・--apply 時は1トランザクション内で
      UPDATE(db_only) → INSERT/UPDATE(download_only) → 整合性再チェック
    をすべて実行し、まだ COMMIT しない。
      - 再チェックで db_only = 0 かつ download_only = 0 なら COMMIT
      - 一致しない場合、または途中で例外が発生した場合は ROLLBACK
    （＝更新結果を検証してから確定させるため、チェックもコミット前の
    同一トランザクション内で行う）。

使い方:
  python repair_listings_consistency.py           # Dry Run（表示のみ）
  python repair_listings_consistency.py --apply    # 実際に更新する
"""
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # D:/apps_nostock
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection, log_listings_change
from apps.inventory.check_listings_consistency import (
    SQL_DB_SIDE,
    SQL_DOWNLOAD_SIDE,
    fetch_rows,
    build_account_stats,
    print_report,
)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

DB_ONLY_DELETE_REASON = "特別"

# vw_db_only / vw_download_only はどちらも is_excluded=1 のアカウント
# （例: 貴文② のように Active Listing 取得自体の対象外にしているアカウント）を
# 除外していない。除外アカウントは ext.ebay_active_download が常に空なので、
# フィルタしないと「取得していないだけの大量の在庫」を db_only として
# is_deleted=1 にしてしまう事故につながる。check_listings_consistency.py と
# 同じ条件で除外する。
_EXCLUDED_ACCOUNTS_SUBQUERY = """
    SELECT account FROM mst.ebay_accounts WHERE is_excluded = 1
"""

SQL_VW_DB_ONLY = f"""
    SELECT account, listing_id, vendor_item_id, start_time, error_message, error_at
    FROM dbo.vw_db_only
    WHERE account NOT IN ({_EXCLUDED_ACCOUNTS_SUBQUERY})
"""

# vw_download_only の行に、登録/復活に必要な start_time・価格を
# ext.ebay_active_download から補って取得する
SQL_VW_DOWNLOAD_ONLY_DETAIL = f"""
    SELECT v.account, v.listing_id, v.vendor_item_id, d.start_time, d.[Start price]
    FROM dbo.vw_download_only v
    JOIN ext.ebay_active_download d
        ON d.account = v.account AND d.listing_id = v.listing_id
    WHERE v.account NOT IN ({_EXCLUDED_ACCOUNTS_SUBQUERY})
"""

SQL_UPDATE_DB_ONLY = """
    UPDATE trx.listings
    SET is_deleted = 1,
        deleted_at = SYSDATETIME(),
        delete_reason = ?
    WHERE listing_id = ?
      AND is_deleted = 0
"""

SQL_EXISTING_LISTING_IDS = """
    SELECT listing_id, is_deleted FROM trx.listings WHERE listing_id IN ({placeholders})
"""

SQL_INSERT_LISTING = """
    INSERT INTO trx.listings
        (listing_id, account, vendor_item_id, vendor_name, start_time, start_price, is_deleted)
    VALUES
        (?, ?, ?, ?, ?, ?, 0)
"""

SQL_REVIVE_LISTING = """
    UPDATE trx.listings
    SET is_deleted = 0,
        deleted_at = NULL,
        delete_reason = NULL,
        vendor_item_id = ?,
        start_time = ?,
        start_price = ?
    WHERE listing_id = ?
"""

SQL_LOOKUP_VENDOR_NAME = """
    SELECT vendor_item_id, vendor_name
    FROM (
        SELECT vendor_item_id, vendor_name,
               ROW_NUMBER() OVER (PARTITION BY vendor_item_id ORDER BY last_checked_at DESC) AS rn
        FROM trx.vendor_item
        WHERE vendor_item_id IN ({placeholders})
    ) t
    WHERE rn = 1
"""


def fetch_db_only_targets(conn):
    """dbo.vw_db_only の内容をそのまま取得する（is_deleted=1に更新する対象）。"""
    return fetch_rows(conn, SQL_VW_DB_ONLY)


def fetch_download_only_targets(conn):
    """
    dbo.vw_download_only の内容を、trx.listings への反映方法別に振り分けて取得する。

    ・(account, listing_id) の既存レコードなし              → 新規INSERT
    ・(account, listing_id) の既存レコードあり、is_deleted=1 → UPDATE（復活）

    vw_download_only の定義上、listing_id が trx.listings に存在するのに
    ここへ現れるのは is_deleted=1 の場合のみのはずだが、念のため
    is_deleted=0 の状態で存在するケースが見つかった場合は異常として
    どちらの対象にも入れず、警告として報告する（安全側に倒す）。

    戻り値: (insert_targets, revive_targets, anomalies)
    """
    rows = fetch_rows(conn, SQL_VW_DOWNLOAD_ONLY_DETAIL)
    if not rows:
        return [], [], []

    listing_ids = [r[1] for r in rows]

    cur = conn.cursor()
    existing_is_deleted = {}  # listing_id -> is_deleted
    # IN句の上限を避けるため、必要ならチャンク分割する
    CHUNK = 1000
    for i in range(0, len(listing_ids), CHUNK):
        chunk = listing_ids[i:i + CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(SQL_EXISTING_LISTING_IDS.format(placeholders=placeholders), chunk)
        for listing_id, is_deleted in cur.fetchall():
            existing_is_deleted[listing_id] = is_deleted

    insert_targets = []
    revive_targets = []
    anomalies = []
    for account, listing_id, vendor_item_id, start_time, start_price in rows:
        target = (account, listing_id, vendor_item_id, start_time, start_price)
        if listing_id not in existing_is_deleted:
            insert_targets.append(target)
        elif existing_is_deleted[listing_id]:
            revive_targets.append(target)
        else:
            anomalies.append(target)

    return insert_targets, revive_targets, anomalies


def lookup_vendor_names(conn, vendor_item_ids):
    """vendor_item_id -> vendor_name の推定値を trx.vendor_item から引く（見つからなければ None）。"""
    vendor_item_ids = sorted({v for v in vendor_item_ids if v})
    if not vendor_item_ids:
        return {}

    cur = conn.cursor()
    result = {}
    CHUNK = 1000
    for i in range(0, len(vendor_item_ids), CHUNK):
        chunk = vendor_item_ids[i:i + CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(SQL_LOOKUP_VENDOR_NAME.format(placeholders=placeholders), chunk)
        for vendor_item_id, vendor_name in cur.fetchall():
            result[vendor_item_id] = vendor_name
    return result


def print_dry_run_summary(db_only_targets, insert_targets, revive_targets, anomalies):
    print("=" * 60)
    print("Dry Run 結果（DB更新は一切行っていません）")
    print("=" * 60)
    print(f"download_only 復活予定件数（is_deleted=0へ）  : {len(revive_targets)}")
    print(f"download_only 新規INSERT予定件数              : {len(insert_targets)}")
    print(f"db_only 論理削除予定件数（is_deleted=1へ）    : {len(db_only_targets)}")
    print("=" * 60)
    if anomalies:
        print(
            f"⚠️ 異常データ {len(anomalies)}件: vw_download_only にあるが "
            "trx.listings 側が既に is_deleted=0 で存在（想定外のため対象外にしました）"
        )
        for account, listing_id, vendor_item_id, *_ in anomalies:
            print(f"    account={account} listing_id={listing_id} vendor_item_id={vendor_item_id}")
        print("=" * 60)
    print("実際に更新する場合は --apply を付けて再実行してください。")


def apply_repair(conn, db_only_targets, insert_targets, revive_targets):
    """
    1トランザクション内で
      1) db_only  -> is_deleted = 1 更新
      2) download_only -> 新規INSERT / 復活UPDATE
      3) 整合性再チェック（check_listings_consistency.py と同じ集計）
    をすべて実行する。まだ COMMIT しない。

    再チェックで db_only = 0 かつ download_only = 0 の場合のみ COMMIT し、
    一致しない場合、または途中で例外が発生した場合は ROLLBACK する。

    更新ログ（logs/trx_listings_changes.log）は実際に COMMIT された変更のみを
    記録する。ROLLBACK された場合は何も起きなかったことになるため、ログにも残さない。

    戻り値: (db_only_updated, inserted, revived, verified, stats)
    """
    vendor_name_map = lookup_vendor_names(conn, [row[2] for row in insert_targets])

    db_only_updated = 0
    inserted = 0
    revived = 0

    # COMMIT成功後にまとめてログ出力するため、ここでは記録内容だけ貯めておく
    pending_log_entries = []  # [(action, account, listing_id, vendor_item_id, delete_reason), ...]

    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # 1) db_only -> is_deleted = 1
            for account, listing_id, vendor_item_id, start_time, error_message, error_at in db_only_targets:
                cur.execute(SQL_UPDATE_DB_ONLY, DB_ONLY_DELETE_REASON, listing_id)
                if cur.rowcount:
                    db_only_updated += cur.rowcount
                    pending_log_entries.append(
                        ("DELETE", account, listing_id, vendor_item_id, DB_ONLY_DELETE_REASON)
                    )

            # 2) download_only -> 新規INSERT（listing_idが本当に存在しない行のみ）
            for account, listing_id, vendor_item_id, start_time, start_price in insert_targets:
                vendor_name = vendor_name_map.get(vendor_item_id)
                cur.execute(
                    SQL_INSERT_LISTING,
                    listing_id, account, vendor_item_id, vendor_name, start_time, start_price,
                )
                inserted += 1
                pending_log_entries.append(
                    ("INSERT", account, listing_id, vendor_item_id, None)
                )

            # 3) download_only -> 復活（listing_idがis_deleted=1で既存の行）
            for account, listing_id, vendor_item_id, start_time, start_price in revive_targets:
                cur.execute(
                    SQL_REVIVE_LISTING,
                    vendor_item_id, start_time, start_price, listing_id,
                )
                if cur.rowcount:
                    revived += cur.rowcount
                    pending_log_entries.append(
                        ("RESTORE", account, listing_id, vendor_item_id, None)
                    )

        # 4) COMMIT前の同一トランザクション内で整合性を再チェックする
        db_rows = fetch_rows(conn, SQL_DB_SIDE)
        download_rows = fetch_rows(conn, SQL_DOWNLOAD_SIDE)
        stats = build_account_stats(db_rows, download_rows)

        total_db_only = sum(row["db_only"] for row in stats)
        total_download_only = sum(row["download_only"] for row in stats)
        verified = (total_db_only == 0 and total_download_only == 0)

        if verified:
            conn.commit()
            for action, account, listing_id, vendor_item_id, delete_reason in pending_log_entries:
                log_listings_change(action, account, listing_id, vendor_item_id, delete_reason)
        else:
            conn.rollback()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True

    return db_only_updated, inserted, revived, verified, stats


def print_final_check(stats, verified):
    print()
    print("=" * 60)
    print("最終整合性チェック結果")
    print("=" * 60)
    print_report(stats)

    total_db_only = sum(row["db_only"] for row in stats)
    total_download_only = sum(row["download_only"] for row in stats)

    print()
    if verified:
        print(f"✅ db_only = 0 / download_only = 0 を確認し、COMMIT しました。")
    else:
        print(
            f"⚠️ 不整合が残っています (db_only={total_db_only}, "
            f"download_only={total_download_only})。ROLLBACK しました（DB更新は反映されていません）。"
        )


def main():
    apply_mode = "--apply" in sys.argv

    start = time.monotonic()
    conn = get_sql_server_connection()
    try:
        db_only_targets = fetch_db_only_targets(conn)
        insert_targets, revive_targets, anomalies = fetch_download_only_targets(conn)

        if not apply_mode:
            print_dry_run_summary(db_only_targets, insert_targets, revive_targets, anomalies)
            return

        print(
            f"--apply 指定: db_only={len(db_only_targets)}件, "
            f"download_only(INSERT)={len(insert_targets)}件, "
            f"download_only(復活)={len(revive_targets)}件 の更新を実行します。"
        )
        if anomalies:
            print(f"⚠️ 異常データ {len(anomalies)}件は対象外にしてスキップします。")

        db_only_updated, inserted, revived, verified, stats = apply_repair(
            conn, db_only_targets, insert_targets, revive_targets
        )

        print_final_check(stats, verified)

        elapsed = time.monotonic() - start

        print()
        print("=" * 60)
        print("修復処理サマリー")
        print("=" * 60)
        print(f"復活件数（is_deleted=0への復元）      : {revived}")
        print(f"新規INSERT件数                        : {inserted}")
        print(f"db_onlyの論理削除件数（is_deleted=1）  : {db_only_updated}")
        print(f"処理時間                              : {elapsed:.1f}秒")
        print("=" * 60)

        if not verified:
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
