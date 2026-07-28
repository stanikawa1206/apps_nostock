"""
GA（Authenticity Guarantee）商品の start_price 一括更新スクリプト（本番用）。

対象: trx.listings / trx.vendor_item / mst.presets_lookup をJOINし、
      is_deleted = 0 かつ start_price >= 500 かつ preset.mode = 'GA' の商品のうち、
      trx.listings.start_time が TARGET_START_DATE（現在は 2026-07-23）の商品のみを
      再実行対象とする。

並列方式:
  account 単位でグルーピングし、1アカウントにつき1ワーカー（1スレッド）を割り当てる。
  同一アカウント内の商品は、そのワーカーのスレッド内で必ず順番に処理する
  （アカウントをまたいだ処理のみが並列に走る）。
  DB接続もアカウントごとに専用のものを使う（pyodbcの接続はスレッド間で共有しない）。

処理順序（1件ずつ）:
  1. compute_start_price_usd で新価格を計算
  2. 現在価格と新価格が同じなら何もしない（API呼び出しなし）
  3. 価格が変わる場合のみ update_ebay_price で eBay 価格を更新
  4. API成功時のみ trx.listings.start_price を更新（失敗時はDB更新しない）

1件処理するごとにコミットするため、途中で停止しても再実行可能。
1件の失敗（API失敗・例外）が発生しても、そのアカウント内の残りの商品・他アカウントの
処理は継続する。

※ 当初は Trading API の revise_price（ReviseFixedPriceItem）を使用していたが、
  Inventory管理（Managed by Inventory）の出品では
  「Inventory-based listing management is not currently supported by this tool.」
  (ErrorCode 21919474) で失敗するため、Inventory API 経由で更新する
  既存の update_ebay_price に切り替えた。update_ebay_price 自体はDBを更新しない
  純粋なAPI呼び出し関数なので、DB更新はこのスクリプト側で行う。
"""

import threading
from collections import defaultdict
from decimal import Decimal

from apps.common.utils import get_sql_server_connection, compute_start_price_usd
from apps.adapters.ebay_api import update_ebay_price


PROGRESS_INTERVAL = 100

TARGET_START_DATE = "2026-07-23"

SQL_SELECT = """
SELECT
    l.listing_id,
    l.account,
    l.vendor_item_id,
    v.price AS cost_jpy,
    l.start_price AS current_start_price
FROM trx.listings AS l
INNER JOIN trx.vendor_item AS v
    ON l.vendor_item_id = v.vendor_item_id
INNER JOIN mst.presets_lookup AS p
    ON v.preset = p.preset
WHERE
    l.is_deleted = 0
    AND l.start_price >= 500
    AND p.mode = 'GA'
    AND CAST(l.start_time AS DATE) = ?
"""

SQL_UPDATE = """
UPDATE trx.listings
SET start_price = ?
WHERE vendor_item_id = ?
"""

_print_lock = threading.Lock()
_progress_lock = threading.Lock()
_processed_count = 0
_total_count = 0


def _safe_print(message: str) -> None:
    with _print_lock:
        print(message)


def _report_progress() -> None:
    global _processed_count
    with _progress_lock:
        _processed_count += 1
        count = _processed_count
    if count % PROGRESS_INTERVAL == 0 or count == _total_count:
        _safe_print(f"[進捗] {count}/{_total_count} 件処理済み")


def _process_account(account, rows, conn, results: dict) -> None:
    cur = conn.cursor()

    success = 0
    failed = 0
    skipped = 0

    for row in rows:
        item_id = row.listing_id
        vendor_item_id = row.vendor_item_id

        try:
            cost_jpy = row.cost_jpy
            old_price = Decimal(str(row.current_start_price))

            new_price_str = compute_start_price_usd(
                cost_jpy=cost_jpy,
                mode="GA",
                low_usd_target=1,
                high_usd_target=3000,
            )

            if new_price_str is None:
                skipped += 1
                _safe_print(
                    f"[SKIP] account={account} item_id={item_id} "
                    f"vendor_item_id={vendor_item_id} 新価格計算不可(レンジ外)"
                )
                continue

            new_price = Decimal(new_price_str)

            if new_price == old_price:
                skipped += 1
                _safe_print(
                    f"[SKIP] account={account} item_id={item_id} "
                    f"vendor_item_id={vendor_item_id} 価格変更なし({old_price})"
                )
                continue

            resp = update_ebay_price(
                account,
                str(item_id),
                str(new_price),
                sku=vendor_item_id,
            )

            if resp.get("success"):
                cur.execute(SQL_UPDATE, str(new_price), vendor_item_id)
                conn.commit()
                success += 1
                _safe_print(
                    f"[OK] account={account} item_id={item_id} "
                    f"vendor_item_id={vendor_item_id} {old_price} -> {new_price}"
                )
            else:
                failed += 1
                _safe_print(
                    f"[NG] account={account} item_id={item_id} "
                    f"vendor_item_id={vendor_item_id} {old_price} -> {new_price} "
                    f"error={resp.get('error')}"
                )

        except Exception as e:
            failed += 1
            _safe_print(
                f"[NG] account={account} item_id={item_id} "
                f"vendor_item_id={vendor_item_id} 例外発生: {e}"
            )

        finally:
            _report_progress()

    results[account] = {"success": success, "failed": failed, "skipped": skipped}


def main():
    global _total_count

    select_conn = get_sql_server_connection()
    select_cur = select_conn.cursor()
    select_cur.execute(SQL_SELECT, TARGET_START_DATE)
    rows = select_cur.fetchall()
    select_conn.close()

    _total_count = len(rows)

    grouped = defaultdict(list)
    for row in rows:
        grouped[row.account].append(row)

    print(f"対象件数: {_total_count}")
    print(f"対象アカウント数: {len(grouped)}")
    print("=" * 60)

    results: dict = {}
    workers = []

    for account, account_rows in grouped.items():
        account_conn = get_sql_server_connection()
        t = threading.Thread(
            target=_process_account,
            args=(account, account_rows, account_conn, results),
            name=f"worker-{account}",
        )
        workers.append((t, account_conn))
        t.start()

    for t, account_conn in workers:
        t.join()
        account_conn.close()

    print("=" * 60)
    print("[アカウント別集計]")

    total_success = 0
    total_failed = 0
    total_skipped = 0

    for account, r in results.items():
        print(
            f"account={account} 成功={r['success']} 失敗={r['failed']} "
            f"スキップ={r['skipped']}"
        )
        total_success += r["success"]
        total_failed += r["failed"]
        total_skipped += r["skipped"]

    print("=" * 60)
    print("対象件数:", _total_count)
    print("更新成功件数:", total_success)
    print("更新失敗件数:", total_failed)
    print("価格変更なし件数:", total_skipped)


if __name__ == "__main__":
    main()
