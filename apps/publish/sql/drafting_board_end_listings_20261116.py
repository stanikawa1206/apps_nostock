# -*- coding: utf-8 -*-
"""
過去に誤って集めた製図板の出品を eBay で終了する（2026-11-16・1回だけ。本番の eBay API を呼ぶ）。

既存の手動終了 apps/tests/delete_ebay_listigs_risk_items.py の処理をそのまま使い、対象の SKU だけを渡す:
  - trx.listings（is_deleted=0）から account・listing_id を引き、EndItems（10件ずつ・EndingReason=NotAvailable）
  - 成功した分だけ trx.listings を is_deleted=1・deleted_at・delete_reason=特別、trx.vendor_item を 出品不可flg=1
  - 失敗・レート制限の分は DB を変えない（もう一度流せば残りだけ処理される）
SKU の一覧は drafting_board_cleanup_20261116_end_skus.txt（drafting_board_cleanup_20261116.py write で作る）。

    python -m apps.publish.sql.drafting_board_end_listings_20261116           … 対象の確認だけ（eBay を呼ばない）
    python -m apps.publish.sql.drafting_board_end_listings_20261116 --run     … 終了する
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SKU_FILE = os.path.join(HERE, "drafting_board_cleanup_20261116_end_skus.txt")


def read_skus():
    with open(SKU_FILE, encoding="utf-8") as f:
        return [s.strip() for s in f if s.strip()]


def main(argv):
    from apps.tests import delete_ebay_listigs_risk_items as d
    skus = read_skus()
    d.TARGET_SKUS = skus
    d.N = len(skus)
    pairs = d.get_target_pairs()
    by_account = {}
    for acct, _lid, _sku in pairs:
        by_account[acct] = by_account.get(acct, 0) + 1
    print(f"SKU {len(skus)}件 → 出品中 {len(pairs)}件 {by_account}")
    if argv != ["--run"]:
        print("確認だけ（eBay は呼んでいない）。終了するには --run")
        return
    d.main()


if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..")))
    sys.stdout.reconfigure(encoding="utf-8")
    main(sys.argv[1:])
