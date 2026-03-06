# -*- coding: utf-8 -*-
"""
壊れた job で残った eBay 出品を 1 件だけ削除する
"""

import os
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

from apps.adapters.ebay_api import delete_item_from_ebay


# =========================
# 手動指定（ここだけ触る）
# =========================
ACCOUNT = "谷川③"
ITEM_ID = "227169305991"

def main():
    print(f"[START] delete one item account={ACCOUNT} itemId={ITEM_ID}")

    resp = delete_item_from_ebay(
        account=ACCOUNT,
        item_id=ITEM_ID,
    )

    if resp.get("success"):
        print(f"✅ 削除成功 itemId={ITEM_ID}")
    else:
        print(f"❌ 削除失敗 itemId={ITEM_ID} resp={resp}")

if __name__ == "__main__":
    main()
