# -*- coding: utf-8 -*-
"""
delete_from_vw_download_only.py

dbo.vw_download_only の listing_id を eBay から削除する。
DB更新（is_deleted 等）は一切行わない。

使い方:
  python delete_from_vw_download_only.py --show   # 1件表示のみ（削除しない）
  python delete_from_vw_download_only.py --test   # 1件だけ eBay 削除
  python delete_from_vw_download_only.py --run    # 全件 eBay 削除
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

from collections import defaultdict
from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import delete_items_from_ebay_batch

BATCH_SIZE = 10


# =========================================================
# DB からターゲットを取得
# =========================================================

def get_targets(limit: int = None):
    """
    dbo.vw_download_only から (account, listing_id, vendor_item_id) を取得。
    limit を指定すると TOP N になる。
    """
    top  = f"TOP {int(limit)} " if limit else ""
    sql  = f"SELECT {top}account, listing_id, vendor_item_id FROM dbo.vw_download_only"
    cn   = get_sql_server_connection()
    cur  = cn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close(); cn.close()
    return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]


# =========================================================
# eBay 削除
# =========================================================

def delete_batch(account: str, listing_ids: list[str]) -> dict:
    """listing_ids を eBay から削除して結果を返す"""
    result = delete_items_from_ebay_batch(account, listing_ids)

    ok_ids, ng_ids = [], []

    for r in (result.get("results") or []):
        iid  = str(r.get("item_id", ""))
        code = str(r.get("error_code") or "")
        if r.get("success") or code == "37":   # 37 = already ended
            ok_ids.append(iid)
        else:
            print(f"  NG code={code} msg={r.get('error_message','')}")
            ng_ids.append(iid)

    return {"ok": ok_ids, "ng": ng_ids}


def delete_all_for_account(account: str, listing_ids: list[str]):
    total_ok, total_ng = 0, 0

    for i in range(0, len(listing_ids), BATCH_SIZE):
        batch = listing_ids[i : i + BATCH_SIZE]
        res   = delete_batch(account, batch)
        total_ok += len(res["ok"])
        total_ng += len(res["ng"])
        for iid in res["ok"]:
            print(f"  ✅ 削除: {iid}")
        if res["ng"]:
            print(f"  🚫 失敗: {len(res['ng'])}件")

    return total_ok, total_ng


# =========================================================
# モード別エントリ
# =========================================================

def cmd_show():
    """1件だけ表示（削除しない）"""
    targets = get_targets(limit=1)
    if not targets:
        print("対象なし")
        return
    account, listing_id, vendor_item_id = targets[0]
    print("=== 削除対象（1件プレビュー）===")
    print(f"  account        : {account}")
    print(f"  listing_id     : {listing_id}")
    print(f"  vendor_item_id : {vendor_item_id}")
    print(f"  eBay URL       : https://www.ebay.com/itm/{listing_id}")
    print("================================")
    print("削除は行いません。--test で1件削除テスト、--run で全件削除します。")


def cmd_test():
    """1件だけ eBay 削除"""
    targets = get_targets(limit=1)
    if not targets:
        print("対象なし")
        return
    account, listing_id, vendor_item_id = targets[0]
    print("=== テスト削除（1件）===")
    print(f"  account    : {account}")
    print(f"  listing_id : {listing_id}")
    print(f"  URL        : https://www.ebay.com/itm/{listing_id}")
    print("削除します...")
    ok, ng = delete_all_for_account(account, [listing_id])
    print(f"結果: 成功={ok} 失敗={ng}")


def cmd_run():
    """全件 eBay 削除"""
    targets = get_targets()
    if not targets:
        print("対象なし")
        return

    by_account = defaultdict(list)
    for account, listing_id, _ in targets:
        by_account[account].append(listing_id)

    total = sum(len(v) for v in by_account.values())
    print(f"=== 全件削除 合計{total}件 ===")
    for acc, ids in by_account.items():
        print(f"  {acc}: {len(ids)}件")
    print("================================")

    grand_ok, grand_ng = 0, 0
    for account, listing_ids in by_account.items():
        print(f"\n[{account}] {len(listing_ids)}件 削除開始")
        ok, ng = delete_all_for_account(account, listing_ids)
        grand_ok += ok
        grand_ng += ng
        print(f"[{account}] 完了: 成功={ok} 失敗={ng}")

    print(f"\n全体合計: 成功={grand_ok} 失敗={grand_ng}")


# =========================================================
# main
# =========================================================

if __name__ == "__main__":
    if "--show" in sys.argv:
        cmd_show()
    elif "--test" in sys.argv:
        cmd_test()
    elif "--run" in sys.argv:
        cmd_run()
    else:
        print("使い方:")
        print("  --show  1件表示のみ（削除なし）")
        print("  --test  1件だけ eBay 削除")
        print("  --run   全件 eBay 削除")
