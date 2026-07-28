# -*- coding: utf-8 -*-
"""
get_active_listings_for_accounts_once.py

【本番復旧用の一時スクリプト】

BUZZ / 谷川③ の ext.ebay_active_download が0件になっており、
make_listing_space.py が削除候補を取得できず、稼働中の publish_manager.py の
LIMIT処理が進まない状態を緊急復旧するためだけに作成した、使い捨てスクリプト。

- 既存の get_active_listings.py / apps.adapters.ebay_api の関数
  （DB接続・トークン取得・Active Listing取得・DB保存処理）をそのまま再利用する。
- mst.execute_pcs を使ったアカウント割当は一切行わない。
  TARGET_ACCOUNTS で対象アカウントを固定指定する。
- close_reason・mst.execute_pcs・出品処理・削除処理には一切触れない。
- API取得に失敗した場合や0件だった場合は、そのアカウントのDB更新を行わない
  （既存データを誤って消さない。get_active_listings.py 側も現状は
  DELETEを行わずINSERTのみのため、失敗時に既存データが失われることはない）。
- 1アカウントの失敗が、もう一方のアカウントの処理を妨げないようにする。

本番復旧が完了したら削除してよい（要否は実行結果を見て判断する）。
"""

import sys

# get_active_listings.py 内の print("✔ ...") 等がWindowsの既定コンソール
# エンコーディング(cp932)でUnicodeEncodeErrorになるのを避けるための設定。
# get_active_listings.py 自体は変更せず、この一時スクリプトのプロセス側で
# stdout/stderrのエンコーディングをUTF-8に固定する（他の本番スクリプトでも
# 同様の reconfigure が使われている）。
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import datetime

from apps.publish.get_active_listings import get_active_listings, insert_items
from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import get_sql_server_connection

# 今回だけ対象アカウントを固定する（execute_pcsによる自動割当は使わない）
TARGET_ACCOUNTS = [
    "BUZZ",
    "谷川③",
]


def fetch_and_save_one_account(conn, account, fetched_at):
    """
    1アカウント分のActive Listingを取得し、ext.ebay_active_download へ保存する。

    順序:
      1. トークン取得
      2. eBay APIから全ページ取得（既存 get_active_listings() をそのまま利用）
      3. 取得件数を確認（0件なら異常として扱い、DB更新はしない）
      4. DB保存（INSERTのみ。既存データのDELETEは行わない）
      5. 保存後にSELECTで件数確認

    成功時 True、失敗時 False を返す。例外は外へ伝播させない。
    """

    print(f"[START] account={account}")

    # --- 1. トークン取得（ログ用に明示的に確認する。値そのものは表示しない） ---
    try:
        token = get_access_token_new(account)
    except Exception as e:
        print(f"[ERROR] account={account} トークン取得中に例外が発生しました: {e}")
        return False

    if not token:
        print(f"[ERROR] account={account} トークン取得に失敗したためDB更新を中止")
        return False

    print(f"[TOKEN] account={account} success")

    # --- 2. eBay APIから全Active Listingを取得（既存ロジックをそのまま利用） ---
    # get_active_listings() は内部で再度トークンを取得するが、
    # ページング・パース処理を含め既存コードを一切変更せず再利用するための仕様。
    try:
        items = get_active_listings(account)
    except Exception as e:
        print(f"[ERROR] account={account} Active Listing取得中に例外が発生しました: {e}")
        return False

    total_count = len(items)
    print(f"[FETCH] account={account} total_count={total_count}")

    # --- 3. 0件は異常として扱う（このアカウントは実データがある前提のため） ---
    if total_count == 0:
        print(f"[ERROR] account={account} Active Listing取得結果が0件のためDB更新を中止")
        return False

    # --- 4. DB保存（取得成功・件数確認が済んでから実行。DELETEは行わずINSERTのみ） ---
    try:
        with conn.cursor() as cur:
            insert_items(cur, account, items, fetched_at)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] account={account} DB保存中に例外が発生しました: {e}")
        return False

    print(f"[SAVE] account={account} delete_count=0（DELETE未実施） insert_count={total_count}")

    # --- 5. 保存後の確認 ---
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS row_count, MAX(fetched_at) AS latest_fetched_at
            FROM ext.ebay_active_download
            WHERE account = ?
        """, account)
        row = cur.fetchone()
        db_count, latest_fetched_at = row[0], row[1]

    print(f"[VERIFY] account={account} db_count={db_count} latest_fetched_at={latest_fetched_at}")
    print(f"[END] account={account} success")

    return True


def main():
    conn = get_sql_server_connection()
    fetched_at = datetime.datetime.now()

    results = {}

    try:
        for account in TARGET_ACCOUNTS:
            try:
                results[account] = fetch_and_save_one_account(conn, account, fetched_at)
            except Exception as e:
                # 1アカウントで想定外の例外が起きても、もう一方の処理は続ける
                print(f"[ERROR] account={account} 想定外の例外が発生しました: {e}")
                results[account] = False
    finally:
        conn.close()

    print("\n=== 結果サマリ ===")
    for account, ok in results.items():
        print(f"account={account}: {'success' if ok else 'failed'}")


if __name__ == "__main__":
    main()
