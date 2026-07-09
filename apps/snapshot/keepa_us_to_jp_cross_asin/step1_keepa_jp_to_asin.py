# step1_keepa_jp_to_asin.py
# -*- coding: utf-8 -*-
#
# 【このプログラムの役割】
# Keepa（Amazonの価格追跡サービス）のAPIを使い、
# 日本Amazonの指定カテゴリ内からASINを大量に取得する。
# 取得条件は「販売ランク」「価格帯（5,000円〜200,000円）」「FBA料金の上限（650円）」で絞り込む。
# 取得したASINはDB（trx.amazon_cross_market_asin）へ登録し、
# 処理済みカテゴリには処理日とランク閾値を記録する。
# 結果はStep2以降で日米共通ASIN判定に使用される。
#
import sys
import os
from datetime import datetime
from my_utils import get_sql_server_connection, keepa_request

# ==========================================
# 設定: 検索条件 & ログ設定
# ==========================================
QUERY_LIMIT = 10000

# --- ログ出力設定 ---
LOG_DIR = r"X:\apps\snapshot\keepa_us_to_jp_cross_asin\logs"
# ------------------

# 2. 価格フィルタ (新品価格 90日平均)
PRICE_MIN_JPY = 5000
PRICE_MAX_JPY = 200000

# 3. サイズ・重量フィルタ
MAX_EDGE_MM = 1600
MAX_WEIGHT_G = 30000

# ==========================================
# カテゴリ管理関数（旧step0_category_manager.pyより移植）
# ==========================================
# mst.amazon_category から処理対象カテゴリを1件取得する。
# ASINが1件以上存在し、かつランキング閾値が目標値と一致していない
# カテゴリをcategory_id昇順で取り出す。
def get_next_category(target_min_rank, target_max_rank):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    query = """
        SELECT TOP 1 category_id, category_name
        FROM mst.amazon_category
        WHERE is_at_least_one_asin_exists = 1
          AND (
              min_rank_threshold IS NULL OR
              max_rank_threshold IS NULL OR
              min_rank_threshold != ? OR
              max_rank_threshold != ?
          )
        ORDER BY category_id ASC
    """
    cursor.execute(query, (target_min_rank, target_max_rank))
    row = cursor.fetchone()
    conn.close()

    if row:
        return {"id": row[0], "name": row[1]}
    return None

# 指定カテゴリのランキング閾値（min/max）と処理日（fetched_at）を
# mst.amazon_category へUPDATEする。
# 更新失敗時はロールバックし、エラー内容を標準出力へ出力する。
def update_category_status(category_id, min_rank, max_rank):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    today = datetime.now().strftime('%Y-%m-%d')
    try:
        cursor.execute("""
            UPDATE mst.amazon_category
            SET fetched_at = ?,
                min_rank_threshold = ?,
                max_rank_threshold = ?
            WHERE category_id = ?
        """, (today, min_rank, max_rank, category_id))
        conn.commit()
    except Exception as e:
        print(f"!!! [Step 1] DB更新エラー (ID: {category_id}): {e}")
        conn.rollback()
    finally:
        conn.close()

# ==========================================
# ログ出力用関数
# ==========================================
# 処理結果をログファイルに記録する。
# ランク範囲・カテゴリID・保存件数を1行で書き出す。
# ログファイルは日付ごとに作成され、追記される。
def write_execution_log(rank_range, cat_id, count):
    # ログ保存フォルダが存在しない場合は作成する
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # 今日の日付でログファイル名を決め、1行追記する
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(LOG_DIR, f"{today}.log")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{now_str}] Rank: {rank_range} | CatID: {cat_id} | Saved: {count} items\n"

    with open(log_file, mode="a", encoding="utf-8") as f:
        f.write(log_line)

# ==========================================
# SQL定義
# ==========================================
SQL_UPSERT = """
MERGE trx.amazon_cross_market_asin WITH (ROWLOCK) AS tgt
USING (SELECT ? AS asin, ? AS jp_category_id) AS src
ON tgt.asin = src.asin
WHEN MATCHED THEN
    UPDATE SET
        last_seen_at = SYSDATETIME(),
        keepa_last_caught_at = SYSDATETIME(),
        jp_category_id = src.jp_category_id,
        step1_flag = 1
WHEN NOT MATCHED THEN
    INSERT (asin, last_seen_at, keepa_last_caught_at, jp_category_id, us_existence, step1_flag)
    VALUES (src.asin, SYSDATETIME(), SYSDATETIME(), src.jp_category_id, 0, 1);
"""

# ASINのリストをデータベースへ保存する。
# 既に登録済みのASINは最終確認日を更新し、
# 未登録のASINは新規に追加する（MERGE処理）。
def save_asins_to_db(cursor, asin_list, cat_id):
    if not asin_list:
        return 0

    count = 0
    # 1件ずつMERGEし、失敗した場合はロールバックして次へ進む
    for asin in asin_list:
        try:
            cursor.execute(SQL_UPSERT, [asin, cat_id])
            cursor.connection.commit()
            count += 1
        except Exception as e:
            print(f"Error {asin}: {e}")
            cursor.connection.rollback()
    return count

# Keepa APIから対象カテゴリのASINを取得してDBへ保存する。
# 取得件数が上限（10,000件）を超えた場合はランク範囲を半分に分割して
# 再帰的に処理する（分割統治法）。
def fetch_and_save_recursive(cat_id, min_rank, max_rank, cursor):
    print(f"Fetching Cat: {cat_id} | Rank(Avg180): {min_rank}-{max_rank}")

    # Keepa APIへの検索条件を組み立てる
    selection = {
        "rootCategory": cat_id,
        "productType": 0,                           # 物理的な商品
        "avg365_SALES_gte": min_rank,               # 180日 → 365日に変更
        "avg365_SALES_lte": max_rank,               # 180日 → 365日に変更
        "avg90_NEW_gte": PRICE_MIN_JPY,
        "avg90_NEW_lte": PRICE_MAX_JPY,
        "fbaFees_lte": 650,                         # サイズ・重量を削除し、FBA料金(上限650円)を追加
        "perPage": QUERY_LIMIT
    }

    # Keepa APIを呼び出して結果を取得する
    res = keepa_request("query", params={"domain": 5}, data=selection)

    total = res.get("totalResults", 0)
    asin_list = res.get("asinList", [])

    # 結果が上限を超えた場合はランク範囲を半分に分割して再帰処理する
    if total > QUERY_LIMIT and (max_rank - min_rank) > 1:
        print(f"   [Split] Hit {total} > Limit. Splitting range...")
        mid = (min_rank + max_rank) // 2

        count_1 = fetch_and_save_recursive(cat_id, min_rank, mid, cursor)
        count_2 = fetch_and_save_recursive(cat_id, mid + 1, max_rank, cursor)

        return count_1 + count_2
    else:
        # 取得したASINをDBに保存し、ログに記録する
        print(f"   -> Got {len(asin_list)} items. Saving...")
        saved_count = save_asins_to_db(cursor, asin_list, cat_id)

        rank_range_str = f"{min_rank}-{max_rank}"
        write_execution_log(rank_range_str, cat_id, saved_count)

        return saved_count

# プログラムの起動点。
# 引数で指定されたランク範囲をもとに、処理が必要なカテゴリを
# DBから1件ずつ取り出してKeepa取得 → DB保存 → カテゴリ更新を繰り返す。
def main():
    # コマンドライン引数からランク範囲を受け取る
    if len(sys.argv) < 3:
        print("エラー: ランキング閾値が指定されていません。")
        print("使用法: python step1_keepa_jp_to_asin.py [min_rank] [max_rank]")
        sys.exit(1)

    try:
        target_min_rank = int(sys.argv[1])
        target_max_rank = int(sys.argv[2])
    except ValueError:
        print("エラー: ランクは数値で指定してください。")
        sys.exit(1)

    print(f"=== Step1: カテゴリー連続処理を開始 (指定ランク: {target_min_rank} - {target_max_rank}) ===")

    while True:
        # 1. DBから次のカテゴリーを取得
        category = get_next_category(target_min_rank, target_max_rank)

        # 未処理カテゴリがなければ終了する
        if not category:
            print(f"\n=== 全ての対象カテゴリーの処理が完了しました (閾値 {target_min_rank}-{target_max_rank}) ===")
            break

        target_category_id = category["id"]
        target_category_name = category["name"]

        print(f"\n実行開始 - カテゴリー: {target_category_name} (ID: {target_category_id})")

        conn = get_sql_server_connection()
        cursor = conn.cursor()

        try:
            # 2. 再帰取得と保存を実行
            total_saved = fetch_and_save_recursive(target_category_id, target_min_rank, target_max_rank, cursor)
            print(f"=== DB保存完了: 合計 {total_saved}件 (CatID: {target_category_id}) ===")

            # 3. 【変更箇所】1カテゴリーの処理が正常に完了した直後にマスタを更新する
            update_category_status(target_category_id, target_min_rank, target_max_rank)
            print(f"=== マスタ更新完了: 実行日と閾値を反映しました ===")

        except Exception as e:
            print(f"Fatal Error: {e}")
            conn.rollback()
            # エラー時はマスタが更新されないため、次回再実行の対象として残ります
        finally:
            conn.close()

if __name__ == "__main__":
    main()
