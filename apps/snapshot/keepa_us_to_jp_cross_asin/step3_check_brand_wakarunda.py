# step3_check_brand_wakarunda.py
# -*- coding: utf-8 -*-
from my_utils import get_sql_server_connection
import wakarunda_utils

# ==========================================
# SQL定義
# ==========================================
# 処理対象：ブランドがあり、wakarunda判定が未実施、または 'E' のもの
SQL_SELECT_TARGET = """
SELECT asin, jp_brand, category_id 
FROM trx.amazon_cross_market_asin WITH (NOLOCK) 
WHERE jp_brand IS NOT NULL 
  AND (wakarunda IS NULL OR wakarunda = '' OR wakarunda = 'E')
"""

# マスタ参照
SQL_SELECT_MASTER = "SELECT [rank] FROM mst.amazon_brand WHERE brand = ?"

# マスタ更新 (UPSERT)
SQL_UPSERT_MASTER = """
MERGE mst.amazon_brand AS tgt
USING (SELECT ? AS brand, ? AS [rank]) AS src
ON tgt.brand = src.brand
WHEN MATCHED THEN
    UPDATE SET [rank] = src.[rank], last_seen_at = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (brand, [rank], last_seen_at)
    VALUES (src.brand, src.[rank], SYSDATETIME());
"""

# メインテーブル更新
SQL_UPDATE_MAIN = """
UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK)
SET wakarunda = ?, last_seen_at = SYSDATETIME() 
WHERE asin = ?
"""

def main():
    # ==========================================
    # 設定：スキップ対象のカテゴリIDを管理
    # ==========================================
    SKIP_CATEGORY_IDS = [465392] 

    conn = get_sql_server_connection()
    cursor = conn.cursor()

    print("判定対象データを検索中...")
    
    # SQLを更新：jp_category_id を取得するように変更
    SQL_SELECT_TARGET = """
        SELECT asin, jp_brand, jp_category_id 
        FROM trx.amazon_cross_market_asin WITH (NOLOCK) 
        WHERE jp_brand IS NOT NULL 
          AND (wakarunda IS NULL OR wakarunda = '' OR wakarunda = 'E')
    """
    
    try:
        cursor.execute(SQL_SELECT_TARGET)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"SQLエラーが発生しました: {e}")
        conn.close()
        return
    
    if not rows:
        print("処理対象のデータはありませんでした。")
        conn.close()
        return

    total_rows = len(rows)
    print(f"判定対象: {total_rows}件")

    # Chromeをクリーン起動
    wakarunda_utils.launch_fresh_chrome()

    brand_rank_cache = {}
    updated_count = 0

    for row in rows:
        asin = row.asin
        db_brand = row.jp_brand.strip() if row.jp_brand else None
        
        # ------------------------------------------
        # Python側でのカテゴリIDスキップ判定
        # ------------------------------------------
        # 取得した jp_category_id で判定を行います
        current_cat_id = getattr(row, 'jp_category_id', None)
        
        # if current_cat_id in SKIP_CATEGORY_IDS:
        #     print(f"[{updated_count + 1}/{total_rows}] スキップ: ASIN={asin} (カテゴリID:{current_cat_id} は除外設定中)")
        #     continue

        if not db_brand: continue
        
        print(f"[{updated_count + 1}/{total_rows}] 処理中: ASIN={asin} | Brand={db_brand}")
        
        rank = None
        needs_judge = False

        # 1. キャッシュ/マスタ確認
        if db_brand in brand_rank_cache:
            rank = brand_rank_cache[db_brand]
            if rank == 'E': needs_judge = True
        else:
            cursor.execute(SQL_SELECT_MASTER, [db_brand])
            master_row = cursor.fetchone()
            if master_row:
                rank = master_row[0]
                if rank == 'E':
                    print("   -> [Master Rank E] 再判定対象です。")
                    needs_judge = True
                else:
                    print(f"   -> [Master Hit] Rank: {rank}")
            else:
                needs_judge = True

        # 2. Wakarunda判定
        if needs_judge:
            print("   -> ASINページから直接判定を取得します...")
            fetched_rank, page_brand = wakarunda_utils.fetch_rank_and_brand_by_asin(asin)
            
            clean_db_brand = db_brand.lower().replace(" ", "").replace("　", "")
            clean_pg_brand = page_brand.lower().replace(" ", "").replace("　", "")

            if clean_db_brand == clean_pg_brand:
                rank = fetched_rank if fetched_rank != "判定不能" else "E"
                print(f"   -> [Match OK] Result Rank: {rank}")
                
                try:
                    cursor.execute(SQL_UPSERT_MASTER, [db_brand, rank])
                    conn.commit()
                except Exception as e:
                    print(f"   [Master DB Error] {e}")
                
                brand_rank_cache[db_brand] = rank
            else:
                print(f"   -> [Mismatch] DB:{db_brand} != Page:{page_brand}")
                rank = 'N'

        # 3. メインテーブルの更新
        try:
            cursor.execute(SQL_UPDATE_MAIN, [rank, asin])
            updated_count += 1
            if updated_count % 10 == 0:
                conn.commit()
                print(f"--- {updated_count}件経過 (中間コミット) ---")
        except Exception as e:
            print(f"   [Main DB Update Error] {e}")

    conn.commit()
    conn.close()
    print(f"\n=== ランク判定完了: {updated_count}件 ===")

if __name__ == "__main__":
    main()