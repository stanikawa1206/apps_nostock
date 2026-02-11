# step3_brand_rank.py
# -*- coding: utf-8 -*-
import sys
from my_utils import get_sql_server_connection

# ★追加: Wakarunda連携用モジュール
import wakarunda_utils 

# ==========================================
# 設定
# ==========================================
# 最終的に判定できなかった場合のデフォルト
DEFAULT_FALLBACK_RANK = 'E'

# ==========================================
# SQL定義
# ==========================================
SQL_SELECT_TARGET = """
SELECT asin, jp_brand 
FROM trx.amazon_cross_market_asin 
WHERE jp_brand IS NOT NULL 
  AND (wakarunda IS NULL OR wakarunda = '')
"""

SQL_SELECT_MASTER = "SELECT [rank] FROM mst.amazon_brand WHERE brand = ?"

SQL_INSERT_MASTER = """
INSERT INTO mst.amazon_brand (brand, [rank], last_seen_at) 
VALUES (?, ?, SYSDATETIME())
"""

SQL_UPDATE_MAIN = """
UPDATE trx.amazon_cross_market_asin 
SET wakarunda = ?, last_seen_at = SYSDATETIME() 
WHERE asin = ?
"""

# ==========================================
# 外部判定関数 (Wakarunda利用)
# ==========================================
def external_brand_rank_judge(brand_name: str) -> str:
    """
    マスタに存在しないブランドのランクを
    Selenium + Wakarunda で判定する。
    """
    # wakarunda_utils の関数を呼び出す
    rank = wakarunda_utils.fetch_brand_rank_from_selenium(brand_name)
    
    # もしエラーなどで返ってこなければデフォルト値
    if not rank:
        return DEFAULT_FALLBACK_RANK
        
    return rank

# ==========================================
# メイン処理
# ==========================================
def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    print("判定対象データを検索中...")
    cursor.execute(SQL_SELECT_TARGET)
    rows = cursor.fetchall()
    
    if not rows:
        print("ランク未判定のデータはありませんでした。")
        conn.close()
        return

    print(f"判定対象: {len(rows)}件")

    brand_rank_cache = {}
    updated_count = 0
    new_brand_count = 0

    # Seleniumを使うのでChromeが起動します
    print("--- Wakarunda連携を開始します (Chromeが動作します) ---")

    for row in rows:
        asin = row.asin
        brand = row.jp_brand
        
        if not brand: continue
        brand = brand.strip()
        
        rank = None
        
        # 1. キャッシュ確認
        if brand in brand_rank_cache:
            rank = brand_rank_cache[brand]
        else:
            # 2. マスタDB確認
            cursor.execute(SQL_SELECT_MASTER, [brand])
            master_row = cursor.fetchone()
            
            if master_row:
                rank = master_row[0]
            else:
                # 3. マスタにない -> Wakarundaで判定 & マスタ登録
                rank = external_brand_rank_judge(brand)
                
                try:
                    cursor.execute(SQL_INSERT_MASTER, [brand, rank])
                    new_brand_count += 1
                    # 1件ごとにコミットして、途中で止まってもマスタに残るようにする
                    conn.commit() 
                except Exception as e:
                    print(f"  [Master Insert Error] {brand}: {e}")
            
            brand_rank_cache[brand] = rank

        # 4. メインテーブル更新
        try:
            cursor.execute(SQL_UPDATE_MAIN, [rank, asin])
            updated_count += 1
        except Exception as e:
            print(f"  [Update Error] {asin}: {e}")

        # 進捗
        if updated_count % 10 == 0:
            conn.commit()
            print(f"   ... {updated_count}件 完了")

    conn.commit()
    conn.close()

    print("\n=== ランク判定完了 ===")
    print(f"  - 処理件数: {updated_count}件")
    print(f"  - 新規ブランド登録: {new_brand_count}件")

if __name__ == "__main__":
    main()