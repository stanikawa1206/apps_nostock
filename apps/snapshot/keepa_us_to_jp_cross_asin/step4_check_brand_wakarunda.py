# step4_check_brand_wakarunda.py
# -*- coding: utf-8 -*-
#
# 【このプログラムの役割】
# Step3でUS or CA に存在すると確認されたASINを対象に、
# そのブランドが「判別可能かどうか」（ワカルンダ判定）を行う。
# まずブランドマスタ（mst.amazon_brand）を参照し、
# 未登録または要再判定のブランドはChromeを使って実際のページから取得する。
# 判定結果（D=判別可/-=ブランドなし/N=名称不一致/E=要再判定）を
# trx.amazon_cross_market_asin とブランドマスタ両方に書き込む。
# ここで「D」または「-」になったASINのみStep5の採算評価対象となる。
#
from my_utils import get_sql_server_connection
import wakarunda_utils

# ==========================================
# SQL定義
# ==========================================
# 判定対象：ブランドがあり、US or CA に存在し、まだ判定されていないASIN
SQL_SELECT_TARGET = """
SELECT asin, jp_brand, jp_category_id
FROM trx.amazon_cross_market_asin WITH (NOLOCK)
WHERE jp_brand IS NOT NULL
  AND (us_existence = 1 OR ca_existence = 1)
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

# プログラムの起動点。
# 判定対象のASINを全件取得し、1件ずつブランド判定を行う。
# ブランドマスタのキャッシュを活用し、同じブランドは再判定しない。
# 10件ごとに中間コミットを行い、全件完了後に最終コミットする。
def main():
    SKIP_CATEGORY_IDS = [465392]

    conn = get_sql_server_connection()
    cursor = conn.cursor()

    print("判定対象データを検索中...")

    # 判定が必要なASINを全件DBから取得する
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

    # 💡 変更点: Chromeの死活監視と必要に応じた自動起動を実行
    # Chrome（ワカルンダ拡張機能）が起動しているか確認する
    if not wakarunda_utils.ensure_chrome_running():
        print("Chromeの準備ができなかったため処理を中断します。")
        conn.close()
        return

    # 同じブランドを何度も検索しないためのメモリ内キャッシュ
    brand_rank_cache = {}
    updated_count = 0

    for row in rows:
        asin = row.asin
        db_brand = row.jp_brand.strip() if row.jp_brand else None

        current_cat_id = getattr(row, 'jp_category_id', None)

        if not db_brand: continue

        print(f"[{updated_count + 1}/{total_rows}] 処理中: ASIN={asin} | Brand={db_brand}")

        rank = None
        needs_judge = False

        # ① メモリ内キャッシュにヒットした場合はキャッシュ値を使う
        if db_brand in brand_rank_cache:
            rank = brand_rank_cache[db_brand]
            if rank == 'E': needs_judge = True
        else:
            # ② キャッシュになければブランドマスタDBを参照する
            cursor.execute(SQL_SELECT_MASTER, [db_brand])
            master_row = cursor.fetchone()
            if master_row:
                rank = master_row[0]
                if rank == 'E':
                    print("   -> [Master Rank E] 再判定対象です。")
                    print("   ワカルンダのチェックをしてください。")
                    print("   -> https://chromewebstore.google.com/detail/%E3%83%AF%E3%82%AB%E3%83%AB%E3%83%B3%E3%83%80/amdhkccebcefomoacnibcemchmfljcoh")
                    needs_judge = True
                else:
                    print(f"   -> [Master Hit] Rank: {rank}")
            else:
                needs_judge = True

        # ③ マスタに未登録または要再判定の場合はChromeでページを開いて判定する
        if needs_judge:
            print("   -> ASINページから直接判定を取得します...")
            fetched_rank, page_brand = wakarunda_utils.fetch_rank_and_brand_by_asin(asin)

            # DBのブランド名とページ上のブランド名を正規化して一致確認する
            clean_db_brand = db_brand.lower().replace(" ", "").replace(" ", "")
            clean_pg_brand = page_brand.lower().replace(" ", "").replace(" ", "")

            if clean_db_brand == clean_pg_brand:
                rank = fetched_rank if fetched_rank != "判定不能" else "E"
                print(f"   -> [Match OK] Result Rank: {rank}")

                # 判定結果をブランドマスタに登録・更新する
                try:
                    cursor.execute(SQL_UPSERT_MASTER, [db_brand, rank])
                    conn.commit()
                except Exception as e:
                    print(f"   [Master DB Error] {e}")

                brand_rank_cache[db_brand] = rank
            else:
                # ブランド名が一致しない場合は「N（不一致）」として扱う
                print(f"   -> [Mismatch] DB:{db_brand} != Page:{page_brand}")
                rank = 'N'

        # ④ 判定結果をメインテーブルに書き込む（10件ごとに中間コミット）
        try:
            cursor.execute(SQL_UPDATE_MAIN, [rank, asin])
            updated_count += 1
            if updated_count % 10 == 0:
                conn.commit()
                print(f"--- {updated_count}件経過 (中間コミット) ---")
        except Exception as e:
            print(f"   [Main DB Update Error] {e}")

    # 全件処理が完了したら最終コミットする
    conn.commit()
    conn.close()
    print(f"\n=== ランク判定完了: {updated_count}件 ===")

if __name__ == "__main__":
    main()
