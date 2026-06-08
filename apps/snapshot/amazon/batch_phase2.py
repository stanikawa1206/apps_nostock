import os
import sys
import time
import pyodbc
import logging
from dotenv import load_dotenv, find_dotenv

# ==========================================
# 0. ログ設定
# ==========================================
# フェーズ1と同じフォルダに phase2_error.log を作成してエラーを記録します
LOG_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\amazon"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "batch_phase2_error.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ==========================================
# 1. 外部モジュールの読み込み
# ==========================================
COMMON_UTILS_DIR = r"\\MOUSE\apps_nostock\apps\common"
if COMMON_UTILS_DIR not in sys.path:
    sys.path.append(COMMON_UTILS_DIR)

# utils.py と amazon_common.py が読み込める前提
from utils import get_sql_server_connection
from amazon_common import create_new_listing

load_dotenv(find_dotenv())

# DB接続先の上書き設定（必要に応じて）
if os.environ.get("DB_SERVER") == "sqlserver":
    os.environ["DB_SERVER"] = "MOUSE"

# ==========================================
# 2. 実行メイン処理
# ==========================================
def run_phase2_batch(target_country="US"):
    """
    指定された国の利益商品を抽出し、Amazonへ相乗り出品を行うバッチ処理。
    target_country: "US" または "CA"
    """
    target_country = target_country.upper()
    print(f"🚀 【フェーズ2】 {target_country}市場向け 相乗り出品バッチを起動します\n")
    print("🔄 データベースに接続中...")
    
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        print("✅ データベースへの接続に成功しました！\n")
    except Exception as e:
        print(f"❌ 初回データベース接続エラー: {e}")
        logging.error(f"初回データベース接続エラー: {e}", exc_info=True)
        return

    # ターゲット国に応じたカラム名の自動切り替え
    if target_country == "US":
        col_profit = "us_is_profitable"
        col_exist = "us_existence"
        col_price = "us_target_price_d"
        col_listed_date = "US_listed_date"
        restriction_clause = "AND (US_restriction IS NULL OR US_restriction = '' OR US_restriction = '〇')"
    elif target_country == "CA":
        col_profit = "ca_is_profitable"
        col_exist = "ca_existence"
        col_price = "ca_target_price_d"
        col_listed_date = "CA_listed_date"
        restriction_clause = ""  # CA用の制限カラムがあればここに追加
    else:
        print("⚠️ target_country の指定が不正です。終了します。")
        return

    # ==========================================
    # 3. 出品対象ASINの抽出 (安全基準フィルター適用)
    # ==========================================
    select_query = f"""
        SELECT 
            asin, 
            {col_price} AS target_price, 
            jp_sourcing_handling_days, 
            jp_sourcing_fulfillment
        FROM trx.amazon_cross_market_asin
        WHERE 
            {col_profit} = 1 
            AND {col_exist} = 1
            AND jp_sourcing_price IS NOT NULL
            AND {col_listed_date} IS NULL
            {restriction_clause}
            
            -- リスク管理条件（安全基準）
            -- 💡 ハンドリングタイムが14日以内、または「FBAかつ空欄(NULL)」のもの
            AND (jp_sourcing_handling_days <= 14 OR (jp_sourcing_fulfillment = 'FBA' AND jp_sourcing_handling_days IS NULL))
            AND jp_sourcing_feedback_percent >= 90
            AND jp_sourcing_feedback_count >= 50
    """

    try:
        cursor.execute(select_query)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"❌ データの抽出中にエラーが発生しました: {e}")
        logging.error(f"データ抽出エラー: {e}", exc_info=True)
        return

    print(f"📦 今回の出品対象: {len(rows)} 件\n")

    if not rows:
        print("出品対象のASINがありません。終了します。")
        conn.close()
        return

    # ==========================================
    # 4. 出品処理ループ
    # ==========================================
    update_query = f"""
        UPDATE trx.amazon_cross_market_asin
        SET {col_listed_date} = GETDATE()
        WHERE asin = ?
    """

    for idx, row in enumerate(rows, 1):
        asin = row.asin
        raw_price = getattr(row, "target_price")
        fulfillment = row.jp_sourcing_fulfillment
        handling_days_jp = row.jp_sourcing_handling_days
        
        print(f"[{idx}/{len(rows)}] 出品処理中: {asin} ...")
        
        try:
            # 💡 出品パラメータの構築
            sku = asin  # SKUはASINと一致させる
            qty = 0     # 初期在庫数は安全のため0
            price = round(float(raw_price), 2) # 小数点第2位で四捨五入
            
            # 💡 ハンドリングタイムの計算ロジック
            # FBAかつ仕入日数が不明(NULLまたは0)の場合は5日
            if fulfillment == 'FBA' and (handling_days_jp is None or handling_days_jp == 0):
                handling_time = 5
            else:
                # それ以外は 日本の仕入日数 + バッファ3日
                handling_time = int(handling_days_jp or 0) + 3

            # APIを通して出品を実行
            result = create_new_listing(target_country, sku, asin, price, qty, handling_time)
            
            if result is not None:
                # 出品成功時のみ、データベースに現在時刻を書き込んで更新
                cursor.execute(update_query, (asin,))
                conn.commit()
                print(f"  ✅ 出品成功 (Price: {price}, Handling: {handling_time}日)")
            else:
                # 出品失敗（create_new_listing内でエラー内容はprintされます）
                logging.error(f"ASIN: {asin} - API出品処理でエラーが返されました。")
                print(f"  ❌ 出品失敗")
                
            # APIの連続呼び出し制限（スロットリング）を避けるためのウェイト
            time.sleep(2)
            
        except Exception as e:
            print(f"  ❌ 予期せぬエラー: {e}")
            logging.error(f"ASIN: {asin} - 予期せぬエラー: {e}", exc_info=True)
            try:
                conn.rollback()
            except:
                pass

    try:
        conn.close()
    except:
        pass
    print(f"\n🎉 フェーズ2の相乗り出品バッチ ({target_country}) がすべて完了しました！")

# ==========================================
# 5. 起動用フック
# ==========================================
if __name__ == "__main__":
    # ファイルを直接実行した場合は、US市場向けに実行します
    run_phase2_batch("US")
    
    # カナダも連続して実行したい場合は以下を有効化してください
    # run_phase2_batch("CA")