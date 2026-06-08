import os
import sys
import time
import pyodbc
import re  
import logging
import math
import pandas as pd
from dotenv import load_dotenv, find_dotenv

# ==========================================
# 0. ログ設定
# ==========================================
LOG_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\amazon"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "batch_error.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ==========================================
# 1. 外部の共通ライブラリを読み込むための設定
# ==========================================
COMMON_UTILS_DIR = r"\\MOUSE\apps_nostock\apps\common"
if COMMON_UTILS_DIR not in sys.path:
    sys.path.append(COMMON_UTILS_DIR)

from utils import get_sql_server_connection
from amazon_common import verify_export_pipeline

load_dotenv(find_dotenv())

if os.environ.get("DB_SERVER") == "sqlserver":
    os.environ["DB_SERVER"] = "MOUSE"  

EXCEL_FILE_PATH = r"X:\apps\snapshot\amazon\ship_cost.xlsx"

def to_py(val):
    # Pandasの機能で NaN や NaT (Not a Time), None をまとめて弾く
    if pd.isna(val):
        return None
    
    # 文字列化して空文字になるものも NULL とする
    if str(val).strip() == "":
        return None
        
    # numpyの型をPython標準型に変換
    if hasattr(val, 'item'):
        val = val.item()
        
    # Python標準の float 型になった後の NaN や Infinity のチェック
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
        
    return val

def parse_handling_days(handling_str):
    if not handling_str:
        return None
    handling_str = str(handling_str)
    if "即日" in handling_str:
        return 0
    numbers = re.findall(r'\d+', handling_str)
    if numbers:
        return int(numbers[-1])
    return None

# 💡 修正: test_asin 引数を追加
def run_phase1_batch(test_asin=None):
    print("🔄 データベースに接続中...")
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        print("✅ データベースへの接続に成功しました！\n")
    except Exception as e:
        print(f"❌ 初回データベース接続エラー: {e}")
        logging.error(f"初回データベース接続エラー: {e}", exc_info=True)
        return

    # ==========================================
    # 2. 対象ASINの抽出
    # ==========================================
    # 💡 修正: test_asin が指定されている場合はそのASINだけを抽出するクエリに変更
    if test_asin:
        print(f"🧪 テストモード: ASIN [{test_asin}] のみ処理します")
        select_query = """
            SELECT asin, Referral_Fee_rate
            FROM trx.amazon_cross_market_asin
            WHERE asin = ?
        """
        try:
            cursor.execute(select_query, (test_asin,))
            rows = cursor.fetchall()
        except Exception as e:
            print(f"❌ テストASINのデータ抽出中にエラーが発生しました: {e}")
            logging.error(f"データ抽出エラー: {e}", exc_info=True)
            return
    else:
        # 通常の全件取得クエリ
        select_query = """
            SELECT asin, Referral_Fee_rate
            FROM trx.amazon_cross_market_asin
            WHERE wakarunda IN ('-', 'D')
            AND (US_restriction IS NULL OR US_restriction = '' OR US_restriction = '〇')
            AND (
                last_seen_at IS NULL 
                OR last_seen_at < DATEADD(day, -3, GETDATE())
                OR (US_listed_date IS NOT NULL AND last_seen_at < DATEADD(day, -1, GETDATE()))
            )
        """
        try:
            cursor.execute(select_query)
            rows = cursor.fetchall()
        except Exception as e:
            print(f"❌ データの抽出中にエラーが発生しました: {e}")
            logging.error(f"データ抽出エラー: {e}", exc_info=True)
            return

    print(f"📦 今回の処理対象: {len(rows)} 件\n")

    if not rows:
        print("処理対象のASINが見つかりません。終了します。")
        conn.close()
        return

    # ==========================================
    # 3. 全項目を更新するUPDATE文
    # ==========================================
    update_query = """
        UPDATE trx.amazon_cross_market_asin
        SET 
            jp_lowest_price_y = ?, us_lowest_price_d = ?, us_lowest_price_y = ?,
            ca_lowest_price_d = ?, ca_lowest_price_y = ?,
            us_existence = ?, ca_existence = ?, Sales_Category = ?,
            
            length = ?, width = ?, height = ?, total_size = ?,
            actual_weight = ?, dim_weight = ?, chargeable_weight = ?,
            jp_shipping_fee = ?, us_shipping_fee = ?, ca_shipping_fee = ?, MyUS_shipping_fee = ?,
            
            jp_sourcing_price = ?, jp_sourcing_fulfillment = ?, jp_sourcing_buybox = ?,
            jp_sourcing_handling_days = ?, jp_sourcing_feedback_count = ?, jp_sourcing_feedback_percent = ?,
            
            us_target_price_d = ?, us_target_price_y = ?, us_is_profitable = ?,
            ca_target_price_d = ?, ca_target_price_y = ?, ca_is_profitable = ?,
            
            last_seen_at = GETDATE(), US_Venesplo = GETDATE(), CA_Venesplo = GETDATE()
        WHERE asin = ?
    """

    for idx, row in enumerate(rows, 1):
        asin = row.asin
        custom_fee = float(row.Referral_Fee_rate) if row.Referral_Fee_rate else None
        
        print(f"[{idx}/{len(rows)}] 処理中: {asin} ...")
        
        try:
            data = verify_export_pipeline(asin, EXCEL_FILE_PATH, custom_fee)
            
            if data["status"] == "error":
                print(f"  ⚠️ スキップされました (理由: {data['error_message']})")
                continue
                
            trade = data.get("raw_trade", {})
            ship = data.get("raw_ship", {})
            jp_source = trade.get("sourcing_candidates", [{}])[0] if trade.get("sourcing_candidates") else {}
            us_mkt = data.get("markets", {}).get("US", {})
            ca_mkt = data.get("markets", {}).get("CA", {})

            lowest = trade.get("lowest_prices", {})
            jp_prices = lowest.get("JP") or {}
            us_prices = lowest.get("US") or {}
            ca_prices = lowest.get("CA") or {}
            has_catalog = trade.get("has_catalog", {})

            params = (
                jp_prices.get("jpy"),
                us_prices.get("original"), us_prices.get("jpy"),
                ca_prices.get("original"), ca_prices.get("jpy"),
                
                1 if has_catalog.get("US") else 0, 1 if has_catalog.get("CA") else 0, trade.get("judgment", "None"),
                
                ship.get("l"), ship.get("w"), ship.get("h"), ship.get("sum_cm"),
                ship.get("actual_w"), ship.get("vol", 0) / 5000.0 * 1000.0, ship.get("chargeable_w"), 
                ship.get("cost_jp"), ship.get("cost_us"), ship.get("cost_ca"), ship.get("cost_myus"),
                
                jp_source.get("total_jpy"), jp_source.get("fulfillment"), 1 if jp_source.get("is_buybox") else 0,
                parse_handling_days(jp_source.get("handling_time")), 
                jp_source.get("feedback_count"), jp_source.get("feedback_percent"),
                
                us_mkt.get("target_native"), us_mkt.get("target_jpy"), 1 if us_mkt.get("is_prof") else 0,
                ca_mkt.get("target_native"), ca_mkt.get("target_jpy"), 1 if ca_mkt.get("is_prof") else 0,
                
                asin
            )

            params = tuple(to_py(v) for v in params)

            max_retries = 3
            success = False
            for attempt in range(max_retries):
                try:
                    cursor.execute(update_query, params)
                    conn.commit()
                    success = True
                    break
                except pyodbc.Error as db_err:
                    err_msg = str(db_err).lower()
                    if "connection" in err_msg or "08s01" in err_msg or "08003" in err_msg:
                        print(f"  ⚠️ データベース切断を検知。再接続します ({attempt+1}/{max_retries})...")
                        logging.warning(f"ASIN: {asin} 更新中のDB切断。再接続試行 ({attempt+1}) - 詳細: {db_err}")
                        
                        try: conn.close()
                        except: pass
                        
                        time.sleep(3)
                        try:
                            conn = get_sql_server_connection()
                            cursor = conn.cursor()
                        except Exception as reconnect_err:
                            logging.error(f"再接続失敗: {reconnect_err}")
                    else:
                        raise db_err
            
            if success:
                print(f"  ✅ DBの更新が完了しました")
            else:
                raise Exception("データベースの更新に失敗しました（リトライ上限到達）")
            
        except Exception as e:
            print(f"  ❌ 処理エラー: {e}")
            logging.error(f"ASIN: {asin} - 処理エラー: {e}", exc_info=True)
            try:
                conn.rollback()
            except:
                pass

    try:
        conn.close()
    except:
        pass
    print("\n🎉 処理が完了しました！")

# 💡 修正: コマンドライン引数を受け取って実行するように変更
if __name__ == "__main__":
    # 引数が渡されていればそれを test_asin として扱う
    target_asin = sys.argv[1] if len(sys.argv) > 1 else None
    run_phase1_batch(target_asin)