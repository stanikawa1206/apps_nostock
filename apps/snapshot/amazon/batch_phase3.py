import os
import sys
import time
import pyodbc
import logging
import pandas as pd
from dotenv import load_dotenv, find_dotenv

# ==========================================
# 0. ログ設定
# ==========================================
BASE_DIR = r"X:\apps\snapshot\amazon"
LOG_DIR = BASE_DIR
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "batch_phase3_error.log")

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

from utils import get_sql_server_connection
from amazon_common import (
    verify_export_pipeline, 
    change_listings_price_and_qty,
    MARKET_CONFIGS, get_access_token, create_report, wait_for_report, download_and_save_report
)

load_dotenv(find_dotenv())

if os.environ.get("DB_SERVER") == "sqlserver":
    os.environ["DB_SERVER"] = "MOUSE"

EXCEL_FILE_PATH = os.path.join(BASE_DIR, "ship_cost.xlsx")

def to_py(val):
    if val is None: return None
    if hasattr(val, 'item'): return val.item()
    return val

# ==========================================
# 2. 実行メイン処理
# ==========================================
def run_phase3_batch(target_country="US"):
    target_country = target_country.upper()
    print(f"🚀 【フェーズ3】 {target_country}市場向け 価格改定・在庫同期バッチを起動します\n")
    
    # --------------------------------------------------
    # Step 1: Amazonから最新の出品レポートを取得
    # --------------------------------------------------
    config = MARKET_CONFIGS.get(target_country)
    if not config or not config["token"]:
        print(f"⚠️ {target_country} のAPI設定が見つかりません。終了します。")
        return

    output_dir = os.path.join(BASE_DIR, "listings")
    os.makedirs(output_dir, exist_ok=True)
    report_file_path = os.path.join(output_dir, f"{target_country}_report.txt")

    print(f"📥 {target_country} の現在出品中レポートをリクエストしています...")
    try:
        token = get_access_token(config["token"])
        report_id = create_report(token, config["endpoint"], config["marketplace_id"])
        doc_id = wait_for_report(token, config["endpoint"], report_id)
        download_and_save_report(token, config["endpoint"], doc_id, target_country, output_dir)
        print("✅ レポートのダウンロードが完了しました。\n")
    except Exception as e:
        print(f"❌ レポート取得エラー: {e}")
        logging.error(f"レポート取得エラー ({target_country}): {e}", exc_info=True)
        return

    # --------------------------------------------------
    # Step 2: レポートの読み込みと SKU=ASIN の抽出
    # --------------------------------------------------
    try:
        # Amazonのレポートは基本的にタブ区切り(TSV)です
        df = pd.read_csv(report_file_path, sep='\t', dtype=str)
        # カラム名が 'seller-sku' と 'asin1' であることを前提とします
        df['seller-sku'] = df['seller-sku'].str.strip()
        df['asin1'] = df['asin1'].str.strip()
        
        # 💡 条件: SKU と ASIN が完全に一致するものだけを対象とする（FBA等を除外）
        target_items = df[df['seller-sku'] == df['asin1']].copy()
    except Exception as e:
        print(f"❌ レポートの読み込みに失敗しました: {e}")
        logging.error(f"レポート読み込みエラー: {e}", exc_info=True)
        return

    print(f"📦 レポート内の総件数: {len(df)} 件")
    print(f"🎯 処理対象 (SKU=ASIN): {len(target_items)} 件\n")

    if target_items.empty:
        print("処理対象のASINがありませんでした。")
        return

    # --------------------------------------------------
    # Step 3: データベース接続と更新用クエリの準備
    # --------------------------------------------------
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ データベース接続エラー: {e}")
        return

    # 💡 もしSQLにデータが存在しない場合、空のレコードを作成するクエリ
    insert_ignore_query = """
        IF NOT EXISTS (SELECT 1 FROM trx.amazon_cross_market_asin WHERE asin = ?)
        BEGIN
            INSERT INTO trx.amazon_cross_market_asin (asin, wakarunda) VALUES (?, '-')
        END
    """

    # フェーズ1と同じ、計算結果を丸ごと保存するクエリ
    update_db_query = """
        UPDATE trx.amazon_cross_market_asin
        SET 
            jp_lowest_price_y = ?, us_lowest_price_d = ?, us_lowest_price_y = ?,
            ca_lowest_price_d = ?, ca_lowest_price_y = ?,
            us_existence = ?, ca_existence = ?, Sales_Category = ?,
            jp_sourcing_price = ?, jp_sourcing_fulfillment = ?, jp_sourcing_buybox = ?,
            us_target_price_d = ?, us_target_price_y = ?, us_is_profitable = ?,
            ca_target_price_d = ?, ca_target_price_y = ?, ca_is_profitable = ?,
            last_seen_at = GETDATE()
        WHERE asin = ?
    """

    # --------------------------------------------------
    # Step 4: 1件ずつ最新の利益検証を行い、価格と在庫を更新
    # --------------------------------------------------
    for idx, row in target_items.iterrows():
        asin = row['asin1']
        current_report_price = row.get('price', 0)
        
        print(f"[{idx+1}/{len(target_items)}] 🔄 ASIN: {asin} の状況を確認中...")

        try:
            # 1. データベースにASINが存在するか確認し、無ければ作る
            cursor.execute(insert_ignore_query, (asin, asin))
            conn.commit()

            # 2. 最新のデータでパイプライン（利益計算）を実行
            data = verify_export_pipeline(asin, EXCEL_FILE_PATH)

            # --- ビジネスロジック（アクション決定） ---
            target_qty = 0
            final_price = current_report_price  # デフォルトは現在の価格

            if data["status"] == "error":
                print(f"  ⚠️ データ取得不可 (理由: {data['error_message']}) -> 在庫0にします")
            else:
                trade = data.get("raw_trade", {})
                mkt_data = data.get("markets", {}).get(target_country, {})
                jp_source = trade.get("sourcing_candidates", [{}])[0] if trade.get("sourcing_candidates") else {}
                
                # 計算結果の取り出し
                is_prof = mkt_data.get("is_prof", False)
                calculated_price = mkt_data.get("target_native")
                
                # 日本の仕入れ元が存在しない場合
                if not jp_source:
                    print("  ⚠️ 日本の仕入先が見つかりません -> 在庫0にします")
                # 利益が出ない場合
                elif not is_prof:
                    print("  ⚠️ 利益基準を満たしていません (赤字転落) -> 在庫0にします")
                # 出品可能な場合
                else:
                    if calculated_price:
                        target_qty = 1  # 利益が出るので在庫1で販売継続
                        final_price = calculated_price
                        print(f"  ✅ 利益確保可！ -> 価格を {round(final_price, 2)} {target_country}ドルに更新します")

            # 3. Amazon APIへ価格・在庫の変更を送信
            final_price_rounded = round(float(final_price), 2)
            api_result = change_listings_price_and_qty(target_country, asin, final_price_rounded, target_qty)
            
            if api_result:
                print(f"  ✨ 更新完了: Qty = {target_qty}, Price = {final_price_rounded}")
            else:
                print(f"  ❌ API更新に失敗しました")

            # 4. データベースの情報を最新の計算結果で上書き（フェーズ1と同じ処理）
            if data["status"] != "error":
                lowest = trade.get("lowest_prices", {})
                jp_prices = lowest.get("JP") or {}
                us_prices = lowest.get("US") or {}
                ca_prices = lowest.get("CA") or {}
                us_mkt = data.get("markets", {}).get("US", {})
                ca_mkt = data.get("markets", {}).get("CA", {})

                params = (
                    jp_prices.get("jpy"), us_prices.get("original"), us_prices.get("jpy"),
                    ca_prices.get("original"), ca_prices.get("jpy"),
                    1 if trade.get("has_catalog", {}).get("US") else 0, 1 if trade.get("has_catalog", {}).get("CA") else 0, trade.get("judgment", "None"),
                    jp_source.get("total_jpy"), jp_source.get("fulfillment"), 1 if jp_source.get("is_buybox") else 0,
                    us_mkt.get("target_native"), us_mkt.get("target_jpy"), 1 if us_mkt.get("is_prof") else 0,
                    ca_mkt.get("target_native"), ca_mkt.get("target_jpy"), 1 if ca_mkt.get("is_prof") else 0,
                    asin
                )
                params = tuple(to_py(v) for v in params)
                cursor.execute(update_db_query, params)
                conn.commit()

            time.sleep(1.5)  # API制限回避

        except Exception as e:
            print(f"  ❌ 予期せぬエラー: {e}")
            logging.error(f"ASIN: {asin} - 同期エラー: {e}", exc_info=True)
            try: conn.rollback()
            except: pass

    try: conn.close()
    except: pass
    print(f"\n🎉 フェーズ3の価格改定・在庫同期バッチ ({target_country}) がすべて完了しました！")

# ==========================================
# 起動用フック
# ==========================================
if __name__ == "__main__":
    run_phase3_batch("US")