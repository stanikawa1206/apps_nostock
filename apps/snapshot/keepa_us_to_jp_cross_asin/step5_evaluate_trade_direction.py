# step5_evaluate_trade_direction.py
# -*- coding: utf-8 -*-
import os
import sys

# 💡 amazon_common.py が存在する common フォルダへパスを通す
COMMON_UTILS_DIR = r"\\MOUSE\apps_nostock\apps\common"
if COMMON_UTILS_DIR not in sys.path:
    sys.path.append(COMMON_UTILS_DIR)

from my_utils import get_sql_server_connection
# 💡 get_exchange_rates をインポートに追加
from amazon_common import calculate_shipping_costs, get_exchange_rates 

# ==========================================
# 設定
# ==========================================
# 💡 送料計算用Excelのパス
EXCEL_FILE_PATH = r"X:\apps\snapshot\amazon\ship_cost.xlsx"

# 目標利益率と暫定手数料率（API消費を抑えるための事前評価用）
TARGET_MARGIN_US = 0.25   # 米国 25%
TARGET_MARGIN_CA = 0.20   # カナダ 20%
TARGET_MARGIN_JP = 0.20   # 日本(輸入時) 20%
DEFAULT_FEE_RATE = 0.15   # Amazon販売手数料 15% (固定)

BATCH_SIZE = 1000         # DBを更新する際のコミット単位

# ==========================================
# 評価用関数 (最安値・送料・最新レート・利益率ベース)
# ==========================================
def evaluate_profitability(
    jp_lowest_price_y,
    us_exist, us_lowest_price_d, 
    ca_exist, ca_lowest_price_d,
    length, width, height, actual_weight,
    exchange_rate_us, exchange_rate_ca  # 💡 引数に最新レートを追加
):
    """
    Step 2で取得した日本の最安値(仮の仕入値)とサイズ、現地の最安値を用いて、
    実質的な利益率が目標(25%等)をクリアするかで輸出入を判定する
    """
    # 1. 必須データの欠損チェック (日本の価格がない、またはサイズ不明な場合は保留)
    if jp_lowest_price_y is None or not length or not width or not height or not actual_weight:
        return "判定保留", "判定保留", None, None

    # 容積重量と実重量から請求重量(Chargeable Weight)を算出
    sum_cm = float(length) + float(width) + float(height)
    vol_w = (float(length) * float(width) * float(height)) / 5000.0 * 1000.0
    chargeable_w = max(float(actual_weight), vol_w)

    # 2. Excelテーブルに基づく正確な送料の算出
    try:
        cost_us, cost_ca, cost_jp, cost_myus_usd, carrier = calculate_shipping_costs(
            chargeable_w, float(actual_weight), sum_cm, EXCEL_FILE_PATH
        )
        # 💡 最新のUSDレートをMyUS送料に適用
        cost_myus_jpy = cost_myus_usd * exchange_rate_us if cost_myus_usd else None
    except Exception as e:
        # サイズや重量がExcelのテーブル上限を超えている商品は不可とする
        return "不可(サイズ超過)", "不可(サイズ超過)", None, None

    # 3. US市場の評価
    eval_us = "不可"
    us_calc_y = None
    
    if us_exist == 1 and us_lowest_price_d:
        # 💡 最新のUSDレートを現地販売価格に適用
        us_sales_jpy = float(us_lowest_price_d) * exchange_rate_us
        us_calc_y = us_sales_jpy
        
        # 【輸出チェック】(日本の最安値で買ってUSで売る仮定)
        us_fee = us_sales_jpy * DEFAULT_FEE_RATE
        us_profit = us_sales_jpy - us_fee - float(jp_lowest_price_y) - cost_us
        us_margin = (us_profit / us_sales_jpy) if us_sales_jpy > 0 else 0
        
        if us_margin >= TARGET_MARGIN_US:
            eval_us = "輸出"
        elif us_margin > 0:
            eval_us = "利益薄" 
            
        # 【輸入チェック】(US最安値で買って日本最安値で売る仮定)
        if eval_us in ("不可", "利益薄") and cost_myus_jpy and jp_lowest_price_y:
            jp_sales_jpy = float(jp_lowest_price_y)
            jp_fee = jp_sales_jpy * DEFAULT_FEE_RATE
            # 💡 最新のUSDレートを現地仕入れ価格に適用
            us_sourcing_jpy = float(us_lowest_price_d) * exchange_rate_us
            
            jp_profit = jp_sales_jpy - jp_fee - us_sourcing_jpy - cost_myus_jpy
            jp_margin = (jp_profit / jp_sales_jpy) if jp_sales_jpy > 0 else 0
            if jp_margin >= TARGET_MARGIN_JP:
                eval_us = "輸入"
                
    elif us_exist == 2:
        # USカタログが存在しないが、日本で仕入れ可能な場合は新規出品の候補
        eval_us = "輸出(新規)"

    # 4. CA市場の評価
    eval_ca = "不可"
    ca_calc_y = None
    
    if ca_exist == 1 and ca_lowest_price_d:
        # 💡 最新のCADレートを現地販売価格に適用
        ca_sales_jpy = float(ca_lowest_price_d) * exchange_rate_ca
        ca_calc_y = ca_sales_jpy
        
        # 【輸出チェック】
        ca_fee = ca_sales_jpy * DEFAULT_FEE_RATE
        ca_profit = ca_sales_jpy - ca_fee - float(jp_lowest_price_y) - cost_ca
        ca_margin = (ca_profit / ca_sales_jpy) if ca_sales_jpy > 0 else 0
        
        if ca_margin >= TARGET_MARGIN_CA:
            eval_ca = "輸出"
        elif ca_margin > 0:
            eval_ca = "利益薄"
            
    elif ca_exist == 2:
        eval_ca = "輸出(新規)"

    return eval_us, eval_ca, us_calc_y, ca_calc_y

# ==========================================
# メイン処理
# ==========================================
def main():
    # 💡 最初に最新の為替レートを取得
    print("為替レートを取得しています...")
    rates = get_exchange_rates()
    current_rate_us = rates.get("USD", 150.0)
    current_rate_ca = rates.get("CAD", 110.0)

    try:
        conn = get_sql_server_connection()
    except Exception as e:
        print(f"DB接続エラー: {e}")
        sys.exit(1)

    cursor = conn.cursor()

    print(f"\n{'='*40}")
    print(f"=== Step5: 輸出入判定(送料・最安値統合版)を開始します ===")
    print(f"適用レート: 1USD = {current_rate_us:.2f}円, 1CAD = {current_rate_ca:.2f}円")
    print(f"{'='*40}")

    sql_select = """
        SELECT 
            asin, 
            jp_lowest_price_y,
            us_existence, us_lowest_price_d, 
            ca_existence, ca_lowest_price_d,
            length, width, height, actual_weight
        FROM trx.amazon_cross_market_asin WITH (NOLOCK)
        WHERE (wakarunda IN ('D', '-')) 
          AND (
              last_evaluated_at IS NULL
              OR last_evaluated_at < jp_price_updated_at
              OR last_evaluated_at < us_price_updated_at
              OR last_evaluated_at < ca_price_updated_at
          )
    """

    try:
        cursor.execute(sql_select)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"データ抽出エラー: {e}")
        conn.close()
        sys.exit(1)

    total_records = len(rows)
    print(f"新規評価対象レコード: {total_records}件\n")

    if total_records == 0:
        print("未評価の対象データがありませんでした。")
        conn.close()
        return

    sql_update = """
        UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK)
        SET 
            JP_vs_US = ?, 
            JP_vs_CA = ?,
            us_lowest_price_y = ?,
            ca_lowest_price_y = ?,
            last_evaluated_at = SYSDATETIME(),
            last_seen_at = SYSDATETIME()
        WHERE asin = ?
    """

    update_data = []
    processed_count = 0

    for row in rows:
        (asin, jp_price_y, 
         us_exist, us_price_d, 
         ca_exist, ca_price_d, 
         length, width, height, actual_weight) = row

        # 評価用関数へデータをパス (最新レートも渡す)
        eval_us, eval_ca, us_calc_y, ca_calc_y = evaluate_profitability(
            jp_price_y,
            us_exist, us_price_d, 
            ca_exist, ca_price_d,
            length, width, height, actual_weight,
            current_rate_us, current_rate_ca  # 💡 引数追加
        )

        update_data.append((eval_us, eval_ca, us_calc_y, ca_calc_y, asin))

        if len(update_data) >= BATCH_SIZE:
            try:
                cursor.executemany(sql_update, update_data)
                conn.commit()
                processed_count += len(update_data)
                print(f"進捗: {processed_count} / {total_records} 件処理完了")
                update_data = []
            except Exception as e:
                print(f"DB更新エラー(バッチ実行中): {e}")
                conn.rollback()

    if update_data:
        try:
            cursor.executemany(sql_update, update_data)
            conn.commit()
            processed_count += len(update_data)
            print(f"進捗: {processed_count} / {total_records} 件処理完了")
        except Exception as e:
            print(f"DB更新エラー(端数実行中): {e}")
            conn.rollback()

    conn.close()
    print("\n=== 全ての評価・更新処理が完了しました ===")

if __name__ == "__main__":
    main()