# step5_evaluate_trade_direction.py
# -*- coding: utf-8 -*-
import os
import sys

# 💡 amazon_common.py が存在する common フォルダへパスを通す
COMMON_UTILS_DIR = r"\\MOUSE\apps_nostock\apps\common"
if COMMON_UTILS_DIR not in sys.path:
    sys.path.append(COMMON_UTILS_DIR)

from my_utils import get_sql_server_connection
from amazon_common import calculate_shipping_costs, get_exchange_rates 

# ==========================================
# 設定
# ==========================================
EXCEL_FILE_PATH = r"X:\apps\snapshot\amazon\ship_cost.xlsx"

TARGET_MARGIN_US = 0.25   # 米国 25%
TARGET_MARGIN_CA = 0.20   # カナダ 20%
TARGET_MARGIN_JP = 0.20   # 日本(輸入時) 20%
DEFAULT_FEE_RATE = 0.15   # Amazon販売手数料 15% (固定)

BATCH_SIZE = 1000         # DBを更新する際のコミット単位

# ==========================================
# 評価用関数 (算出データを辞書型ですべて返す)
# ==========================================
def evaluate_profitability(
    jp_lowest_price_y, us_exist, us_lowest_price_d, 
    ca_exist, ca_lowest_price_d, length, width, height, actual_weight,
    exchange_rate_us, exchange_rate_ca
):
    res = {
        'eval_us': None, 'eval_ca': None,
        'us_p_y': None, 'ca_p_y': None,
        'us_target_d': None, 'us_target_y': None,
        'ca_target_d': None, 'ca_target_y': None,
        'jp_target_y': None,
        'total_size': None, 'dim_weight': None, 'chargeable_weight': None,
        'cost_jp': None, 'cost_us': None, 'cost_ca': None, 'cost_myus_usd': None
    }

    # 💡 必須データチェック
    if not length or not width or not height or not actual_weight:
        return res

    # 💡 US/CAのどちらかは存在フラグ(1か2)を持っていないと評価できない
    if us_exist not in (1, 2) and ca_exist not in (1, 2):
        return res

    jp_p = float(jp_lowest_price_y) if jp_lowest_price_y is not None else None
    us_p = float(us_lowest_price_d) * exchange_rate_us if us_lowest_price_d else None
    ca_p = float(ca_lowest_price_d) * exchange_rate_ca if ca_lowest_price_d else None
    
    res['us_p_y'] = us_p
    res['ca_p_y'] = ca_p

    sum_cm = float(length) + float(width) + float(height)
    vol_w = (float(length) * float(width) * float(height)) / 5000.0 * 1000.0
    chargeable_w = max(float(actual_weight), vol_w)
    
    res['total_size'] = sum_cm
    res['dim_weight'] = vol_w
    res['chargeable_weight'] = chargeable_w

    try:
        cost_us, cost_ca, cost_jp, cost_myus_usd, carrier = calculate_shipping_costs(
            chargeable_w, float(actual_weight), sum_cm, EXCEL_FILE_PATH
        )
        res['cost_us'] = cost_us
        res['cost_ca'] = cost_ca
        res['cost_jp'] = cost_jp
        res['cost_myus_usd'] = cost_myus_usd
        cost_myus_jpy = cost_myus_usd * exchange_rate_us if cost_myus_usd else None
    except Exception:
        res['eval_us'] = "不可(サイズ超過)"
        res['eval_ca'] = "不可(サイズ超過)"
        return res

    # ========================================
    # US市場の評価
    # ========================================
    eval_us = None
    us_target_price_d = None
    us_target_price_y = None
    jp_target_price_y_us = None
    
    if us_exist == 1:
        if us_p:
            if jp_p is None:
                if cost_myus_jpy:
                    jp_target_price_y_us = (us_p + cost_myus_jpy) / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    eval_us = "輸入(ライバル不在)"
                else:
                    eval_us = "不可"
            elif jp_p < us_p:
                total_cost_us = jp_p + (cost_us or 0)
                us_target_price_y = total_cost_us / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_US)
                us_target_price_d = us_target_price_y / exchange_rate_us
                if float(us_lowest_price_d) >= us_target_price_d:
                    eval_us = "輸出"
                else:
                    eval_us = "不可"
            elif jp_p > us_p:
                if cost_myus_jpy:
                    jp_target_price_y_us = (us_p + cost_myus_jpy) / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    if jp_p >= jp_target_price_y_us:
                        eval_us = "輸入"
                    else:
                        eval_us = "不可"
                else:
                    eval_us = "不可"
            else:
                eval_us = "不可"
        else:
            if jp_p is None:
                eval_us = "不可(価格基準なし)"
            else:
                total_cost_us = jp_p + (cost_us or 0)
                us_target_price_y = total_cost_us / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_US)
                us_target_price_d = us_target_price_y / exchange_rate_us
                eval_us = "輸出(ライバル不在)"
    elif us_exist == 2:
        eval_us = "不可(カタログなし)"

    res['eval_us'] = eval_us
    res['us_target_d'] = us_target_price_d
    res['us_target_y'] = us_target_price_y

    # ========================================
    # CA市場の評価
    # ========================================
    eval_ca = None
    ca_target_price_d = None
    ca_target_price_y = None
    jp_target_price_y_ca = None
    
    if ca_exist == 1:
        if ca_p:
            if jp_p is None:
                if cost_myus_jpy:
                    jp_target_price_y_ca = (ca_p + cost_myus_jpy) / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    eval_ca = "輸入(ライバル不在)"
                else:
                    eval_ca = "不可"
            elif jp_p < ca_p:
                total_cost_ca = jp_p + (cost_ca or 0)
                ca_target_price_y = total_cost_ca / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_CA)
                ca_target_price_d = ca_target_price_y / exchange_rate_ca
                if float(ca_lowest_price_d) >= ca_target_price_d:
                    eval_ca = "輸出"
                else:
                    eval_ca = "不可"
            elif jp_p > ca_p:
                if cost_myus_jpy:
                    jp_target_price_y_ca = (ca_p + cost_myus_jpy) / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    if jp_p >= jp_target_price_y_ca:
                        eval_ca = "輸入"
                    else:
                        eval_ca = "不可"
                else:
                    eval_ca = "不可"
            else:
                eval_ca = "不可"
        else:
            if jp_p is None:
                eval_ca = "不可(価格基準なし)"
            else:
                total_cost_ca = jp_p + (cost_ca or 0)
                ca_target_price_y = total_cost_ca / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_CA)
                ca_target_price_d = ca_target_price_y / exchange_rate_ca
                eval_ca = "輸出(ライバル不在)"
    elif ca_exist == 2:
        eval_ca = "不可(カタログなし)"

    res['eval_ca'] = eval_ca
    res['ca_target_d'] = ca_target_price_d
    res['ca_target_y'] = ca_target_price_y

    # ========================================
    # JP目標価格の最終決定 (最小値を採用)
    # ========================================
    jp_target_price_y_final = None
    if jp_target_price_y_us and jp_target_price_y_ca:
        jp_target_price_y_final = min(jp_target_price_y_us, jp_target_price_y_ca)
    elif jp_target_price_y_us:
        jp_target_price_y_final = jp_target_price_y_us
    elif jp_target_price_y_ca:
        jp_target_price_y_final = jp_target_price_y_ca

    res['jp_target_y'] = jp_target_price_y_final

    return res

# ==========================================
# メイン処理 (一括バッチアップデート)
# ==========================================
def main():
    print("為替レートを取得しています...")
    rates = get_exchange_rates()
    current_rate_us = rates.get("USD", 150.0)
    current_rate_ca = rates.get("CAD", 110.0)
    print(f"適用レート: 1USD = {current_rate_us:.2f}円, 1CAD = {current_rate_ca:.2f}円")

    try:
        conn = get_sql_server_connection()
    except Exception as e:
        print(f"DB接続エラー: {e}")
        sys.exit(1)

    cursor = conn.cursor()

    # 💡 抽出条件に「usかcaのどちらかが1または2であること」を追加
    sql_select = """
        SELECT 
            asin, 
            jp_lowest_price_y,
            us_existence, us_lowest_price_d, 
            ca_existence, ca_lowest_price_d,
            length, width, height, actual_weight
        FROM trx.amazon_cross_market_asin WITH (NOLOCK)
        WHERE (wakarunda IN ('D', '-')) 
          AND (us_existence IN (1, 2) OR ca_existence IN (1, 2))
          AND (
            last_evaluated_at IS NULL
              OR last_evaluated_at < jp_price_updated_at
              OR last_evaluated_at < us_price_updated_at
              OR last_evaluated_at < ca_price_updated_at
          )
    """

    sql_update = """
        UPDATE trx.amazon_cross_market_asin
        SET 
            JP_vs_US = ?,
            JP_vs_CA = ?,
            us_lowest_price_y = ?,
            ca_lowest_price_y = ?,
            us_target_price_d = ?,
            us_target_price_y = ?,
            ca_target_price_d = ?,
            ca_target_price_y = ?,
            jp_target_price_y = ?,
            total_size = ?,
            dim_weight = ?,
            chargeable_weight = ?,
            jp_shipping_fee = ?,
            us_shipping_fee = ?,
            ca_shipping_fee = ?,
            MyUS_shipping_fee = ?,
            last_evaluated_at = CURRENT_TIMESTAMP,
            last_seen_at = CURRENT_TIMESTAMP
        WHERE asin = ?
    """

    print("評価対象データを抽出しています...")
    try:
        cursor.execute(sql_select)
        rows = cursor.fetchall()
        total_records = len(rows)
        print(f"抽出完了: 対象 {total_records} 件")
    except Exception as e:
        print(f"データ抽出エラー: {e}")
        conn.close()
        sys.exit(1)

    update_data = []
    processed_count = 0

    for row in rows:
        (asin, jp_price_y, 
         us_exist, us_price_d, 
         ca_exist, ca_price_d, 
         length, width, height, actual_weight) = row

        res = evaluate_profitability(
            jp_price_y, us_exist, us_price_d, ca_exist, ca_price_d,
            length, width, height, actual_weight,
            current_rate_us, current_rate_ca
        )

        val_jp_us = res['eval_us']
        val_jp_ca = res['eval_ca']
        
        val_us_py = int(res['us_p_y']) if res['us_p_y'] is not None else None
        val_ca_py = int(res['ca_p_y']) if res['ca_p_y'] is not None else None
        
        val_us_td = round(res['us_target_d'], 2) if res['us_target_d'] is not None else None
        val_us_ty = int(res['us_target_y']) if res['us_target_y'] is not None else None
        val_ca_td = round(res['ca_target_d'], 2) if res['ca_target_d'] is not None else None
        val_ca_ty = int(res['ca_target_y']) if res['ca_target_y'] is not None else None
        val_jp_ty = int(res['jp_target_y']) if res['jp_target_y'] is not None else None
        
        val_tot_s = round(res['total_size'], 2) if res['total_size'] is not None else None
        val_dim_w = int(res['dim_weight']) if res['dim_weight'] is not None else None
        val_chr_w = int(res['chargeable_weight']) if res['chargeable_weight'] is not None else None
        
        val_jp_sf = int(res['cost_jp']) if res['cost_jp'] is not None else None
        val_us_sf = int(res['cost_us']) if res['cost_us'] is not None else None
        val_ca_sf = int(res['cost_ca']) if res['cost_ca'] is not None else None
        val_my_sf = round(res['cost_myus_usd'], 2) if res['cost_myus_usd'] is not None else None

        update_data.append((
            val_jp_us, val_jp_ca,
            val_us_py, val_ca_py,
            val_us_td, val_us_ty,
            val_ca_td, val_ca_ty,
            val_jp_ty,
            val_tot_s, val_dim_w, val_chr_w,
            val_jp_sf, val_us_sf, val_ca_sf, val_my_sf,
            asin
        ))

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
            print(f"DB更新エラー(最終バッチ): {e}")
            conn.rollback()

    print("\n=== 全件の評価とDB更新が完了しました ===")
    conn.close()

if __name__ == "__main__":
    main()