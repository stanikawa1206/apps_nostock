# step5_test.py
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
TEST_ASIN = "B0BLCTTDKH"  # 💡 テスト対象のASINを指定

EXCEL_FILE_PATH = r"X:\apps\snapshot\amazon\ship_cost.xlsx"

TARGET_MARGIN_US = 0.25   # 米国 25%
TARGET_MARGIN_CA = 0.20   # カナダ 20%
TARGET_MARGIN_JP = 0.20   # 日本(輸入時) 20%
DEFAULT_FEE_RATE = 0.15   # Amazon販売手数料 15% (固定)

# ==========================================
# 評価用関数
# ==========================================
def evaluate_profitability(
    jp_lowest_price_y,
    us_exist, us_lowest_price_d, 
    ca_exist, ca_lowest_price_d,
    length, width, height, actual_weight,
    exchange_rate_us, exchange_rate_ca
):
    if not length or not width or not height or not actual_weight:
        print("  👉 [判定保留] 必須データ(サイズ、実重量のいずれか)が不足しています。")
        return None, None, None, None, None

    jp_p = float(jp_lowest_price_y) if jp_lowest_price_y is not None else None
    us_p = float(us_lowest_price_d) * exchange_rate_us if us_lowest_price_d else None
    ca_p = float(ca_lowest_price_d) * exchange_rate_ca if ca_lowest_price_d else None

    # ========================================
    # 0. 価格差による輸出入の目星 (事前判定)
    # ========================================
    print("\n--- 0. 最安価格比較による方向性の目星 ---")
    print(f"  日本: {jp_p:.0f}円" if jp_p is not None else "  日本: N/A (現在出品者なし)")
    print(f"  米国: {us_p:.0f}円" if us_p else "  米国: N/A")
    print(f"  加国: {ca_p:.0f}円" if ca_p else "  加国: N/A")
    
    meboshi = "データ不足 (判定スキップ)"
    if jp_p is None:
        if us_p or ca_p:
            meboshi = "JP出品者なし: 海外からの輸入(ライバル不在)の候補"
        else:
            meboshi = "全市場で出品者なし"
    else:
        if us_p and ca_p:
            if jp_p > us_p and jp_p > ca_p:
                meboshi = "2カ国から輸入"
            elif jp_p > us_p and jp_p < ca_p:
                meboshi = "USから輸入、CAへ輸出"
            elif jp_p < us_p and jp_p > ca_p:
                meboshi = "USへ輸出、CAから輸入"
            elif jp_p < us_p and jp_p < ca_p:
                meboshi = "それぞれ輸出"
        elif us_p:
            meboshi = "USから輸入" if jp_p > us_p else "USへ輸出"
        elif ca_p:
            meboshi = "CAから輸入" if jp_p > ca_p else "CAへ輸出"
        
    print(f"  👉 目星: {meboshi}")


    # ========================================
    # 1. 送料計算プロセス
    # ========================================
    sum_cm = float(length) + float(width) + float(height)
    vol_w = (float(length) * float(width) * float(height)) / 5000.0 * 1000.0
    chargeable_w = max(float(actual_weight), vol_w)

    print("\n--- 1. 送料計算プロセス ---")
    print(f"  3辺合計(cm) : {sum_cm:.1f}")
    print(f"  実重量(g)   : {actual_weight}")
    print(f"  容積重量(g) : {vol_w:.1f}")
    print(f"  請求重量(g) : {chargeable_w:.1f} (実重量と容積重量の重い方を採用)")

    try:
        cost_us, cost_ca, cost_jp, cost_myus_usd, carrier = calculate_shipping_costs(
            chargeable_w, float(actual_weight), sum_cm, EXCEL_FILE_PATH
        )
        cost_myus_jpy = cost_myus_usd * exchange_rate_us if cost_myus_usd else None
        
        print(f"  [算出送料]")
        print(f"    US向け(円)    : {cost_us}")
        print(f"    CA向け(円)    : {cost_ca}")
        print(f"    MyUS経由(USD) : {cost_myus_usd} -> (円換算: {cost_myus_jpy:.0f}円)" if cost_myus_jpy else "    MyUS経由(USD) : None")
        print(f"    適用キャリア  : {carrier}")
        
    except Exception as e:
        print(f"  👉 [送料エラー] サイズや重量が上限を超えている、または計算エラーです。({e})")
        return "不可(サイズ超過)", "不可(サイズ超過)", None, None, None


    # ========================================
    # 2. US市場の評価
    # ========================================
    print("\n--- 2. US市場 評価プロセス ---")
    eval_us = None
    us_target_price_d = None
    jp_target_price_y_us = None
    
    if us_exist == 1:
        if us_p:
            print(f"  [ライバル比較]")
            print(f"    現在最安値 : {us_p:.0f}円 (USD {float(us_lowest_price_d):.2f})")

            if jp_p is None:
                print("  👉 日本市場に出品者がいないため、[輸入(ライバル不在)ルート]を検証します")
                if cost_myus_jpy:
                    us_sourcing_jpy = us_p
                    total_cost_jp = us_sourcing_jpy + cost_myus_jpy
                    
                    jp_target_price_y_us = total_cost_jp / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    jp_fee_jpy = jp_target_price_y_us * DEFAULT_FEE_RATE
                    jp_profit_jpy = jp_target_price_y_us * TARGET_MARGIN_JP
                    
                    print(f"  [JP輸入チェック (US仕入 -> JP販売)]")
                    print(f"    JP基準価格 : {jp_target_price_y_us:.0f}円 (目標利益率 {TARGET_MARGIN_JP*100}%)")
                    print(f"    (内訳) US仕入 {us_sourcing_jpy:.0f}円 + MyUS送料 {cost_myus_jpy:.0f}円 + 手数料 {jp_fee_jpy:.0f}円 + 利益 {jp_profit_jpy:.0f}円")
                    print("    👉 判定: 輸入(ライバル不在) - 基準価格以上で日本で独占出品が可能です")
                    eval_us = "輸入(ライバル不在)"
                else:
                    eval_us = "不可"
                    print("    👉 判定: 不可 (MyUS送料が計算できないため輸入できません)")

            elif jp_p < us_p:
                print("  👉 日本の最安値の方が安いため、[輸出ルート]を検証します")
                total_cost_us = jp_p + (cost_us or 0)
                us_target_price_jpy = total_cost_us / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_US)
                us_target_price_d = us_target_price_jpy / exchange_rate_us
                
                us_fee_jpy = us_target_price_jpy * DEFAULT_FEE_RATE
                us_profit_jpy = us_target_price_jpy * TARGET_MARGIN_US
                
                print(f"  [US基準価格 (目標利益率 {TARGET_MARGIN_US*100}%)]")
                print(f"    必要販売額 : {us_target_price_jpy:.0f}円 (USD {us_target_price_d:.2f})")
                print(f"    (内訳) 仕入 {jp_p:.0f}円 + 送料 {cost_us}円 + 手数料 {us_fee_jpy:.0f}円 + 利益 {us_profit_jpy:.0f}円")
                
                if float(us_lowest_price_d) >= us_target_price_d:
                    eval_us = "輸出"
                    print("    👉 判定: 輸出 (最安値が基準価格を上回っています)")
                else:
                    eval_us = "不可"
                    print("    👉 判定: 不可 (輸出目標基準に届きません)")

            elif jp_p > us_p:
                print("  👉 USの最安値の方が安いため、輸出はスキップし[輸入ルート]を検証します")
                if cost_myus_jpy:
                    us_sourcing_jpy = us_p
                    total_cost_jp = us_sourcing_jpy + cost_myus_jpy
                    
                    jp_target_price_y_us = total_cost_jp / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    jp_fee_jpy = jp_target_price_y_us * DEFAULT_FEE_RATE
                    jp_profit_jpy = jp_target_price_y_us * TARGET_MARGIN_JP
                    
                    print(f"  [JP輸入チェック (US仕入 -> JP販売)]")
                    print(f"    JP基準価格 : {jp_target_price_y_us:.0f}円 (目標利益率 {TARGET_MARGIN_JP*100}%)")
                    print(f"    (内訳) US仕入 {us_sourcing_jpy:.0f}円 + MyUS送料 {cost_myus_jpy:.0f}円 + 手数料 {jp_fee_jpy:.0f}円 + 利益 {jp_profit_jpy:.0f}円")
                    print(f"    JP現在最安 : {jp_p:.0f}円")
                    
                    if jp_p >= jp_target_price_y_us:
                        eval_us = "輸入"
                        print("    👉 判定: 輸入 (JP最安値が基準価格を上回っています)")
                    else:
                        eval_us = "不可"
                        print(f"    👉 判定: 不可 (輸入目標基準に届きません)")
                else:
                    eval_us = "不可"
                    print("    👉 判定: 不可 (MyUS送料が計算できないため輸入できません)")
            else:
                eval_us = "不可"
                print("  👉 日米の最安値が同額のため、手数料・送料分で確実に赤字になります (判定: 不可)")

        else:
            if jp_p is None:
                eval_us = "不可(価格基準なし)"
                print("  👉 [判定] 日米ともにライバル(価格)が存在しないため判定できません。")
            else:
                total_cost_us = jp_p + (cost_us or 0)
                us_target_price_jpy = total_cost_us / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_US)
                us_target_price_d = us_target_price_jpy / exchange_rate_us
                eval_us = "輸出(ライバル不在)"
                print(f"  👉 [判定] ライバル不在。基準価格(USD {us_target_price_d:.2f})以上での出品を推奨します。")
            
    elif us_exist == 2:
        eval_us = "不可(カタログなし)"
        print("  👉 [判定] カタログなしのため出品不可")
    else:
        print(f"  👉 [判定] 存在フラグが未定義({us_exist})のため、判定を保留(None)します")


    # ========================================
    # 3. CA市場の評価
    # ========================================
    print("\n--- 3. CA市場 評価プロセス ---")
    eval_ca = None
    ca_target_price_d = None
    jp_target_price_y_ca = None
    
    if ca_exist == 1:
        if ca_p:
            print(f"  [ライバル比較]")
            print(f"    現在最安値 : {ca_p:.0f}円 (CAD {float(ca_lowest_price_d):.2f})")

            if jp_p is None:
                print("  👉 日本市場に出品者がいないため、[輸入(ライバル不在)ルート]を検証します (送料はMyUS代用)")
                if cost_myus_jpy:
                    ca_sourcing_jpy = ca_p
                    total_cost_jp_ca = ca_sourcing_jpy + cost_myus_jpy
                    
                    jp_target_price_y_ca = total_cost_jp_ca / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    ca_fee_jpy = jp_target_price_y_ca * DEFAULT_FEE_RATE
                    ca_profit_jpy = jp_target_price_y_ca * TARGET_MARGIN_JP
                    
                    print(f"  [JP輸入チェック (CA仕入 -> JP販売)]")
                    print(f"    JP基準価格 : {jp_target_price_y_ca:.0f}円 (目標利益率 {TARGET_MARGIN_JP*100}%)")
                    print(f"    (内訳) CA仕入 {ca_sourcing_jpy:.0f}円 + MyUS送料代用 {cost_myus_jpy:.0f}円 + 手数料 {ca_fee_jpy:.0f}円 + 利益 {ca_profit_jpy:.0f}円")
                    print("    👉 判定: 輸入(ライバル不在) - 基準価格以上で日本で独占出品が可能です")
                    eval_ca = "輸入(ライバル不在)"
                else:
                    eval_ca = "不可"
                    print("    👉 判定: 不可 (MyUS送料が計算できないため輸入検証ができません)")

            elif jp_p < ca_p:
                print("  👉 日本の最安値の方が安いため、[輸出ルート]を検証します")
                total_cost_ca = jp_p + (cost_ca or 0)
                ca_target_price_jpy = total_cost_ca / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_CA)
                ca_target_price_d = ca_target_price_jpy / exchange_rate_ca
                
                ca_fee_jpy = ca_target_price_jpy * DEFAULT_FEE_RATE
                ca_profit_jpy = ca_target_price_jpy * TARGET_MARGIN_CA
                
                print(f"  [CA基準価格 (目標利益率 {TARGET_MARGIN_CA*100}%)]")
                print(f"    必要販売額 : {ca_target_price_jpy:.0f}円 (CAD {ca_target_price_d:.2f})")
                print(f"    (内訳) 仕入 {jp_p:.0f}円 + 送料 {cost_ca}円 + 手数料 {ca_fee_jpy:.0f}円 + 利益 {ca_profit_jpy:.0f}円")

                if float(ca_lowest_price_d) >= ca_target_price_d:
                    eval_ca = "輸出"
                    print("    👉 判定: 輸出 (最安値が基準価格を上回っています)")
                else:
                    eval_ca = "不可"
                    print("    👉 判定: 不可 (輸出目標基準に届きません)")

            elif jp_p > ca_p:
                print("  👉 CAの最安値の方が安いため、輸出はスキップし[輸入ルート]を検証します (送料はMyUS代用)")
                if cost_myus_jpy:
                    ca_sourcing_jpy = ca_p
                    total_cost_jp_ca = ca_sourcing_jpy + cost_myus_jpy
                    
                    jp_target_price_y_ca = total_cost_jp_ca / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_JP)
                    ca_fee_jpy = jp_target_price_y_ca * DEFAULT_FEE_RATE
                    ca_profit_jpy = jp_target_price_y_ca * TARGET_MARGIN_JP
                    
                    print(f"  [JP輸入チェック (CA仕入 -> JP販売)]")
                    print(f"    JP基準価格 : {jp_target_price_y_ca:.0f}円 (目標利益率 {TARGET_MARGIN_JP*100}%)")
                    print(f"    (内訳) CA仕入 {ca_sourcing_jpy:.0f}円 + MyUS送料代用 {cost_myus_jpy:.0f}円 + 手数料 {ca_fee_jpy:.0f}円 + 利益 {ca_profit_jpy:.0f}円")
                    print(f"    JP現在最安 : {jp_p:.0f}円")
                    
                    if jp_p >= jp_target_price_y_ca:
                        eval_ca = "輸入"
                        print("    👉 判定: 輸入 (JP最安値が基準価格を上回っています)")
                    else:
                        eval_ca = "不可"
                        print(f"    👉 判定: 不可 (輸入目標基準に届きません)")
                else:
                    eval_ca = "不可"
                    print("    👉 判定: 不可 (MyUS送料が計算できないため輸入検証ができません)")

            else:
                eval_ca = "不可"
                print("  👉 日加の最安値が同額のため、手数料・送料分で確実に赤字になります (判定: 不可)")

        else:
            if jp_p is None:
                eval_ca = "不可(価格基準なし)"
                print("  👉 [判定] 日加ともにライバル(価格)が存在しないため判定できません。")
            else:
                total_cost_ca = jp_p + (cost_ca or 0)
                ca_target_price_jpy = total_cost_ca / (1 - DEFAULT_FEE_RATE - TARGET_MARGIN_CA)
                ca_target_price_d = ca_target_price_jpy / exchange_rate_ca
                eval_ca = "輸出(ライバル不在)"
                print(f"  👉 [判定] ライバル不在。基準価格(CAD {ca_target_price_d:.2f})以上での出品を推奨します。")
            
    elif ca_exist == 2:
        eval_ca = "不可(カタログなし)"
        print("  👉 [判定] カタログなしのため出品不可")
    else:
        print(f"  👉 [判定] 存在フラグが未定義({ca_exist})のため、判定を保留(None)します")


    # ========================================
    # 4. JP目標価格の最終決定 (最小値の採用)
    # ========================================
    print("\n--- 4. JP目標価格の最終決定 ---")
    jp_target_price_y_final = None
    
    print(f"  JP目標(US発): {jp_target_price_y_us:.0f}円" if jp_target_price_y_us else "  JP目標(US発): N/A")
    print(f"  JP目標(CA発): {jp_target_price_y_ca:.0f}円" if jp_target_price_y_ca else "  JP目標(CA発): N/A")

    if jp_target_price_y_us and jp_target_price_y_ca:
        if jp_target_price_y_us <= jp_target_price_y_ca:
            jp_target_price_y_final = jp_target_price_y_us
            print(f"  👉 採用: US発ルート ({jp_target_price_y_final:.0f}円)")
        else:
            jp_target_price_y_final = jp_target_price_y_ca
            print(f"  👉 採用: CA発ルート ({jp_target_price_y_final:.0f}円)")
    elif jp_target_price_y_us:
        jp_target_price_y_final = jp_target_price_y_us
        print(f"  👉 採用: US発ルート ({jp_target_price_y_final:.0f}円)")
    elif jp_target_price_y_ca:
        jp_target_price_y_final = jp_target_price_y_ca
        print(f"  👉 採用: CA発ルート ({jp_target_price_y_final:.0f}円)")
    else:
        print("  👉 採用: なし (輸入条件を満たすルートがありません)")

    # 💡 戻り値を、比較決定済みの1つのJP目標価格に変更
    return eval_us, eval_ca, us_target_price_d, ca_target_price_d, jp_target_price_y_final

# ==========================================
# メイン処理 (テスト用)
# ==========================================
def main():
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

    print(f"\n{'='*50}")
    print(f"=== 単一ASIN テスト実行: {TEST_ASIN} ===")
    print(f"適用レート: 1USD = {current_rate_us:.2f}円, 1CAD = {current_rate_ca:.2f}円")
    print(f"{'='*50}\n")

    sql_select = """
        SELECT 
            asin, 
            jp_lowest_price_y,
            us_existence, us_lowest_price_d, 
            ca_existence, ca_lowest_price_d,
            length, width, height, actual_weight
        FROM trx.amazon_cross_market_asin WITH (NOLOCK)
        WHERE asin = ?
          AND (wakarunda IN ('D', '-')) 
          AND (
              last_evaluated_at IS NULL
              OR last_evaluated_at < jp_price_updated_at
              OR last_evaluated_at < us_price_updated_at
              OR last_evaluated_at < ca_price_updated_at
          )
    """

    cursor.execute(sql_select, (TEST_ASIN,))
    rows = cursor.fetchall()

    if not rows:
        print(f"⚠️ 指定されたASIN ({TEST_ASIN}) は抽出条件に合致しません。")
        conn.close()
        return

    for row in rows:
        (asin, jp_price_y, 
         us_exist, us_price_d, 
         ca_exist, ca_price_d, 
         length, width, height, actual_weight) = row

        print("--- 取得データ ---")
        print(f"  JP最安値(円): {jp_price_y if jp_price_y is not None else 'None'}")
        print(f"  US存在フラグ: {us_exist} | US最安値(USD): {us_price_d}")
        print(f"  CA存在フラグ: {ca_exist} | CA最安値(CAD): {ca_price_d}")
        print(f"  サイズ(cm)  : L:{length}, W:{width}, H:{height}")
        print(f"  実重量(g)   : {actual_weight}")

        eval_us, eval_ca, us_target_price, ca_target_price, jp_target_final = evaluate_profitability(
            jp_price_y, us_exist, us_price_d, ca_exist, ca_price_d,
            length, width, height, actual_weight,
            current_rate_us, current_rate_ca
        )

        print("\n--- 最終評価結果 ---")
        print(f"  US市場 判定 : {eval_us}")
        print(f"  US目標価格  : USD {us_target_price:.2f}" if us_target_price else "  US目標価格  : N/A")
        print(f"  CA市場 判定 : {eval_ca}")
        print(f"  CA目標価格  : CAD {ca_target_price:.2f}" if ca_target_price else "  CA目標価格  : N/A")
        print(f"  JP目標価格  : JPY {jp_target_final:.0f}" if jp_target_final else "  JP目標価格  : N/A")

    print("\n=== テスト完了 ===")
    conn.close()

if __name__ == "__main__":
    main()