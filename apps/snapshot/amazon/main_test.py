import os
import time
import sys  
import re      # 💡 日数の数値化処理のために追加
import pprint  # 💡 生データを綺麗に表示するために追加
import pandas as pd
from amazon_common import (
    get_exchange_rates, analyze_trade_opportunity, print_price_info,
    get_item_dimensions_and_shipping, print_shipping_info,
    verify_export_pipeline, print_pipeline_result,
    change_listings_price_and_qty, create_new_listing,
    MARKET_CONFIGS, get_access_token, create_report, wait_for_report, download_and_save_report
)

# ==========================================
# 基本設定
# ==========================================
BASE_DIR = r"X:\apps\snapshot\amazon"
EXCEL_FILE_PATH = os.path.join(BASE_DIR, "ship_cost.xlsx")
INPUT_CSV = os.path.join(BASE_DIR, "input_asins.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "output_results.csv")

# ==========================================
# トレース用関数
# ==========================================
def trace_calls(frame, event, arg):
    if event == 'call':
        func_name = frame.f_code.co_name
        file_name = frame.f_code.co_filename
        
        if "amazon_common.py" in file_name and func_name != "<module>":
            print(f"  🔍 [Trace] 実行中: amazon_common.{func_name}()")
    return trace_calls

# ==========================================
# ユーティリティ関数
# ==========================================
def parse_handling_days(handling_str):
    """日数文字列から最大日数を数値として抽出する"""
    if not handling_str:
        return None
    handling_str = str(handling_str)
    if "即日" in handling_str:
        return 0
    numbers = re.findall(r'\d+', handling_str)
    if numbers:
        return int(numbers[-1])
    return None

# ==========================================
# メニュー画面
# ==========================================
def menu():
    while True:
        print("\n" + "="*50)
        print(" 🚀 Amazon API 統合テスト・ツール")
        print("="*50)
        print(" [1] 📊 単一ASIN: 価格分析 (日米加)")
        print(" [2] 📦 単一ASIN: 送料・サイズ計算")
        print(" [3] 💰 単一ASIN: 総合利益検証")
        print(" [4] 🔄 出品管理: 価格・在庫の変更")
        print(" [5] ✨ 新規出品: 相乗り出品")
        print(" [6] 📥 レポート: 出品情報の取得")
        print(" [7] 📁 一括処理: 複数ASINの総合検証 (DB書き込み用データ出力)") # 💡 変更
        print(" [8] 🔍 単一ASIN: 生データ確認 (デバッグ用)")
        print(" [0] ❌ 終了")
        print("="*50)
        
        choice = input("実行するメニュー番号を入力してください: ").strip()

        if choice == '0':
            print("終了します。お疲れ様でした！")
            break
            
        if choice in ['1', '2', '3', '4', '5', '6', '7', '8']:
            print("\n" + "-"*40)
            sys.settrace(trace_calls) 
            try:
                if choice == '1': run_price_analysis()
                elif choice == '2': run_shipping_calc()
                elif choice == '3': run_single_pipeline()
                elif choice == '4': run_change_listing()
                elif choice == '5': run_create_listing()
                elif choice == '6': run_get_reports()
                elif choice == '7': run_batch_pipeline()
                elif choice == '8': run_debug_raw_data()
            finally:
                sys.settrace(None) 
            print("-"*40)
        else:
            print("⚠️ 正しい番号を入力してください。")

# ==========================================
# 各メニューの実行処理
# ==========================================
def run_price_analysis():
    asin = input("🔎 分析するASINを入力してください: ").strip()
    if not asin: return
    rates = get_exchange_rates()
    trade_data = analyze_trade_opportunity(asin, rates)
    print_price_info(trade_data)

def run_shipping_calc():
    asin = input("📦 送料を計算するASINを入力してください: ").strip()
    if not asin: return
    rates = get_exchange_rates()
    ship_data = get_item_dimensions_and_shipping(asin, EXCEL_FILE_PATH, rates)
    print_shipping_info(ship_data)

def run_single_pipeline():
    asin = input("💰 総合検証するASINを入力してください: ").strip()
    if not asin: return
    print(f"========== ASIN: {asin} のデータ取得中 ==========")
    pipeline_data = verify_export_pipeline(asin, EXCEL_FILE_PATH)
    print_pipeline_result(pipeline_data)

def run_debug_raw_data():
    print("\n--- 生データ確認 (デバッグ用) ---")
    asin = input("🔍 生データを確認するASINを入力してください: ").strip()
    if not asin: return
    
    print(f"========== ASIN: {asin} のデータ取得中 ==========")
    pipeline_data = verify_export_pipeline(asin, EXCEL_FILE_PATH)
    
    print("\n" + "="*60)
    print(f" 📦 ASIN: {asin} の取得生データ（辞書形式）")
    print("="*60)
    pprint.pprint(pipeline_data, sort_dicts=False)
    print("="*60 + "\n")

def run_change_listing():
    print("\n--- 価格・在庫の変更 ---")
    country = input("対象国 (JP/US/CA): ").strip().upper()
    sku = input("対象のSKU: ").strip()
    price = input("新しい価格 (例: 52000): ").strip()
    qty = input("新しい在庫数 (例: 1): ").strip()
    
    if all([country, sku, price, qty]):
        change_listings_price_and_qty(country, sku, price, qty)
    else:
        print("⚠️ 入力に不備があります。キャンセルしました。")

def run_create_listing():
    print("\n--- 新規相乗り出品 ---")
    country = input("対象国 (JP/US/CA): ").strip().upper()
    asin = input("対象のASIN: ").strip()
    sku = input("設定するSKU (通常はASINと同じ): ").strip() or asin
    price = input("設定価格: ").strip()
    qty = input("初期在庫数: ").strip()
    handling = input("出荷準備日数 (Handling Time): ").strip()
    
    if all([country, asin, sku, price, qty, handling]):
        create_new_listing(country, sku, asin, price, qty, handling)
    else:
        print("⚠️ 入力に不備があります。キャンセルしました。")

def run_get_reports():
    print("\n--- 出品レポートの取得 ---")
    output_dir = os.path.join(BASE_DIR, "listings")
    os.makedirs(output_dir, exist_ok=True)
    
    for country, config in MARKET_CONFIGS.items():
        if not config["token"]: continue
        try:
            print(f"\n========== {country} のレポート処理 ==========")
            token = get_access_token(config["token"])
            report_id = create_report(token, config["endpoint"], config["marketplace_id"])
            doc_id = wait_for_report(token, config["endpoint"], report_id)
            download_and_save_report(token, config["endpoint"], doc_id, country, output_dir)
        except Exception as e:
            print(f"❌ {country} の処理エラー: {e}")

# 💡 修正：DB書き込み用フォーマットでの出力
def run_batch_pipeline():
    print(f"\n--- 複数ASINの総合検証 (DB書き込み用データ出力) ---")
    if not os.path.exists(INPUT_CSV):
        print("⚠️ 入力CSVが見つかりません。")
        return
        
    try:
        with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f:
            unique_asins = list(dict.fromkeys([line.split(']')[-1].strip() for line in f if line.split(']')[-1].strip()]))
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {e}")
        return

    results = []
    for idx, asin in enumerate(unique_asins, 1):
        print(f"\n[{idx}/{len(unique_asins)}] ASIN: {asin} を処理中...")
        data = verify_export_pipeline(asin, EXCEL_FILE_PATH)
        
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

        vol = ship.get("vol")
        dim_weight = (vol / 5000.0 * 1000.0) if vol is not None else None

        row = {
            "asin": asin,
            "status": data["status"],
            "error_message": data.get("error_message", ""),
            
            "jp_lowest_price_y": jp_prices.get("jpy"),
            "us_lowest_price_d": us_prices.get("original"),
            "us_lowest_price_y": us_prices.get("jpy"),
            "ca_lowest_price_d": ca_prices.get("original"),
            "ca_lowest_price_y": ca_prices.get("jpy"),
            
            "us_existence": 1 if has_catalog.get("US") else 0,
            "ca_existence": 1 if has_catalog.get("CA") else 0,
            "Sales_Category": trade.get("judgment", "None"),
            
            "length": ship.get("l"),
            "width": ship.get("w"),
            "height": ship.get("h"),
            "total_size": ship.get("sum_cm"),
            "actual_weight": ship.get("actual_w"),
            "dim_weight": dim_weight,
            "chargeable_weight": ship.get("chargeable_w"),
            
            "jp_shipping_fee": ship.get("cost_jp"),
            "us_shipping_fee": ship.get("cost_us"),
            "ca_shipping_fee": ship.get("cost_ca"),
            "MyUS_shipping_fee": ship.get("cost_myus"),
            
            "jp_sourcing_price": jp_source.get("total_jpy"),
            "jp_sourcing_fulfillment": jp_source.get("fulfillment"),
            "jp_sourcing_buybox": 1 if jp_source.get("is_buybox") else 0,
            "jp_sourcing_handling_days": parse_handling_days(jp_source.get("handling_time")),
            "jp_sourcing_feedback_count": jp_source.get("feedback_count"),
            "jp_sourcing_feedback_percent": jp_source.get("feedback_percent"),
            
            "us_target_price_d": us_mkt.get("target_native") if us_mkt else None,
            "us_target_price_y": us_mkt.get("target_jpy") if us_mkt else None,
            "us_is_profitable": 1 if us_mkt.get("is_prof") else 0,
            
            "ca_target_price_d": ca_mkt.get("target_native") if ca_mkt else None,
            "ca_target_price_y": ca_mkt.get("target_jpy") if ca_mkt else None,
            "ca_is_profitable": 1 if ca_mkt.get("is_prof") else 0
        }
        
        results.append(row)
        time.sleep(1.5)

    if results:
        pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n🎉 一括処理完了！ DB書き込み用データを保存しました: {OUTPUT_CSV}")

# ==========================================
# 起動
# ==========================================
if __name__ == "__main__":
    menu()