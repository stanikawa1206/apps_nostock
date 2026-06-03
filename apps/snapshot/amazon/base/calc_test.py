import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from sp_api.api import ProductsV0, CatalogItemsV20220401
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

# 環境変数の読み込み
load_dotenv()
LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
REFRESH_TOKEN_US = os.environ.get("REFRESH_TOKEN_US")

# --- 単位変換用 ---
WEIGHT_RATES = {"grams": 1.0, "kilograms": 1000.0, "pounds": 453.592, "ounces": 28.3495, "milligrams": 0.001}
LENGTH_RATES = {"centimeters": 1.0, "millimeters": 0.1, "meters": 100.0, "inches": 2.54, "feet": 30.48}

def to_grams(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * WEIGHT_RATES.get(unit.lower(), 1.0)

def to_cm(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * LENGTH_RATES.get(unit.lower(), 1.0)

# ==========================================
# ログ出力用関数
# ==========================================
def write_error_log(log_path, asin, message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] [ASIN: {asin}] {message}\n")

# ==========================================
# 1. 為替レート取得
# ==========================================
def get_exchange_rates():
    print("🔄 最新の為替レートを取得中...")
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        res = requests.get(url).json()
        usd_jpy = res.get("rates", {}).get("JPY", 150.0)
        usd_cad = res.get("rates", {}).get("CAD", 1.35)
        cad_jpy = usd_jpy / usd_cad
        print(f"✅ レート取得: 1 USD = {usd_jpy:.2f}円 | 1 CAD = {cad_jpy:.2f}円\n")
        return {"USD": usd_jpy, "CAD": cad_jpy, "JPY": 1.0}
    except Exception as e:
        print(f"⚠️ レート取得失敗。仮レートを使用します。理由: {e}\n")
        return {"USD": 150.0, "CAD": 110.0, "JPY": 1.0}

# ==========================================
# 2. 価格分析・仕入先選定
# ==========================================
def filter_and_sort_candidates(offers):
    safe_offers = []
    for offer in offers:
        is_buybox = offer.get('is_buybox', False)
        count = int(offer.get('feedback_count', 0))
        try:
            percent = float(offer.get('feedback_percent', 0.0))
        except (ValueError, TypeError):
            percent = 0.0

        if not is_buybox and (count < 10 or percent < 80.0):
            continue
        safe_offers.append(offer)
    safe_offers.sort(key=lambda x: x['total_jpy'])
    return safe_offers

def analyze_trade_opportunity(asin: str, exchange_rates: dict) -> dict:
    targets = [
        {"code": "US", "marketplace": Marketplaces.US, "token": REFRESH_TOKEN_US},
        {"code": "CA", "marketplace": Marketplaces.CA, "token": REFRESH_TOKEN_US}, 
        {"code": "JP", "marketplace": Marketplaces.JP, "token": REFRESH_TOKEN},
    ]

    result_data = {
        "asin": asin,
        "lowest_prices": {"US": None, "CA": None, "JP": None},
        "all_offers": {"US": [], "CA": [], "JP": []},
        "has_catalog": {"US": False, "CA": False, "JP": False}, 
        "judgment": "判定不可",       
        "sourcing_country": None,   
        "sourcing_candidates": [],
        "api_errors": []  
    }

    for target in targets:
        country_code = target["code"]
        token = target["token"]
        if not token: 
            result_data["api_errors"].append(f"{country_code}: リフレッシュトークンが未設定です。")
            continue

        credentials = {"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": token}
        try:
            res = ProductsV0(credentials=credentials, marketplace=target["marketplace"]).get_item_offers(asin=asin, item_condition="New")
            result_data["has_catalog"][country_code] = True
            
            offers = res.payload.get('Offers', [])
            lowest_price_data = None
            
            for offer in offers:
                price = offer.get('ListingPrice', {}).get('Amount', 0.0)
                currency = offer.get('ListingPrice', {}).get('CurrencyCode', '')
                shipping_fee = offer.get('Shipping', {}).get('Amount', 0.0)
                total_price = float(price) + float(shipping_fee)
                
                rate = exchange_rates.get(currency, 1.0)
                jpy_price = int(total_price * rate)
                
                if lowest_price_data is None or jpy_price < lowest_price_data["jpy"]:
                    lowest_price_data = {"jpy": jpy_price, "original": total_price, "currency": currency}
                
                # 出荷目安の取得
                shipping_time = offer.get('ShippingTime', {})
                min_hours = shipping_time.get('minimumHours')
                max_hours = shipping_time.get('maximumHours')
                
                handling_time_str = "不明"
                if max_hours is not None:
                    min_days = int(min_hours) // 24 if min_hours is not None else 0
                    max_days = int(max_hours) // 24
                    if min_days == 0 and max_days == 0:
                        handling_time_str = "即日(0日)"
                    elif min_days == max_days:
                        handling_time_str = f"{max_days}日"
                    else:
                        handling_time_str = f"{min_days}-{max_days}日"

                feedback_info = offer.get('SellerFeedbackRating', {})
                result_data["all_offers"][country_code].append({
                    "total_jpy": jpy_price,
                    "original_price": total_price,
                    "currency": currency,
                    "is_buybox": offer.get('IsBuyBoxWinner', False),
                    "fulfillment": "FBA" if offer.get('IsFulfilledByAmazon', False) else "FBM",
                    "feedback_count": feedback_info.get('FeedbackCount', 0),
                    "feedback_percent": feedback_info.get('SellerPositiveFeedbackRating', '不明'),
                    "handling_time": handling_time_str
                })
            result_data["lowest_prices"][country_code] = lowest_price_data
        except Exception as e:
            result_data["has_catalog"][country_code] = False
            result_data["api_errors"].append(f"{country_code} 価格APIエラー: {str(e)}")
        time.sleep(2)

    jp_price = result_data["lowest_prices"]["JP"]["jpy"] if result_data["lowest_prices"]["JP"] else None
    us_price = result_data["lowest_prices"]["US"]["jpy"] if result_data["lowest_prices"]["US"] else None
    ca_price = result_data["lowest_prices"]["CA"]["jpy"] if result_data["lowest_prices"]["CA"] else None

    has_us = result_data["has_catalog"]["US"]
    has_ca = result_data["has_catalog"]["CA"]
    has_jp = result_data["has_catalog"]["JP"]

    if jp_price is not None:
        if (has_us and us_price is None) or (has_ca and ca_price is None):
            result_data["judgment"] = "輸出向き(海外競合なし)"
            result_data["sourcing_country"] = "JP"
        elif us_price is not None or ca_price is not None:
            us = us_price if us_price is not None else float('inf')
            ca = ca_price if ca_price is not None else float('inf')
            jp = jp_price
            
            if (us < jp < ca) or (ca < jp < us):
                result_data["judgment"] = "双方向の可能性あり"
                cheapest = min(us, ca, jp)
                result_data["sourcing_country"] = "US" if cheapest == us else ("CA" if cheapest == ca else "JP")
            elif jp < us and jp < ca:
                result_data["judgment"] = "輸出向き"
                result_data["sourcing_country"] = "JP"
            elif jp > us and jp > ca:
                result_data["judgment"] = "輸入向き"
                result_data["sourcing_country"] = "US" if us < ca else "CA"
            else:
                result_data["judgment"] = "価格差なし"
        else:
            result_data["judgment"] = "海外カタログなし"
            result_data["api_errors"].append("USおよびCAの両方でカタログが存在しません。")
    elif has_jp:
        result_data["judgment"] = "仕入先なし(JP競合なし)"
        result_data["api_errors"].append("JPのカタログはありますが、出品者がいません。")
    else:
        result_data["judgment"] = "判定不可"
        result_data["api_errors"].append("JPのカタログが存在しません。")

    sourcing_ctry = result_data["sourcing_country"]
    if sourcing_ctry and result_data["all_offers"][sourcing_ctry]:
        result_data["sourcing_candidates"] = filter_and_sort_candidates(result_data["all_offers"][sourcing_ctry])

    return result_data

# ==========================================
# 3. サイズ取得・送料計算
# ==========================================
def calculate_shipping_costs(chargeable_weight_g, actual_weight_g, sum_cm, excel_path):
    try:
        df_weight = pd.read_excel(excel_path, sheet_name="重量")
        df_size = pd.read_excel(excel_path, sheet_name="サイズ")
    except Exception as e:
        raise Exception(f"送料Excelの読み込みに失敗しました。({str(e)})")

    df_weight = df_weight[df_weight['g'] >= 0].sort_values('g')
    df_size = df_size[df_size['cm'] >= 0].sort_values('cm')

    valid_weights = df_weight[df_weight['g'] >= chargeable_weight_g]
    w_row = df_weight.iloc[-1] if valid_weights.empty else valid_weights.iloc[0]
    cost_us, cost_ca = w_row['us'], w_row['ca']

    valid_jp_weights = df_weight[df_weight['g'] >= actual_weight_g]
    cost_jp_weight = df_weight.iloc[-1]['jp'] if valid_jp_weights.empty else valid_jp_weights.iloc[0]['jp']
    valid_jp_sizes = df_size[df_size['cm'] >= sum_cm]
    cost_jp_size = df_size.iloc[-1]['jp'] if valid_jp_sizes.empty else valid_jp_sizes.iloc[0]['jp']
    
    cost_jp = max(cost_jp_weight, cost_jp_size)
    return cost_us, cost_ca, cost_jp

def get_item_dimensions_and_shipping(asin: str, excel_path: str):
    credentials = {"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": REFRESH_TOKEN}
    try:
        api = CatalogItemsV20220401(credentials=credentials, marketplace=Marketplaces.JP)
        res = api.get_catalog_item(asin=asin, marketplaceIds=[Marketplaces.JP.marketplace_id], includedData=["dimensions"])
        dims = res.payload.get('dimensions', [])
        
        if not dims or 'package' not in dims[0]:
            raise Exception("APIからディメンション(サイズ・重量)情報が返されませんでした。")

        pkg = dims[0]['package']
        weight_g = to_grams(pkg.get('weight', {}).get('value', 0), pkg.get('weight', {}).get('unit', ''))
        l_cm = to_cm(pkg.get('length', {}).get('value', 0), pkg.get('length', {}).get('unit', ''))
        w_cm = to_cm(pkg.get('width', {}).get('value', 0), pkg.get('width', {}).get('unit', ''))
        h_cm = to_cm(pkg.get('height', {}).get('value', 0), pkg.get('height', {}).get('unit', ''))
        
        sum_cm = l_cm + w_cm + h_cm
        vol_weight_g = ((l_cm * w_cm * h_cm) / 5000.0) * 1000.0
        chargeable_g = max(weight_g, vol_weight_g)
        
        cost_us, cost_ca, cost_jp = calculate_shipping_costs(chargeable_g, weight_g, sum_cm, excel_path)
        
        return {
            "l": l_cm, "w": w_cm, "h": h_cm, "vol": l_cm * w_cm * h_cm, "sum_cm": sum_cm,
            "actual_w": weight_g, "vol_w": vol_weight_g, "chargeable_w": chargeable_g,
            "cost_us": cost_us, "cost_ca": cost_ca, "cost_jp": cost_jp,
            "error": None
        }
    except Exception as e:
        return {"error": str(e)}

# ==========================================
# 4. 一括処理＆CSV出力パイプライン
# ==========================================
def process_multiple_asins(input_csv_path: str, output_csv_path: str, excel_path: str, log_path: str, target_profit_rates: dict):
    us_rate = target_profit_rates.get('US', 0) * 100
    ca_rate = target_profit_rates.get('CA', 0) * 100
    print(f"📁 入力ファイルを読み込んでいます: {input_csv_path}")
    print(f"📊 設定目標利益率: US={us_rate:.1f}%, CA={ca_rate:.1f}%")
    
    try:
        with open(input_csv_path, 'r', encoding='utf-8-sig') as f:
            raw_lines = f.readlines()
            
        asins = []
        for line in raw_lines:
            clean_asin = line.split(']')[-1].strip()
            if clean_asin: asins.append(clean_asin)
                
        unique_asins = list(dict.fromkeys(asins))
    except Exception as e:
        print(f"❌ 入力ファイルの読み込みエラー: {e}")
        return

    rates = get_exchange_rates()
    results = []

    for idx, asin in enumerate(unique_asins, 1):
        print(f"\n--- [{idx}/{len(unique_asins)}] ASIN: {asin} の処理を開始 ---")
        
        row_data = {
            "ASIN": asin, "判定": "判定不可/エラー", "仕入対象国": "", 
            "確定仕入値(円)": None, 
            "仕入先_出荷元": "", 
            "仕入先_出荷目安": "",
            "US_送料目安(円)": None, "CA_送料目安(円)": None, "JP_送料目安(円)": None
        }
        
        # --- A. 価格分析 ---
        trade_data = analyze_trade_opportunity(asin, rates)
        row_data["判定"] = trade_data.get("judgment", "判定不可")
        row_data["仕入対象国"] = trade_data.get("sourcing_country", "")
        
        if "判定不可" in row_data["判定"] or "カタログなし" in row_data["判定"]:
            reasons = " | ".join(trade_data["api_errors"]) if trade_data["api_errors"] else "理由不明"
            write_error_log(log_path, asin, f"【価格分析フェーズ】判定: {row_data['判定']} / 詳細: {reasons}")

        candidates = trade_data.get("sourcing_candidates", [])
        cost_jpy = None
        if candidates:
            best_cand = candidates[0]
            cost_jpy = best_cand["total_jpy"]
            row_data["確定仕入値(円)"] = cost_jpy
            row_data["仕入先_出荷元"] = best_cand.get("fulfillment", "")
            row_data["仕入先_出荷目安"] = best_cand.get("handling_time", "")
        elif row_data["判定"] not in ["判定不可", "海外カタログなし"]:
            row_data["判定"] = "仕入先なし(足切り)"
            
        # --- B. サイズ・送料取得 ---
        ship_data = get_item_dimensions_and_shipping(asin, excel_path)
        if ship_data.get("error"):
            print(f"⚠️ {asin}: サイズまたは送料データが取得できませんでした。")
            write_error_log(log_path, asin, f"【サイズ・送料フェーズ】エラー: {ship_data['error']}")
            results.append(row_data)
            continue
            
        row_data["US_送料目安(円)"] = int(ship_data.get("cost_us", 0))
        row_data["CA_送料目安(円)"] = int(ship_data.get("cost_ca", 0))
        row_data["JP_送料目安(円)"] = int(ship_data.get("cost_jp", 0))
        
        # --- C. 利益計算 ---
        if cost_jpy is not None:
            lowest_prices = trade_data.get("lowest_prices", {})
            fee_rate = 0.15
            
            for country, cost_key in [("US", "cost_us"), ("CA", "cost_ca")]:
                shipping_cost = ship_data.get(cost_key)
                has_catalog = trade_data["has_catalog"].get(country, False)
                
                if not has_catalog:
                    row_data[f"{country}_利益判定"] = "カタログなし"
                    continue
                
                if shipping_cost is None:
                    continue

                country_profit_rate = target_profit_rates.get(country, 0.15)
                denom = 1.0 - fee_rate - country_profit_rate
                
                if denom <= 0:
                    row_data[f"{country}_利益判定"] = False
                    continue

                # 販売に必要な最低目標価格を算出 (仕入+送料から逆算)
                target_price_jpy = (cost_jpy + shipping_cost) / denom
                
                currency_code = "USD" if country == "US" else "CAD"
                comp_data = lowest_prices.get(country)
                
                if comp_data and "currency" in comp_data:
                    currency_code = comp_data["currency"]

                rate = rates.get(currency_code, 1.0)
                target_native = target_price_jpy / rate

                amazon_fee_jpy = target_price_jpy * fee_rate
                profit_jpy = target_price_jpy - amazon_fee_jpy - cost_jpy - shipping_cost
                profit_margin = (profit_jpy / target_price_jpy) * 100 if target_price_jpy > 0 else 0

                # ★ True / False の判定ロジック ★
                if comp_data:
                    comp_price_native = comp_data.get("native", comp_data.get("original"))
                    is_prof = target_native <= (comp_price_native * 1.25)
                    row_data[f"{country}_利益判定"] = bool(is_prof)
                    row_data[f"{country}_現地最安値"] = round(comp_price_native, 2)
                else:
                    row_data[f"{country}_利益判定"] = True
                    row_data[f"{country}_現地最安値"] = "出品者なし"

                row_data[f"{country}_利益額(円)"] = int(profit_jpy)
                row_data[f"{country}_利益率(%)"] = round(profit_margin, 1)
                row_data[f"{country}_販売額"] = round(target_native, 2)

        results.append(row_data)
        time.sleep(1.5)

    if results:
        df = pd.DataFrame(results)
        df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
        print(f"\n🎉 処理が完了しました！ 結果CSV: {output_csv_path}")

if __name__ == "__main__":
    BASE_DIR = r"X:\apps\snapshot\amazon"
    INPUT_CSV = os.path.join(BASE_DIR, "input_asins.csv")
    OUTPUT_CSV = os.path.join(BASE_DIR, "output_results.csv")
    EXCEL_FILE_PATH = os.path.join(BASE_DIR, "ship_cost.xlsx")
    ERROR_LOG_TXT = os.path.join(BASE_DIR, "error_log.txt") 
    
    MY_TARGET_PROFIT_RATES = { "US": 0.25, "CA": 0.15 }
    
    process_multiple_asins(INPUT_CSV, OUTPUT_CSV, EXCEL_FILE_PATH, ERROR_LOG_TXT, MY_TARGET_PROFIT_RATES)