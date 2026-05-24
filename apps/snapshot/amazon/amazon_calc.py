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
    except Exception:
        print("⚠️ レート取得失敗。仮レートを使用します。\n")
        return {"USD": 150.0, "CAD": 110.0, "JPY": 1.0}

# ==========================================
# 2. 価格分析・仕入先選定 (amazon_price.py統合)
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
        "judgment": "判定不可",       
        "sourcing_country": None,   
        "sourcing_candidates": []   
    }

    for target in targets:
        country_code = target["code"]
        token = target["token"]
        if not token: continue

        credentials = {"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": token}
        try:
            pricing_api = ProductsV0(credentials=credentials, marketplace=target["marketplace"])
            offers = pricing_api.get_item_offers(asin=asin, item_condition="New").payload.get('Offers', [])
            
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
        except Exception:
            pass
        time.sleep(2)

    jp = result_data["lowest_prices"]["JP"]["jpy"] if result_data["lowest_prices"]["JP"] else None
    us = result_data["lowest_prices"]["US"]["jpy"] if result_data["lowest_prices"]["US"] else None
    ca = result_data["lowest_prices"]["CA"]["jpy"] if result_data["lowest_prices"]["CA"] else None

    if jp is not None:
        if us is not None and ca is not None:
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
        elif us is not None:
            if jp < us:
                result_data["judgment"] = "輸出向き"
                result_data["sourcing_country"] = "JP"
            elif jp > us:
                result_data["judgment"] = "輸入向き"
                result_data["sourcing_country"] = "US"
        elif ca is not None:
            if jp < ca:
                result_data["judgment"] = "輸出向き"
                result_data["sourcing_country"] = "JP"
            elif jp > ca:
                result_data["judgment"] = "輸入向き"
                result_data["sourcing_country"] = "CA"

    sourcing_ctry = result_data["sourcing_country"]
    if sourcing_ctry and result_data["all_offers"][sourcing_ctry]:
        result_data["sourcing_candidates"] = filter_and_sort_candidates(result_data["all_offers"][sourcing_ctry])

    return result_data

def print_price_result(result):
    print("【各国の最安値 (単純なカタログ最安値)】")
    for country in ["JP", "US", "CA"]:
        price_info = result['lowest_prices'][country]
        if price_info:
            print(f"{country}: {price_info['jpy']:,}円 ({price_info['original']:.2f} {price_info['currency']})")
        else:
            print(f"{country}: 出品なし / 取得不可")

    print(f"\n【判定結果】: {result['judgment']}")
    print(f"【仕入対象国】: {result['sourcing_country']}")

    print("\n【✨ 安全な仕入先候補 (価格が安い順) ✨】")
    if not result['sourcing_candidates']:
        print("⚠️ 安全基準を満たす仕入先が見つかりませんでした (全員足切りされました)")
    else:
        for idx, cand in enumerate(result['sourcing_candidates'], 1):
            buybox = "★カート" if cand['is_buybox'] else "    "
            percent_str = f"{cand['feedback_percent']}%" if cand['feedback_percent'] != '不明' else '不明'
            if idx == 1:
                print(f"🥇 確定仕入先 -> {buybox} | {cand['total_jpy']:,}円 ({cand['original_price']:.2f} {cand['currency']}) | {cand['fulfillment']} | 評価:{cand['feedback_count']}件 ({percent_str}) | 出荷目安: {cand['handling_time']}")
            else:
                print(f"   {idx}.         {buybox} | {cand['total_jpy']:,}円 ({cand['original_price']:.2f} {cand['currency']}) | {cand['fulfillment']} | 評価:{cand['feedback_count']}件 ({percent_str}) | 出荷目安: {cand['handling_time']}")
    print("")

# ==========================================
# 3. サイズ取得・送料計算 (amazon_ship.py統合)
# ==========================================
def calculate_shipping_costs(chargeable_weight_g, actual_weight_g, sum_cm, excel_path):
    try:
        df_weight = pd.read_excel(excel_path, sheet_name="重量")
        df_size = pd.read_excel(excel_path, sheet_name="サイズ")
    except Exception:
        return None, None, None

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
            return None

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
            "cost_us": cost_us, "cost_ca": cost_ca, "cost_jp": cost_jp
        }
    except Exception:
        return None

# ==========================================
# 4. 利益判定
# ==========================================
def check_profitability(cost_jpy, shipping_jpy, fee_rate, lowest_price_foreign, exchange_rate):
    if fee_rate >= 0.75:
        return False, 0.0
    target_price_jpy = (cost_jpy + shipping_jpy) / (0.75 - fee_rate)
    target_price_foreign = target_price_jpy / exchange_rate
    max_allowed_price = lowest_price_foreign * 1.25
    is_profitable = target_price_foreign <= max_allowed_price
    return is_profitable, target_price_foreign

# ==========================================
# 総合検証パイプライン
# ==========================================
def verify_export_pipeline(asin: str, excel_path: str):
    print(f"========== ASIN: {asin} の総合検証を開始 ==========\n")
    
    # 1. レート取得
    rates = get_exchange_rates()
    
    # 2. 価格分析と仕入先の選定
    print(f"🔎 日米加の価格データと仕入先候補を取得中...\n")
    trade_data = analyze_trade_opportunity(asin, rates)
    print_price_result(trade_data)

    # 3. サイズと送料の計算
    print("📦 カタログからサイズを取得し、送料を計算中...")
    ship_data = get_item_dimensions_and_shipping(asin, excel_path)
    
    if not ship_data:
        print("❌ サイズデータまたは送料データが取得できないため、検証を終了します。")
        return

    print("\n【パッケージ（梱包）サイズ・重量情報】")
    print(f"寸法  : {ship_data['l']:.1f} cm × {ship_data['w']:.1f} cm × {ship_data['h']:.1f} cm")
    print(f"体積(積): {ship_data['vol']:.1f} cm³")
    print(f"３辺合計: {ship_data['sum_cm']:.1f} cm")
    print("-" * 45)
    print(f"実重量 : {ship_data['actual_w']:.1f} g")
    print(f"請求重量: {ship_data['chargeable_w']:.1f} g (実重量と容積重量の重い方)")
    print("-" * 45)
    print("\n【国別 送料目安】")
    print(f"US (アメリカ) : {int(ship_data['cost_us']):,} 円 (請求重量ベース)")
    print(f"CA (カナダ)   : {int(ship_data['cost_ca']):,} 円 (請求重量ベース)")
    print(f"JP (日 本)   : {int(ship_data['cost_jp']):,} 円 (実重量とサイズの高い方)")
    print("-" * 45)

    # 4. 利益判定 (代表1つの仕入先 = リストの先頭)
    candidates = trade_data.get("sourcing_candidates", [])
    if not candidates:
        print("\n❌ 安全な仕入先がないため、利益計算をスキップします。")
        return
        
    best_supplier = candidates[0]
    cost_jpy = best_supplier["total_jpy"]
    lowest_prices = trade_data["lowest_prices"]
    fee_rate = 0.15 
    
    print("\n========== 📊 最終利益判定 (確定仕入先ベース) ==========")
    for country, cost_key in [("US", "cost_us"), ("CA", "cost_ca")]:
        shipping_cost = ship_data[cost_key]
        comp_data = lowest_prices.get(country)
        
        if not comp_data:
            print(f"[{country}市場] 競合出品なし (ブルーオーシャン)")
            continue
            
        comp_price_native = comp_data["native"] if "native" in comp_data else comp_data["original"]
        currency_code = comp_data["currency"]
        rate = rates.get(currency_code, 1.0)
        
        is_prof, target_native = check_profitability(cost_jpy, shipping_cost, fee_rate, comp_price_native, rate)
        
        target_jpy = target_native * rate
        amazon_fee_jpy = target_jpy * fee_rate
        profit_jpy = target_jpy - amazon_fee_jpy - cost_jpy - shipping_cost
        profit_margin = (profit_jpy / target_jpy) * 100 if target_jpy > 0 else 0
        
        result_mark = "⭕️ 利益確保可能" if is_prof else "❌ 利益確保困難"
        
        print(f"[{country}市場] {result_mark}")
        print(f"  - 仕入値   : {int(cost_jpy):,} 円")
        print(f"  - 国際送料  : {int(shipping_cost):,} 円")
        print(f"  - 仕入＋送料 : {int(cost_jpy + shipping_cost):,} 円")
        print(f"  - 現地最安値 : {comp_price_native:.2f} {currency_code} (約 {comp_data['jpy']:,} 円)")
        print(f"  - 目標販売額 : {target_native:.2f} {currency_code} (最安値の1.25倍以内か: {is_prof})")
        print(f"  --- 💡 目標販売額の内訳検証 ---")
        print(f"    ・販売額(円) : {int(target_jpy):,} 円")
        print(f"    ・手数料(円) : {int(amazon_fee_jpy):,} 円 ({fee_rate*100:.0f}%)")
        print(f"    ・利益額(円) : {int(profit_jpy):,} 円")
        print(f"    ・利益率   : {profit_margin:.1f} %")
        print("-" * 45)

if __name__ == "__main__":
    TARGET_ASIN = "B00J3K2Z6K" 
    EXCEL_FILE_PATH = r"X:\apps\snapshot\amazon\ship_cost.xlsx"
    
    verify_export_pipeline(TARGET_ASIN, EXCEL_FILE_PATH)