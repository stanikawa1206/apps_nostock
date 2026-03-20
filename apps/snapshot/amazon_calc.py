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
# 2. 仕入先選定・価格取得 (amazon_price.py 相当)
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

def get_sourcing_and_competitor_data(asin: str, exchange_rates: dict):
    print("🔎 日米加の価格データと仕入先候補を取得中...")
    targets = [
        {"code": "US", "marketplace": Marketplaces.US, "token": REFRESH_TOKEN_US},
        {"code": "CA", "marketplace": Marketplaces.CA, "token": REFRESH_TOKEN_US}, 
        {"code": "JP", "marketplace": Marketplaces.JP, "token": REFRESH_TOKEN},
    ]

    lowest_prices = {"US": None, "CA": None, "JP": None}
    jp_offers = []

    for target in targets:
        if not target["token"]: continue
        credentials = {"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": target["token"]}
        try:
            api = ProductsV0(credentials=credentials, marketplace=target["marketplace"])
            offers = api.get_item_offers(asin=asin, item_condition="New").payload.get('Offers', [])
            
            lowest_data = None
            for offer in offers:
                price = offer.get('ListingPrice', {}).get('Amount', 0.0)
                shipping = offer.get('Shipping', {}).get('Amount', 0.0)
                currency = offer.get('ListingPrice', {}).get('CurrencyCode', '')
                total_native = float(price) + float(shipping)
                jpy_price = int(total_native * exchange_rates.get(currency, 1.0))
                
                if lowest_data is None or jpy_price < lowest_data["jpy"]:
                    lowest_data = {"jpy": jpy_price, "native": total_native, "currency": currency}
                
                if target["code"] == "JP":
                    # 💡 出荷日数の計算ロジックを追加
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

                    fb_info = offer.get('SellerFeedbackRating', {})
                    jp_offers.append({
                        "total_jpy": jpy_price,
                        "is_buybox": offer.get('IsBuyBoxWinner', False),
                        "fulfillment": "FBA" if offer.get('IsFulfilledByAmazon', False) else "FBM",
                        "feedback_count": fb_info.get('FeedbackCount', 0),
                        "feedback_percent": fb_info.get('SellerPositiveFeedbackRating', '0.0'),
                        "handling_time": handling_time_str # ← ここに追加
                    })
            lowest_prices[target["code"]] = lowest_data
        except Exception:
            pass
        time.sleep(2)

    sourcing_candidates = filter_and_sort_candidates(jp_offers)
    best_supplier = sourcing_candidates[0] if sourcing_candidates else None
    
    return best_supplier, lowest_prices

# ==========================================
# 3. 送料計算 (amazon_ship.py 相当)
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

def get_shipping_costs(asin: str, excel_path: str):
    print("📦 カタログからサイズを取得し、送料を計算中...")
    credentials = {"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": REFRESH_TOKEN}
    try:
        api = CatalogItemsV20220401(credentials=credentials, marketplace=Marketplaces.JP)
        res = api.get_catalog_item(asin=asin, marketplaceIds=[Marketplaces.JP.marketplace_id], includedData=["dimensions"])
        dims = res.payload.get('dimensions', [])
        
        if not dims or 'package' not in dims[0]:
            return None, None, None

        pkg = dims[0]['package']
        weight_g = to_grams(pkg.get('weight', {}).get('value', 0), pkg.get('weight', {}).get('unit', ''))
        l_cm = to_cm(pkg.get('length', {}).get('value', 0), pkg.get('length', {}).get('unit', ''))
        w_cm = to_cm(pkg.get('width', {}).get('value', 0), pkg.get('width', {}).get('unit', ''))
        h_cm = to_cm(pkg.get('height', {}).get('value', 0), pkg.get('height', {}).get('unit', ''))
        
        sum_cm = l_cm + w_cm + h_cm
        vol_weight_g = ((l_cm * w_cm * h_cm) / 5000.0) * 1000.0
        chargeable_g = max(weight_g, vol_weight_g)
        
        return calculate_shipping_costs(chargeable_g, weight_g, sum_cm, excel_path)
    except Exception:
        return None, None, None

# ==========================================
# 4. 利益判定 (amazon_calc.py 相当)
# ==========================================
def check_profitability(cost_jpy, shipping_jpy, fee_rate, lowest_price_foreign, exchange_rate):
    if fee_rate >= 0.75:
        return False, 0.0
    
    # 日本円での目標売価を算出 (利益率25%想定)
    target_price_jpy = (cost_jpy + shipping_jpy) / (0.75 - fee_rate)
    target_price_foreign = target_price_jpy / exchange_rate
    
    # 競合最安値の1.25倍に収まるか
    max_allowed_price = lowest_price_foreign * 1.25
    is_profitable = target_price_foreign <= max_allowed_price
    
    return is_profitable, target_price_foreign

# ==========================================
# メイン実行ブロック
# ==========================================
def verify_export_pipeline(asin: str, excel_path: str):
    print(f"========== ASIN: {asin} の輸出検証を開始 ==========\n")
    
    # 1. レート取得
    rates = get_exchange_rates()
    
    # 2. 仕入先と競合価格の取得
    best_supplier, lowest_prices = get_sourcing_and_competitor_data(asin, rates)
    if not best_supplier:
        print("❌ 安全な仕入先が見つからないため、検証を終了します。")
        return
        
    # 仕入先の詳細情報を整形
    cost_jpy = best_supplier["total_jpy"]
    buybox_str = "True" if best_supplier['is_buybox'] else "False"
    
    percent_str = str(best_supplier['feedback_percent'])
    if percent_str != '0.0' and percent_str != '不明':
        percent_str = f"{percent_str}%"
    else:
        percent_str = "0.0%"

    print(f"✅ 確定仕入先: {cost_jpy:,}円")
    print(f"  - 配送方法 : {best_supplier['fulfillment']}")
    print(f"  - BuyBox   : {buybox_str}")
    print(f"  - 出荷日数 : {best_supplier['handling_time']}")
    print(f"  - 評価　　 : {best_supplier['feedback_count']}件")
    print(f"  - 評価率　 : {percent_str}\n")

    # 3. 送料の計算
    cost_us, cost_ca, cost_jp = get_shipping_costs(asin, excel_path)
    if cost_us is None:
        print("❌ サイズデータまたは送料データが取得できないため、検証を終了します。")
        return
        
    print(f"✅ 国際送料: US={int(cost_us):,}円, CA={int(cost_ca):,}円\n")

    # 4. 利益判定 (US・CAそれぞれ)
    fee_rate = 0.15 # Amazon手数料率(15%想定)
    
    print("========== 📊 最終利益判定 ==========")
    for country, shipping_cost in [("US", cost_us), ("CA", cost_ca)]:
        comp_data = lowest_prices.get(country)
        
        if not comp_data:
            print(f"[{country}市場] 競合出品なし (ブルーオーシャン)")
            continue
            
        comp_price_native = comp_data["native"]
        currency_code = comp_data["currency"]
        rate = rates.get(currency_code, 1.0)
        
        # 目標売価の算出
        is_prof, target_native = check_profitability(
            cost_jpy, shipping_cost, fee_rate, comp_price_native, rate
        )
        
        # 💡 検算・内訳用の計算を追加
        target_jpy = target_native * rate
        amazon_fee_jpy = target_jpy * fee_rate
        profit_jpy = target_jpy - amazon_fee_jpy - cost_jpy - shipping_cost
        profit_margin = (profit_jpy / target_jpy) * 100 if target_jpy > 0 else 0
        
        result_mark = "⭕️ 利益確保可能" if is_prof else "❌ 利益確保困難"
        
        print(f"[{country}市場] {result_mark}")
        print(f"  - 仕入値　　 : {int(cost_jpy):,} 円")
        print(f"  - 国際送料　 : {int(shipping_cost):,} 円")
        print(f"  - 仕入＋送料 : {int(cost_jpy + shipping_cost):,} 円")
        print(f"  - 現地最安値 : {comp_price_native:.2f} {currency_code} (約 {comp_data['jpy']:,} 円)")
        print(f"  - 目標販売額 : {target_native:.2f} {currency_code} (最安値の1.25倍以内か: {is_prof})")
        print(f"  --- 💡 目標販売額の内訳検証 ---")
        print(f"    ・販売額(円) : {int(target_jpy):,} 円")
        print(f"    ・手数料(円) : {int(amazon_fee_jpy):,} 円 ({fee_rate*100:.0f}%)")
        print(f"    ・利益額(円) : {int(profit_jpy):,} 円")
        print(f"    ・利益率　　 : {profit_margin:.1f} %")
        print("-" * 45)

if __name__ == "__main__":
    # テスト対象のASIN
    TARGET_ASIN = "B000VWDER8" 
    
    # Excelファイルのパス
    EXCEL_FILE_PATH = r"X:\apps\snapshot\ship_cost.xlsx"
    
    verify_export_pipeline(TARGET_ASIN, EXCEL_FILE_PATH)