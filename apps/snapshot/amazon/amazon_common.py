# amazon_common.py
import os
import time
import requests
import gzip
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sp_api.api import ProductsV0, CatalogItemsV20220401, ProductFees
from sp_api.base import Marketplaces

# ==========================================
# 1. 環境変数と共通設定
# ==========================================
load_dotenv(find_dotenv())
LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")

REFRESH_TOKEN_JP = os.environ.get("REFRESH_TOKEN")
REFRESH_TOKEN_US = os.environ.get("REFRESH_TOKEN_US")
SELLER_ID_JP = os.environ.get("SELLER_ID")
SELLER_ID_US = os.environ.get("SELLER_ID_US")

MARKET_CONFIGS = {
    "US": {"endpoint": "https://sellingpartnerapi-na.amazon.com", "marketplace_id": "ATVPDKIKX0DER", "currency": "USD", "token": REFRESH_TOKEN_US, "seller_id": SELLER_ID_US, "sp_api_market": Marketplaces.US},
    "CA": {"endpoint": "https://sellingpartnerapi-na.amazon.com", "marketplace_id": "A2EUQ1WTGCTBG2", "currency": "CAD", "token": REFRESH_TOKEN_US, "seller_id": SELLER_ID_US, "sp_api_market": Marketplaces.CA},
    "JP": {"endpoint": "https://sellingpartnerapi-fe.amazon.com", "marketplace_id": "A1VC38T7YXB528", "currency": "JPY", "token": REFRESH_TOKEN_JP, "seller_id": SELLER_ID_JP, "sp_api_market": Marketplaces.JP}
}

WEIGHT_RATES = {"grams": 1.0, "kilograms": 1000.0, "pounds": 453.592, "ounces": 28.3495, "milligrams": 0.001}
LENGTH_RATES = {"centimeters": 1.0, "millimeters": 0.1, "meters": 100.0, "inches": 2.54, "feet": 30.48}

# ==========================================
# 2. 汎用ユーティリティ関数
# ==========================================
def get_access_token(refresh_token):
    url = "https://api.amazon.com/auth/o2/token"
    res = requests.post(url, data={"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": LWA_APP_ID, "client_secret": LWA_CLIENT_SECRET})
    res.raise_for_status()
    return res.json()["access_token"]

def get_exchange_rates():
    print("🔄 最新の為替レートを取得中...")
    try:
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD").json()
        usd_jpy = res.get("rates", {}).get("JPY", 150.0)
        usd_cad = res.get("rates", {}).get("CAD", 1.35)
        cad_jpy = usd_jpy / usd_cad
        print(f"✅ レート取得: 1 USD = {usd_jpy:.2f}円 | 1 CAD = {cad_jpy:.2f}円\n")
        return {"USD": usd_jpy, "CAD": cad_jpy, "JPY": 1.0}
    except Exception as e:
        print(f"⚠️ レート取得失敗。仮レートを使用します。理由: {e}\n")
        return {"USD": 150.0, "CAD": 110.0, "JPY": 1.0}

def to_grams(value, unit): return value * WEIGHT_RATES.get(unit.lower(), 1.0) if isinstance(value, (int, float)) else 0.0
def to_cm(value, unit): return value * LENGTH_RATES.get(unit.lower(), 1.0) if isinstance(value, (int, float)) else 0.0

def write_error_log(log_path, asin, message):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [ASIN: {asin}] {message}\n")

# ==========================================
# 3. リサーチ・計算系 (Read & Calculate)
# ==========================================
def fetch_amazon_fee_rate(asin: str, country_code: str) -> float:
    """
    SP-API (ProductFees) を使用して、指定したASINと国の販売手数料率を動的に取得します。
    """
    config = MARKET_CONFIGS.get(country_code.upper())
    if not config or not config["token"]:
        raise ValueError(f"{country_code}のAPI設定がありません")

    credentials = {
        "lwa_app_id": LWA_APP_ID,
        "lwa_client_secret": LWA_CLIENT_SECRET,
        "refresh_token": config["token"]
    }
    
    currency = config["currency"]
    
    # 💡 最低手数料(ミニマムフィー)の固定額の影響をなくし、純粋な%を算出するため、高めの仮価格を設定します
    dummy_price = 10000.0 if currency == "JPY" else 100.0
    
    try:
        api = ProductFees(credentials=credentials, marketplace=config["sp_api_market"])
        
        # ASINと仮の価格を渡して手数料の見積もりをリクエスト (今回は自社発送[FBM]として見積もり)
        res = api.get_my_fees_estimate_for_asin(
            asin=asin,
            price=dummy_price,
            currency=currency,
            is_amazon_fulfilled=False 
        )
        
        # レスポンスから手数料(Amount)を抽出
        payload = res.payload
        estimate = payload.get("FeesEstimateResult", {}).get("FeesEstimate", {})
        total_fee = float(estimate.get("TotalFeesEstimate", {}).get("Amount", 0.0))
        
        if total_fee > 0:
            # (取得した手数料 ÷ 仮の価格) で手数料率(%)を割り出す
            fee_rate = total_fee / dummy_price
            return fee_rate
        else:
            raise ValueError("手数料が0として返されました")
            
    except Exception as e:
        raise Exception(f"API手数料取得失敗: {str(e)}")
    
def filter_and_sort_candidates(offers):
    safe_offers = []
    for offer in offers:
        if not offer.get('is_buybox', False):
            count, percent = int(offer.get('feedback_count', 0)), float(offer.get('feedback_percent', 0.0) if str(offer.get('feedback_percent', '0')).replace('.','',1).isdigit() else 0.0)
            if count < 10 or percent < 80.0: continue
        safe_offers.append(offer)
    return sorted(safe_offers, key=lambda x: x['total_jpy'])

def analyze_trade_opportunity(asin: str, exchange_rates: dict) -> dict:
    result = {"asin": asin, "lowest_prices": {"US": None, "CA": None, "JP": None}, "all_offers": {"US": [], "CA": [], "JP": []}, "has_catalog": {"US": False, "CA": False, "JP": False}, "judgment": "判定不可", "sourcing_country": None, "sourcing_candidates": [], "api_errors": []}
    for code in ["US", "CA", "JP"]:
        config = MARKET_CONFIGS[code]
        if not config["token"]: 
            result["api_errors"].append(f"{code}: トークン未設定")
            continue
        try:
            res = ProductsV0(credentials={"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": config["token"]}, marketplace=config["sp_api_market"]).get_item_offers(asin=asin, item_condition="New")
            result["has_catalog"][code] = True
            low_data = None
            for offer in res.payload.get('Offers', []):
                price, currency, shipping = offer.get('ListingPrice', {}).get('Amount', 0.0), offer.get('ListingPrice', {}).get('CurrencyCode', ''), offer.get('Shipping', {}).get('Amount', 0.0)
                tot = float(price) + float(shipping)
                jpy_p = int(tot * exchange_rates.get(currency, 1.0))
                if low_data is None or jpy_p < low_data["jpy"]: low_data = {"jpy": jpy_p, "original": tot, "currency": currency}
                
                ship_time = offer.get('ShippingTime', {})
                max_h = ship_time.get('maximumHours')
                h_str = f"{int(ship_time.get('minimumHours', 0))//24}-{int(max_h)//24}日" if max_h else "不明"
                if h_str == "0-0日": h_str = "即日(0日)"
                fb = offer.get('SellerFeedbackRating', {})
                result["all_offers"][code].append({"total_jpy": jpy_p, "original_price": tot, "currency": currency, "is_buybox": offer.get('IsBuyBoxWinner', False), "fulfillment": "FBA" if offer.get('IsFulfilledByAmazon', False) else "FBM", "feedback_count": fb.get('FeedbackCount', 0), "feedback_percent": fb.get('SellerPositiveFeedbackRating', '不明'), "handling_time": h_str})
            result["lowest_prices"][code] = low_data
        except Exception as e:
            result["api_errors"].append(f"{code} エラー: {e}")
        time.sleep(1)

    jp_p, us_p, ca_p = (result["lowest_prices"][c]["jpy"] if result["lowest_prices"][c] else None for c in ["JP", "US", "CA"])
    h_us, h_ca, h_jp = result["has_catalog"]["US"], result["has_catalog"]["CA"], result["has_catalog"]["JP"]

    if jp_p is not None:
        if (h_us and us_p is None) or (h_ca and ca_p is None): result["judgment"], result["sourcing_country"] = "輸出向き(競合なし)", "JP"
        elif us_p or ca_p:
            u, c = us_p or float('inf'), ca_p or float('inf')
            if (u < jp_p < c) or (c < jp_p < u): result["judgment"], result["sourcing_country"] = "双方向", "US" if min(u,c,jp_p)==u else ("CA" if min(u,c,jp_p)==c else "JP")
            elif jp_p < u and jp_p < c: result["judgment"], result["sourcing_country"] = "輸出向き", "JP"
            elif jp_p > u and jp_p > c: result["judgment"], result["sourcing_country"] = "輸入向き", "US" if u < c else "CA"
            else: result["judgment"] = "価格差なし"
        else: result["judgment"] = "海外カタログなし"
    elif h_jp: result["judgment"] = "仕入先なし"
    
    if result["sourcing_country"] and result["all_offers"][result["sourcing_country"]]:
        result["sourcing_candidates"] = filter_and_sort_candidates(result["all_offers"][result["sourcing_country"]])
    return result

def calculate_shipping_costs(chargeable_weight_g, actual_weight_g, sum_cm, excel_path):
    df_weight = pd.read_excel(excel_path, sheet_name="重量")
    df_size = pd.read_excel(excel_path, sheet_name="サイズ")
    
    val_w = df_weight[df_weight['g'] >= chargeable_weight_g].sort_values('g')
    if val_w.empty: raise Exception(f"重量オーバー: {chargeable_weight_g}g が輸出テーブル超過")
    w_row = val_w.iloc[0]
    
    val_jp_w = df_weight[df_weight['g'] >= actual_weight_g].sort_values('g')
    if val_jp_w.empty: raise Exception(f"重量オーバー: {actual_weight_g}g が国内テーブル超過")
    val_jp_s = df_size[df_size['cm'] >= sum_cm].sort_values('cm')
    if val_jp_s.empty: raise Exception(f"サイズオーバー: {sum_cm}cm が国内テーブル超過")
    cost_jp = max(val_jp_w.iloc[0]['jp'], val_jp_s.iloc[0]['jp'])

    cost_myus, carrier = None, None
    try:
        df_im = pd.read_excel(excel_path, sheet_name="輸入").iloc[1:].copy()
        df_im['g'] = pd.to_numeric(df_im['g'], errors='coerce')
        val_i = df_im[df_im['g'] >= chargeable_weight_g].sort_values('g')
        if val_i.empty: raise Exception("輸入テーブル超過")
        c_costs = pd.to_numeric(val_i.iloc[0][['FedEco', 'FedPrio', 'DHLExp']], errors='coerce')
        cost_myus, carrier = c_costs.min(), c_costs.idxmin()
    except Exception as e:
        if "超過" in str(e): raise e
    return w_row['us'], w_row['ca'], cost_jp, cost_myus, carrier

def get_item_dimensions_and_shipping(asin: str, excel_path: str, exchange_rates: dict):
    try:
        api = CatalogItemsV20220401(credentials={"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": MARKET_CONFIGS["JP"]["token"]}, marketplace=Marketplaces.JP)
        res = api.get_catalog_item(asin=asin, marketplaceIds=[Marketplaces.JP.marketplace_id], includedData=["dimensions"])
        dims = res.payload.get('dimensions', [])
        if not dims or 'package' not in dims[0]: raise Exception("サイズ情報なし")
        pkg = dims[0]['package']
        w, l, wd, h = to_grams(pkg.get('weight', {}).get('value', 0), pkg.get('weight', {}).get('unit', '')), to_cm(pkg.get('length', {}).get('value', 0), pkg.get('length', {}).get('unit', '')), to_cm(pkg.get('width', {}).get('value', 0), pkg.get('width', {}).get('unit', '')), to_cm(pkg.get('height', {}).get('value', 0), pkg.get('height', {}).get('unit', ''))
        chg_g = max(w, ((l*wd*h)/5000)*1000)
        c_us, c_ca, c_jp, c_myus_usd, carrier = calculate_shipping_costs(chg_g, w, l+wd+h, excel_path)
        return {"l": l, "w": wd, "h": h, "vol": l*wd*h, "sum_cm": l+wd+h, "actual_w": w, "chargeable_w": chg_g, "cost_us": c_us, "cost_ca": c_ca, "cost_jp": c_jp, "cost_myus": (c_myus_usd * exchange_rates.get("USD", 1.0)) if c_myus_usd else None, "best_carrier": carrier, "error": None}
    except Exception as e: return {"error": str(e)}

def check_profitability(cost_jpy, shipping_jpy, fee_rate, target_profit_rate, lowest_price_foreign, exchange_rate):
    """
    販売手数料と目標利益率を動的に受け取って目標販売額を逆算する
    """
    denom = 1.0 - fee_rate - target_profit_rate
    # 手数料と利益率の合計が100%以上の場合は計算不可（赤字確定）
    if denom <= 0: return False, 0.0
    
    target_price_foreign = ((cost_jpy + shipping_jpy) / denom) / exchange_rate
    return target_price_foreign <= (lowest_price_foreign * 1.25), target_price_foreign

def verify_export_pipeline(asin: str, excel_path: str, custom_fee_rate: float = None) -> dict:
    """
    custom_fee_rate: 外部から指定する販売手数料（指定がなければAPIまたは15%を適用）
    """
    result = {"asin": asin, "status": "success", "error_message": "", "cost_jpy": 0, "markets": {}}
    rates = get_exchange_rates()
    trade = analyze_trade_opportunity(asin, rates)
    ship = get_item_dimensions_and_shipping(asin, excel_path, rates)
    
    if ship.get("error"): return {**result, "status": "error", "error_message": f"送料エラー: {ship['error']}"}
    if not trade.get("sourcing_candidates"): return {**result, "status": "error", "error_message": "安全な仕入先なし"}
        
    cost_jpy = trade["sourcing_candidates"][0]["total_jpy"]
    result["cost_jpy"] = cost_jpy

    # 🎯 国ごとの確保したい目標利益率
    target_margins = {
        "US": 0.25, # 米国 25%
        "CA": 0.20, # カナダ 20%
        "JP": 0.20  # 日本 20%
    }

    for country, c_key in [("US", "cost_us"), ("CA", "cost_ca")]:
        shipping_cost = ship.get(c_key)
        comp = trade["lowest_prices"].get(country)
        
        # 📊 手数料の設定ロジック (引数 > API取得 > デフォルト15%)
        applied_fee_rate = custom_fee_rate
        if applied_fee_rate is None:
            try:
                # 💡 API経由で実際の手数料率を取得する
                applied_fee_rate = fetch_amazon_fee_rate(asin, country)
            except Exception as e:
                # 取得に失敗した場合（カタログが存在しない等）は安全のため15%を適用
                applied_fee_rate = 0.15

        target_margin = target_margins.get(country, 0.20)

        if not comp:
            result["markets"][country] = {"has_competitor": False, "shipping_cost": shipping_cost}
            continue
            
        c_p, curr = comp.get("native", comp["original"]), comp["currency"]
        rate = rates.get(curr, 1.0)
        
        # 利益計算関数に、手数料と目標利益率を渡す
        is_prof, tgt_nat = check_profitability(cost_jpy, shipping_cost, applied_fee_rate, target_margin, c_p, rate)
        tgt_jpy = tgt_nat * rate
        
        # 最終的な利益額(円) ＝ 販売額(円) － (販売額(円) × 手数料) － 原価
        profit_jpy = tgt_jpy - (tgt_jpy * applied_fee_rate) - cost_jpy - shipping_cost
        
        result["markets"][country] = {
            "has_competitor": True, 
            "shipping_cost": shipping_cost, 
            "comp_price_native": c_p, 
            "currency_code": curr, 
            "comp_price_jpy": comp['jpy'], 
            "is_prof": is_prof, 
            "target_native": tgt_nat, 
            "target_jpy": tgt_jpy, 
            "profit_jpy": profit_jpy,
            "fee_rate": applied_fee_rate,
            "margin_rate": target_margin
        }
    result["raw_trade"] = trade
    result["raw_ship"] = ship
    return result

# ==========================================
# 4. 出品・管理・レポート系 (Write & Manage)
# ==========================================
def change_listings_price_and_qty(country_code, sku, price, quantity):
    conf = MARKET_CONFIGS.get(country_code.upper())
    if not conf or not conf["token"] or not conf["seller_id"]: return print(f"⚠️ {country_code}設定不足")
    tk = get_access_token(conf["token"])
    res = requests.patch(
        f"{conf['endpoint']}/listings/2021-08-01/items/{conf['seller_id']}/{sku}",
        headers={"x-amz-access-token": tk, "Content-Type": "application/json"},
        params={"marketplaceIds": conf["marketplace_id"]},
        json={"productType": "PRODUCT", "patches": [{"op": "replace", "path": "/attributes/purchasable_offer", "value": [{"marketplace_id": conf["marketplace_id"], "currency": conf["currency"], "our_price": [{"schedule": [{"value_with_tax": float(price)}]}]}]}, {"op": "replace", "path": "/attributes/fulfillment_availability", "value": [{"fulfillment_channel_code": "DEFAULT", "quantity": int(quantity)}]}]}
    )
    print(f"✅ 更新成功" if res.status_code == 200 else f"❌ エラー({res.status_code}): {res.text}")
    return res.json() if res.status_code == 200 else None

def create_new_listing(country_code, sku, asin, price, quantity, handling_time):
    conf = MARKET_CONFIGS.get(country_code.upper())
    if not conf or not conf["token"] or not conf["seller_id"]: return print(f"⚠️ {country_code}設定不足")
    tk = get_access_token(conf["token"])
    res = requests.put(
        f"{conf['endpoint']}/listings/2021-08-01/items/{conf['seller_id']}/{sku}",
        headers={"x-amz-access-token": tk, "Content-Type": "application/json"},
        params={"marketplaceIds": conf["marketplace_id"], "issueLocale": "en_US" if country_code in ["US", "CA"] else "ja_JP"},
        json={"productType": "PRODUCT", "attributes": {"merchant_suggested_asin": [{"marketplace_id": conf["marketplace_id"], "value": asin}], "condition_type": [{"marketplace_id": conf["marketplace_id"], "value": "new_new"}], "purchasable_offer": [{"marketplace_id": conf["marketplace_id"], "currency": conf["currency"], "our_price": [{"schedule": [{"value_with_tax": float(price)}]}]}], "fulfillment_availability": [{"fulfillment_channel_code": "DEFAULT", "quantity": int(quantity), "lead_time_to_ship_max_days": int(handling_time)}]}}
    )
    print(f"✅ 出品成功" if res.status_code in (200,201,202) else f"❌ エラー({res.status_code}): {res.text}")
    return res.json() if res.status_code in (200,201,202) else None

def create_report(token, endpoint, marketplace_id):
    res = requests.post(f"{endpoint}/reports/2021-06-30/reports", headers={"x-amz-access-token": token, "Content-Type": "application/json"}, json={"reportType": "GET_MERCHANT_LISTINGS_ALL_DATA", "marketplaceIds": [marketplace_id]})
    res.raise_for_status()
    return res.json()["reportId"]

def wait_for_report(token, endpoint, report_id):
    url = f"{endpoint}/reports/2021-06-30/reports/{report_id}"
    while True:
        res = requests.get(url, headers={"x-amz-access-token": token})
        res.raise_for_status()
        data, status = res.json(), res.json().get("processingStatus")
        print(f"  -> 状況: {status}")
        if status == "DONE": return data["reportDocumentId"]
        elif status in ["CANCELLED", "FATAL"]: raise Exception(f"レポート失敗: {status}")
        time.sleep(15)

def download_and_save_report(token, endpoint, document_id, country_code, output_dir):
    res = requests.get(f"{endpoint}/reports/2021-06-30/documents/{document_id}", headers={"x-amz-access-token": token})
    res.raise_for_status()
    doc_data = res.json()
    file_res = requests.get(doc_data["url"])
    file_res.raise_for_status()
    content = gzip.decompress(file_res.content).decode('utf-8') if doc_data.get("compressionAlgorithm") == "GZIP" else file_res.text
    file_path = os.path.join(output_dir, f"listings_{country_code}_{time.strftime('%Y%m%d_%H%M%S')}.tsv")
    with open(file_path, "w", encoding="utf-8") as f: f.write(content)
    print(f"  -> 保存完了: {file_path}")

# ==========================================
# 5. コンソール出力・表示用関数 (Testing/CLI)
# ==========================================
def print_pipeline_result(data: dict):
    print(f"\n========== ASIN: {data['asin']} の総合検証 ==========")
    if data["status"] == "error": return print(f"❌ {data['error_message']}")
    
    for country, mkt in data["markets"].items():
        if not mkt["has_competitor"]:
            print(f"[{country}市場] 競合出品なし")
            continue
            
        profit_mark = '⭕️ 利益確保可能' if mkt['is_prof'] else '❌ 利益確保困難'
        margin_pct = int(mkt['margin_rate'] * 100)
        fee_pct = int(mkt['fee_rate'] * 100)
        
        print(f"[{country}市場] {profit_mark} (設定: 利益{margin_pct}% / 手数料{fee_pct}%)")
        print(f"  - 仕入＋送料 : {int(data['cost_jpy'] + mkt['shipping_cost']):,} 円")
        print(f"  - 現地最安値 : {mkt['comp_price_native']:.2f} {mkt['currency_code']}")
        print(f"  - 目標販売額 : {mkt['target_native']:.2f} {mkt['currency_code']} (見込利益: {int(mkt['profit_jpy']):,} 円)\n" + "-"*40)

def print_price_info(result: dict):
    print("\n【各国の最安値】")
    for country in ["JP", "US", "CA"]:
        p = result['lowest_prices'][country]
        print(f"{country}: {p['jpy']:,}円 ({p['original']:.2f} {p['currency']})" if p else f"{country}: 取得不可")
    print(f"\n【仕入対象国】: {result['sourcing_country']}")
    print("\n【✨ 安全な仕入先候補 ✨】")
    for idx, cand in enumerate(result['sourcing_candidates'], 1):
        print(f" {idx}. {'★カート' if cand['is_buybox'] else '　　　'} | {cand['total_jpy']:,}円 | {cand['fulfillment']} | 評価:{cand['feedback_count']}件")

def print_shipping_info(ship_data: dict):
    if ship_data.get("error"): return print(f"❌ エラー: {ship_data['error']}")
    print(f"\n【寸法・重量】 {ship_data['l']:.1f}x{ship_data['w']:.1f}x{ship_data['h']:.1f}cm | 実重量: {ship_data['actual_w']:.1f}g | 請求重量: {ship_data['chargeable_w']:.1f}g")
    if ship_data.get("cost_us"):
        print(f"【輸出送料】 US: {int(ship_data['cost_us']):,}円 | CA: {int(ship_data['cost_ca']):,}円")
        if ship_data.get("cost_myus"): print(f"【輸入送料】 MyUS: {int(ship_data['cost_myus']):,}円 ({ship_data['best_carrier']})")
        print(f"【国内送料】 JP: {int(ship_data['cost_jp']):,}円")