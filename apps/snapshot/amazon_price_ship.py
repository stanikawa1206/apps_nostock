import os
import time
import pandas as pd
import requests
import pyodbc
from dotenv import load_dotenv
from sp_api.api import CatalogItemsV20220401, ProductsV0
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

# .env ファイルの読み込み
load_dotenv()

# --- 環境変数 (Amazon API) ---
LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
REFRESH_TOKEN_US = os.environ.get("REFRESH_TOKEN_US")

# --- 環境変数 (SQL Server) ---
DB_DRIVER = os.environ.get("DB_DRIVER", "ODBC Driver 17 for SQL Server")
DB_SERVER = os.environ.get("DB_SERVER")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

# --- 接続文字列の作成 ---
CONN_STR = f"DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}"

# --- 定数・辞書 ---
WEIGHT_RATES = {"grams": 1.0, "kilograms": 1000.0, "pounds": 453.592, "ounces": 28.3495, "milligrams": 0.001}
LENGTH_RATES = {"centimeters": 1.0, "millimeters": 0.1, "meters": 100.0, "inches": 2.54, "feet": 30.48}

def to_grams(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * WEIGHT_RATES.get(unit.lower(), 1.0)

def to_cm(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * LENGTH_RATES.get(unit.lower(), 1.0)

def get_exchange_rates():
    """最新の為替レート(USD→JPY, CAD→JPY)を取得"""
    print("最新の為替レートを取得中...")
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url)
        rates = response.json().get("rates", {})
        usd_to_jpy = rates.get("JPY", 150.0)
        usd_to_cad = rates.get("CAD", 1.35)
        cad_to_jpy = usd_to_jpy / usd_to_cad
        print(f"✅ レート取得成功: 1 USD = {usd_to_jpy:.2f} 円 | 1 CAD = {cad_to_jpy:.2f} 円\n")
        return {"USD": usd_to_jpy, "CAD": cad_to_jpy, "JPY": 1.0}
    except Exception:
        print("⚠️ 為替レートの取得に失敗。仮レートで計算します。\n")
        return {"USD": 150.0, "CAD": 110.0, "JPY": 1.0}

def calculate_shipping_costs(chargeable_weight_g, actual_weight_g, sum_cm, excel_path):
    """Excelの料金表から各国の送料を算出"""
    try:
        df_weight = pd.read_excel(excel_path, sheet_name="重量")
        df_size = pd.read_excel(excel_path, sheet_name="サイズ")
    except Exception as e:
        print(f"Excel読み込みエラー: {e}")
        return None, None, None

    df_weight = df_weight[df_weight['g'] >= 0].sort_values('g')
    df_size = df_size[df_size['cm'] >= 0].sort_values('cm')

    # US, CA (請求重量ベース)
    valid_weights = df_weight[df_weight['g'] >= chargeable_weight_g]
    w_row = df_weight.iloc[-1] if valid_weights.empty else valid_weights.iloc[0]
    cost_us, cost_ca = w_row['us'], w_row['ca']

    # JP (実重量とサイズの高い方)
    valid_jp_weights = df_weight[df_weight['g'] >= actual_weight_g]
    cost_jp_weight = df_weight.iloc[-1]['jp'] if valid_jp_weights.empty else valid_jp_weights.iloc[0]['jp']

    valid_jp_sizes = df_size[df_size['cm'] >= sum_cm]
    cost_jp_size = df_size.iloc[-1]['jp'] if valid_jp_sizes.empty else valid_jp_sizes.iloc[0]['jp']

    cost_jp = max(cost_jp_weight, cost_jp_size)
    return cost_us, cost_ca, cost_jp

def get_item_data(asin: str, exchange_rates: dict, excel_path: str):
    """ASINの価格、サイズ、送料を取得し、DB更新用の辞書を返す"""
    data = {
        "jp_price": None, "us_price": None, "ca_price": None,
        "sales_category": "None",
        "length": None, "width": None, "height": None, "total_size": None,
        "actual_weight": None, "dim_weight": None, "chargeable_weight": None,
        "jp_shipping_fee": None, "us_shipping_fee": None, "ca_shipping_fee": None
    }

    # ==========================
    # 1. 価格の取得と販売区分の判定
    # ==========================
    targets = [
        {"code": "US", "marketplace": Marketplaces.US, "token": REFRESH_TOKEN_US},
        {"code": "CA", "marketplace": Marketplaces.CA, "token": REFRESH_TOKEN_US},
        {"code": "JP", "marketplace": Marketplaces.JP, "token": REFRESH_TOKEN},
    ]

    for target in targets:
        code, mp, token = target["code"], target["marketplace"], target["token"]
        if not token: continue

        credentials = {"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": token}
        try:
            api = ProductsV0(credentials=credentials, marketplace=mp)
            offers = api.get_item_offers(asin=asin, item_condition="New").payload.get('Offers', [])
            
            lowest_jpy = None
            for offer in offers:
                price = offer.get('ListingPrice', {}).get('Amount', 0.0)
                shipping = offer.get('Shipping', {}).get('Amount', 0.0)
                currency = offer.get('ListingPrice', {}).get('CurrencyCode', '')
                
                total_jpy = int((float(price) + float(shipping)) * exchange_rates.get(currency, 1.0))
                if lowest_jpy is None or total_jpy < lowest_jpy:
                    lowest_jpy = total_jpy
                    
            if code == "JP": data["jp_price"] = lowest_jpy
            elif code == "US": data["us_price"] = lowest_jpy
            elif code == "CA": data["ca_price"] = lowest_jpy

        except SellingApiException:
            pass # カタログにない場合はスキップ

    # 販売区分 (Sales Category) の判定: Export / Import / Both / None
    jp, us, ca = data["jp_price"], data["us_price"], data["ca_price"]
    if jp is not None:
        if us is not None and ca is not None:
            if (us < jp < ca) or (ca < jp < us): data["sales_category"] = "Both"
            elif jp < us and jp < ca: data["sales_category"] = "Export"
            elif jp > us and jp > ca: data["sales_category"] = "Import"
        elif us is not None:
            data["sales_category"] = "Export" if jp < us else "Import"
        elif ca is not None:
            data["sales_category"] = "Export" if jp < ca else "Import"

    # ==========================
    # 2. サイズ・重量・送料の取得
    # ==========================
    credentials_jp = {"lwa_app_id": LWA_APP_ID, "lwa_client_secret": LWA_CLIENT_SECRET, "refresh_token": REFRESH_TOKEN}
    try:
        catalog_api = CatalogItemsV20220401(credentials=credentials_jp, marketplace=Marketplaces.JP)
        res = catalog_api.get_catalog_item(asin=asin, marketplaceIds=[Marketplaces.JP.marketplace_id], includedData=["dimensions"])
        
        dimensions_list = res.payload.get('dimensions', [])
        if dimensions_list and 'package' in dimensions_list[0]:
            pkg = dimensions_list[0]['package']
            
            w_val, w_unit = pkg.get('weight', {}).get('value', 0), pkg.get('weight', {}).get('unit', '')
            l_val, l_unit = pkg.get('length', {}).get('value', 0), pkg.get('length', {}).get('unit', '')
            wd_val, wd_unit = pkg.get('width', {}).get('value', 0), pkg.get('width', {}).get('unit', '')
            h_val, h_unit = pkg.get('height', {}).get('value', 0), pkg.get('height', {}).get('unit', '')
            
            data["length"] = round(to_cm(l_val, l_unit), 2)
            data["width"] = round(to_cm(wd_val, wd_unit), 2)
            data["height"] = round(to_cm(h_val, h_unit), 2)
            data["total_size"] = round(data["length"] + data["width"] + data["height"], 2)
            
            data["actual_weight"] = int(to_grams(w_val, w_unit))
            volume_cm3 = data["length"] * data["width"] * data["height"]
            data["dim_weight"] = int((volume_cm3 / 5000.0) * 1000.0)
            data["chargeable_weight"] = max(data["actual_weight"], data["dim_weight"])

            # 送料計算
            cost_us, cost_ca, cost_jp = calculate_shipping_costs(
                data["chargeable_weight"], data["actual_weight"], data["total_size"], excel_path
            )
            
            if cost_us is not None:
                data["us_shipping_fee"] = int(cost_us)
                data["ca_shipping_fee"] = int(cost_ca)
                data["jp_shipping_fee"] = int(cost_jp)

    except SellingApiException:
        pass # サイズデータがない場合

    return data

def get_target_asins() -> list:
    """DBからすべてのASINを取得する"""
    print("データベースから対象のASINをすべて取得します...")
    asins = []
    # TOPを外し、全件取得するSQLに変更
    sql = "SELECT [asin] FROM [nostock].[trx].[amazon_cross_market_asin]"
    
    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            asins = [row[0] for row in rows]
    except Exception as e:
        print(f"❌ ASIN取得エラー: {e}")
        
    return asins

def update_sql_database(asin: str, data: dict):
    """取得したデータをSQL Serverに書き込む"""
    sql = """
    UPDATE [nostock].[trx].[amazon_cross_market_asin]
    SET [jp_price] = ?, [us_price] = ?, [ca_price] = ?,
        [Sales_Category] = ?,
        [length] = ?, [width] = ?, [height] = ?, [total_size] = ?,
        [actual_weight] = ?, [dim_weight] = ?, [chargeable_weight] = ?,
        [jp_shipping_fee] = ?, [us_shipping_fee] = ?, [ca_shipping_fee] = ?
    WHERE [asin] = ?
    """
    
    params = (
        data["jp_price"], data["us_price"], data["ca_price"],
        data["sales_category"],
        data["length"], data["width"], data["height"], data["total_size"],
        data["actual_weight"], data["dim_weight"], data["chargeable_weight"],
        data["jp_shipping_fee"], data["us_shipping_fee"], data["ca_shipping_fee"],
        asin
    )

    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            print(f"✅ [{asin}] 更新完了！ (区分: {data['sales_category']})")
    except Exception as e:
        print(f"❌ [{asin}] データベース更新エラー: {e}")

# ==========================================
# 実行ブロック
# ==========================================
if __name__ == "__main__":
    excel_file_path = r"X:\apps\snapshot\ship_cost.xlsx"
    
    # 1. 為替レートの取得
    rates = get_exchange_rates()
    
    # 2. DBから全てのASINを取得
    target_asins = get_target_asins()
    
    if not target_asins:
        print("⚠️ 更新対象のASINが見つかりませんでした。終了します。")
    else:
        print(f"\n--- 計 {len(target_asins)} 件の処理を開始します ---\n")
        
        # 3. 取得したASINリストをループ処理
        for index, current_asin in enumerate(target_asins, 1):
            print(f"[{index}/{len(target_asins)}] 処理中: {current_asin} ...")
            
            # APIからデータ取得
            item_data = get_item_data(current_asin, rates, excel_file_path)
            
            # DBへ書き込み
            update_sql_database(current_asin, item_data)
            
            # APIのレートリミット対策で2秒待機
            if index < len(target_asins):
                time.sleep(2)
                
        print("\n🎉 すべての処理が完了しました！")