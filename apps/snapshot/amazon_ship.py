import os
import pandas as pd
from dotenv import load_dotenv
from sp_api.api import CatalogItemsV20220401 
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

load_dotenv()

LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")

# --- 単位変換用の辞書 ---
WEIGHT_RATES = {
    "grams": 1.0,
    "kilograms": 1000.0,
    "pounds": 453.592,
    "ounces": 28.3495,
    "milligrams": 0.001
}

LENGTH_RATES = {
    "centimeters": 1.0,
    "millimeters": 0.1,
    "meters": 100.0,
    "inches": 2.54,
    "feet": 30.48
}

def to_grams(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * WEIGHT_RATES.get(unit.lower(), 1.0)

def to_cm(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * LENGTH_RATES.get(unit.lower(), 1.0)

def calculate_shipping_costs(chargeable_weight_g, actual_weight_g, sum_cm, excel_path):
    """Excelの料金表から各国の送料を算出する"""
    try:
        # Excelファイルの「重量」シートと「サイズ」シートを読み込む
        df_weight = pd.read_excel(excel_path, sheet_name="重量")
        df_size = pd.read_excel(excel_path, sheet_name="サイズ")
    except Exception as e:
        print(f"Excelファイルの読み込みエラー: {e}")
        return None, None, None

    # -1などの無効な数値を弾き、昇順に並び替え
    df_weight = df_weight[df_weight['g'] >= 0].sort_values('g')
    df_size = df_size[df_size['cm'] >= 0].sort_values('cm')

    # --- US, CA の送料判定 (請求重量ベース) ---
    valid_weights = df_weight[df_weight['g'] >= chargeable_weight_g]
    if valid_weights.empty:
        w_row = df_weight.iloc[-1]  # 重量オーバー時は一番下の行を適用
    else:
        w_row = valid_weights.iloc[0] # 条件を満たす最小の行
        
    cost_us = w_row['us']
    cost_ca = w_row['ca']

    # --- JP の送料判定 (実重量とサイズの高い方) ---
    # 1. 実重量から判定
    valid_jp_weights = df_weight[df_weight['g'] >= actual_weight_g]
    cost_jp_weight = df_weight.iloc[-1]['jp'] if valid_jp_weights.empty else valid_jp_weights.iloc[0]['jp']

    # 2. サイズ(3辺合計)から判定
    valid_jp_sizes = df_size[df_size['cm'] >= sum_cm]
    cost_jp_size = df_size.iloc[-1]['jp'] if valid_jp_sizes.empty else valid_jp_sizes.iloc[0]['jp']

    # 3. サイズと重量で金額が高い方を適用 (福山通運のルール)
    cost_jp = max(cost_jp_weight, cost_jp_size)
    
    return cost_us, cost_ca, cost_jp

def get_item_dimensions(asin: str):
    if not REFRESH_TOKEN:
        print("エラー: REFRESH_TOKEN が.envに設定されていません。")
        return

    credentials = {
        "lwa_app_id": LWA_APP_ID,
        "lwa_client_secret": LWA_CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    }

    try:
        print(f"=== {asin} のサイズ・重量データを取得中 ===")
        catalog_api = CatalogItemsV20220401(credentials=credentials, marketplace=Marketplaces.JP)
        
        response = catalog_api.get_catalog_item(
            asin=asin,
            marketplaceIds=[Marketplaces.JP.marketplace_id],
            includedData=["dimensions"]
        )
        
        payload = response.payload
        dimensions_list = payload.get('dimensions', [])
        
        if not dimensions_list:
            print("この商品にはサイズ・重量データが登録されていません。")
            return
            
        dim_data = dimensions_list[0]
        package_dim = dim_data.get('package', {})
        
        if not package_dim:
            print("パッケージサイズのデータがありません。")
            return
            
        # 生のデータを取得
        raw_weight = package_dim.get('weight', {}).get('value', 0)
        raw_weight_unit = package_dim.get('weight', {}).get('unit', '')
        
        raw_length = package_dim.get('length', {}).get('value', 0)
        raw_length_unit = package_dim.get('length', {}).get('unit', '')
        
        raw_width = package_dim.get('width', {}).get('value', 0)
        raw_width_unit = package_dim.get('width', {}).get('unit', '')
        
        raw_height = package_dim.get('height', {}).get('value', 0)
        raw_height_unit = package_dim.get('height', {}).get('unit', '')
        
        # --- g と cm に変換 ---
        weight_g = to_grams(raw_weight, raw_weight_unit)
        length_cm = to_cm(raw_length, raw_length_unit)
        width_cm = to_cm(raw_width, raw_width_unit)
        height_cm = to_cm(raw_height, raw_height_unit)
        
        actual_weight_g = weight_g
        volume_cm3 = length_cm * width_cm * height_cm
        sum_cm = length_cm + width_cm + height_cm
        volume_weight_g = (volume_cm3 / 5000.0) * 1000.0
        chargeable_weight_g = max(actual_weight_g, volume_weight_g)
        
        print("\n【パッケージ（梱包）サイズ・重量情報】")
        print(f"寸法　　: {length_cm:.1f} cm × {width_cm:.1f} cm × {height_cm:.1f} cm")
        print(f"体積(積): {volume_cm3:.1f} cm³")
        print(f"３辺合計: {sum_cm:.1f} cm")
        print("-" * 45)
        print(f"実重量　: {actual_weight_g:.1f} g")
        print(f"容積重量: {volume_weight_g:.1f} g (体積 ÷ 5000 × 1000)")
        print(f"請求重量: {chargeable_weight_g:.1f} g (実重量と容積重量の重い方)")
        print("-" * 45)

        # ====== ここから追加：Excelを参照して送料を計算 ======
        excel_file_path = r"X:\apps\snapshot\ship_cost.xlsx"
        
        cost_us, cost_ca, cost_jp = calculate_shipping_costs(
            chargeable_weight_g, 
            actual_weight_g, 
            sum_cm, 
            excel_file_path
        )

        if cost_us is not None:
            print("\n【国別 送料目安】")
            print(f"US (アメリカ) : {int(cost_us):,} 円 (請求重量ベース)")
            print(f"CA (カナダ)   : {int(cost_ca):,} 円 (請求重量ベース)")
            print(f"JP (日　本)   : {int(cost_jp):,} 円 (実重量とサイズの高い方)")
            print("-" * 45)

    except SellingApiException as e:
        print(f"SP-APIエラーが発生しました:\n{e}")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")

if __name__ == "__main__":
    target_asin = "B00008B3QH"  # ← 実際のASINに変更してください
    get_item_dimensions(target_asin)