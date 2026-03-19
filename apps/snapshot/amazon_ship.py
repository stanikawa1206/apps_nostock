import os
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
        volume_weight_g = (volume_cm3 / 5000.0) * 1000.0
        chargeable_weight_g = max(actual_weight_g, volume_weight_g)
        
        print("\n【パッケージ（梱包）サイズ・重量情報】")
        print(f"寸法　　: {length_cm:.1f} cm × {width_cm:.1f} cm × {height_cm:.1f} cm")
        print(f"体積(積): {volume_cm3:.1f} cm³")
        print("-" * 45)
        print(f"実重量　: {actual_weight_g:.1f} g")
        print(f"容積重量: {volume_weight_g:.1f} g (体積 ÷ 5000 × 1000)")
        print(f"請求重量: {chargeable_weight_g:.1f} g (実重量と容積重量の重い方)")
        print("-" * 45)

    except SellingApiException as e:
        print(f"SP-APIエラーが発生しました:\n{e}")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")

if __name__ == "__main__":
    target_asin = "B001P4ZR6C"  # ← 実際のASINに変更してください
    get_item_dimensions(target_asin)