#   4. 商品サイズ取得および送料計算プログラム (File 4)
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
    "grams": 1.0, "kilograms": 1000.0, "pounds": 453.592, "ounces": 28.3495, "milligrams": 0.001
}
LENGTH_RATES = {
    "centimeters": 1.0, "millimeters": 0.1, "meters": 100.0, "inches": 2.54, "feet": 30.48
}

def to_grams(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * WEIGHT_RATES.get(unit.lower(), 1.0)

def to_cm(value, unit):
    if not isinstance(value, (int, float)): return 0.0
    return value * LENGTH_RATES.get(unit.lower(), 1.0)

def calculate_shipping_costs(chargeable_weight_g, actual_weight_g, sum_cm, excel_path):
    """
    Excelから輸出用(US/CA)・日本国内用・輸入用(MyUS)の送料を算出する
    """
    try:
        # 3つのシートを読み込む
        df_weight = pd.read_excel(excel_path, sheet_name="重量")
        df_size = pd.read_excel(excel_path, sheet_name="サイズ")
        df_import = pd.read_excel(excel_path, sheet_name="輸入")
    except Exception as e:
        print(f"❌ Excelファイルの読み込みエラー: {e}")
        return None, None, None, None, None

    # ==========================================
    # 1. 輸出用 (US / CA) の計算
    # ==========================================
    df_weight_data = df_weight[df_weight['g'] >= 0].sort_values('g')
    valid_weights = df_weight_data[df_weight_data['g'] >= chargeable_weight_g]
    w_row = df_weight_data.iloc[-1] if valid_weights.empty else valid_weights.iloc[0]
    cost_us, cost_ca = w_row['us'], w_row['ca']

    # ==========================================
    # 2. 日本国内 (JP) の計算
    # ==========================================
    df_size_data = df_size[df_size['cm'] >= 0].sort_values('cm')
    
    # 重量の基準
    valid_jp_weights = df_weight_data[df_weight_data['g'] >= actual_weight_g]
    cost_jp_weight = df_weight_data.iloc[-1]['jp'] if valid_jp_weights.empty else valid_jp_weights.iloc[0]['jp']
    
    # サイズの基準
    valid_jp_sizes = df_size_data[df_size_data['cm'] >= sum_cm]
    cost_jp_size = df_size_data.iloc[-1]['jp'] if valid_jp_sizes.empty else valid_jp_sizes.iloc[0]['jp']
    
    cost_jp = max(cost_jp_weight, cost_jp_size)

    # ==========================================
    # 3. 輸入用 (MyUS → JP) の計算
    # ==========================================
    # 1行目は「day (配達日数)」のデータなので、2行目以降を実データとして扱う
    df_import_data = df_import.iloc[1:].copy()
    
    # 'g' 列を数値型に変換して処理
    df_import_data['g'] = pd.to_numeric(df_import_data['g'], errors='coerce')
    valid_imports = df_import_data[df_import_data['g'] >= chargeable_weight_g].sort_values('g')
    
    if valid_imports.empty:
        # テーブルの上限を超えている場合は、一番重い設定を採用
        i_row = df_import_data.iloc[-1]
    else:
        i_row = valid_imports.iloc[0]

    # 利用可能なキャリア (FedEco, FedPrio, DHLExp) の中で一番安い金額を採用する
    carrier_costs = pd.to_numeric(i_row[['FedEco', 'FedPrio', 'DHLExp']], errors='coerce')
    cost_myus_to_jp = carrier_costs.min()
    
    # 最安だったキャリア名を特定 (参考用)
    best_carrier = carrier_costs.idxmin()

    return cost_us, cost_ca, cost_jp, cost_myus_to_jp, best_carrier

def main():
    asin = input("ASINを入力してください: ").strip()
    if not asin:
        return
        
    print(f"\n=== {asin} のサイズ・重量データを取得中 ===")
    
    credentials = {
        "lwa_app_id": LWA_APP_ID,
        "lwa_client_secret": LWA_CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN
    }
    
    try:
        api = CatalogItemsV20220401(credentials=credentials, marketplace=Marketplaces.JP)
        res = api.get_catalog_item(
            asin=asin,
            marketplaceIds=[Marketplaces.JP.marketplace_id],
            includedData=["dimensions"]
        )
        
        dims = res.payload.get('dimensions', [])
        
        if not dims or 'package' not in dims[0]:
            print("❌ サイズ・重量データがカタログに登録されていません。")
            return
            
        pkg = dims[0]['package']
        
        actual_weight_g = to_grams(pkg.get('weight', {}).get('value', 0), pkg.get('weight', {}).get('unit', ''))
        length_cm = to_cm(pkg.get('length', {}).get('value', 0), pkg.get('length', {}).get('unit', ''))
        width_cm = to_cm(pkg.get('width', {}).get('value', 0), pkg.get('width', {}).get('unit', ''))
        height_cm = to_cm(pkg.get('height', {}).get('value', 0), pkg.get('height', {}).get('unit', ''))
        
        volume_cm3 = length_cm * width_cm * height_cm
        sum_cm = length_cm + width_cm + height_cm
        volume_weight_g = (volume_cm3 / 5000.0) * 1000.0
        chargeable_weight_g = max(actual_weight_g, volume_weight_g)
        
        print("\n【パッケージ（梱包）サイズ・重量情報】")
        print(f"寸法  : {length_cm:.1f} cm × {width_cm:.1f} cm × {height_cm:.1f} cm")
        print(f"体積(積): {volume_cm3:.1f} cm³")
        print(f"３辺合計: {sum_cm:.1f} cm")
        print("-" * 45)
        print(f"実重量 : {actual_weight_g:.1f} g")
        print(f"容積重量: {volume_weight_g:.1f} g (体積 ÷ 5000 × 1000)")
        print(f"請求重量: {chargeable_weight_g:.1f} g (実重量と容積重量の重い方)")
        print("-" * 45)

        # ====== Excelを参照して送料を計算 ======
        excel_file_path = r"X:\apps\snapshot\amazon\ship_cost.xlsx"
        
        cost_us, cost_ca, cost_jp, cost_myus, best_carrier = calculate_shipping_costs(
            chargeable_weight_g, 
            actual_weight_g, 
            sum_cm, 
            excel_file_path
        )
        
        if cost_us is None:
            return
            
        print("\n【国別 送料目安 (輸出)】")
        print(f"US 送料 : {int(cost_us):,} 円 (請求重量ベース)")
        print(f"CA 送料 : {int(cost_ca):,} 円 (請求重量ベース)")
        print("-" * 45)
        
        print("\n【国別 送料目安 (輸入)】")
        print(f"MyUS → JP 送料 : {int(cost_myus):,} 円 (最安キャリア: {best_carrier})")
        print(f"JP 国内送料    : {int(cost_jp):,} 円 (実重量とサイズの高い方)")
        print("-" * 45)

    except SellingApiException as e:
        print(f"❌ APIエラー: {e}")
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    main()