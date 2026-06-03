import os
import time
import math
import pandas as pd
from dotenv import load_dotenv
from sp_api.api import CatalogItemsV20220401 
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

# 環境変数の読み込み
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

def calculate_export_shipping_costs(chargeable_weight_g, excel_path):
    """
    Excelの「重量」シートからUS・CA向けの送料のみを算出する
    """
    try:
        df_weight = pd.read_excel(excel_path, sheet_name="重量")
    except Exception as e:
        print(f"❌ Excelファイルの読み込みエラー: {e}")
        return None, None

    # 重量シートの処理
    df_weight_data = df_weight[df_weight['g'] >= 0].sort_values('g')
    valid_weights = df_weight_data[df_weight_data['g'] >= chargeable_weight_g]
    
    if valid_weights.empty:
        w_row = df_weight_data.iloc[-1] # 上限超過時は一番重い設定を適用
    else:
        w_row = valid_weights.iloc[0]
        
    cost_us, cost_ca = w_row['us'], w_row['ca']
    return cost_us, cost_ca

def process_asin_batch(input_csv_path, output_csv_path, excel_path):
    """
    CSVからASINを読み込み、APIでサイズを取得して送料を計算し、CSVに出力する
    """
    print(f"📁 入力ファイルの読み込み: {input_csv_path}")
    try:
        # ヘッダーの有無に関わらず最初の列をASINとして扱う
        df_input = pd.read_csv(input_csv_path, header=None)
        asin_list = df_input[0].dropna().astype(str).tolist()
    except Exception as e:
        print(f"❌ 入力CSVの読み込みに失敗しました: {e}")
        return

    print(f"📦 合計 {len(asin_list)} 件のASINを処理します。\n")

    credentials = {
        "lwa_app_id": LWA_APP_ID,
        "lwa_client_secret": LWA_CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN
    }
    api = CatalogItemsV20220401(credentials=credentials, marketplace=Marketplaces.JP)

    results = []

    for idx, asin in enumerate(asin_list, 1):
        asin = asin.strip()
        if not asin:
            continue
            
        print(f"[{idx}/{len(asin_list)}] 取得中: {asin} ... ", end="")
        
        row_data = {
            "ASIN": asin,
            "寸法_長さ(cm)": None,
            "寸法_幅(cm)": None,
            "寸法_高さ(cm)": None,
            "体積(cm3)": None,
            "実重量(g)": None,
            "容積重量(g)": None,
            "請求重量(g)": None,
            "US送料(円)": None,
            "CA送料(円)": None,
            "ステータス": "成功"
        }

        try:
            res = api.get_catalog_item(
                asin=asin,
                marketplaceIds=[Marketplaces.JP.marketplace_id],
                includedData=["dimensions"]
            )
            dims = res.payload.get('dimensions', [])
            
            if not dims or 'package' not in dims[0]:
                row_data["ステータス"] = "サイズデータなし"
                print("⚠️ サイズデータなし")
            else:
                pkg = dims[0]['package']
                
                # サイズ・重量の計算
                actual_w = to_grams(pkg.get('weight', {}).get('value', 0), pkg.get('weight', {}).get('unit', ''))
                len_cm = to_cm(pkg.get('length', {}).get('value', 0), pkg.get('length', {}).get('unit', ''))
                width_cm = to_cm(pkg.get('width', {}).get('value', 0), pkg.get('width', {}).get('unit', ''))
                height_cm = to_cm(pkg.get('height', {}).get('value', 0), pkg.get('height', {}).get('unit', ''))
                
                vol_cm3 = len_cm * width_cm * height_cm
                vol_w = (vol_cm3 / 5000.0) * 1000.0
                chargeable_w = max(actual_w, vol_w)

                # 送料の計算
                cost_us, cost_ca = calculate_export_shipping_costs(chargeable_w, excel_path)
                
                row_data.update({
                    "寸法_長さ(cm)": round(len_cm, 1),
                    "寸法_幅(cm)": round(width_cm, 1),
                    "寸法_高さ(cm)": round(height_cm, 1),
                    "体積(cm3)": round(vol_cm3, 1),
                    "実重量(g)": round(actual_w, 1),
                    "容積重量(g)": round(vol_w, 1),
                    "請求重量(g)": round(chargeable_w, 1),
                    "US送料(円)": int(cost_us) if cost_us else None,
                    "CA送料(円)": int(cost_ca) if cost_ca else None
                })
                print("✅ 完了")

        except SellingApiException as e:
            row_data["ステータス"] = f"APIエラー"
            print(f"❌ APIエラー")
        except Exception as e:
            row_data["ステータス"] = f"その他エラー"
            print(f"❌ エラー")

        results.append(row_data)
        
        # 連続リクエストによる制限回避のための待機
        time.sleep(1.5)

    # 結果をデータフレーム化してCSV出力
    df_results = pd.DataFrame(results)
    df_results.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
    print(f"\n🎉 すべての処理が完了しました！")
    print(f"💾 結果を保存しました: {output_csv_path}")


if __name__ == "__main__":
    # 💡 入出力ファイルと料金表の設定
    INPUT_CSV = r"X:\apps\snapshot\amazon\input_asins.csv"      # 読み込むASINリスト (A列にASINを記載)
    OUTPUT_CSV = r"X:\apps\snapshot\amazon\shipping_simulation.csv" # 出力される結果ファイル
    EXCEL_FILE = r"X:\apps\snapshot\amazon\ship_cost.xlsx"      # 料金テーブル
    
    # 入力ファイルが存在するかチェック
    if not os.path.exists(INPUT_CSV):
        print(f"⚠️ 入力ファイルが見つかりません。以下に作成してください:\n{INPUT_CSV}")
    else:
        process_asin_batch(INPUT_CSV, OUTPUT_CSV, EXCEL_FILE)