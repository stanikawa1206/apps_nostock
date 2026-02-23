# test_brand_step3_final_logic.py
# -*- coding: utf-8 -*-
import sys
import time
import requests
from my_utils import get_spapi_access_token, keepa_request

# ==========================================
# 1. SP-API 制限チェック関数
# ==========================================
def check_us_restriction(asin, access_token, retry_count=3):
    seller_id = "A3LDC1YQ3725LV"
    marketplace_id = "ATVPDKIKX0DER"
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    url = f"{endpoint}/listings/2021-08-01/restrictions"
    
    params = {
        "asin": asin,
        "sellerId": seller_id,
        "marketplaceIds": marketplace_id,
        "conditionType": "new_new"
    }
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    for attempt in range(retry_count):
        try:
            time.sleep(1.0) 
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code == 200:
                data = resp.json()
                restrictions = data.get("restrictions", [])
                if not restrictions:
                    return "OK"
                return "RESTRICTED"
            elif resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(3.0)
                continue
            else:
                return "ERROR"
        except Exception:
            time.sleep(3.0)
            continue
    return "ERROR"

# ==========================================
# 2. Keepa API ASIN全件取得関数
# ==========================================
def get_step3_all_asins(brand):
    """Step3の条件(Rank 1-350000)で全件取得"""
    selection = {
        "brand": brand,
        "avg90_SALES_gte": 1,
        "avg90_SALES_lte": 350000,
        "productType": 0,
        "perPage": 10000,
        "page": 0
    }
    try:
        res = keepa_request("query", params={"domain": 1}, data=selection)
        return res.get("asinList", [])
    except Exception as e:
        print(f"   [Keepa API Error]: {e}")
        return []

# ==========================================
# 3. メイン処理
# ==========================================
def main():
    target_brand = "Makita"  # ★調査したいブランド名に変更してください
    
    print(f"=== ブランド調査開始: 【{target_brand}】 ===")
    
    # 1. 条件に合う全ASINを取得
    all_asins = get_step3_all_asins(target_brand)
    total_found = len(all_asins)
    
    if total_found == 0:
        print(" -> 条件(Rank 1-350000)に合致するASINは見つかりませんでした。")
        return

    print(f" -> 条件一致ASIN: 合計 {total_found} 件見つかりました。")
    
    # 2. 代表3件を抽出
    test_asins = all_asins[:5]
    print(f" -> うち上位 {len(test_asins)} 件で出品制限を確認します。")
    print("-" * 40)

    # 3. SP-APIトークン取得
    token = get_spapi_access_token(region="US")
    
    brand_status = "×" # デフォルトは不可
    
    # 4. 制限チェック実行
    for i, asin in enumerate(test_asins, 1):
        status = check_us_restriction(asin, token)
        print(f"   [{i}/{len(test_asins)}] ASIN: {asin} => {status}")
        
        if status == "OK":
            brand_status = "〇"
            print(f"   ★出品可能なASINが見つかったため、判定を『〇』で確定します。")
            break # 1件でもOKなら終了

    # 5. 最終結果
    print("-" * 40)
    print(f"【最終判定結果】")
    print(f"ブランド名: {target_brand}")
    print(f"条件一致数: {total_found} 件")
    print(f"出品可否  : {brand_status}")
    print("-" * 40)

if __name__ == "__main__":
    main()