# step2_brand_restriction_checker_us_v3.py
# -*- coding: utf-8 -*-
import sys
import time
import requests
from my_utils import get_sql_server_connection, get_spapi_access_token, keepa_request

# ==========================================
# 1. SQL定義
# ==========================================
SQL_SELECT_BRANDS = """
SELECT TOP 100 brand 
FROM mst.amazon_brand 
WHERE US IS NULL
"""

SQL_UPDATE_BRAND_STATUS = """
UPDATE mst.amazon_brand 
SET US = ?, last_seen_at = GETDATE() 
WHERE brand = ?
"""

# ==========================================
# 2. 関数定義
# ==========================================
def get_representative_asins(brand, count=10):
    """
    Keepa APIの/queryを使用して、そのブランドのASINを取得する
    """
    query_selection = {
        "brand": [brand],     
        "productType": 0,
        "perPage": count,      # Keepa側にも一応10を要求
        "page": 0
    }
    
    try:
        res = keepa_request("query", params={"domain": 1}, data={"selection": query_selection})
        
        asin_list = res.get("asinList", [])
        
        # ★追加: 取得できたリストから、強制的に最初の10件（count）だけを抽出する
        return asin_list[:count]
        
    except Exception as e:
        print(f"   [Keepa API Error Detail]: {e}")
        return []

def check_us_restriction(asin, access_token):
    """
    SP-API Restrictions API を叩き、ダメな理由も出力するテスト用関数
    """
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

    try:
        time.sleep(1.0) 
        resp = requests.get(url, headers=headers, params=params)
        
        # --- API通信が正常な場合 ---
        if resp.status_code == 200:
            data = resp.json()
            restrictions = data.get("restrictions", [])
            
            # 制限なし！
            if not restrictions:
                return True 
                
            # 制限ありの場合、理由を出力する
            reasons = []
            for r in restrictions:
                for inner in r.get("reasons", []):
                    reasons.append(inner.get("message", "理由不明"))
            print(f"      -> 制限理由: {reasons}")
            return False

        # --- API通信エラーの場合 (403など) ---
        else:
            print(f"      -> [APIエラー] Status:{resp.status_code} | Body:{resp.text}")
            return False

    except Exception as e:
        print(f"      -> [例外エラー] {e}")
        return False

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    try:
        # SP-APIアクセストークン取得 (USリージョン)
        token = get_spapi_access_token(region="US")

        # USカラムがNULLのブランドを100件抽出
        cursor.execute(SQL_SELECT_BRANDS)
        brands = cursor.fetchall()

        if not brands:
            print("処理対象のブランド（USがNULL）が見つかりませんでした。")
            return

        for row in brands:
            brand_name = row[0]
            print(f"--- 調査開始: {brand_name} ---")
            # print(f"--- 調査開始: ダイワ(DAIWA) ---")

            # 代表ASINを最大10件取得
            asins = get_representative_asins(brand_name, count=10)
            # asins = get_representative_asins("ダイワ(DAIWA)", count=10)            
            if not asins:
                print(" -> ASINを取得できませんでした。判定不可のため『』とします。")
                final_status = ""
            else:
                final_status = "×" 
                print(f" -> 最大{len(asins)}件のASINをチェック中...")
                
                # 10件中1件でもOKなら「〇」
                for i, asin in enumerate(asins, 1):
                    is_ok = check_us_restriction(asin, token)
                    print(f"    ({i}) ASIN {asin}: {'〇' if is_ok else '×'}")
                    
                    if is_ok:
                        final_status = "〇"
                        break 

            # DB更新
            cursor.execute(SQL_UPDATE_BRAND_STATUS, [final_status, brand_name])
            conn.commit()
            print(f" -> 結果保存: {final_status}")

    except Exception as e:
        print(f"Fatal Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()