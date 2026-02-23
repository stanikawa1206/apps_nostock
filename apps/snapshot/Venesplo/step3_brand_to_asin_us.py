# -*- coding: utf-8 -*-
import time
import requests
from my_utils import get_sql_server_connection, get_spapi_access_token, keepa_request

# ==========================================
# 1. SQL定義
# ==========================================

# 未処理のブランドを取得 (trxに存在しない、かつ mstのUSがNULL)
SQL_SELECT_TARGETS = """
SELECT m.brand
FROM [nostock].[mst].[amazon_brand] m
WHERE m.brand <> 'N/A'
  AND m.US IS NULL
  AND NOT EXISTS (
    SELECT 1 
    FROM [nostock].[trx].[amazon_export_brand] t 
    WHERE t.brand COLLATE DATABASE_DEFAULT = m.brand COLLATE DATABASE_DEFAULT
  )
"""

# トランザクションへの挿入
SQL_INSERT_TRX = """
INSERT INTO [nostock].[trx].[amazon_export_brand] 
(brand, store, from_rank, to_rank, batch, number_of_item, seen_date)
VALUES (?, ?, ?, ?, ?, ?, GETDATE())
"""

# マスタへの出品可否更新
SQL_UPDATE_MST = """
UPDATE [nostock].[mst].[amazon_brand] 
SET US = ?, last_seen_at = GETDATE() 
WHERE brand = ?
"""

# ==========================================
# 2. 関数定義
# ==========================================

def check_us_restriction(asin, access_token):
    """SP-APIでの制限チェック (1=OK, 0=NG, -1=Error)"""
    seller_id = "A3LDC1YQ3725LV"
    marketplace_id = "ATVPDKIKX0DER"
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    url = f"{endpoint}/listings/2021-08-01/restrictions"
    
    params = {
        "asin": asin, "sellerId": seller_id,
        "marketplaceIds": marketplace_id, "conditionType": "new_new"
    }
    headers = {"x-amz-access-token": access_token}

    try:
        time.sleep(1.0)
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            return "OK" if not resp.json().get("restrictions") else "NG"
        return "ERR"
    except:
        return "ERR"

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    # 設定値
    F_RANK, T_RANK = 1, 350000
    STORE, BATCH = 'US', 3
    
    try:
        cursor.execute(SQL_SELECT_TARGETS)
        rows = cursor.fetchall()
        if not rows:
            print("処理対象のブランドは見つかりませんでした。")
            return

        print(f"{len(rows)} 件のブランドを処理します。")
        token = get_spapi_access_token(region="US")

        for brand in [r[0] for r in rows]:
            print(f"--- {brand} ---")
            
            # 1. Keepaで全件取得
            selection = {
                "brand": brand, "avg90_SALES_gte": F_RANK, "avg90_SALES_lte": T_RANK,
                "productType": 0, "perPage": 10000
            }
            res = keepa_request("query", params={"domain": 1}, data=selection)
            asins = res.get("asinList", [])
            num_items = len(asins)
            
            if num_items == 0:
                print(" -> 条件一致ASINなし。スキップ。")
                continue

            # 2. 代表3件で制限チェック
            final_us_status = "×"
            for asin in asins[:3]:
                res_status = check_us_restriction(asin, token)
                print(f"   ASIN {asin}: {res_status}")
                if res_status == "OK":
                    final_us_status = "〇"
                    break
            
            # 3. DB書き込み (trx & mst)
            try:
                # トランザクション記録
                cursor.execute(SQL_INSERT_TRX, [brand, STORE, F_RANK, T_RANK, BATCH, num_items])
                # マスタ更新
                cursor.execute(SQL_UPDATE_MST, [final_us_status, brand])
                conn.commit()
                print(f" -> 完了: 件数={num_items}, 判定={final_us_status}")
            except Exception as e:
                print(f" -> DB更新エラー: {e}")
                conn.rollback()

    finally:
        conn.close()
        print("\n全処理を終了しました。")

if __name__ == "__main__":
    main()