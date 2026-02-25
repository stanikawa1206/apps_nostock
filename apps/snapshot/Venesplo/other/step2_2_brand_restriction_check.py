# test_brand_step3_check_all_asins_fixed.py
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
                    return "OK", "" 
                
                reasons = []
                for r in restrictions:
                    for inner in r.get("reasons", []):
                        reasons.append(inner.get("message", "理由不明"))
                return "RESTRICTED", str(reasons)
            
            elif resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(3.0)
                continue
            else:
                return "ERROR", f"Status:{resp.status_code} Body:{resp.text}"

        except Exception as e:
            time.sleep(3.0)
            continue
            
    return "ERROR", "リトライ上限到達"

# ==========================================
# 2. Keepa API ASIN取得関数 (Step3の条件)
# ==========================================
def get_step3_all_asins(brand):
    """
    指定したブランドのASINをStep3の条件(Rank: 1 - 350000)で全件(最大10000件)取得する
    """
    # ★修正点: Step1の正常なコードと全く同じデータ構造に戻しました
    selection = {
        "brand": brand,  
        "avg90_SALES_gte": 1,
        "avg90_SALES_lte": 350000,
        "productType": 0,
        "perPage": 10000,
        "page": 0
    }
    
    print(f"   [Keepa API] 条件に基づくASINを全件検索中...")
    try:
        # ★最大の修正点: data={"selection": selection} ではなく、data=selection に修正しました
        res = keepa_request("query", params={"domain": 1}, data=selection)
        asin_list = res.get("asinList", [])
        return asin_list
    except Exception as e:
        print(f"   [Keepa API Error Detail]: {e}")
        return []

# ==========================================
# 3. メイン処理
# ==========================================
def main():
    try:
        # 1. テストするブランド名を指定
        target_brand = "Makita"  # ★ここをテストしたいブランド名に変更してください

        print(f"=== テスト開始: ブランド【{target_brand}】 ===")
        print(f"条件: Rank 1 - 350000")
        
        # 2. KeepaからStep3の条件に合うASINを全件取得
        asins = get_step3_all_asins(target_brand)
        
        if not asins:
            print(f" -> 条件に合致するASINは見つかりませんでした。")
            return

        total_count = len(asins)
        print(f" -> 取得完了: 合計 {total_count} 件のASINが見つかりました。")
        print("-" * 50)

        # 3. 初回アクセストークン取得
        print("SP-APIアクセストークンを取得中...")
        token = get_spapi_access_token(region="US")
        token_fetch_time = time.time()

        # 4. 全件のASINの制限をチェック
        for i, asin in enumerate(asins, 1):
            
            # トークン有効期限対策(50分経過で再取得)
            if time.time() - token_fetch_time > 3000:
                print(f"\n[システム] トークン取得から50分経過したため、新しいトークンを再取得します...")
                token = get_spapi_access_token(region="US")
                token_fetch_time = time.time()
                print("[システム] トークンの再取得が完了しました。\n")

            # 制限チェック実行
            status, msg = check_us_restriction(asin, token)
            
            # 結果出力
            if status == "OK":
                print(f"[{i:04d}/{total_count:04d}] {asin} => 〇 (出品可能)")
            elif status == "RESTRICTED":
                print(f"[{i:04d}/{total_count:04d}] {asin} => × (制限あり: {msg})")
            else:
                print(f"[{i:04d}/{total_count:04d}] {asin} => － (判定不可/エラー: {msg})")

        print("-" * 50)
        print("全件の出品可否チェックが完了しました。")

        # 5. Keepaコピペ用出力 (改行区切り)
        print("\n" + "=" * 50)
        print("【Keepaコピペ用 ASINリスト（改行区切り）】")
        print("=" * 50)
        print("\n".join(asins))
        print("=" * 50 + "\n")

    except Exception as e:
        print(f"致命的なエラー: {e}")

if __name__ == "__main__":
    main()