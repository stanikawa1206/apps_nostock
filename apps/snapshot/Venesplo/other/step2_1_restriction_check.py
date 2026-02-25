# test_single_asin_restriction.py
# -*- coding: utf-8 -*-
import time
import requests
from my_utils import get_spapi_access_token

def check_us_restriction(asin, access_token, retry_count=3):
    """
    SP-APIを叩き、状態を3パターンで返す
    戻り値: "OK" (出品可), "RESTRICTED" (出品不可), "ERROR" (判定不能/エラー)
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

    print(f"[{asin}] SP-APIへリクエストを送信します...")

    for attempt in range(retry_count):
        try:
            time.sleep(1.0) # APIレートリミット対策
            resp = requests.get(url, headers=headers, params=params)
            
            # 正常応答
            if resp.status_code == 200:
                data = resp.json()
                restrictions = data.get("restrictions", [])
                
                if not restrictions:
                    return "OK" # 制限なし（出品可能）
                
                # 制限ありの場合、理由を抽出
                reasons = []
                for r in restrictions:
                    for inner in r.get("reasons", []):
                        reasons.append(inner.get("message", "理由不明"))
                print(f"   -> 制限理由: {reasons}")
                return "RESTRICTED"
            
            # レート制限 (429) または サーバーエラー (5xx) の場合はリトライ
            elif resp.status_code in (429, 500, 502, 503, 504):
                print(f"   -> [API一時エラー] Status:{resp.status_code}. 再試行します ({attempt+1}/{retry_count})")
                time.sleep(3.0)
                continue
            
            # その他のエラー (400: カタログにないASIN, 403: 権限エラーなど)
            else:
                print(f"   -> [APIエラー(スキップ)] Status:{resp.status_code} | Body:{resp.text}")
                return "ERROR"

        except Exception as e:
            print(f"   -> [通信例外エラー] {e}. 再試行します ({attempt+1}/{retry_count})")
            time.sleep(3.0)
            continue
            
    print("   -> [エラー] リトライ上限に達しました。")
    return "ERROR"

def main():
    try:
        # 1. SP-APIアクセストークン取得 (USリージョン)
        print("アクセストークンを取得中...")
        token = get_spapi_access_token(region="US")
        
        # 2. テストしたいASINを指定してください
        test_asin = "0140136347"  # ★ここを任意のASINに書き換えてテストしてください
        
        # 3. 制限チェック実行
        status = check_us_restriction(test_asin, token)
        
        # 4. 結果表示
        print("-" * 30)
        print(f"最終判定結果: {status}")
        print("-" * 30)

    except Exception as e:
        print(f"致命的なエラー: {e}")

if __name__ == "__main__":
    main()