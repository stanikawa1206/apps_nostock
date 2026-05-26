import requests
import os
import base64
from dotenv import load_dotenv

load_dotenv()

def create_inventory_location():
    # 貴文さんの新しいリフレッシュトークン（DBに入れたもの）
    refresh_token = "v^1.1#i^1#r^1#p^3#f^0#I^3#t^Ul4xMF8xMDpGMUI3NkM4MTA0OEJBMUE3Qzc2MzMwOEMwMkEwNkJEN18yXzEjRV4yNjA="
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")

    # 1. Access Token 取得
    auth_str = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    token_res = requests.post("https://api.ebay.com/identity/v1/oauth2/token", 
        data={"grant_type": "refresh_token", "refresh_token": refresh_token, "scope": "https://api.ebay.com/oauth/api_scope/sell.inventory"},
        headers={"Authorization": f"Basic {auth_str}"})
    access_token = token_res.json().get("access_token")

    # 2. 拠点の作成 (Location Key: Default)
    url = "https://api.ebay.com/sell/inventory/v1/location/Default"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    # 住所データ（3枚目のスクショに基づいた正規の住所）
    payload = {
        "location": {
            "address": {
                "addressLine1": "Ro 36banchi",
                "addressLine2": "Komatsu Nagatamachi",
                "city": "Ishikawa",
                "postalCode": "9230034",
                "country": "JP"
            }
        },
        "locationInstructions": "Ships from Japan",
        "name": "Main Warehouse",
        "merchantLocationStatus": "ENABLED",
        "locationTypes": ["WAREHOUSE"]
    }

    # 作成（または更新）を実行
    res = requests.post(url, headers=headers, json=payload)
    
    if res.status_code in [201, 204]:
        print("✅ SUCCESS: Inventory Location 'Default' created/updated!")
    else:
        print(f"❌ Failed: {res.status_code}")
        print(res.text)

if __name__ == "__main__":
    create_inventory_location()