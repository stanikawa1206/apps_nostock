import os
import requests
from dotenv import load_dotenv

# .envファイルの読み込み
load_dotenv()

# --- 認証情報・出品者IDの読み込み ---
LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")

REFRESH_TOKEN_JP = os.environ.get("REFRESH_TOKEN")       # 日本用
REFRESH_TOKEN_US = os.environ.get("REFRESH_TOKEN_US")    # 北米（US・CA）用

SELLER_ID_JP = os.environ.get("SELLER_ID")               # 日本の出品者ID
SELLER_ID_US = os.environ.get("SELLER_ID_US")            # 北米の出品者ID

# --- 各国の接続設定（Seller IDもここに統合） ---
MARKET_CONFIGS = {
    "US": {
        "endpoint": "https://sellingpartnerapi-na.amazon.com",
        "marketplace_id": "ATVPDKIKX0DER",
        "currency": "USD",
        "token": REFRESH_TOKEN_US,
        "seller_id": SELLER_ID_US
    },
    "CA": {
        "endpoint": "https://sellingpartnerapi-na.amazon.com",
        "marketplace_id": "A2EUQ1WTGCTBG2",
        "currency": "CAD",
        "token": REFRESH_TOKEN_US,
        "seller_id": SELLER_ID_US
    },
    "JP": {
        "endpoint": "https://sellingpartnerapi-fe.amazon.com",
        "marketplace_id": "A1VC38T7YXB528",
        "currency": "JPY",
        "token": REFRESH_TOKEN_JP,
        "seller_id": SELLER_ID_JP
    }
}

def get_access_token(refresh_token):
    """LWA Access Tokenの取得"""
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": LWA_APP_ID,
        "client_secret": LWA_CLIENT_SECRET
    }
    res = requests.post(url, data=payload)
    res.raise_for_status()
    return res.json()["access_token"]

def change_listings_price_and_qty(country_code, sku, price, quantity):
    """
    Listings Items API (PATCH) を使用し、指定した国のSKUの価格と在庫数を即時書き換える
    """
    config = MARKET_CONFIGS.get(country_code.upper())
    if not config:
        print(f"❌ 未対応の国コードです: {country_code}")
        return

    # トークンとSeller IDの設定チェック
    if not config["token"] or not config["seller_id"]:
        print(f"⚠️ {country_code}用のリフレッシュトークンまたはSELLER_IDが.envに設定されていません。")
        return

    # 1. アクセストークンの取得
    access_token = get_access_token(config["token"])

    # 2. APIエンドポイントURLの構築 (設定からSeller IDを動的に読み込む)
    url = f"{config['endpoint']}/listings/2021-08-01/items/{config['seller_id']}/{sku}"
    
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    params = {
        "marketplaceIds": config["marketplace_id"]
    }

    # 3. パッチ（差分データ）の組み立て
    patches = [
        {
            "op": "replace",
            "path": "/attributes/purchasable_offer",
            "value": [
                {
                    "marketplace_id": config["marketplace_id"],
                    "currency": config["currency"],
                    "our_price": [
                        {
                            "schedule": [
                                {
                                    "value_with_tax": float(price)
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "op": "replace",
            "path": "/attributes/fulfillment_availability",
            "value": [
                {
                    "fulfillment_channel_code": "DEFAULT",  # 自己発送(FBM)
                    "quantity": int(quantity)
                }
            ]
        }
    ]

    body = {
        "productType": "PRODUCT",
        "patches": patches
    }

    print(f"🔄 [{country_code}] SKU: {sku} に価格: {price} / 在庫: {quantity} を反映中...")
    
    # 4. PATCHリクエストの送信
    res = requests.patch(url, headers=headers, params=params, json=body)
    
    if res.status_code == 200:
        result = res.json()
        print(f"✅ 反映成功。ステータス: {result.get('status')}")
        return result
    else:
        print(f"❌ エラー発生 ({res.status_code}): {res.text}")
        return None

# --- 実行ブロック ---
if __name__ == "__main__":
    
    SKU_US = "B000000SKA"
    PRICE_US = 120.00
    QUANTITY_US = 1

    SKU_CA = "B00004R7IE"
    PRICE_CA = 95.00
    QUANTITY_CA = 1

    SKU_JP = "B0FD6WDW1L"
    PRICE_JP = 52000
    QUANTITY_JP = 1

    # パターンA: US市場で、在庫数1、価格を $45.50 に改定
    # change_listings_price_and_qty(
    #     country_code="US",
    #     sku=SKU_US,
    #     price=PRICE_US,
    #     quantity=QUANTITY_US
    # )

    # パターンB: CA市場で、在庫数1、価格を $60.00 (CAD) に改定
    # change_listings_price_and_qty(
    #     country_code="CA",
    #     sku=SKU_CA,
    #     price=PRICE_CA,
    #     quantity=QUANTITY_CA
    # )

    # パターンC: JP市場で、在庫数0、価格を ¥5000 に改定
    change_listings_price_and_qty(
        country_code="JP",
        sku=SKU_JP,
        price=PRICE_JP,
        quantity=QUANTITY_JP
    )