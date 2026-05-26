import os
import requests
from dotenv import load_dotenv

# ==========================================
# 1. 環境変数の読み込み
# ==========================================
load_dotenv()

LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")

# リフレッシュトークン
REFRESH_TOKEN_US = os.environ.get("REFRESH_TOKEN_US")    # 北米（US・CA）共通
REFRESH_TOKEN_JP = os.environ.get("REFRESH_TOKEN")       # 日本用

# 出品者ID（Seller ID）
SELLER_ID_US = os.environ.get("SELLER_ID_US")            # 北米（US・CA）共通
SELLER_ID_JP = os.environ.get("SELLER_ID")               # 日本用

# ==========================================
# 2. 各国の接続設定（MARKET_CONFIGS）
# ==========================================
# 💡 ここで国ごとのエンドポイント、通貨、使用するトークン・IDを一元管理します
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

# ==========================================
# 3. API認証処理
# ==========================================
def get_access_token(refresh_token):
    """LWA Access Tokenを取得する"""
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

# ==========================================
# 4. 新規出品処理（PUT リクエスト）
# ==========================================
def create_new_listing(country_code, sku, asin, price, quantity, handling_time):
    """
    Listings Items API (PUT) を使用し、ASINに対して自社SKUを新規登録（相乗り出品）する
    """
    config = MARKET_CONFIGS.get(country_code.upper())
    if not config:
        print(f"❌ 未対応の国コードです: {country_code}")
        return
        
    if not config["token"] or not config["seller_id"]:
        print(f"⚠️ {country_code}用の設定（トークンまたはSELLER_ID）が不足しています。")
        return

    # アクセストークンの取得
    access_token = get_access_token(config["token"])
    
    # APIエンドポイントURLの構築
    url = f"{config['endpoint']}/listings/2021-08-01/items/{config['seller_id']}/{sku}"
    
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    params = {
        "marketplaceIds": config["marketplace_id"],
        "issueLocale": "en_US" if country_code in ["US", "CA"] else "ja_JP" 
    }

    # 💡 TSVデータをJSONの属性(attributes)にマッピング
    body = {
        "productType": "PRODUCT",
        "attributes": {
            # TSV: product-id
            "merchant_suggested_asin": [
                {
                    "marketplace_id": config["marketplace_id"],
                    "value": asin
                }
            ],
            # TSV: item-condition (11 = 新品)
            "condition_type": [
                {
                    "marketplace_id": config["marketplace_id"],
                    "value": "new_new"  
                }
            ],
            # TSV: price
            "purchasable_offer": [
                {
                    "marketplace_id": config["marketplace_id"],
                    "currency": config["currency"],
                    "our_price": [
                        {
                            "schedule": [{"value_with_tax": float(price)}]
                        }
                    ]
                }
            ],
            # TSV: quantity と handling-time
            "fulfillment_availability": [
                {
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": int(quantity),
                    "lead_time_to_ship_max_days": int(handling_time) 
                }
            ]
        }
    }

    print(f"🚀 [{country_code.upper()}] 新規出品(PUT)を送信中 | SKU: {sku} | ASIN: {asin} | 価格: {price}")
    res = requests.put(url, headers=headers, params=params, json=body)
    
    # HTTPステータス 200(OK), 201(Created), 202(Accepted) はすべて成功とみなす
    if res.status_code in (200, 201, 202):
        print(f"✅ 新規出品リクエストが受理されました。(HTTP {res.status_code})")
        return res.json()
    else:
        print(f"❌ エラー発生 ({res.status_code}): {res.text}")
        return None

# ==========================================
# 5. 実行ブロック
# ==========================================
if __name__ == "__main__":

    ASIN_US = "B00KBK5FYK"
    SKU_US = "B00KBK5FYK"
    PRICE_US = 130.00
    HANDLING_TIME_US = 5
    QUANTITY_US = 0

    ASIN_CA = "B00KBK5FYK"
    SKU_CA = "B00KBK5FYK"
    PRICE_CA = 220.00
    HANDLING_TIME_CA = 5
    QUANTITY_CA = 0

    ASIN_JP = "B00KBK5FYK"
    SKU_JP = "B00KBK5FYK"
    PRICE_JP = 13000
    HANDLING_TIME_JP = 5    
    QUANTITY_JP = 0

    # パターン1: アメリカ(US)へ出品
    create_new_listing(
        country_code="US",
        sku=SKU_US,
        asin=ASIN_US,
        price=PRICE_US,        # USD
        quantity=QUANTITY_US,
        handling_time=HANDLING_TIME_US
    )

    print("-" * 40)

    # パターン2: カナダ(CA)へ出品
    create_new_listing(
        country_code="CA",
        sku=SKU_CA,
        asin=ASIN_CA,
        price=PRICE_CA,        # CAD
        quantity=QUANTITY_CA,
        handling_time=HANDLING_TIME_CA
    )

    print("-" * 40)

    # パターン3: 日本(JP)へ出品
    create_new_listing(
        country_code="JP",
        sku=SKU_JP,
        asin=ASIN_JP,
        price=PRICE_JP,       # JPY
        quantity=QUANTITY_JP,
        handling_time=HANDLING_TIME_JP
    )