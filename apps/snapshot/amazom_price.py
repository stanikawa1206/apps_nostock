from sp_api.api import ProductPricing
from sp_api.base import Marketplaces
from sp_api.base.exceptions import sp_api_client_exception

# 認証情報を設定 (環境変数などで管理することを推奨します)
credentials = {
    "refresh_token": "YOUR_REFRESH_TOKEN",
    "lwa_app_id": "YOUR_LWA_CLIENT_ID",
    "lwa_client_secret": "YOUR_LWA_CLIENT_SECRET",
    # ※旧方式のアプリの場合は以下も必要です
    # "aws_secret_key": "YOUR_AWS_SECRET_KEY",
    # "aws_access_key": "YOUR_AWS_ACCESS_KEY",
    # "role_arn": "YOUR_ROLE_ARN",
}

def get_amazon_price(asin: str):
    try:
        # 日本市場(Marketplaces.JP)を指定してAPIクライアントを初期化
        pricing_api = ProductPricing(credentials=credentials, marketplace=Marketplaces.JP)
        
        # ASINと商品の状態(New/Used)を指定してオファーを取得
        response = pricing_api.get_item_offers(asin=asin, item_condition="New")
        
        # レスポンスのペイロードからデータを抽出
        payload = response.payload
        offers = payload.get('Offers', [])
        
        if not offers:
            print(f"{asin} の出品はありません。")
            return
            
        print(f"--- {asin} の価格情報 ---")
        for offer in offers:
            price = offer.get('ListingPrice', {}).get('Amount')
            is_buybox = offer.get('IsBuyBoxWinner', False)
            print(f"価格: {price}円 | カート取得: {'はい' if is_buybox else 'いいえ'}")
            
    except sp_api_client_exception as e:
        print(f"エラーが発生しました: {e}")

# 実行
get_amazon_price("B0XXXXXXXX") # 実際のASINに置き換えてください