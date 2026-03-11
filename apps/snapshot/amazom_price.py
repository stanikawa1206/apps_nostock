import os
from dotenv import load_dotenv
from sp_api.api import ProductsV0
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

# 1. .envファイルから環境変数を読み込む
load_dotenv()

# 2. 認証情報の設定 (.envファイルから取得)
credentials = {
    "lwa_app_id": os.environ.get("LWA_CLIENT_ID"),
    "lwa_client_secret": os.environ.get("LWA_CLIENT_SECRET"),
    "refresh_token": os.environ.get("REFRESH_TOKEN_US"),
}

def get_amazon_price(asin: str):
    # 環境変数が正しく読み込めているか簡単なチェック
    if not credentials["refresh_token"]:
        print("エラー: .envファイルから認証情報が読み込めませんでした。キー名を確認してください。")
        return

    try:
        # 3. ProductsV0 クラスでAPIクライアントを初期化 (US市場に設定)
        print(f"{asin} の価格データを取得中...")
        pricing_api = ProductsV0(credentials=credentials, marketplace=Marketplaces.US)
        
        # 4. ASINと商品の状態(New/Used)を指定してオファーを取得
        response = pricing_api.get_item_offers(asin=asin, item_condition="New")
        
        # 5. レスポンスのペイロードからデータを抽出
        payload = response.payload
        offers = payload.get('Offers', [])
        
        if not offers:
            print(f"{asin} の出品はありません。")
            return
            
        print(f"\n--- {asin} の価格情報 ---")
        for offer in offers:
            # 価格、通貨コード、カート取得状況を取得
            price = offer.get('ListingPrice', {}).get('Amount')
            currency = offer.get('ListingPrice', {}).get('CurrencyCode')
            is_buybox = offer.get('IsBuyBoxWinner', False)
            
            print(f"価格: {price} {currency} | カート取得: {'はい' if is_buybox else 'いいえ'}")
            
    except SellingApiException as e:
        # API通信時のエラー（認証エラーやASIN間違いなど）
        print(f"SP-APIエラーが発生しました:\n{e}")
    except Exception as e:
        # その他の予期せぬエラー
        print(f"予期せぬエラーが発生しました: {e}")

if __name__ == "__main__":
    # 実行 (USの有効なASINを指定してください。例: B07M85O7N)
    target_asin = "B002R08DEQ"  # ← ここを実際のUS ASINに変更して実行してください
    get_amazon_price(target_asin)