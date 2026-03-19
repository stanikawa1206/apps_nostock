import os
import requests
from dotenv import load_dotenv
from sp_api.api import ProductsV0
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

# .envファイルから環境変数を読み込む
load_dotenv()

LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")

def get_exchange_rates():
    """起動時に最新の為替レート(USD→JPY, CAD→JPY)を取得する関数"""
    print("最新の為替レートを取得中...")
    try:
        # USDベースの最新レートを取得 (無料・キー不要のAPI)
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url)
        data = response.json()
        rates = data.get("rates", {})
        
        usd_to_jpy = rates.get("JPY", 150.0)
        usd_to_cad = rates.get("CAD", 1.35)
        
        # CAD -> JPY のレートを計算 (USD/JPY ÷ USD/CAD)
        cad_to_jpy = usd_to_jpy / usd_to_cad
        
        print(f"✅ レート取得成功: 1 USD = {usd_to_jpy:.2f} 円 | 1 CAD = {cad_to_jpy:.2f} 円\n")
        return {"USD": usd_to_jpy, "CAD": cad_to_jpy, "JPY": 1.0}
        
    except Exception as e:
        print(f"⚠️ 為替レートの取得に失敗しました。仮のレートで計算します。({e})\n")
        return {"USD": 150.0, "CAD": 110.0, "JPY": 1.0} # 失敗時のフォールバック値

def get_amazon_price(asin: str, country_name: str, marketplace, refresh_token: str, exchange_rates: dict):
    if not refresh_token:
        print(f"[{country_name}] エラー: 対応するリフレッシュトークンが.envから読み込めませんでした。")
        return

    credentials = {
        "lwa_app_id": LWA_APP_ID,
        "lwa_client_secret": LWA_CLIENT_SECRET,
        "refresh_token": refresh_token,
    }

    try:
        print(f"=== {country_name} ({asin}) の価格データを取得中 ===")
        pricing_api = ProductsV0(credentials=credentials, marketplace=marketplace)
        
        response = pricing_api.get_item_offers(asin=asin, item_condition="New")
        payload = response.payload
        offers = payload.get('Offers', [])
        
        if not offers:
            print(f"出品はありません。\n")
            return
            
        for offer in offers:
            # 1. 基本価格と送料
            price = offer.get('ListingPrice', {}).get('Amount', 0.0)
            currency = offer.get('ListingPrice', {}).get('CurrencyCode', '')
            shipping_fee = offer.get('Shipping', {}).get('Amount', 0.0)
            
            total_price = float(price) + float(shipping_fee)
            
            # ★ 追加: 日本円への換算 ★
            rate = exchange_rates.get(currency, 1.0)
            jpy_price = int(total_price * rate) # 円は小数点以下を切り捨てて見やすくする
            
            # その他の情報
            is_buybox = offer.get('IsBuyBoxWinner', False)
            feedback_count = offer.get('SellerFeedbackRating', {}).get('FeedbackCount', 0)
            sub_condition = offer.get('SubCondition', '不明')
            is_prime = offer.get('PrimeInformation', {}).get('IsPrime', False)
            prime_text = "プライム対象" if is_prime else "対象外"
            is_fba = offer.get('IsFulfilledByAmazon', False)
            fulfillment = "FBA" if is_fba else "自社発送(FBM)"
            
            # 発送目安
            shipping_time = offer.get('ShippingTime', {})
            max_hours = shipping_time.get('maximumHours', 0)
            min_hours = shipping_time.get('minimumHours', 0)
            if max_hours > 0:
                max_days, min_days = max_hours // 24, min_hours // 24
                shipping_info = f"{max_days}日以内" if min_days == max_days else f"{min_days}〜{max_days}日"
            else:
                availability = shipping_time.get('availabilityType')
                shipping_info = "即日(在庫あり)" if availability == "NOW" else "不明"
            
            # --- 出力のフォーマット ---
            buybox_mark = "★カート" if is_buybox else "　　　　"
            
            # 日本円換算額をメインに表示し、元の通貨をカッコ内に配置
            print(f"{buybox_mark} | 総額: 約 {jpy_price:,} 円 ({total_price:.2f} {currency})")
            print(f"　　　　 | コンディション: {sub_condition} | セラー評価数: {feedback_count}件")
            print(f"　　　　 | 配送: {fulfillment} ({prime_text}) | 発送目安: {shipping_info}")
            print("-" * 65)
            
    except SellingApiException as e:
            # エラーメッセージを文字列化
            error_msg = str(e)
            if "invalid ASIN" in error_msg:
                print(f"　-> ⚠️ このASINは {country_name} のAmazonカタログには存在しません。\n")
            else:
                print(f"[{country_name}] SP-APIエラーが発生しました:\n{e}\n")
    except Exception as e:
        print(f"[{country_name}] 予期せぬエラーが発生しました: {e}\n")

if __name__ == "__main__":
    # 1. 起動時に為替レートを取得
    rates = get_exchange_rates()

    target_asin = "B001P4ZR6C"  # ← ここを実際のASINに変更してください
    
    targets = [
        {"name": "アメリカ", "marketplace": Marketplaces.US, "token": os.environ.get("REFRESH_TOKEN_US")},
        {"name": "カナダ",   "marketplace": Marketplaces.CA, "token": os.environ.get("REFRESH_TOKEN_US")},
        {"name": "日本",     "marketplace": Marketplaces.JP, "token": os.environ.get("REFRESH_TOKEN")},
    ]

    # 2. 取得したレートを各国の処理に渡す
    for target in targets:
        get_amazon_price(target_asin, target["name"], target["marketplace"], target["token"], rates)