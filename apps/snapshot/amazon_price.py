import os
import json
import requests
from dotenv import load_dotenv
from sp_api.api import ProductsV0
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

# 環境変数の読み込み
load_dotenv()
LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")

def get_exchange_rates():
    """起動時に最新の為替レート(USD→JPY, CAD→JPY)を取得する関数"""
    print("最新の為替レートを取得中...")
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url)
        data = response.json()
        rates = data.get("rates", {})
        usd_to_jpy = rates.get("JPY", 150.0)
        usd_to_cad = rates.get("CAD", 1.35)
        cad_to_jpy = usd_to_jpy / usd_to_cad
        print(f"✅ レート取得成功: 1 USD = {usd_to_jpy:.2f} 円 | 1 CAD = {cad_to_jpy:.2f} 円\n")
        return {"USD": usd_to_jpy, "CAD": cad_to_jpy, "JPY": 1.0}
    except Exception as e:
        print(f"⚠️ 為替レートの取得に失敗しました。仮レートで計算します。({e})\n")
        return {"USD": 150.0, "CAD": 110.0, "JPY": 1.0}

def analyze_trade_opportunity(asin: str, exchange_rates: dict) -> dict:
    """
    ASINを受け取り、3カ国の価格を取得、比較し、仕入先候補を返す関数
    """
    targets = [
        {"code": "US", "marketplace": Marketplaces.US, "token": os.environ.get("REFRESH_TOKEN_US")},
        {"code": "CA", "marketplace": Marketplaces.CA, "token": os.environ.get("REFRESH_TOKEN_US")},
        {"code": "JP", "marketplace": Marketplaces.JP, "token": os.environ.get("REFRESH_TOKEN")},
    ]

    # 返り値としてまとめるデータ構造
    result_data = {
        "asin": asin,
        "lowest_prices": {"US": None, "CA": None, "JP": None},
        "all_offers": {"US": [], "CA": [], "JP": []},
        "judgment": "判定不可",       # 輸出、輸入、双方向などの判定結果
        "sourcing_country": None,   # どこから仕入れるべきか (US, CA, JP)
        "sourcing_candidates": []   # 実際の仕入先となる最安出品者のリスト
    }

    # 1. 各国のデータを取得
    for target in targets:
        country_code = target["code"]
        token = target["token"]
        
        if not token:
            continue

        credentials = {
            "lwa_app_id": LWA_APP_ID,
            "lwa_client_secret": LWA_CLIENT_SECRET,
            "refresh_token": token,
        }

        try:
            pricing_api = ProductsV0(credentials=credentials, marketplace=target["marketplace"])
            response = pricing_api.get_item_offers(asin=asin, item_condition="New")
            offers = response.payload.get('Offers', [])
            
            lowest_jpy = None
            
            for offer in offers:
                price = offer.get('ListingPrice', {}).get('Amount', 0.0)
                currency = offer.get('ListingPrice', {}).get('CurrencyCode', '')
                shipping_fee = offer.get('Shipping', {}).get('Amount', 0.0)
                total_price = float(price) + float(shipping_fee)
                
                rate = exchange_rates.get(currency, 1.0)
                jpy_price = int(total_price * rate)
                
                if lowest_jpy is None or jpy_price < lowest_jpy:
                    lowest_jpy = jpy_price
                
                # 出品者情報をリストに保存
                offer_detail = {
                    "total_jpy": jpy_price,
                    "original_price": total_price,
                    "currency": currency,
                    "is_buybox": offer.get('IsBuyBoxWinner', False),
                    "fulfillment": "FBA" if offer.get('IsFulfilledByAmazon', False) else "FBM",
                    "feedback_count": offer.get('SellerFeedbackRating', {}).get('FeedbackCount', 0),
                    "condition": offer.get('SubCondition', '不明')
                }
                result_data["all_offers"][country_code].append(offer_detail)

            result_data["lowest_prices"][country_code] = lowest_jpy

        except SellingApiException as e:
            pass # カタログに存在しない場合はスキップ (Noneのままになる)
        except Exception as e:
            pass

    # 2. 輸出・輸入の判定処理
    jp = result_data["lowest_prices"]["JP"]
    us = result_data["lowest_prices"]["US"]
    ca = result_data["lowest_prices"]["CA"]

    if jp is not None:
        if us is not None and ca is not None:
            if (us < jp < ca) or (ca < jp < us):
                result_data["judgment"] = "双方向の可能性あり"
                # 双方向の場合、一番安い国をメインの仕入先候補とする
                cheapest = min(us, ca, jp)
                result_data["sourcing_country"] = "US" if cheapest == us else ("CA" if cheapest == ca else "JP")
            elif jp < us and jp < ca:
                result_data["judgment"] = "輸出向き"
                result_data["sourcing_country"] = "JP"
            elif jp > us and jp > ca:
                result_data["judgment"] = "輸入向き"
                result_data["sourcing_country"] = "US" if us < ca else "CA"
            else:
                result_data["judgment"] = "価格差なし"
                
        elif us is not None:
            if jp < us:
                result_data["judgment"] = "輸出向き"
                result_data["sourcing_country"] = "JP"
            elif jp > us:
                result_data["judgment"] = "輸入向き"
                result_data["sourcing_country"] = "US"
                
        elif ca is not None:
            if jp < ca:
                result_data["judgment"] = "輸出向き"
                result_data["sourcing_country"] = "JP"
            elif jp > ca:
                result_data["judgment"] = "輸入向き"
                result_data["sourcing_country"] = "CA"

    # 3. 仕入先候補（一番安い国の出品者リスト）を価格順に並び替えて抽出
    sourcing_ctry = result_data["sourcing_country"]
    if sourcing_ctry and result_data["all_offers"][sourcing_ctry]:
        # 安い順にソートして格納
        sorted_offers = sorted(result_data["all_offers"][sourcing_ctry], key=lambda x: x["total_jpy"])
        result_data["sourcing_candidates"] = sorted_offers

    return result_data

def print_price_result(result):
    # 取得した辞書データを分かりやすく表示
    print("\n【各国の最安値 (JPY)】")
    print(f"JP: {result['lowest_prices']['JP']}")
    print(f"US: {result['lowest_prices']['US']}")
    print(f"CA: {result['lowest_prices']['CA']}")

    print(f"\n【判定結果】: {result['judgment']}")
    print(f"【仕入対象国】: {result['sourcing_country']}")

    print("\n【仕入先候補の出品者一覧 (安い順)】")
    for idx, cand in enumerate(result['sourcing_candidates'], 1):
        buybox = "★カート" if cand['is_buybox'] else "　　　　"
        print(f"{idx}. {buybox} | {cand['total_jpy']:,}円 ({cand['original_price']:.2f} {cand['currency']}) | {cand['fulfillment']} | 評価:{cand['feedback_count']}")

# ==========================================
# 実行テスト用ブロック
# ==========================================
if __name__ == "__main__":
    rates = get_exchange_rates()
    test_asin = "B001P4ZR6C"  # 実際のASINに変更してください

    print(f"--- {test_asin} の分析を開始します ---")
    result = analyze_trade_opportunity(test_asin, rates)

    print_price_result(result)

    # print("\n【分析結果 (JSON形式)】")
    # print(json.dumps(result, ensure_ascii=False, indent=4))

