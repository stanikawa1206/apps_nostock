import os
import json
import time
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

def filter_and_sort_candidates(offers):
    """
    危険なセラーを除外し、残った安全な候補を価格の安い順にソートして返す
    条件: カート未獲得 ＋ (評価10件未満 または 高評価80%未満) を除外
    """
    safe_offers = []

    for offer in offers:
        is_buybox = offer.get('is_buybox', False)
        
        count = int(offer.get('feedback_count', 0))
        # "不明" などの文字列が入っている場合に備えてエラーハンドリング
        try:
            percent = float(offer.get('feedback_percent', 0.0))
        except (ValueError, TypeError):
            percent = 0.0

        # 【足切り条件】カート未獲得 ＋ (評価10未満 または 高評価80%未満) は除外
        if not is_buybox and (count < 10 or percent < 80.0):
            continue 

        # フィルターを生き残った安全なオファーをリストに追加
        safe_offers.append(offer)

    # 生き残ったオファーを単純に「合計価格（円）」が安い順にソート
    safe_offers.sort(key=lambda x: x['total_jpy'])

    return safe_offers

def analyze_trade_opportunity(asin: str, exchange_rates: dict) -> dict:
    """
    ASINを受け取り、3カ国の価格を取得、比較し、仕入先候補を返す関数
    """
    targets = [
        {"code": "US", "marketplace": Marketplaces.US, "token": os.environ.get("REFRESH_TOKEN_US")},
        {"code": "CA", "marketplace": Marketplaces.CA, "token": os.environ.get("REFRESH_TOKEN_US")}, 
        {"code": "JP", "marketplace": Marketplaces.JP, "token": os.environ.get("REFRESH_TOKEN")},
    ]

    result_data = {
        "asin": asin,
        "lowest_prices": {"US": None, "CA": None, "JP": None},
        "all_offers": {"US": [], "CA": [], "JP": []},
        "judgment": "判定不可",       
        "sourcing_country": None,   
        "sourcing_candidates": []   
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
            
            lowest_price_data = None
            
            for offer in offers:
                price = offer.get('ListingPrice', {}).get('Amount', 0.0)
                currency = offer.get('ListingPrice', {}).get('CurrencyCode', '')
                shipping_fee = offer.get('Shipping', {}).get('Amount', 0.0)
                total_price = float(price) + float(shipping_fee)
                
                rate = exchange_rates.get(currency, 1.0)
                jpy_price = int(total_price * rate)
                
                if lowest_price_data is None or jpy_price < lowest_price_data["jpy"]:
                    lowest_price_data = {
                        "jpy": jpy_price,
                        "original": total_price,
                        "currency": currency
                    }
                
                # ハンドリングタイムの計算
                shipping_time = offer.get('ShippingTime', {})
                min_hours = shipping_time.get('minimumHours')
                max_hours = shipping_time.get('maximumHours')
                
                handling_time_str = "不明"
                if max_hours is not None:
                    min_days = int(min_hours) // 24 if min_hours is not None else 0
                    max_days = int(max_hours) // 24
                    
                    if min_days == 0 and max_days == 0:
                        handling_time_str = "即日(0日)"
                    elif min_days == max_days:
                        handling_time_str = f"{max_days}日"
                    else:
                        handling_time_str = f"{min_days}-{max_days}日"

                # 評価情報の取得
                feedback_info = offer.get('SellerFeedbackRating', {})
                feedback_count = feedback_info.get('FeedbackCount', 0)
                feedback_percent = feedback_info.get('SellerPositiveFeedbackRating', '不明')

                # 出品者情報をリストに保存
                offer_detail = {
                    "total_jpy": jpy_price,
                    "original_price": total_price,
                    "currency": currency,
                    "is_buybox": offer.get('IsBuyBoxWinner', False),
                    "fulfillment": "FBA" if offer.get('IsFulfilledByAmazon', False) else "FBM",
                    "feedback_count": feedback_count,
                    "feedback_percent": feedback_percent,
                    "condition": offer.get('SubCondition', '不明'),
                    "handling_time": handling_time_str
                }
                result_data["all_offers"][country_code].append(offer_detail)

            result_data["lowest_prices"][country_code] = lowest_price_data

        except SellingApiException as e:
            pass
        except Exception as e:
            pass
            
        # 💡 APIの制限(QuotaExceeded)を回避するために2秒待機
        time.sleep(2)

    # 2. 輸出・輸入の判定処理
    jp = result_data["lowest_prices"]["JP"]["jpy"] if result_data["lowest_prices"]["JP"] else None
    us = result_data["lowest_prices"]["US"]["jpy"] if result_data["lowest_prices"]["US"] else None
    ca = result_data["lowest_prices"]["CA"]["jpy"] if result_data["lowest_prices"]["CA"] else None

    if jp is not None:
        if us is not None and ca is not None:
            if (us < jp < ca) or (ca < jp < us):
                result_data["judgment"] = "双方向の可能性あり"
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

    # 3. 仕入先候補の抽出とフィルタリング
    sourcing_ctry = result_data["sourcing_country"]
    if sourcing_ctry and result_data["all_offers"][sourcing_ctry]:
        raw_offers = result_data["all_offers"][sourcing_ctry]
        # 💡 ここで新しく作成したフィルター関数を通す
        result_data["sourcing_candidates"] = filter_and_sort_candidates(raw_offers)

    return result_data

def print_price_result(result):
    print("\n【各国の最安値 (単純なカタログ最安値)】")
    for country in ["JP", "US", "CA"]:
        price_info = result['lowest_prices'][country]
        if price_info:
            print(f"{country}: {price_info['jpy']:,}円 ({price_info['original']:.2f} {price_info['currency']})")
        else:
            print(f"{country}: 出品なし / 取得不可")

    print(f"\n【判定結果】: {result['judgment']}")
    print(f"【仕入対象国】: {result['sourcing_country']}")

    print("\n【✨ 安全な仕入先候補 (価格が安い順) ✨】")
    if not result['sourcing_candidates']:
        print("⚠️ 安全基準を満たす仕入先が見つかりませんでした (全員足切りされました)")
    else:
        for idx, cand in enumerate(result['sourcing_candidates'], 1):
            buybox = "★カート" if cand['is_buybox'] else "　　　　"
            percent_str = f"{cand['feedback_percent']}%" if cand['feedback_percent'] != '不明' else '不明'
            
            # 1番目を確定仕入先としてハイライト表示
            if idx == 1:
                print(f"🥇 確定仕入先 -> {buybox} | {cand['total_jpy']:,}円 ({cand['original_price']:.2f} {cand['currency']}) | {cand['fulfillment']} | 評価:{cand['feedback_count']}件 ({percent_str}) | 出荷目安: {cand['handling_time']}")
            else:
                print(f"   {idx}.         {buybox} | {cand['total_jpy']:,}円 ({cand['original_price']:.2f} {cand['currency']}) | {cand['fulfillment']} | 評価:{cand['feedback_count']}件 ({percent_str}) | 出荷目安: {cand['handling_time']}")

# ==========================================
# 実行テスト用ブロック
# ==========================================
if __name__ == "__main__":
    rates = get_exchange_rates()
    test_asin = "B000VWDER8"  # 実際のASINに変更してください

    print(f"--- {test_asin} の分析を開始します ---")
    result = analyze_trade_opportunity(test_asin, rates)

    print_price_result(result)