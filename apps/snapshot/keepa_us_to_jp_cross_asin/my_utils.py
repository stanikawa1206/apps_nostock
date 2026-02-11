# -*- coding: utf-8 -*-
import os
import json
import time
import requests
import pyodbc
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 設定値
# ============================================================
KEEPA_API_KEY = os.getenv("KEEPA_API_KEY")
KEEPA_BASE = "https://api.keepa.com"

SPAPI_ENDPOINT_JP = "https://sellingpartnerapi-fe.amazon.com"
SPAPI_ENDPOINT_US = "https://sellingpartnerapi-na.amazon.com"
MARKETPLACE_ID_JP = "A1VC38T7YXB528"
MARKETPLACE_ID_US = "ATVPDKIKX0DER"

# ============================================================
# 1. DB接続
# ============================================================
def get_sql_server_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASS')};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# ============================================================
# 2. Keepa API (トークン管理付き)
# ============================================================
def get_keepa_token_status() -> dict:
    url = f"{KEEPA_BASE}/token"
    params = {"key": KEEPA_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return {
            "tokens_left": data.get("tokensLeft", 0), 
            "refill_in_ms": data.get("refillIn", 0)
        }
    except Exception:
        return {"tokens_left": 0, "refill_in_ms": 5000}

def ensure_keepa_tokens(required_tokens=150):
    status = get_keepa_token_status()
    while status["tokens_left"] < required_tokens:
        tokens_left = status["tokens_left"]
        refill_in_ms = status["refill_in_ms"]
        wait_sec = (refill_in_ms / 1000.0) + 2.0
        print(f"   [Keepa Token 待機] 残り: {tokens_left}/{required_tokens}。{wait_sec:.1f}秒待機します...")
        time.sleep(wait_sec)
        status = get_keepa_token_status()

def keepa_request(endpoint: str, params: dict = None, data: dict = None) -> dict:
    if not KEEPA_API_KEY:
        raise ValueError("KEEPA_API_KEY が .env に設定されていません。")

    ensure_keepa_tokens(required_tokens=150)
    time.sleep(0.5)

    url = f"{KEEPA_BASE}/{endpoint}"
    p = {"key": KEEPA_API_KEY}
    if params: p.update(params)
    
    try:
        if data:
            r = requests.post(url, params=p, data=json.dumps(data), timeout=180)
        else:
            r = requests.get(url, params=p, timeout=180)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[Keepa API Error] {e}")
        return {}

# ============================================================
# 3. SP-API 連携
# ============================================================
def get_spapi_access_token(region: str = "JP") -> str:
    client_id = os.getenv("LWA_CLIENT_ID")
    client_secret = os.getenv("LWA_CLIENT_SECRET")
    refresh_token = os.getenv("REFRESH_TOKEN_US") if region == "US" else os.getenv("REFRESH_TOKEN")

    if not (refresh_token and client_id and client_secret):
        target = "REFRESH_TOKEN_US" if region == "US" else "REFRESH_TOKEN"
        raise RuntimeError(f"SP-API認証情報不足: {target}")

    url = "https://api.amazon.com/auth/o2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    resp = requests.post(url, data=data, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_spapi_items_batch(asin_list: List[str], region: str, access_token: str) -> List[Dict[str, Any]]:
    if not asin_list: return []

    if region == "US":
        base_url = SPAPI_ENDPOINT_US
        mp_id = MARKETPLACE_ID_US
    else:
        base_url = SPAPI_ENDPOINT_JP
        mp_id = MARKETPLACE_ID_JP

    url = f"{base_url}/catalog/2022-04-01/items"
    
    # 【重要】offers を削除しました (これがエラーの原因でした)
    included_data = "summaries,attributes"

    params = {
        "identifiers": ",".join(asin_list),
        "identifiersType": "ASIN",
        "marketplaceIds": mp_id,
        "includedData": included_data
    }
    headers = {
        "X-Amz-Access-Token": access_token, 
        "Content-Type": "application/json"
    }

    time.sleep(1.2)
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        
        if r.status_code == 404:
            return []
            
        r.raise_for_status()
        data = r.json()
        return data.get("items", [])
        
    except Exception as e:
        # デバッグ用に詳細エラーを表示
        if 'r' in locals() and r is not None:
             print(f"[SP-API Error Detail] {r.text}")
        print(f"[SP-API Error ({region})] {e}")
        return []
    
# ============================================================
# 4. SP-API 価格取得 (Pricing API) - my_utils.pyの末尾に追加
# ============================================================
def get_spapi_prices_batch(asin_list: List[str], region: str, access_token: str) -> Dict[str, float]:
    """
    getPricing API を使用して、ASINリストの価格情報を取得する。
    戻り値: { 'ASIN': 価格(float), ... } の辞書
    """
    if not asin_list: return {}

    if region == "US":
        base_url = SPAPI_ENDPOINT_US
        mp_id = MARKETPLACE_ID_US
    else:
        base_url = SPAPI_ENDPOINT_JP
        mp_id = MARKETPLACE_ID_JP

    # getPricing API endpoint
    url = f"{base_url}/products/pricing/v0/price"
    
    params = {
        "Asins": ",".join(asin_list),
        "ItemType": "Asin",
        "MarketplaceId": mp_id
    }
    headers = {
        "X-Amz-Access-Token": access_token, 
        "Content-Type": "application/json"
    }

    # レートリミット対策
    time.sleep(1.0)
    
    price_map = {}
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        
        # Pricing APIは404を返さず、空の結果などを返すことが多いが念のため
        if r.status_code == 404:
            return {}
            
        r.raise_for_status()
        data = r.json()
        
        # ペイロードの解析
        payload = data.get("payload", [])
        for item in payload:
            asin = item.get("ASIN")
            product = item.get("Product", {})
            offers = product.get("Offers", [])
            
            # 最安値を探す (BuyingPrice > ListingPrice)
            price = None
            if offers:
                # Offersがある場合（新品最安値など）
                price_info = offers[0].get("BuyingPrice", {})
                price = price_info.get("ListingPrice", {}).get("Amount")
            
            # Offersがない場合、Attributeの参考価格等はここには含まれないことが多い
            
            if asin and price:
                try:
                    price_map[asin] = float(price)
                except:
                    pass
                    
        return price_map
        
    except Exception as e:
        print(f"[Pricing API Error ({region})] {e}")
        return {}