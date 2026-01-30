# -*- coding: utf-8 -*-
"""
listings/ebay_api.py — 概要と関数一覧（公開関数のみ）
============================================================

■ 概要（端的に）
- eBay への「出品・更新・公開」「価格改定」「出品終了（削除）」をまとめた実用関数群。
- アクセストークンは DB の refresh_token から自動取得・更新。
- 価格改定は Trading で失敗したら Inventory に切替（自動フォールバック）。

■ 例外（必要最小限）
- ApiHandledError: eBay API 側の明示エラー。
- ListingLimitError: 出品上限などのリミット系エラー。

■ 関数一覧（補助関数は除外）
- get_access_token_new(account:str) -> Optional[str]
  指定アカウントのアクセストークンを取得（内部キャッシュあり）。

- register_inventory_item(row:dict, token:str) -> dict
  SKU の商品情報を作成/更新（Inventory Item を PUT）。

- create_offer(row:dict, token:str, acct_policies:dict) -> str
  Offer を作成し、offerId を返す（既存時は既存 offerId を返却）。

- update_offer(offer_id:str, row:dict, token:str, acct_policies:dict) -> dict
  既存 Offer の価格やポリシーを更新。

- publish_offer(offer_id:str, token:str) -> dict
  Offer を公開して出品（listingId を含むレスポンス）。

- post_one_item(payload:dict, account_name:str, acct_policies:dict) -> str
  「登録→Offer作成→更新→公開」を一括実行して itemId を返す。

- revise_price(*, item_id:str, new_price_usd:Union[str,float,int], account_name:str) -> dict
  Trading API で価格を変更。成功/失敗を辞書で返す。

- delete_item_from_ebay(account:str, item_id:str) -> dict
  単品の出品を終了（EndItem）。既に終了済みも判定。

- delete_items_from_ebay_batch(account:str, item_ids:list[str]) -> dict
  最大10件までまとめて終了（EndItems）。結果を配列で返す。

- update_ebay_price(account:str, ebay_item_id:str, new_price_usd, *, sku:Optional[str]=None, debug:bool=False) -> dict
  価格改定の統合関数。まず Trading、ダメなら（在庫管理品は）Inventory 経由で更新→公開。

============================================================
"""

# --- Standard library ---
import os
import re
import time
import random
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse, quote
import xml.etree.ElementTree as ET

from dotenv import load_dotenv
load_dotenv()


# --- Third-party ---
import requests
from selenium.webdriver.common.by import By

# ====== 設定（あなたの環境に合わせて）======
def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v

# OAuth
CLIENT_ID = _require_env("EBAY_CLIENT_ID")
CLIENT_SECRET = _require_env("EBAY_CLIENT_SECRET")
TOKEN_URL = _require_env("EBAY_TOKEN_URL")

# Trading API
TRADING_ENDPOINT = _require_env("EBAY_TRADING_ENDPOINT")
TRADING_COMPAT_LEVEL = os.getenv("EBAY_TRADING_COMPAT_LEVEL", "1149")

# DB（mst.ebay_accounts.refresh_token を読む）
# ここは SQLAlchemy でも pyodbc でもOK。簡便に SQLAlchemy を利用。
from sqlalchemy import create_engine, text
import urllib.parse

DB_DRIVER = _require_env("DB_DRIVER")
DB_SERVER = _require_env("DB_SERVER")
DB_NAME = _require_env("DB_NAME")
DB_USER = _require_env("DB_USER")
DB_PASS = _require_env("DB_PASS")

_ODBC = urllib.parse.quote_plus(
    f"DRIVER={DB_DRIVER};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASS};"
)
ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={_ODBC}", pool_pre_ping=True)

# ====== 例外 ======
class ApiHandledError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message

class ListingLimitError(Exception):
    pass

# ====== OAuth（refresh→access）======
_TOKEN_CACHE: dict[str, dict] = {}

def _get_refresh_token(account: str) -> Optional[str]:
    with ENGINE.begin() as conn:
        row = conn.execute(
            text("SELECT refresh_token FROM mst.ebay_accounts WHERE LTRIM(RTRIM(account)) = LTRIM(RTRIM(:acc))"),
            {"acc": account}
        ).fetchone()
    return row[0] if row else None

def _fetch_access_token_from_refresh(refresh_token: str) -> Tuple[Optional[str], Optional[int]]:
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    resp = requests.post(
        TOKEN_URL,
        data=data,
        auth=(CLIENT_ID, CLIENT_SECRET),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=20,
    )
    if resp.status_code == 200:
        j = resp.json()
        return j.get("access_token"), int(j.get("expires_in", 0)) or None
    else:
        print("❌ TOKEN REFRESH ERROR")
        print("status:", resp.status_code)
        print(resp.text)
        return None, None

def get_access_token_new(account: str) -> Optional[str]:
    now = time.time()
    c = _TOKEN_CACHE.get(account)
    if c and now < c["exp"] - 60:
        return c["token"]

    rt = _get_refresh_token(account)
    if not rt:
        print(f"❌ refresh_token が DB にありません: {account}")
        return None
    token, expires_in = _fetch_access_token_from_refresh(rt)
    if not token:
        print("❌ access_token 取得失敗")
        return None
    _TOKEN_CACHE[account] = {"token": token, "exp": now + (expires_in or 3600)}
    return token

# ====== 共通小物 ======
def _to_price_str(v) -> str:
    return f"{Decimal(str(v)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"

def _ebay_json_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Content-Language": "en-US",
    }

def _safe_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception:
        return {"message": getattr(resp, "text", "") or ""}

def _extract_error(err: Dict[str, Any]) -> tuple[int, str]:
    try:
        first = (err.get("errors") or [{}])[0]
    except Exception:
        first = {}
    code = first.get("errorId") or first.get("code") or -1
    msg  = first.get("message") or first.get("longMessage") or err.get("message") or "Unknown error"
    try:
        code = int(code)
    except Exception:
        code = -1
    return code, str(msg)

def _is_listing_limit(code: int, msg: str) -> bool:
    m = (msg or "").lower()
    return code in (21916611,) or any(kw in m for kw in ["limit", "selling limit", "sell limit", "exceeded"])

# ====== Inventory API（出品・更新・公開）======
def register_inventory_item(row: Dict[str, Any], token: str) -> Dict[str, Any]:
    """
    Inventory Item を作成/更新（PUT）。
    - Type は未指定なら送らない（誤って "Wallet" などが入らないように）
    - Platform / Game Name が payload にあれば Item Specifics に反映
    - *ConditionID の綴り修正
    """
    sku = str(row["CustomLabel"]).strip()
    url = f"https://api.ebay.com/sell/inventory/v1/inventory_item/{sku}"

    # Condition（*ConditionID に合わせて綴り修正）
    cond_map = {"1000": "NEW", "3000": "USED_EXCELLENT", "4000": "USED_GOOD", "5000": "USED_ACCEPTABLE"}
    cond = cond_map.get(str(row.get("*ConditionID", "3000")).strip(), "USED_EXCELLENT")

    # Title/Description 長さ調整
    title = (row.get("*Title", "") or "").strip()
    if len(title) > 80:
        cut = title[:77]
        if " " in cut and not cut.endswith(" "):
            cut = cut[: cut.rfind(" ")]
        title = cut.rstrip() + "..."

    desc = (row.get("*Description", "") or "").strip()
    if len(desc) > 4000:
        desc = desc[:3997] + "..."

    # 画像
    def _imgs(s: str) -> list[str]:
        return [u.strip() for u in str(s or "").split("|") if u and u.strip()]

    # Item Specifics
    brand = (row.get("C:Brand") or "Unbranded").strip() or "Unbranded"
    department = (row.get("department") or "").strip()
    color = (row.get("C:Color") or row.get("Color") or "").strip()
    type_name = (row.get("C:Type") or row.get("Type") or "").strip()
    platform = (row.get("platform") or row.get("C:Platform") or "").strip()
    game_name = (row.get("game_name") or row.get("C:Game Name") or "").strip()

    aspects: Dict[str, list[str]] = {"Brand": [brand]}
    if department:
        aspects["Department"] = [department]
    if type_name:
        aspects["Type"] = [type_name]          # 既定値は入れない
    if color:
        aspects["Color"] = [color]
    if platform:
        aspects["Platform"] = [platform]
    if game_name:
        aspects["Game Name"] = [game_name]

    payload = {
        "sku": sku,
        "product": {
            "title": title,
            "description": desc,
            "aspects": aspects,
            "brand": brand,
            "mpn": sku,
            "imageUrls": _imgs(row.get("PicURL")),
        },
        "availability": {"shipToLocationAvailability": {"quantity": int(row.get("*Quantity", 1))}},
        "condition": cond,
    }

    r = requests.put(url, headers=_ebay_json_headers(token), json=payload, timeout=45)
    if r.status_code >= 400:
        err = _safe_json(r)
        code, msg = _extract_error(err)
        if _is_listing_limit(code, msg):
            raise ListingLimitError(f"Listing limit (register): {code} {msg}")
        raise ApiHandledError(code, msg)
    return _safe_json(r)


def create_offer(row: Dict[str, Any], token: str, acct_policies: Dict[str, Any]) -> str:
    url = "https://api.ebay.com/sell/inventory/v1/offer"
    start_price = str(row.get("*StartPrice","")).strip()
    category_id = str(row.get("category_id","")).strip()
    if not start_price or not category_id:
        raise ValueError("category_id と *StartPrice は必須")

    payload = {
        "sku": row["CustomLabel"],
        "marketplaceId": "EBAY_US",
        "format": "FIXED_PRICE",
        "merchantLocationKey": (acct_policies.get("merchant_location_key") or "Default").strip() or "Default",
        "availableQuantity": int(row.get("*Quantity", 1)),
        "categoryId": category_id,
        "pricingSummary": {"price": {"value": start_price, "currency": "USD"}},
        "listingPolicies": {
            "fulfillmentPolicyId": str(acct_policies.get("fulfillment_policy_id")),
            "paymentPolicyId":     str(acct_policies.get("payment_policy_id")),
            "returnPolicyId":      str(acct_policies.get("return_policy_id")),
        },
    }
    r = requests.post(url, headers=_ebay_json_headers(token), json=payload, timeout=45)

    if r.status_code == 201:
        return _safe_json(r).get("offerId") or ""

    if r.status_code == 400:
        err = _safe_json(r)
        try:
            first = (err.get("errors") or [{}])[0]
            if "Offer entity already exists" in (first.get("message") or ""):
                for p in first.get("parameters", []):
                    if p.get("name") == "offerId":
                        return p.get("value")
        except Exception:
            pass
        code, msg = _extract_error(err)
        if _is_listing_limit(code, msg):
            raise ListingLimitError(f"Listing limit (create): {code} {msg}")
        raise ApiHandledError(code, msg)

    if r.status_code >= 400:
        err = _safe_json(r)
        code, msg = _extract_error(err)
        if _is_listing_limit(code, msg):
            raise ListingLimitError(f"Listing limit (create): {code} {msg}")
        raise ApiHandledError(code, msg)

    return _safe_json(r).get("offerId", "")

def update_offer(offer_id: str, row: Dict[str, Any], token: str, acct_policies: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.ebay.com/sell/inventory/v1/offer/{offer_id}"
    payload = {
        "merchantLocationKey": (acct_policies.get("merchant_location_key") or "Default").strip() or "Default",
        "categoryId": str(row.get("category_id","")),
        "listingPolicies": {
            "fulfillmentPolicyId": str(acct_policies.get("fulfillment_policy_id")),
            "paymentPolicyId":     str(acct_policies.get("payment_policy_id")),
            "returnPolicyId":      str(acct_policies.get("return_policy_id")),
        },
        "pricingSummary": {"price": {"value": str(row.get("*StartPrice","")), "currency": "USD"}},
    }
    r = requests.put(url, headers=_ebay_json_headers(token), json=payload, timeout=45)
    if r.status_code >= 400:
        err = _safe_json(r)
        code, msg = _extract_error(err)
        if _is_listing_limit(code, msg):
            raise ListingLimitError(f"Listing limit (update): {code} {msg}")
        raise ApiHandledError(code, msg)
    return _safe_json(r)

def publish_offer(offer_id: str, token: str) -> Dict[str, Any]:
    url = f"https://api.ebay.com/sell/inventory/v1/offer/{offer_id}/publish/"
    r = requests.post(url, headers=_ebay_json_headers(token), json={}, timeout=45)
    if r.status_code == 200:
        return _safe_json(r)
    err = _safe_json(r)
    code, msg = _extract_error(err)
    if _is_listing_limit(code, msg):
        raise ListingLimitError(f"Listing limit (publish): {code} {msg}")
    raise ApiHandledError(code, msg)

def post_one_item(payload: Dict[str, Any], account_name: str, acct_policies: Dict[str, Any]) -> str:
    token = get_access_token_new(account_name)
    if not token:
        raise RuntimeError("access_token 取得失敗")
    register_inventory_item(payload, token)
    offer_id = create_offer(payload, token, acct_policies)
    update_offer(offer_id, payload, token, acct_policies)
    res = publish_offer(offer_id, token)
    return res.get("itemId") or res.get("listingId") or ""

# ====== Trading API（価格改定 / 削除）======
def revise_price(*, item_id: str, new_price_usd: str | float | int,
                 account_name: str,
                 shipping_profile_id: str | None = None,
                 payment_profile_id: str | None = None,
                 return_profile_id: str | None = None) -> Dict[str, Any]:
    token = get_access_token_new(account_name)
    if not token:
        return {'success': False, 'error': 'missing_oauth_token'}

    price = _to_price_str(new_price_usd)

    # --- 可変: SellerProfiles ブロック ---
    seller_profiles = ""
    if shipping_profile_id or payment_profile_id or return_profile_id:
        sp = []
        if shipping_profile_id:
            sp.append(f"<SellerShippingProfile><ShippingProfileID>{shipping_profile_id}</ShippingProfileID></SellerShippingProfile>")
        if return_profile_id:
            sp.append(f"<SellerReturnProfile><ReturnProfileID>{return_profile_id}</ReturnProfileID></SellerReturnProfile>")
        if payment_profile_id:
            sp.append(f"<SellerPaymentProfile><PaymentProfileID>{payment_profile_id}</PaymentProfileID></SellerPaymentProfile>")
        seller_profiles = f"<SellerProfiles>{''.join(sp)}</SellerProfiles>"

    headers = {
        "X-EBAY-API-CALL-NAME": "ReviseFixedPriceItem",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": TRADING_COMPAT_LEVEL,
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<ReviseFixedPriceItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <ErrorLanguage>en_US</ErrorLanguage>
  <WarningLevel>High</WarningLevel>
  <Item>
    <ItemID>{item_id}</ItemID>
    <StartPrice currencyID="USD">{price}</StartPrice>
    {seller_profiles}
  </Item>
</ReviseFixedPriceItemRequest>""".encode("utf-8")

    try:
        r = requests.post(TRADING_ENDPOINT, headers=headers, data=body, timeout=45)
    except Exception as e:
        return {'success': False, 'error': f'http_error:{e}'}

    if r.status_code != 200:
        return {'success': False, 'http_status': r.status_code,
                'error': f'http_status:{r.status_code}', 'raw': r.text[:1500]}

    try:
        root = ET.fromstring(r.text)
        ns = {"e": "urn:ebay:apis:eBLBaseComponents"}
        ack = (root.findtext("e:Ack", default="", namespaces=ns) or "").strip()
        if ack in ("Success", "Warning"):
            return {'success': True, 'Ack': ack, 'http_status': r.status_code}

        msgs, codes = [], []
        for err in root.findall("e:Errors", namespaces=ns):
            code = (err.findtext("e:ErrorCode", default="", namespaces=ns) or "").strip()
            long = (err.findtext("e:LongMessage", default="", namespaces=ns) or "").strip()
            short= (err.findtext("e:ShortMessage", default="", namespaces=ns) or "").strip()
            if code:
                codes.append(code)
            s = f"{code} {long or short}".strip()
            if s:
                msgs.append(s)

        return {
            'success': False,
            'Ack': ack or 'Failure',
            'http_status': r.status_code,
            'error': "; ".join(msgs) or "unknown_failure",
            'codes': codes,
            'raw': r.text[:1500]
        }
    except Exception as e:
        return {'success': False, 'error': f'parse_error:{e}',
                'http_status': r.status_code, 'raw': r.text[:1500]}



def delete_item_from_ebay(account: str, item_id: str) -> Dict[str, Any]:
    token = get_access_token_new(account)
    if not token:
        return {"success": False, "error_code": "no_token"}
    headers = {
        "X-EBAY-API-CALL-NAME": "EndItem",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "1175",
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<EndItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <ItemID>{item_id}</ItemID>
  <EndingReason>NotAvailable</EndingReason>
</EndItemRequest>"""
    r = requests.post(TRADING_ENDPOINT, headers=headers, data=body, timeout=30)
    text = r.text
    if r.status_code == 200 and "<Ack>Success</Ack>" in text:
        return {"success": True, "note": "deleted"}
    if "<ErrorCode>1047</ErrorCode>" in text:
        return {"success": True, "note": "already_deleted"}
    m = re.search(r"<ErrorCode>(\d+)</ErrorCode>", text)
    return {"success": False, "error_code": int(m.group(1)) if m else "api_error", "raw_response": text}

def delete_items_from_ebay_batch(account: str, item_ids: list[str]) -> Dict[str, Any]:
    if not item_ids:
        return {"success": True, "results": []}
    if len(item_ids) > 10:
        raise ValueError("EndItems は最大10件まで。")
    token = get_access_token_new(account)
    if not token:
        return {"success": False, "error_code": "no_token"}

    headers = {
        "X-EBAY-API-CALL-NAME": "EndItems",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "1175",
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }
    containers = "\n".join(
        f"""
  <EndItemRequestContainer>
    <MessageID>{iid}</MessageID>
    <ItemID>{iid}</ItemID>
    <EndingReason>NotAvailable</EndingReason>
  </EndItemRequestContainer>""".strip()
        for iid in item_ids
    )
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<EndItemsRequest xmlns="urn:ebay:apis:eBLBaseComponents">
{containers}
</EndItemsRequest>"""
    r = requests.post(TRADING_ENDPOINT, headers=headers, data=body, timeout=30)
    text = r.text

    results = []
    blocks = re.findall(r"<EndItemResponseContainer>(.*?)</EndItemResponseContainer>", text, flags=re.S)
    for b in blocks:
        mid = None
        m = re.search(r"<MessageID>(.*?)</MessageID>", b) or re.search(r"<CorrelationID>(.*?)</CorrelationID>", b) or re.search(r"<ItemID>(\d+)</ItemID>", b)
        if m: mid = m.group(1)
        has_errors = re.search(r"<Errors>", b) is not None
        has_endtime = re.search(r"<EndTime>", b) is not None
        em = re.search(r"<ErrorCode>(\d+)</ErrorCode>", b)
        err = int(em.group(1)) if em else None
        ok = (not has_errors) or (err == 1047) or has_endtime
        results.append({"item_id": mid, "success": bool(ok), "error_code": err})

    if not results:
        em = re.search(r"<ErrorCode>(\d+)</ErrorCode>", text)
        return {"success": False, "error_code": int(em.group(1)) if em else "parse_error", "raw_response": text}
    if any(r.get("error_code") in (518, 429) for r in results):
        return {"success": False, "error_code": 518, "results": results, "message": "per-container rate limit", "raw_response": text}
    return {"success": True, "results": results, "raw_response": text}

# ====== 価格改定：Trading の失敗を Inventory にフォールバック ======
def _inventory_get_offer_id_by_sku(token: str, sku: str, marketplace_id: str = "EBAY_US") -> tuple[Optional[str], dict]:
    url = "https://api.ebay.com/sell/inventory/v1/offer"
    r = requests.get(url, headers=_ebay_json_headers(token), params={"sku": sku, "marketplaceId": marketplace_id}, timeout=45)
    data = _safe_json(r)
    offers = (data or {}).get("offers") or []
    if offers:
        for o in offers:
            if (o.get("marketplaceId") or "").upper() == marketplace_id:
                return o.get("offerId"), data
        return offers[0].get("offerId"), data
    return None, data

def _inventory_get_offer(token: str, offer_id: str) -> dict:
    url = f"https://api.ebay.com/sell/inventory/v1/offer/{offer_id}"
    r = requests.get(url, headers=_ebay_json_headers(token), timeout=45)
    return _safe_json(r)

def _inventory_put_offer(token: str, offer_id: str, body: dict) -> tuple[bool, dict]:
    url = f"https://api.ebay.com/sell/inventory/v1/offer/{offer_id}"
    r = requests.put(url, headers=_ebay_json_headers(token), json=body, timeout=45)
    return (r.status_code < 400), _safe_json(r)

def _inventory_publish_offer(token: str, offer_id: str) -> tuple[bool, dict]:
    url = f"https://api.ebay.com/sell/inventory/v1/offer/{offer_id}/publish/"
    r = requests.post(url, headers=_ebay_json_headers(token), json={}, timeout=45)
    return (r.status_code == 200), _safe_json(r)

def update_ebay_price(account: str, ebay_item_id: str, new_price_usd, *, sku: Optional[str] = None, debug: bool=False) -> dict:
    if not account or not ebay_item_id:
        return {'success': False, 'error': 'missing_account_or_item_id'}

    price_str = _to_price_str(new_price_usd)

    # 1) Trading で試行
    res = revise_price(item_id=str(ebay_item_id), new_price_usd=price_str, account_name=account)
    if isinstance(res, dict) and res.get('success'):
        out = {'success': True, 'item_id': str(ebay_item_id), 'price': price_str, 'note': 'via_trading'}
        if debug: out['raw'] = res
        return out

    # 21919474 / Inventory管理ならフォールバック（メッセージ判定）
    err_blob = str(res)
    needs_inventory = ("21919474" in err_blob) or ("MANAGE_BY_INVENTORY" in err_blob) or ("Inventory-based listing" in err_blob)
    if not needs_inventory:
        out = {'success': False, 'item_id': str(ebay_item_id), 'price': price_str, 'error': res.get('error') or 'unknown_failure_from_ebay'}
        if debug: out['raw'] = res
        return out

    if not sku:
        return {'success': False, 'item_id': str(ebay_item_id), 'price': price_str, 'error': 'inventory_managed_but_sku_missing'}

    # 2) Inventory で更新→公開
    token = get_access_token_new(account)
    if not token:
        return {'success': False, 'item_id': str(ebay_item_id), 'price': price_str, 'error': 'get_token_failed'}

    offer_id, list_res = _inventory_get_offer_id_by_sku(token, sku)
    if not offer_id:
        out = {'success': False, 'item_id': str(ebay_item_id), 'price': price_str, 'error': 'offer_not_found_for_sku'}
        if debug: out['raw'] = {'listOffers': list_res}
        return out

    offer_obj = _inventory_get_offer(token, offer_id) or {}
    offer_obj.setdefault('pricingSummary', {})['price'] = {"value": price_str, "currency": "USD"}
    ok, put_res = _inventory_put_offer(token, offer_id, offer_obj)
    if not ok:
        out = {'success': False, 'item_id': str(ebay_item_id), 'price': price_str, 'error': 'inventory_put_failed'}
        if debug: out['raw'] = {'offerId': offer_id, 'putOffer': put_res}
        return out

    ok2, pub_res = _inventory_publish_offer(token, offer_id)
    if not ok2:
        out = {'success': False, 'item_id': str(ebay_item_id), 'price': price_str, 'error': 'inventory_publish_failed'}
        if debug: out['raw'] = {'offerId': offer_id, 'publishOffer': pub_res}
        return out

    out = {'success': True, 'item_id': str(pub_res.get('listingId') or ebay_item_id), 'price': price_str, 'note': 'via_inventory'}
    if debug: out['raw'] = {'offerId': offer_id, 'publishOffer': pub_res}
    return out

# ==== Mercari Shops: 検索結果 → 商品URL収集（page_token対応） ====
_SHOPS_ID_RE = re.compile(r"/shops/product/(?P<id>[^/?#]+)")

def _add_or_replace_query(url: str, **params) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = str(v)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def _page_url(base_url: str, idx_zero_based: int) -> str:
    # 1ページ目はそのまま、2ページ目以降は page_token=v1:{n}
    return base_url if idx_zero_based == 0 else _add_or_replace_query(base_url, page_token=f"v1:{idx_zero_based}")

def _incremental_scroll(driver, *, max_loops=32, pause=0.6, stagnant_loops=2):
    """
    ページ内でアンカー数が増えている限りスクロールを続ける。
    shops検索は“無限スクロール”ではないが、遅延描画分を拾うために軽く回す。
    """
    last_count = -1
    stagnant = 0
    for _ in range(max_loops):
        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/shops/product/']")
        cur = len(anchors)
        if cur <= last_count:
            stagnant += 1
            if stagnant >= stagnant_loops:
                break
        else:
            stagnant = 0
            last_count = cur
        driver.execute_script("window.scrollBy(0, Math.max(900, Math.floor(window.innerHeight*0.9)));")
        time.sleep(pause + random.uniform(0.15, 0.35))

