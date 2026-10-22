# -*- coding: utf-8 -*-
"""
listings/ebay_api.py — 概要と関数一覧（公開関数のみ）
============================================================
NOTE:
- DBアクセスは pyodbc に統一する
- SQLAlchemy は使用しない（ODBC18 の SSL 検証差異で事故るため）

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
import logging
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse, quote
import xml.etree.ElementTree as ET

# 自前ユーティリティ（pyodbc統一）
from apps.common.utils import get_sql_server_connection, log_listings_change

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# --- Third-party ---
import requests
from selenium.webdriver.common.by import By

# ====== [一時デバッグ] DELETE DEBUG専用ログファイル ======
# apps/publish/make_listing_space.py と同じ logs/delete_debug.log に出力する。
# logging.getLogger("delete_debug") はプロセス内で共有されるため、
# どちらのモジュールが先にimportされてもハンドラは1つだけ追加される。
# 調査が終わったらこのブロックごと削除してよい。
_DELETE_DEBUG_LOG_PATH = Path(__file__).resolve().parents[2] / "logs" / "delete_debug.log"
_DELETE_DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

_delete_debug_logger = logging.getLogger("delete_debug")
if not _delete_debug_logger.handlers:
    _delete_debug_logger.setLevel(logging.DEBUG)
    _delete_debug_handler = logging.FileHandler(_DELETE_DEBUG_LOG_PATH, encoding="utf-8")
    _delete_debug_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _delete_debug_logger.addHandler(_delete_debug_handler)
    _delete_debug_logger.propagate = False


def _log_delete_debug(msg: str) -> None:
    """[一時デバッグ] コンソール(print)と logs/delete_debug.log の両方に同じ内容を出力する。"""
    print(msg)
    _delete_debug_logger.debug(msg)

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

# ====== 例外 ======
class ApiHandledError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message

class ListingLimitError(Exception):
    pass

class ListingStateUnknownError(ApiHandledError):
    """publishOffer が一時的エラー/通信エラーで、eBay側で出品されたかを確認できなかった"""
    pass

# ====== OAuth（refresh→access）======
_TOKEN_CACHE: dict[str, dict] = {}

def _get_refresh_token(account: str) -> Optional[str]:
    conn = get_sql_server_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT refresh_token FROM mst.ebay_accounts WHERE LTRIM(RTRIM(account)) = LTRIM(RTRIM(?))",
            account
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()

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

_SELLING_LIMIT_PHRASES = (
    "amount you can list",
    "request to list more",
    "you can list up to",
)

def _is_listing_limit(code: int, msg: str) -> bool:
    # eBay公式の出品上限(月間セリングリミット)エラーのみをリミット扱いにする。
    # "limit"/"exceeded" 等の単語一致は、25002(duplicate listing等)の
    # 無関係なエラーを誤ってリミット扱いにしてしまうため使用しない。
    if code == 21916611:
        return True
    if code == 25002:
        # 25002 は duplicate listing 等でも使われる汎用コードのため、
        # 月間セリングリミット特有の複数フレーズが全て含まれる場合のみ判定する。
        m = (msg or "").lower()
        return all(phrase in m for phrase in _SELLING_LIMIT_PHRASES)
    return False

# ====== Inventory API（出品・更新・公開）======
def register_inventory_item(row: Dict[str, Any], token: str) -> Dict[str, Any]:
    """
    Inventory Item を作成/更新（PUT）。
    - Type は未指定なら送らない（誤って "Wallet" などが入らないように）
    - Platform / Game Name が payload にあれば Item Specifics に反映
    - *ConditionID の綴り修正
    - トレカカテゴリ（183050/183454/261328）は ConditionID の意味が異なるため専用マッピングを使用
    """
    sku = str(row["CustomLabel"]).strip()
    url = f"https://api.ebay.com/sell/inventory/v1/inventory_item/{sku}"

    # Condition（*ConditionID に合わせて綴り修正）
    cond_map = {"1000": "NEW", "3000": "USED_EXCELLENT", "4000": "USED_GOOD", "5000": "USED_ACCEPTABLE"}
    # トレカ専用: 2750(鑑定済み)=LIKE_NEW, 4000(未鑑定)=USED_VERY_GOOD
    TCG_CATEGORY_IDS = {"183050", "183454", "261328"}
    TCG_COND_MAP = {"2750": "LIKE_NEW", "4000": "USED_VERY_GOOD"}

    category_id = str(row.get("category_id", "")).strip()
    if category_id in TCG_CATEGORY_IDS:
        cond = TCG_COND_MAP.get(str(row.get("*ConditionID", "4000")).strip(), "USED_VERY_GOOD")
    else:
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
    style_name = (row.get("C:Style") or row.get("Style") or "").strip()
    platform = (row.get("platform") or row.get("C:Platform") or "").strip()
    game_name = (row.get("game_name") or row.get("C:Game Name") or "").strip()
    game = (row.get("C:Game") or "").strip()

    ink_color = row.get("C:Ink Color")
    material = row.get("C:Material")
    model_name = row.get("C:Model")

    max_aperture = row.get("C:Maximum Aperture")
    focal_length = row.get("C:Focal Length")
    mount = row.get("C:Mount")
    focus_type = row.get("C:Focus Type")

    aspects: Dict[str, list[str]] = {"Brand": [brand]}
    if department:
        aspects["Department"] = [department]
    if type_name:
        aspects["Type"] = [type_name]          # 既定値は入れない
    if style_name:
        aspects["Style"] = [style_name]          # 既定値は入れない
    if color:
        aspects["Color"] = [color]
    if platform:
        aspects["Platform"] = [platform]
    if game_name:
        aspects["Game Name"] = [game_name]
    if game:
        aspects["Game"] = [game]
    if ink_color:
        aspects["Ink Color"] = ink_color if isinstance(ink_color, list) else [str(ink_color)]
    if material:
        aspects["Material"] = material if isinstance(material, list) else [str(material)]
    if model_name:                            # ← 追加②（aspects変換）
        aspects["Model"] = [model_name]
    if max_aperture:
        aspects["Maximum Aperture"] = [max_aperture]
    if focal_length:
        aspects["Focal Length"] = [focal_length]
    if mount:
        aspects["Mount"] = [mount]
    if focus_type:
        aspects["Focus Type"] = [focus_type]

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

    # conditionDescriptors（トレカの鑑定情報 / Card Condition 等）
    condition_descriptors = row.get("conditionDescriptors")
    if condition_descriptors:
        payload["conditionDescriptors"] = condition_descriptors

    for attempt in range(3):
        try:
            r = requests.put(
                url,
                headers=_ebay_json_headers(token),
                json=payload,
                timeout=90
            )
            break

        except requests.exceptions.ReadTimeout:
            if attempt == 2:
                raise
            print("RETRY (timeout)")
            time.sleep(2)

    if r.status_code >= 400:
        err = _safe_json(r)
        print(f"DEBUG [Register]: {err}")  # ★この行を追加
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
        print(f"DEBUG [Create Offer]: {err}")
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
        print(f"DEBUG [Create Offer]: {err}")
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
        print(f"DEBUG [Update Offer]: {err}")
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
    print(f"DEBUG [Publish Offer]: {err}")
    code, msg = _extract_error(err)
    if _is_listing_limit(code, msg):
        raise ListingLimitError(f"Listing limit (publish): {code} {msg}")
    raise ApiHandledError(code, msg)

def post_one_item(payload: Dict[str, Any], account_name: str, acct_policies: Dict[str, Any]) -> str:
    token = get_access_token_new(account_name)
    if not token:
        raise RuntimeError("access_token 取得失敗")
 
    resp = requests.get(
        "https://api.ebay.com/sell/account/v1/privilege",
        headers=_ebay_json_headers(token),
        timeout=20
    )

    register_inventory_item(payload, token)
    offer_id = create_offer(payload, token, acct_policies)
    update_offer(offer_id, payload, token, acct_policies)
    published_after = datetime.utcnow()
    try:
        res = publish_offer(offer_id, token)
    except ApiHandledError as e:
        if not _is_publish_state_unknown(e.code, e.message):
            raise
        return _confirm_published_listing(account_name, token, payload["CustomLabel"], offer_id, published_after, e)
    except requests.exceptions.RequestException as e:
        return _confirm_published_listing(account_name, token, payload["CustomLabel"], offer_id, published_after,
                                          ApiHandledError(-1, f"publish_request_error:{e}"))
    item_id = res.get("itemId") or res.get("listingId") or ""

    return item_id


# publishOffer が 25001/25002(System error)を返しても eBay側では出品されていることがあり、
# 出品失敗として DB に登録せず、在庫管理されない出品が残っていた（2026-09-26〜10-14 に4件。offerはUNPUBLISHEDのまま）。
PUBLISH_CONFIRM_WAITS = [5, 15, 30]


def _is_publish_state_unknown(code: int, message: str) -> bool:
    m = (message or "").lower()
    return code in (25001, -1) or (code == 25002 and ("system error" in m or "internal application error" in m))


def _find_listing_by_sku_started_since(token: str, sku: str, since_utc: datetime) -> Tuple[bool, Optional[str]]:
    """
    GetSellerList で since_utc 以降に開始した出品から SKU が一致する出品中のものを探す。
    （UNPUBLISHED の offer には listingId が無く、GetItem の SKU 指定も使えないため、開始時刻で絞って探す）
    戻り値: (確認できたか, 見つかったItemID)。Trading API の利用上限(518)中は (None, None)。
    """
    global _getitem_rate_limited_until
    if time.time() < _getitem_rate_limited_until:
        return None, None
    ns = {"e": "urn:ebay:apis:eBLBaseComponents"}
    headers = {
        "X-EBAY-API-CALL-NAME": "GetSellerList",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": TRADING_COMPAT_LEVEL,
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }
    time_from = (since_utc - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    time_to = (datetime.utcnow() + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    page = 1
    while True:
        body = f"""<?xml version="1.0" encoding="utf-8"?>
<GetSellerListRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <StartTimeFrom>{time_from}</StartTimeFrom>
  <StartTimeTo>{time_to}</StartTimeTo>
  <GranularityLevel>Coarse</GranularityLevel>
  <Pagination><EntriesPerPage>200</EntriesPerPage><PageNumber>{page}</PageNumber></Pagination>
  <OutputSelector>ItemArray.Item.ItemID</OutputSelector>
  <OutputSelector>ItemArray.Item.SKU</OutputSelector>
  <OutputSelector>ItemArray.Item.SellingStatus.ListingStatus</OutputSelector>
  <OutputSelector>PaginationResult</OutputSelector>
</GetSellerListRequest>""".encode("utf-8")
        try:
            r = requests.post(TRADING_ENDPOINT, headers=headers, data=body, timeout=60)
            root = ET.fromstring(r.text)
        except (requests.exceptions.RequestException, ET.ParseError) as e:
            print(f"[PUBLISH_CONFIRM] GetSellerList失敗 sku={sku} err={e}", flush=True)
            return False, None
        if r.status_code != 200 or root.findtext("e:Ack", default="", namespaces=ns) not in ("Success", "Warning"):
            print(f"[PUBLISH_CONFIRM] GetSellerList失敗 sku={sku} http={r.status_code} body={r.text[:300]}", flush=True)
            # 518 はアプリ全体の Trading API 上限のため、GetItem と同じく一定時間呼ばない
            if root.findtext(".//e:Errors/e:ErrorCode", default="", namespaces=ns) == "518":
                _getitem_rate_limited_until = time.time() + GETITEM_RATE_LIMIT_COOLDOWN_SEC
                return None, None
            return False, None
        for item in root.findall(".//e:ItemArray/e:Item", ns):
            if (item.findtext("e:SKU", default="", namespaces=ns) == sku
                    and item.findtext(".//e:ListingStatus", default="", namespaces=ns) == "Active"):
                return True, item.findtext("e:ItemID", default="", namespaces=ns)
        pages = int(root.findtext(".//e:PaginationResult/e:TotalNumberOfPages", default="1", namespaces=ns) or 1)
        if page >= pages:
            return True, None
        page += 1


def _confirm_published_listing(account_name: str, token: str, sku: str, offer_id: str,
                               published_after: datetime, publish_error: ApiHandledError) -> str:
    """
    publishOffer が一時的エラー/通信エラーの時、eBay側で実際に出品されたかを確認する。
    出品されていれば ItemID を返す（呼び出し元で通常の出品成功としてDB登録する）。
    出品されていないと確認できた場合は元のエラーを送出、確認できない場合は ListingStateUnknownError を送出する
    （確認できないまま出品失敗にすると、再出品で二重出品になる恐れがあるため）。
    """
    print(f"[PUBLISH_CONFIRM] account={account_name} sku={sku} offer_id={offer_id} "
          f"publish_error={publish_error} → eBay側の出品状態を確認", flush=True)
    confirmed_absent = False
    for wait in PUBLISH_CONFIRM_WAITS:
        time.sleep(wait)
        try:
            offer = _inventory_get_offer(token, offer_id)
        except requests.exceptions.RequestException as e:
            print(f"[PUBLISH_CONFIRM] offer取得失敗 sku={sku} err={e}", flush=True)
            offer = None
        listing_id = str(((offer or {}).get("listing") or {}).get("listingId") or "")
        if (offer or {}).get("status") == "PUBLISHED" and listing_id:
            print(f"[PUBLISH_CONFIRM][FOUND] account={account_name} sku={sku} listing_id={listing_id} (offer)", flush=True)
            return listing_id
        ok, item_id = _find_listing_by_sku_started_since(token, sku, published_after)
        if ok is None:
            print(f"[PUBLISH_CONFIRM] Trading API 利用上限中のため確認を打ち切り sku={sku}", flush=True)
            confirmed_absent = False
            break
        if item_id:
            print(f"[PUBLISH_CONFIRM][FOUND] account={account_name} sku={sku} listing_id={item_id} "
                  f"(GetSellerList, offer_status={(offer or {}).get('status')})", flush=True)
            return item_id
        # 「出品なし」は、offer が UNPUBLISHED と取得でき、かつ GetSellerList を最後まで取得できた時だけ
        confirmed_absent = ok and (offer or {}).get("status") == "UNPUBLISHED"
    if confirmed_absent:
        print(f"[PUBLISH_CONFIRM][ABSENT] account={account_name} sku={sku} → 出品されていないことを確認", flush=True)
        raise publish_error
    print(f"[PUBLISH_CONFIRM][UNKNOWN] account={account_name} sku={sku} → 出品状態を確認できず", flush=True)
    raise ListingStateUnknownError(publish_error.code, f"出品状態を確認できず(publish: {publish_error.message})")

# ====== Trading API（バイヤーへのメッセージ送信）======
# send_buyer_thankyou_message() 用フォールバックlogger。
# 呼び出し側(fetch_orders_ebay.py等)が自前のloggerを渡さなかった場合のみ使う。
_THANKYOU_FALLBACK_LOG = logging.getLogger("apps.adapters.ebay_api")


def send_buyer_thankyou_message(
    account: str,
    order_id: str,
    buyer_username: str,
    ebay_id: str,
    logger: Optional[logging.Logger] = None,
) -> dict:
    """
    Trading API AddMemberMessageAAQToPartner でバイヤーへサンキューメッセージを送信。
    1注文につき1回だけ呼ぶこと（呼び出し側で制御）。

    logger: 呼び出し側のロガーを渡すと、そのログへ「API送信開始」「APIレスポンス(Ack)」
    「成功/失敗」「エラーコード/メッセージ」「処理時間」を記録する。省略時はこの
    モジュール用のフォールバックloggerを使う（fetch_orders_ebay.py以外から呼ばれた場合など）。
    """
    log = logger or _THANKYOU_FALLBACK_LOG
    start = time.monotonic()

    def _elapsed_ms() -> float:
        return round((time.monotonic() - start) * 1000, 1)

    log.info(
        f"[ThankYou] API送信開始: order_id={order_id} ebay_id={ebay_id} buyer={buyer_username}"
    )

    if not ebay_id or not buyer_username:
        log.warning(
            f"[ThankYou] SKIP（ebay_idまたはbuyerが未設定）: order_id={order_id} "
            f"ebay_id={ebay_id} buyer={buyer_username} elapsed_ms={_elapsed_ms()}"
        )
        return {"success": False, "error": "missing_ebay_id_or_buyer"}

    token = get_access_token_new(account)
    if not token:
        log.error(
            f"[ThankYou] 失敗（トークン取得エラー）: order_id={order_id} ebay_id={ebay_id} "
            f"buyer={buyer_username} elapsed_ms={_elapsed_ms()}"
        )
        return {"success": False, "error": "no_token"}

    subject = "Thank you for your purchase!"
    body = (
        "Thank you very much for your order! We truly appreciate your business. "
        "We will do our best to ship your item as quickly as possible and aim to "
        "dispatch it within 5 business days. If you have any questions, please "
        "don't hesitate to contact us."
    )

    xml_data = f"""<?xml version="1.0" encoding="utf-8"?>
<AddMemberMessageAAQToPartnerRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <ErrorLanguage>en_US</ErrorLanguage>
  <WarningLevel>High</WarningLevel>
  <ItemID>{ebay_id}</ItemID>
  <MemberMessage>
    <Body>{body}</Body>
    <MessageType>AskSellerQuestion</MessageType>
    <QuestionType>CustomizedSubject</QuestionType>
    <Subject>{subject}</Subject>
    <RecipientID>{buyer_username}</RecipientID>
  </MemberMessage>
</AddMemberMessageAAQToPartnerRequest>""".encode("utf-8")

    headers = {
        "X-EBAY-API-CALL-NAME": "AddMemberMessageAAQToPartner",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": TRADING_COMPAT_LEVEL,
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }

    try:
        r = requests.post(TRADING_ENDPOINT, headers=headers, data=xml_data, timeout=30)

        if r.status_code != 200:
            log.error(
                f"[ThankYou] 失敗（HTTPエラー）: order_id={order_id} ebay_id={ebay_id} "
                f"buyer={buyer_username} http_status={r.status_code} "
                f"response={r.text[:300]!r} elapsed_ms={_elapsed_ms()}"
            )
            return {"success": False, "http_status": r.status_code}

        root = ET.fromstring(r.text)
        ns = {"e": "urn:ebay:apis:eBLBaseComponents"}
        ack = (root.findtext("e:Ack", default="", namespaces=ns) or "").strip()

        if ack in ("Success", "Warning"):
            log.info(
                f"[ThankYou] 成功: order_id={order_id} ebay_id={ebay_id} buyer={buyer_username} "
                f"ack={ack} elapsed_ms={_elapsed_ms()}"
            )
            return {"success": True}

        errs = []
        for err in root.findall("e:Errors", namespaces=ns):
            code = (err.findtext("e:ErrorCode", default="", namespaces=ns) or "").strip()
            msg  = (err.findtext("e:LongMessage", default="", namespaces=ns) or
                    err.findtext("e:ShortMessage", default="", namespaces=ns) or "").strip()
            errs.append((code, msg))

        error_code = ",".join(code for code, _ in errs if code) or ""
        err_str = "; ".join(f"{code}: {msg}" for code, msg in errs) or f"Ack={ack}"
        log.error(
            f"[ThankYou] 失敗（APIエラー応答）: order_id={order_id} ebay_id={ebay_id} "
            f"buyer={buyer_username} ack={ack} error_code={error_code} "
            f"error_message={err_str} elapsed_ms={_elapsed_ms()}"
        )
        return {"success": False, "error": err_str}

    except Exception as e:
        log.error(
            f"[ThankYou] 失敗（例外発生）: order_id={order_id} ebay_id={ebay_id} "
            f"buyer={buyer_username} error={e} elapsed_ms={_elapsed_ms()}",
            exc_info=True,
        )
        return {"success": False, "error": str(e)}


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

def delete_item_from_ebay(account: str, sku: str) -> Dict[str, Any]:
    print(f"delete_item_from_ebay_REST: account={account}, sku={sku}")

    token = get_access_token_new(account)
    if not token:
        return {"success": False, "error_code": "no_token"}

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # --------------------------------
    # 1 OfferID取得
    # --------------------------------
    url = f"https://api.ebay.com/sell/inventory/v1/offer?sku={sku}"

    r = requests.get(url, headers=headers, timeout=30)

    if r.status_code != 200:
        return {"success": False, "error_code": r.status_code, "raw_response": r.text}

    data = r.json()

    offers = data.get("offers", [])
    if not offers:
        return {"success": False, "error_code": "offer_not_found", "raw_response": data}

    offer_id = offers[0]["offerId"]

    # --------------------------------
    # 2 Offer削除
    # --------------------------------
    delete_url = f"https://api.ebay.com/sell/inventory/v1/offer/{offer_id}"

    r = requests.delete(delete_url, headers=headers, timeout=30)

    if r.status_code in (200, 204):
        return {"success": True, "offer_id": offer_id}

    return {
        "success": False,
        "error_code": r.status_code,
        "offer_id": offer_id,
        "raw_response": r.text
    }


# ====== Trading API（出品状態の確認・読み取り専用）======
# REST版の削除(delete_item_from_ebay)は、offerが既に無い(=eBay側で終了済みの可能性がある)場合も
# 失敗を返すため、DB(trx.listings)が出品中のまま残り続ける不整合が発生していた（2026-10-19調査）。
# 「eBay側の終了を確認できた時だけDBを削除済みにする」判定に使う。
_ENDED_LISTING_STATUSES = {"Completed", "Ended"}

# Trading API はアプリ全体で1日5,000回の上限があり、超えると 518 が返る（リセットは太平洋時間0時）。
# 価格更新後の GetItem 確認で上限に達し、518 のまま GetItem を呼び続けていた（2026-10-22 09:54〜）。
# 518 を受けたら一定時間このプロセスでは GetItem を呼ばず、取得失敗として返す。
GETITEM_RATE_LIMIT_COOLDOWN_SEC = 1800
_getitem_rate_limited_until = 0.0


def get_listing_status(account: str, item_id: str) -> Dict[str, Any]:
    """
    GetItem で出品の状態を取得する。
    ended は ListingStatus が Completed/Ended の時だけ True。
    取得に失敗した場合は ok=False / ended=False を返す（終了扱いにはしない）。
    518(利用上限)の場合は rate_limited=True を付ける。
    """
    global _getitem_rate_limited_until
    if time.time() < _getitem_rate_limited_until:
        return {"ok": False, "ended": False, "rate_limited": True, "error": "getitem_rate_limited(skip)"}

    token = get_access_token_new(account)
    if not token:
        return {"ok": False, "ended": False, "error": "no_token"}

    headers = {
        "X-EBAY-API-CALL-NAME": "GetItem",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": TRADING_COMPAT_LEVEL,
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <ItemID>{item_id}</ItemID>
  <OutputSelector>Item.ListingDetails</OutputSelector>
  <OutputSelector>Item.SellingStatus</OutputSelector>
</GetItemRequest>""".encode("utf-8")

    try:
        r = requests.post(TRADING_ENDPOINT, headers=headers, data=body, timeout=30)
    except requests.exceptions.RequestException as e:
        return {"ok": False, "ended": False, "error": f"http_error:{e}"}

    if r.status_code != 200:
        return {"ok": False, "ended": False, "error": f"http_status:{r.status_code}"}

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError as e:
        return {"ok": False, "ended": False, "error": f"parse_error:{e}"}

    ns = {"e": "urn:ebay:apis:eBLBaseComponents"}
    ack = (root.findtext("e:Ack", default="", namespaces=ns) or "").strip()
    if ack not in ("Success", "Warning"):
        code = root.findtext(".//e:Errors/e:ErrorCode", default="", namespaces=ns)
        msg = root.findtext(".//e:Errors/e:LongMessage", default="", namespaces=ns)
        if code == "518":
            _getitem_rate_limited_until = time.time() + GETITEM_RATE_LIMIT_COOLDOWN_SEC
            print(f"[GETITEM_RATE_LIMITED] {GETITEM_RATE_LIMIT_COOLDOWN_SEC}秒間 GetItem を停止", flush=True)
            return {"ok": False, "ended": False, "rate_limited": True, "error": f"ack={ack} {code} {msg}".strip()}
        return {"ok": False, "ended": False, "error": f"ack={ack} {code} {msg}".strip()}

    listing_status = root.findtext(".//e:SellingStatus/e:ListingStatus", default="", namespaces=ns)
    return {
        "ok": True,
        "ended": listing_status in _ENDED_LISTING_STATUSES,
        "listing_status": listing_status,
        "end_time": root.findtext(".//e:ListingDetails/e:EndTime", default="", namespaces=ns),
        "ending_reason": root.findtext(".//e:ListingDetails/e:EndingReason", default="", namespaces=ns),
        "quantity_sold": root.findtext(".//e:SellingStatus/e:QuantitySold", default="", namespaces=ns),
        "current_price": root.findtext(".//e:SellingStatus/e:CurrentPrice", default="", namespaces=ns),
    }


def mark_listing_deleted_if_ended(
    account: str,
    listing_id: str,
    sku: str,
    delete_reason: str,
    error_message: Optional[str] = None,
) -> bool:
    """
    GetItem で eBay側の終了を確認できた場合だけ、trx.listings の該当行を削除済みにする。
    確認できない場合（出品中・取得失敗）はログを残し、DBは変更しない。
    削除済みにした場合 True を返す。
    """
    status = get_listing_status(account, listing_id)

    if not status.get("ended"):
        print(
            f"[ENDED_CHECK_NG] account={account} listing_id={listing_id} sku={sku} "
            f"reason={delete_reason} status={status!r} → DB変更なし",
            flush=True,
        )
        return False

    # deleted_at は eBay側の終了時刻(EndTime, UTC)を優先し、取得できない場合だけ検知時刻にする。
    # UTC→DBサーバーのローカル時刻への変換はSQL側で行う（VPSのPythonはタイムゾーンが異なるため）。
    end_time_utc = None
    try:
        end_time_utc = datetime.strptime(
            (status.get("end_time") or "")[:19], "%Y-%m-%dT%H:%M:%S"
        )
    except ValueError:
        pass

    conn = get_sql_server_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE trx.listings
            SET
                is_deleted = 1,
                deleted_at = ISNULL(
                    DATEADD(minute, DATEDIFF(minute, SYSUTCDATETIME(), SYSDATETIME()), CAST(? AS datetime2)),
                    SYSDATETIME()
                ),
                delete_reason = ?,
                error_message = ISNULL(?, error_message),
                error_at = CASE WHEN ? IS NULL THEN error_at ELSE SYSDATETIME() END
            OUTPUT deleted.listing_id
            WHERE account = ?
              AND listing_id = ?
              AND is_deleted = 0
            """,
            end_time_utc, delete_reason, error_message, error_message, account, listing_id,
        )
        updated = cursor.fetchall()
        conn.commit()
    finally:
        conn.close()

    for _ in updated:
        log_listings_change("DELETE", account, listing_id, sku, delete_reason)

    print(
        f"[ENDED_CHECK_OK] account={account} listing_id={listing_id} sku={sku} "
        f"reason={delete_reason} listing_status={status.get('listing_status')} "
        f"end_time={status.get('end_time')} ending_reason={status.get('ending_reason')} "
        f"deleted_at={'EndTime' if end_time_utc else '検知時刻'} updated={len(updated)}",
        flush=True,
    )
    return bool(updated)


def _delete_listing_after_price_failure(
    account: str,
    sku: str,
    error_message: str,
    delete_reason: str,
) -> bool:
    """
    価格更新失敗時の eBay削除 + trx.listings 論理削除。
    eBay削除の成否にかかわらず、そのアカウントの出品中行ごとに GetItem で終了を確認できたものだけ削除済みにする
    （以前は削除結果を見ずに、また削除成功時はGetItem確認・アカウント条件なしにDBを削除済みにしていた）。
    全行を削除済みにできた場合 True を返す。False の場合、呼び出し元で失敗を記録・通知する。
    """
    res = delete_item_from_ebay(account, sku)

    if not res.get("success"):
        print(
            f"[PRICE_DELETE_NG] account={account} sku={sku} reason={delete_reason} "
            f"delete_result={res!r}",
            flush=True,
        )

    conn = get_sql_server_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT listing_id
            FROM trx.listings
            WHERE vendor_item_id = ?
              AND account = ?
              AND is_deleted = 0
            """,
            sku, account,
        )
        listing_ids = [str(row[0]) for row in cursor.fetchall()]
    finally:
        conn.close()

    deleted = [
        mark_listing_deleted_if_ended(account, listing_id, sku, delete_reason, error_message)
        for listing_id in listing_ids
    ]
    if res.get("success") and not all(deleted):
        print(
            f"[PRICE_DELETE_UNCONFIRMED] account={account} sku={sku} reason={delete_reason} "
            f"offer_id={res.get('offer_id')} listing_ids={listing_ids} → offer削除後もeBayの終了を確認できずDB変更なし",
            flush=True,
        )
    return bool(deleted) and all(deleted)


## Trading API版　今後削除
def delete_item_from_ebay_bk(account: str, item_id: str) -> Dict[str, Any]:
    print(f"delete_item_from_ebay: account={account}, item_id={item_id}")
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

def delete_items_from_ebay_batch(account: str, item_ids: list[str], batch_no: Optional[int] = None) -> Dict[str, Any]:
    # batch_no は[一時デバッグ]ログ表示専用の任意引数。渡さなくても動作は変わらない。
    if not item_ids:
        return {"success": True, "results": []}
    if len(item_ids) > 10:
        raise ValueError("EndItems は最大10件まで。")
    token = get_access_token_new(account)
    if not token:
        # ===== [一時デバッグ] トークン取得失敗でここで打ち切られていないか =====
        _log_delete_debug(f"[DELETE DEBUG][ebay_api] account={account} batch_no={batch_no} item_ids={item_ids} token取得失敗のため no_token を返します")
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

    # ===== [一時デバッグ] EndItems APIの生レスポンスをそのまま出力 =====
    _log_delete_debug(f"[DELETE DEBUG][ebay_api] account={account} batch_no={batch_no} item_ids={item_ids} http_status={r.status_code}")
    _log_delete_debug(f"[DELETE DEBUG][ebay_api] account={account} batch_no={batch_no} raw_response={text!r}")

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
        lm = re.search(r"<LongMessage>(.*?)</LongMessage>", b, flags=re.S)
        sm = re.search(r"<ShortMessage>(.*?)</ShortMessage>", b, flags=re.S)
        error_message = (lm.group(1).strip() if lm else (sm.group(1).strip() if sm else ""))
        ok = (not has_errors) or (err == 1047) or has_endtime
        results.append({
            "item_id": mid,
            "success": bool(ok),
            "error_code": err,
            "error_message": error_message,
        })

    if not results:
        em = re.search(r"<ErrorCode>(\d+)</ErrorCode>", text)
        lm = re.search(r"<LongMessage>(.*?)</LongMessage>", text, flags=re.S)
        result = {
            "success": False,
            "error_code": int(em.group(1)) if em else "parse_error",
            "error_message": lm.group(1).strip() if lm else "",
            "raw_response": text,
        }
        # ===== [一時デバッグ] EndItemResponseContainerが1件も取れなかった(パース失敗) =====
        _log_delete_debug(f"[DELETE DEBUG][ebay_api] account={account} batch_no={batch_no} results抽出0件(parse_error) result={result!r}")

        # 10007(System error)はバッチ全体が失敗応答でも、eBay側では実際に終了していることがある
        # （2026-10-01 BUZZ batch_no=9 で実測。DBが出品中のまま残り、後の価格更新で再出品された）。
        # 結果不明として1件ずつ GetItem で確認し、終了を確認できたものだけ success=True で返す。
        if result["error_code"] == 10007:
            verified = []
            for iid in item_ids:
                status = get_listing_status(account, iid)
                verified.append({
                    "item_id": str(iid),
                    "success": bool(status.get("ended")),
                    "error_code": 10007,
                    "error_message": f"10007後のGetItem確認: {status!r}",
                })
            result["results"] = verified
            _log_delete_debug(f"[DELETE DEBUG][ebay_api] account={account} batch_no={batch_no} 10007のためGetItemで確認 results={verified!r}")

        return result
    if any(r.get("error_code") in (518, 429) for r in results):
        result = {"success": False, "error_code": 518, "results": results, "message": "per-container rate limit", "raw_response": text}
        # ===== [一時デバッグ] レート制限(518/429)を検知 =====
        _log_delete_debug(f"[DELETE DEBUG][ebay_api] account={account} batch_no={batch_no} レート制限検知 result={result!r}")
        return result
    result = {"success": True, "results": results, "raw_response": text}
    # ===== [一時デバッグ] 正常応答(success=Trueだが、個々のitemがsuccess=Falseの場合もある) =====
    _log_delete_debug(f"[DELETE DEBUG][ebay_api] account={account} batch_no={batch_no} 正常応答 result={result!r}")
    return result

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


def _retry_after_sec(resp: requests.Response) -> Optional[int]:
    try:
        return int(resp.headers.get("Retry-After") or "")
    except ValueError:
        return None


def _inventory_find_offer_by_sku(token: str, sku: str, marketplace_id: str = "EBAY_US") -> Dict[str, Any]:
    """
    getOffers を呼び、HTTPステータスとエラー内容で結果を分類する。
    kind: found(offer_idあり) / not_found(200でoffer無し・404) / transient(429・5xx・通信エラー) / permanent(その他4xx)
    以前は HTTPステータスを見ず、エラー応答も「offer無し」と同じ扱いにしていた
    （2026-10-21 出品中のofferで offer_not_found_for_sku が14件、生レスポンスも記録されていなかった）。
    """
    url = "https://api.ebay.com/sell/inventory/v1/offer"
    try:
        r = requests.get(url, headers=_ebay_json_headers(token), params={"sku": sku, "marketplaceId": marketplace_id}, timeout=45)
    except requests.exceptions.RequestException as e:
        return {"kind": "transient", "error": f"getoffers_request_error:{e}"[:300]}

    data = _safe_json(r)
    offers = (data or {}).get("offers") or []
    if r.status_code == 200 and offers:
        for o in offers:
            if (o.get("marketplaceId") or "").upper() == marketplace_id:
                return {"kind": "found", "offer_id": o.get("offerId"), "raw": data}
        return {"kind": "found", "offer_id": offers[0].get("offerId"), "raw": data}

    if r.status_code == 200:
        detail = f"getoffers_http_200:offers=0 total={data.get('total')}"
    else:
        error_id, error_msg = _extract_error(data)
        detail = f"getoffers_http_{r.status_code}:{error_id}:{error_msg}"[:300]

    if r.status_code in (200, 404):
        return {"kind": "not_found", "error": detail, "raw": data}
    if r.status_code == 429:
        return {"kind": "transient", "error": detail, "raw": data, "retry_after": _retry_after_sec(r)}
    if r.status_code >= 500:
        return {"kind": "transient", "error": detail, "raw": data}
    return {"kind": "permanent", "error": detail, "raw": data}

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
    for attempt in range(3):
        try:
            r = requests.post(
                url,
                headers=_ebay_json_headers(token),
                json={},
                timeout=45
            )
            return (r.status_code == 200), _safe_json(r)
        except requests.exceptions.ConnectionError:
            if attempt == 2:
                raise
            print("RETRY publish_offer (connection)")
            time.sleep(2)
        except requests.exceptions.ReadTimeout:
            if attempt == 2:
                raise
            print("RETRY publish_offer (timeout)")
            time.sleep(2)

def _is_transient_inventory_failure(error_id, error_message: str) -> bool:
    """
    価格更新(PUT/publish)の失敗のうち、時間をおけば通る一時的なものか。
    25001(system error)、25002 のうち "System error"、エラー本文の無い失敗(HTTP 5xx等)。
    25002 "System error. Unable to process your request. Please try again later." で
    出品を終了していた（2026-10-19〜21に33回）。
    """
    if error_id is None or error_id == 25001:
        return True
    return error_id == 25002 and "system error" in (error_message or "").lower()


def update_ebay_price_rest(
    account: str,
    sku: str,
    new_price_usd,
    *,
    debug: bool = False,
    expected_listing_id: Optional[str] = None,
) -> dict:
    if not account or not sku:
        return {"success": False, "error": "missing_account_or_sku"}

    price_str = _to_price_str(new_price_usd)

    # 1) token取得
    token = get_access_token_new(account)
    if not token:
        return {"success": False, "sku": sku, "error": "get_token_failed"}

    # 2) offer_id取得
    found = _inventory_find_offer_by_sku(token, sku)
    if found["kind"] != "found":
        kind, error = found["kind"], found["error"]
        # offerが見つからなくても GetItem で出品中(または確認できない)なら、eBay側の一時的な取得失敗として再試行する
        if kind == "not_found" and expected_listing_id:
            status = get_listing_status(account, str(expected_listing_id))
            if status.get("listing_status") == "Active":
                kind, error = "transient", f"offer_not_found_but_listing_active:{error}"
            elif not status.get("ok"):
                kind, error = "transient", f"offer_not_found_getitem_failed:{error} getitem={status.get('error')}"
        print(
            f"[PRICE_OFFER_LOOKUP_NG] account={account} sku={sku} kind={kind} error={error} "
            f"raw={str(found.get('raw'))[:300]}",
            flush=True,
        )
        out = {
            "success": False,
            "sku": sku,
            "error": "offer_not_found_for_sku" if kind == "not_found" else error,
        }
        if kind == "transient":
            out["transient"] = True
            out["retry_after"] = found.get("retry_after")
        if debug:
            out["raw"] = found.get("raw")
        return out
    offer_id = found["offer_id"]

    # 3) offer取得
    offer_obj = _inventory_get_offer(token, offer_id)
    # 取得失敗時はエラー内容のJSON（status無し）が返る。以前はこれを終了済みofferと同じ扱いで
    # 価格更新をスキップしており、再試行もされず価格が古いまま残っていた（2026-10-21調査）。
    if not offer_obj or not offer_obj.get("status"):
        print(
            f"[PRICE_OFFER_GET_FAILED] account={account} sku={sku} offer_id={offer_id} "
            f"res={str(offer_obj)[:300]}",
            flush=True,
        )
        return {
            "success": False,
            "sku": sku,
            "error": "offer_get_failed",
            "transient": True,
        }

    # 3') 出品状態の確認
    # 終了済み(UNPUBLISHED/ENDED)のofferを publishOffer すると、新しいlisting_idで再出品され、
    # trx.listings に記録されない出品が発生していた（2026-10-01 BUZZ 2JWDS... 等で実測）。
    # PUBLISHED かつ ACTIVE の場合だけ価格更新へ進む（公開中offerのpublishは既存出品の更新で、新規出品にならない）。
    # OUT_OF_STOCK でも、DBと同じlisting_idで GetItem が Active なら価格更新する
    # （購入可能なのにofferだけOUT_OF_STOCKの出品があり、価格更新でACTIVEに戻ることを2026-10-21に実測）。
    listing = offer_obj.get("listing") or {}
    offer_status = offer_obj.get("status")
    listing_status = listing.get("listingStatus")
    offer_listing_id = str(listing.get("listingId") or "")
    if (
        offer_status == "PUBLISHED"
        and listing_status == "OUT_OF_STOCK"
        and expected_listing_id
        and offer_listing_id == str(expected_listing_id)
        and get_listing_status(account, offer_listing_id).get("listing_status") == "Active"
    ):
        print(
            f"[PRICE_OUT_OF_STOCK_ACTIVE] account={account} sku={sku} listing_id={offer_listing_id} "
            f"→ GetItemはActiveのため価格更新を継続",
            flush=True,
        )
    elif offer_status != "PUBLISHED" or listing_status != "ACTIVE":
        not_active_error = f"listing_not_active:{offer_status}/{listing_status}/{offer_listing_id}"
        print(
            f"[PRICE_SKIP] account={account} sku={sku} expected_listing_id={expected_listing_id} "
            f"{not_active_error} → 価格更新・再出品しない",
            flush=True,
        )
        db_deleted = False
        if expected_listing_id:
            db_deleted = mark_listing_deleted_if_ended(
                account, str(expected_listing_id), sku, "eBay終了確認", not_active_error
            )
        return {"success": False, "sku": sku, "error": not_active_error, "db_deleted": db_deleted}

    # eBay側で公開中だがDBと listing_id が違う場合も価格更新は続ける（DBの既存行は変更しない）。
    if expected_listing_id and offer_listing_id != str(expected_listing_id):
        print(
            f"[PRICE_LISTING_ID_MISMATCH] account={account} sku={sku} "
            f"db_listing_id={expected_listing_id} ebay_listing_id={offer_listing_id} → 価格更新は継続",
            flush=True,
        )

    # 4) 価格変更
    old_price_str = str(((offer_obj.get("pricingSummary") or {}).get("price") or {}).get("value") or "")
    offer_obj.setdefault("pricingSummary", {})
    offer_obj["pricingSummary"]["price"] = {
        "value": price_str,
        "currency": "USD",
    }

    # 5) PUT

    ok = False
    put_res = None

    for attempt in range(3):
        ok, put_res = _inventory_put_offer(token, offer_id, offer_obj)

        if ok:
            break
        if attempt < 2:
            time.sleep(2)

    if not ok:
        errors = put_res.get("errors", [])

        if errors:
            e = errors[0]
            error_id = e.get("errorId")
            msg = e.get("message", "")
            error_message = f"inventory_put_error_{error_id}:{msg}"
        else:
            error_id = None
            error_message = "inventory_put_failed"

        print("PUT FAILED:", account, sku, error_message)

        # =========================
        # 一時的なエラー → 削除せず再試行に回す
        # =========================
        if _is_transient_inventory_failure(error_id, error_message):
            return {"success": False, "sku": sku, "error": error_message, "transient": True}

        # =========================
        # 25097 → 削除＋出品不可
        # =========================
        if error_id == 25097:
            conn = get_sql_server_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE trx.vendor_item
                SET 出品不可flg = 1
                WHERE vendor_item_id = ?
            """, (sku,))

            conn.commit()
            conn.close()

            db_deleted = _delete_listing_after_price_failure(account, sku, error_message, "IP違反")

            return {"success": False, "sku": sku, "error": error_message, "db_deleted": db_deleted}

        # -------------------------
        # 出品削除 + DB更新
        # -------------------------
        print(
            "DELETE LISTING:",
            "account=", account,
            "sku=", sku,
            "reason=", error_message
        )
        db_deleted = _delete_listing_after_price_failure(account, sku, error_message, "価格更新失敗")

        return {
            "success": False,
            "sku": sku,
            "error": error_message,
            "db_deleted": db_deleted,
        }

    # 6) publish
    ok2=False
    pub_res=None
    for attempt in range(3):
        ok2,pub_res=_inventory_publish_offer(token,offer_id)

        if ok2:break
        if attempt<2:time.sleep(2)

    # -------------------------
    # DB更新（ここ追加）
    # -------------------------
    if ok2:
        published_listing_id = str((pub_res or {}).get("listingId") or "")
        if published_listing_id != offer_listing_id:
            print(
                f"[PRICE_PUBLISH_LISTING_ID_CHANGED] account={account} sku={sku} "
                f"before={offer_listing_id} after={published_listing_id}",
                flush=True,
            )

        # publish の成功だけでなく、eBay上の実際の価格が変わったことを GetItem で確認する。
        # GetItem はアプリ全体で1日5,000回の上限があり、全価格更新で確認すると上限に達するため、
        # 未反映だと赤字販売の恐れがある値上げの時だけ確認する（値上げは価格変更の約3%）。
        # GetItem 自体が取得できない場合(518等)は PUT/publish は成功しているため、未確認のまま成功扱いにする
        # （以前は失敗扱いで PUT/publish を繰り返し、GetItem の呼び出しとメールが大量に発生していた）。
        verify_listing_id = published_listing_id or offer_listing_id
        try:
            is_price_up = not old_price_str or Decimal(price_str) > Decimal(old_price_str)
        except Exception:
            is_price_up = True
        verified = False
        if is_price_up:
            status = get_listing_status(account, verify_listing_id)
            if status.get("ok"):
                try:
                    price_ok = Decimal(status.get("current_price") or "") == Decimal(price_str)
                except Exception:
                    price_ok = False
                if not price_ok:
                    verify_error = (
                        f"price_verify_failed:listing_id={verify_listing_id} expected={price_str} "
                        f"actual={status.get('current_price')} status={status.get('listing_status')}"
                    )
                    print(f"[PRICE_VERIFY_NG] account={account} sku={sku} {verify_error}", flush=True)
                    return {"success": False, "sku": sku, "error": verify_error, "transient": True}
                verified = True
            else:
                print(
                    f"[PRICE_VERIFY_UNCONFIRMED] account={account} sku={sku} listing_id={verify_listing_id} "
                    f"{old_price_str}->{price_str} getitem_error={status.get('error')} → PUT/publish成功のため更新済み扱い（実価格未確認）",
                    flush=True,
                )

        conn = get_sql_server_connection()
        cursor = conn.cursor()

        # 同じSKUが別アカウントにも出品されている場合があるため、アカウントも条件にする
        cursor.execute(
            """
            UPDATE trx.listings
            SET start_price = ?
            WHERE vendor_item_id = ?
            AND account = ?
            AND is_deleted = 0
            """,
            new_price_usd,
            sku,
            account,
        )
        conn.commit()   # ★これ必須
        conn.close()    # ★これも必須
        print(
            f"[PRICE_UPDATED] account={account} sku={sku} listing_id={verify_listing_id} "
            f"{old_price_str}->{price_str} verified={verified}",
            flush=True,
        )

    if not ok2:
        errors = pub_res.get("errors", [])

        if errors:
            e = errors[0]
            error_id = e.get("errorId")
            msg = e.get("message", "")
            error_message = f"publish_error_{error_id}:{msg}"
        else:
            error_id = None
            error_message = "publish_failed"

        print("PUBLISH FAILED:", account, sku, error_message)

        # 一時的なエラー → 削除せず再試行に回す
        if _is_transient_inventory_failure(error_id, error_message):
            return {"success": False, "sku": sku, "error": error_message, "transient": True}

        # 25097 → 削除＋出品不可
        if error_id == 25097:
            conn = get_sql_server_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE trx.vendor_item
                SET 出品不可flg = 1
                WHERE vendor_item_id = ?
            """, (sku,))

            conn.commit()
            conn.close()

            db_deleted = _delete_listing_after_price_failure(account, sku, error_message, "IP違反")

            return {"success": False, "sku": sku, "error": error_message, "db_deleted": db_deleted}

        # その他 → 従来通り削除
        db_deleted = _delete_listing_after_price_failure(account, sku, error_message, "価格更新失敗")

        return {"success":False,"sku":sku,"error":error_message,"db_deleted":db_deleted}
    return {"success":True,"sku":sku,"price_verified":verified}

def update_ebay_price(account: str, ebay_item_id: str, new_price_usd, *, sku: Optional[str] = None) -> dict:

    if not account or not ebay_item_id:
        err = {'success': False, 'error': 'missing_account_or_item_id'}
        print("ERROR:", err)
        return err

    if not sku:
        err = {'success': False, 'item_id': str(ebay_item_id), 'error': 'sku_missing'}
        print("ERROR:", err)
        return err

    price_str = _to_price_str(new_price_usd)

    token = get_access_token_new(account)
    if not token:
        err = {'success': False, 'item_id': str(ebay_item_id), 'price': price_str, 'error': 'get_token_failed'}
        print("ERROR:", err)
        return err

    offer_id, list_res = _inventory_get_offer_id_by_sku(token, sku)
    if not offer_id:
        err = {
            'success': False,
            'item_id': str(ebay_item_id),
            'price': price_str,
            'error': 'offer_not_found_for_sku',
            'raw': {'listOffers': list_res}
        }
        print("ERROR:", err)
        return err

    offer_obj = _inventory_get_offer(token, offer_id) or {}
    offer_obj.setdefault('pricingSummary', {})['price'] = {"value": price_str, "currency": "USD"}

    ok, put_res = _inventory_put_offer(token, offer_id, offer_obj)
    if not ok:
        err = {
            'success': False,
            'item_id': str(ebay_item_id),
            'price': price_str,
            'error': 'inventory_put_failed',
            'raw': {'offerId': offer_id, 'putOffer': put_res}
        }
        print("ERROR:", err)
        return err

    ok2, pub_res = _inventory_publish_offer(token, offer_id)
    if not ok2:
        err = {
            'success': False,
            'item_id': str(ebay_item_id),
            'price': price_str,
            'error': 'inventory_publish_failed',
            'raw': {'offerId': offer_id, 'publishOffer': pub_res}
        }
        print("ERROR:", err)
        return err

    return {
        'success': True,
        'item_id': str(pub_res.get('listingId') or ebay_item_id),
        'price': price_str,
        'note': 'via_inventory'
    }

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
