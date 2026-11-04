"""
メルカリ検索API（entities:search）をブラウザを使わずに直接呼ぶ。

背景（2026-11-04）:
  scrape_worker は検索ページをブラウザで開き、ページ自身が呼ぶ entities:search の応答を受け取っていた。
  VPS（3コア）ではこのブラウザがCPUの大半を使い、出品処理と取り合っていた。
  VPS 210.131.209.232 での比較テスト（16ページ）では、ここで組み立てる本文がブラウザの送る本文と
  セッションID以外完全に一致し、商品ID・並び順・全項目・次ページトークンも一致した。
  1ページのCPUは 5.3秒 → 0.02秒。

方式:
  - 本文は検索URL（apps.adapters.mercari_search.make_search_url の結果）のクエリから組み立てる。
  - DPoP はこのプロセスで生成した P-256 鍵による ES256 署名（ブラウザと同じく鍵はクライアントが自分で作る）。
    ログイン・Cookie は使わない。
  - アクセス頻度を増やさないよう、1プロセス内の呼び出し間隔を MIN_INTERVAL_SEC 以上あける
    （ブラウザ方式の1ワーカーの呼び出し間隔は 232 で中央値17秒・平均20秒）。

切り替え・停止は scrape_worker 側（SEARCH_FETCH_MODE と状態ファイル）で行う。
"""
import os
import json
import time
import uuid
import base64
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import requests
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

API_URL = "https://api.mercari.jp/v2/entities:search"

# ブラウザ（Playwright 1.58 の chromium headless shell）が送るものと同じ User-Agent
USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) HeadlessChrome/145.0.7632.6 Safari/537.36")

MIN_INTERVAL_SEC = float(os.environ.get("SEARCH_API_MIN_INTERVAL_SEC", "17"))
CONNECT_TIMEOUT_SEC = 10
READ_TIMEOUT_SEC = 30

# 検索URLの「出品形式：定額販売」の条件（mercari_search.MERCARI_BASE_URL）。本文では attributes に入る
LISTING_TYPE_ATTR_ID = "d664efe3-ae5a-4824-b729-e789bf93aba9"

STATUS_MAP = {
    "on_sale": ["STATUS_ON_SALE"],
    "sold_out|trading": ["STATUS_SOLD_OUT", "STATUS_TRADING"],
}
ITEM_TYPE_MAP = {
    "mercari": ["ITEM_TYPE_MERCARI"],
    "beyond": ["ITEM_TYPE_BEYOND"],
}


class SearchApiBlocked(Exception):
    """401・403・429。アクセス制限・認証拒否の可能性。追加のアクセスをしてはならない"""


class SearchApiError(Exception):
    """上記以外の失敗（通信エラー・5xx・400・応答の形の違いなど）"""


def _ints(q, key):
    return [int(x) for v in q.get(key, []) for x in v.split(",") if x != "" and int(x) != 0]


def build_search_body(page_url: str, device_uuid: str) -> dict:
    """検索URLのクエリから entities:search の本文を作る。対応していない条件があれば例外にする。"""
    q = parse_qs(urlparse(page_url).query, keep_blank_values=True)

    known = {"status", "item_types", "brand_id", "category_id", "price_min", "price_max",
             "item_condition_id", "shipping_payer_id", "sort", "order", "page_token", LISTING_TYPE_ATTR_ID}
    unknown = set(q) - known
    if unknown:
        raise SearchApiError(f"未対応の検索条件: {sorted(unknown)}")
    if q.get("sort", ["created_time"])[0] != "created_time" or q.get("order", ["desc"])[0] != "desc":
        raise SearchApiError(f"未対応の並び順: sort={q.get('sort')} order={q.get('order')}")

    status = STATUS_MAP.get(q.get("status", [""])[0])
    item_types = ITEM_TYPE_MAP.get(q.get("item_types", [""])[0])
    if status is None or item_types is None:
        raise SearchApiError(f"未対応の status/item_types: {q.get('status')} {q.get('item_types')}")

    # make_search_url は price_min/price_max を2回付ける（同じ値）。値が食い違う場合は扱わない
    for k in ("price_min", "price_max"):
        if len(set(q.get(k, []))) > 1:
            raise SearchApiError(f"{k} の値が複数ある: {q.get(k)}")
    price_min = int(q["price_min"][0]) if q.get("price_min") else 0
    price_max = int(q["price_max"][0]) if q.get("price_max") else 0

    attributes = []
    if q.get(LISTING_TYPE_ATTR_ID):
        attributes.append({"id": LISTING_TYPE_ATTR_ID, "values": q[LISTING_TYPE_ATTR_ID]})

    return {
        "userId": "",
        "config": {"responseToggles": ["QUERY_SUGGESTION_WEB_1"]},
        "pageSize": 120,
        "pageToken": q.get("page_token", [""])[0],
        "searchSessionId": uuid.uuid4().hex,
        "source": "BaseSerp",
        "indexRouting": "INDEX_ROUTING_UNSPECIFIED",
        "thumbnailTypes": [],
        "searchCondition": {
            "keyword": "", "excludeKeyword": "",
            "sort": "SORT_CREATED_TIME", "order": "ORDER_DESC",
            "status": status,
            "sizeId": [],
            "categoryId": _ints(q, "category_id"),
            "brandId": _ints(q, "brand_id"),
            "sellerId": [],
            "priceMin": price_min,
            "priceMax": price_max,
            "itemConditionId": _ints(q, "item_condition_id"),
            "shippingPayerId": _ints(q, "shipping_payer_id"),
            "shippingFromArea": [], "shippingMethod": [], "colorId": [],
            "hasCoupon": False,
            "attributes": attributes,
            "itemTypes": item_types,
            "skuIds": [], "shopIds": [], "excludeShippingMethodIds": [],
        },
        "serviceFrom": "suruga",
        "withItemBrand": True, "withItemSize": False, "withItemPromotions": True, "withItemSizes": True,
        "withShopname": False, "useDynamicAttribute": True, "withSuggestedItems": True,
        "withOfferPricePromotion": True, "withProductSuggest": True, "withParentProducts": False,
        "withProductArticles": True, "withSearchConditionId": False, "withAuction": True,
        "laplaceDeviceUuid": device_uuid,
    }


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _b64url_json(obj) -> str:
    return _b64url(json.dumps(obj, separators=(",", ":")).encode())


class _Client:
    def __init__(self):
        self.key = ec.generate_private_key(ec.SECP256R1())
        nums = self.key.public_key().public_numbers()
        self.jwk = {"crv": "P-256", "kty": "EC",
                    "x": _b64url(nums.x.to_bytes(32, "big")), "y": _b64url(nums.y.to_bytes(32, "big"))}
        self.device_uuid = str(uuid.uuid4())
        self.session = requests.Session()
        self.last_call = 0.0

    def dpop(self) -> str:
        """DPoP（RFC 9449）: ES256 で署名した JWT。署名は r||s（各32バイト）"""
        header = {"typ": "dpop+jwt", "alg": "ES256", "jwk": self.jwk}
        payload = {"iat": int(time.time()), "jti": str(uuid.uuid4()), "htu": API_URL, "htm": "POST",
                   "uuid": self.device_uuid}
        signing_input = f"{_b64url_json(header)}.{_b64url_json(payload)}"
        r, s = decode_dss_signature(self.key.sign(signing_input.encode(), ec.ECDSA(hashes.SHA256())))
        return f"{signing_input}.{_b64url(r.to_bytes(32, 'big') + s.to_bytes(32, 'big'))}"


_client = None


def _get_client() -> _Client:
    global _client
    if _client is None:
        _client = _Client()
    return _client


def _log_path() -> str:
    base = "D:/apps_nostock/logs" if os.name == "nt" else "/opt/apps_nostock/logs"
    d = os.path.join(base, "search_api")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"search_api_{datetime.now():%Y%m%d}.log")


def _log(rec: dict):
    try:
        with open(_log_path(), "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[SEARCH_API][LOG ERROR] {e}", flush=True)


def fetch_search_json(page_url: str) -> dict:
    """
    検索URL1ページ分の entities:search 応答（dict）を返す。
    401/403/429 は SearchApiBlocked、その他の失敗は SearchApiError。
    200 でも items・meta が無い応答は SearchApiError（0件ページと区別するため。0件は items=[] で返る）。
    """
    c = _get_client()
    body = build_search_body(page_url, c.device_uuid)

    wait = MIN_INTERVAL_SEC - (time.time() - c.last_call)
    if wait > 0:
        time.sleep(wait)

    t0 = time.time()
    cpu0 = time.thread_time()
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "pid": os.getpid(),
           "page_token": body["pageToken"], "status_cond": body["searchCondition"]["status"][0],
           "item_type": body["searchCondition"]["itemTypes"][0]}
    try:
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "ja",
            "content-type": "application/json",
            "origin": "https://jp.mercari.com",
            "referer": "https://jp.mercari.com/",
            "user-agent": USER_AGENT,
            "x-country-code": "JP",
            "x-platform": "web",
            "dpop": c.dpop(),
        }
        try:
            r = c.session.post(API_URL, headers=headers, json=body,
                               timeout=(CONNECT_TIMEOUT_SEC, READ_TIMEOUT_SEC))
        except requests.RequestException as e:
            rec.update(result="network_error", err=f"{type(e).__name__}: {str(e)[:200]}")
            raise SearchApiError(rec["err"])
        finally:
            c.last_call = time.time()

        rec["http"] = r.status_code
        if r.status_code in (401, 403, 429):
            rec.update(result="blocked", err=r.text[:300])
            raise SearchApiBlocked(f"status={r.status_code} {r.text[:300]}")
        if r.status_code != 200:
            rec.update(result="http_error", err=r.text[:300])
            raise SearchApiError(f"status={r.status_code} {r.text[:300]}")
        try:
            js = r.json()
        except ValueError as e:
            rec.update(result="bad_json", err=str(e)[:200])
            raise SearchApiError(f"JSONでない応答: {e}")
        if not isinstance(js, dict) or not isinstance(js.get("items"), list) or not isinstance(js.get("meta"), dict):
            rec.update(result="bad_shape", err=str(list(js)[:10]) if isinstance(js, dict) else type(js).__name__)
            raise SearchApiError(f"応答の形が想定と違う: {rec['err']}")
        rec.update(result="ok", items=len(js["items"]), next=bool(js["meta"].get("nextPageToken")))
        return js
    finally:
        rec.update(sec=round(time.time() - t0, 3), cpu=round(time.thread_time() - cpu0, 4))
        _log(rec)
