# -*- coding: utf-8 -*-
"""
yahoo_fleamarket.py — Yahoo!フリマ（vendor_name='ＰａｙＰａｙフリマ'）の検索・商品詳細・在庫確認

Playwright/Selenium は使わず、サイト自身が使う JSON API を HTTP GET で呼ぶ（ログイン・Cookie 不要）。
  検索     /api/v1/search                 1ページ100件、offset は 9,900 件まで
  商品詳細 /api/item/v2/items/{id}         削除済みは 404
  ブランド /api/v1/brands/{id}/children    brandList にそのブランドの親から自分までの階層が入る

【ブランドと preset】（2026-11-10 実測）
  検索の brandIds は子ブランドの商品も返す（SEIKO で検索すると Grand Seiko・CREDOR・ALBA なども混ざる）。
  商品の preset は「検索した preset」ではなく、商品に付いたブランドから決める（resolve_item_preset）。
  - 商品ブランドの階層を下からたどり、同じカテゴリーの Yahoo!フリマ preset があるブランドの preset にする。
    例: SEIKO の検索で見つかった Grand Seiko の商品 → グランドセイコーの preset。
  - 子ブランドを Grand Seiko・CREDOR にせず SEIKO とだけ設定された商品は、タイトルにブランド名が
    明記されていれば付け替える（TITLE_BRAND_RULES。「GS」単独では判定しない）。
  - FURLA の下にある「ALBA」(124244) は出品しない（NG_BRAND_IDS。Yahoo!フリマ側のブランド構造の不備）。
  eBay の Brand・タイトル・説明文・高リスク判定は preset のブランド（presets_brand.default_brand_en）を使うため、
  preset を付け替えれば4つとも商品のブランドになる。
"""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import requests
from PIL import Image

VENDOR_NAME = "ＰａｙＰａｙフリマ"

BASE = "https://paypayfleamarket.yahoo.co.jp"
SEARCH_API = f"{BASE}/api/v1/search"
ITEM_API = f"{BASE}/api/item/v2/items/{{item_id}}"
BRAND_CHILDREN_API = f"{BASE}/api/v1/brands/{{brand_id}}/children"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
FETCH_TIMEOUT_SEC = 20

SEARCH_PAGE_SIZE = 100
# offset は 9,900 件目まで（それ以上は返らない）
SEARCH_OFFSET_LIMIT = 9900

# 商品の状態はメルカリの検索条件（新品・未使用に近い・目立った傷なし・やや傷）と同じ範囲
SEARCH_ITEM_CONDITIONS = "NEW,USED10,USED20,USED40"

# 商品の状態キー → メルカリと同じ item_condition_id
ITEM_CONDITION_MAP = {
    "new": 1,
    "used10": 2,   # 未使用に近い
    "used20": 3,   # 目立った傷や汚れなし
    "used40": 4,   # やや傷や汚れあり
    "used60": 5,   # 傷や汚れあり
    "used80": 6,   # 全体的に状態が悪い
}

# 出品しないブランド（Yahoo!フリマのブランドID）
NG_BRAND_IDS = {
    124244: "FURLA配下のALBA",
}

# 親ブランドだけが設定された商品を、タイトルのブランド名で子ブランドへ付け替える。
# {親のブランドID: [(子のブランドID, タイトルの正規表現), ...]}。タイトルは NFKC 正規化・小文字化してから照合する。
TITLE_BRAND_RULES = {
    101058: [  # SEIKO
        (102478, re.compile(r"グランドセイコー|grand\s*seiko")),  # Grand Seiko（「GS」単独では判定しない）
        (102541, re.compile(r"クレドール|credor")),               # CREDOR
    ],
}

IMAGE_MIN_PX = 500
IMAGE_LIMIT = 20


class YahooHttpError(Exception):
    """API が 2xx / 404 以外を返した。"""

    def __init__(self, url: str, status_code: int, body_head: str):
        self.url = url
        self.status_code = status_code
        super().__init__(f"YAHOO_HTTP status={status_code} url={url} body_head={body_head!r}")


class YahooItemUnavailableError(Exception):
    """商品が販売中でない（state: '削除' / '売り切れ' / '判定不可'）。"""

    def __init__(self, message: str, state: str):
        super().__init__(message)
        self.state = state


_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})


def _get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    """GET して JSON を返す。404 は None。それ以外の失敗は YahooHttpError（通信エラーは requests の例外のまま）。"""
    resp = _session.get(url, params=params, timeout=FETCH_TIMEOUT_SEC)
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise YahooHttpError(resp.url, resp.status_code, " ".join(resp.text[:300].split()))
    return resp.json()


def item_url(item_id: str) -> str:
    return f"{BASE}/item/{item_id}"


# =========================
# 検索
# =========================
def search_params(*, brand_id: int, category_id: int, status: str,
                  min_price: Optional[int], max_price: Optional[int], offset: int) -> dict:
    """status は 'open'（販売中）または 'sold'（売り切れ）。並び順は新着順。"""
    params = {
        "results": SEARCH_PAGE_SIZE,
        "offset": offset,
        "itemStatus": status,
        "brandIds": int(brand_id),
        "genreCategoryIds": int(category_id),
        "itemConditions": SEARCH_ITEM_CONDITIONS,
        "sort": "openTime",
        "order": "DESC",
    }
    if min_price is not None:
        params["minPrice"] = int(min_price)
    if max_price is not None:
        params["maxPrice"] = int(max_price)
    return params


def fetch_search_page(params: dict) -> Tuple[List[Dict[str, Any]], int]:
    """検索1ページ分。(商品の一覧, 総件数)。"""
    js = _get_json(SEARCH_API, params)
    if js is None or not isinstance(js.get("items"), list):
        raise YahooHttpError(SEARCH_API, 404 if js is None else 200, f"検索の応答に items が無い params={params}")
    items = []
    for it in js["items"]:
        brand = it.get("brand") or {}
        items.append({
            "vendor_item_id": str(it["id"]),
            "title_jp": it.get("title"),
            "price": int(it["price"]) if it.get("price") is not None else None,
            "brand_id": brand.get("id"),
            "brand_name": brand.get("name"),
            "item_condition_id": ITEM_CONDITION_MAP.get((it.get("condition") or "").lower()),
        })
    return items, int(js.get("totalResultsAvailable") or 0)


# =========================
# ブランドの階層と preset の決定
# =========================
# {ブランドID: 親から自分までのブランドID}。プロセス内で保持する。
_brand_path_cache: Dict[int, List[int]] = {}
# 子孫をたどり終えたブランドID（検索に使ったブランド）
_crawled_roots: set = set()


def _crawl_brand_tree(root_id: int) -> None:
    """
    root_id の子孫すべての階層を _brand_path_cache に入れる。
    子ブランドの一覧API は子のあるブランドにしか使えない（子の無いブランドは 404）ため、
    子のブランドの階層は親の階層に自分を足して作る。
    """
    js = _get_json(BRAND_CHILDREN_API.format(brand_id=root_id))
    if js is None:  # 子が無い
        _brand_path_cache.setdefault(root_id, [root_id])
        return
    root_path = [int(b["id"]) for b in js.get("brandList") or []] or [root_id]
    _brand_path_cache[root_id] = root_path
    queue = [(root_path, js)]
    while queue:
        parent_path, parent_js = queue.pop()
        for child in parent_js.get("brands") or []:
            child_path = parent_path + [int(child["id"])]
            _brand_path_cache[int(child["id"])] = child_path
            if child.get("hasChildren"):
                child_js = _get_json(BRAND_CHILDREN_API.format(brand_id=int(child["id"])))
                if child_js is not None:
                    queue.append((child_path, child_js))


def get_brand_path(brand_id: int, search_brand_id: int) -> List[int]:
    """
    商品のブランドの階層（親から自分まで）。search_brand_id は検索に使ったブランド
    （検索結果の商品のブランドはこのブランドかその子孫）。初めてのブランドなら子孫をたどってから引く。
    たどっても見つからないブランド（検索後に追加されたブランド等）は [brand_id] を返す。
    """
    brand_id, search_brand_id = int(brand_id), int(search_brand_id)
    if brand_id not in _brand_path_cache and search_brand_id not in _crawled_roots:
        _crawl_brand_tree(search_brand_id)
        _crawled_roots.add(search_brand_id)
    return _brand_path_cache.get(brand_id, [brand_id])


def _normalize_title(title: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", title or "").lower()


def resolve_item_preset(item_brand_id: Optional[int], title: Optional[str], category_id: int,
                        preset_by_brand: Dict[Tuple[int, int], str],
                        search_brand_id: int) -> Tuple[Optional[str], str]:
    """
    商品のブランドから preset を決める。戻り値は (preset, 説明)。出品しない商品は preset=None。
    preset_by_brand: {(Yahoo!フリマのブランドID, カテゴリーID): preset}（mst.presets_lookup の ＰａｙＰａｙフリマ 分）
    search_brand_id: 検索に使ったブランドID
    """
    if item_brand_id is None:
        return None, "ブランドなし"

    path = get_brand_path(int(item_brand_id), search_brand_id)
    for bid in path:
        if bid in NG_BRAND_IDS:
            return None, f"NGブランド {NG_BRAND_IDS[bid]}({bid})"

    resolved = next((bid for bid in reversed(path) if (bid, category_id) in preset_by_brand), None)
    if resolved is None:
        return None, f"preset のあるブランドが階層に無い brand_id={item_brand_id} path={path}"

    reason = f"brand_id={item_brand_id}" if resolved == item_brand_id else f"brand_id={item_brand_id}→{resolved}"
    rules = TITLE_BRAND_RULES.get(resolved)
    if rules:
        norm = _normalize_title(title)
        hits = []
        for child_id, pattern in rules:
            m = pattern.search(norm)
            if m and (child_id, category_id) in preset_by_brand:
                hits.append((m.start(), child_id))
        if hits:
            child_id = min(hits)[1]  # 両方あればタイトルで先に出てくる方
            reason += f"→{child_id}(タイトル)"
            resolved = child_id

    return preset_by_brand[(resolved, category_id)], reason


# =========================
# 商品詳細・在庫確認
# =========================
def fetch_item_json(item_id: str) -> dict:
    js = _get_json(ITEM_API.format(item_id=item_id))
    if js is None:
        raise YahooItemUnavailableError(f"item not found (404): {item_id}", state="削除")
    return js


def check_yahoo_item_status(item_id: str) -> Tuple[str, Optional[int]]:
    """
    check_remaining_ebay.py 用。戻り値は (status, price_jpy)。
    OPEN → 販売中、SOLD → 売り切れ、404 → YahooItemUnavailableError（呼び出し側で「削除」）。
    それ以外の状態（実物を確認できていない CLOSE・HOLD 等）は 判定不可（出品維持・次回再確認）。
    """
    js = fetch_item_json(item_id)
    status = js.get("status")
    if status == "SOLD":
        return "売り切れ", None
    if status == "OPEN" and js.get("price") is not None:
        return "販売中", int(js["price"])
    return "判定不可", None


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    """'2026-11-03T21:48:36+09:00' → このマシンの現地時刻（タイムゾーンなし。メルカリの fromtimestamp と同じ扱い）"""
    if not s:
        return None
    return datetime.fromisoformat(s).astimezone().replace(tzinfo=None)


def _valid_images(images: List[dict]) -> List[str]:
    """500px 以上の画像だけ。サイズが応答に無い（-1）画像は取得して確かめる。"""
    out: List[str] = []
    for im in images:
        url = (im or {}).get("url")
        if not url or not url.startswith("http"):
            continue
        w, h = im.get("width") or -1, im.get("height") or -1
        if w <= 0 or h <= 0:
            try:
                r = _session.get(url, timeout=10)
                r.raise_for_status()
                w, h = Image.open(BytesIO(r.content)).size
            except Exception as e:
                print(f"[IMAGE ERROR] {url} {e}", flush=True)
                continue
        if w >= IMAGE_MIN_PX and h >= IMAGE_MIN_PX:
            out.append(url)
        if len(out) >= IMAGE_LIMIT:
            break
    return out


def parse_detail_yahoo(item_id: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """
    parse_detail_personal / parse_detail_rakuma と同じ形の rec dict を返す。
    販売中でなければ YahooItemUnavailableError（state は 削除・売り切れ・判定不可）。
    """
    js = fetch_item_json(item_id)
    status = js.get("status")
    if status == "SOLD":
        raise YahooItemUnavailableError(f"status=SOLD: {item_id}", state="売り切れ")
    if status != "OPEN":
        raise YahooItemUnavailableError(f"status={status}: {item_id}", state="判定不可")

    images = _valid_images(js.get("images") or [])
    if not images:
        # メルカリと同じ文言（出品候補の条件で恒久的に除外される）
        raise Exception("no valid images (all <500px or error)")

    seller = js.get("seller") or {}
    rating = seller.get("rating") or {}
    return {
        "vendor_name": vendor_name,
        "title_jp": js.get("title"),
        "title_en": "",
        "price": int(js["price"]) if js.get("price") is not None else None,
        "vendor_created_at": _parse_dt(js.get("createDate")),
        # ヤフオク由来の商品（type=auction）には更新日時が無い → None（古い更新の判定対象外。ラクマと同じ）
        "vendor_updated_at": _parse_dt(js.get("updateDate")),
        "shipping_region": js.get("location"),
        "shipping_days": (js.get("deliverySchedule") or {}).get("text"),
        "seller_id": seller.get("id"),
        "seller_name": seller.get("nickname"),
        "rating_count": int(rating.get("total") or 0),
        "num_likes": int(js.get("likeCount") or 0),
        "images": images,
        "preset": preset,
        "description": (js.get("description") or "").strip(),
        "description_en": "",
        "item_attributes": [],
        "item_condition_id": ITEM_CONDITION_MAP.get(((js.get("condition") or {}).get("key") or "").lower()),
        "is_variant": False,
        "variants": [],
    }
