# -*- coding: utf-8 -*-
"""
rakuma_item_status.py — ラクマ(fril.jp)商品詳細ページ取得・パース

- Playwright/Seleniumを使わず、商品詳細ページをプレーンHTTP GETで取得する。
- 既存の parse_detail_shops / parse_detail_personal と同じ形の rec dict を返し、
  heavy_check_detail / upsert_vendor_item / post_to_ebay にそのまま渡せるようにする。

【購入申請の判定】
  <span class="item__icon request-required">すぐに購入可</span>
  が存在する → 購入申請なし（すぐ購入可能）→ 出品対象
  存在しない → 購入申請あり → 出品NG
  （class名の request-required から意味を逆推測しない。A/Bテストで確認済みの仕様）
"""

from __future__ import annotations
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

RAKUMA_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
FETCH_TIMEOUT_SEC = 20

# Mercariと共通の商品状態ラベル体系（ラクマ商品詳細ページも同じ日本語ラベルを使用）
RAKUMA_ITEM_CONDITION_MAP = {
    "新品、未使用": 1,
    "未使用に近い": 2,
    "目立った傷や汚れなし": 3,
    "やや傷や汚れあり": 4,
    "傷や汚れあり": 5,
    "全体的に状態が悪い": 6,
}

_SELLER_ID_PATTERN = re.compile(r"/user/(\d+)/")

# ラクマ商品詳細ページの <div class="time_ago"> 表記（例: "6分前" "約22時間前" "30日前"
# "約1ヶ月前" "1ヶ月前" "2ヶ月前" "6ヶ月前" "2年弱前" "3年以上前" "約4年前"）を判定する。
# ラクマには正確な更新日時が無いため、この文字列の単位・数値のみで判定し、
# 疑似的な日数へ換算はしない（メルカリの vendor_updated_at ベースの判定とは完全に分離）。
# 仕様: 分/時間/日は常にOK、ヶ月は1ヶ月まで(約1ヶ月前含む)OK・2ヶ月以上はNG、年は常にNG。
_UPDATE_AGO_PATTERN = re.compile(r"^(約)?(\d+)(分|時間|日|ヶ月|年)(弱|以上)?前$")


def is_rakuma_update_too_old(last_updated_str: Optional[str]) -> Optional[bool]:
    """
    True  = NG（2ヶ月前以上、または年単位）
    False = OK（分/時間/日、または1ヶ月前・約1ヶ月前）
    None  = 未取得または未知の形式（呼び出し側でNG扱いにしない。ログに残すだけ）
    """
    if not last_updated_str:
        return None
    m = _UPDATE_AGO_PATTERN.match(last_updated_str.strip())
    if not m:
        return None
    _approx, n_str, unit, _suf = m.groups()
    n = int(n_str)
    if unit in ("分", "時間", "日"):
        return False
    if unit == "ヶ月":
        return n >= 2
    if unit == "年":
        return True
    return None


class RakumaItemUnavailableError(Exception):
    """商品詳細ページが取得できない（削除済み等）場合に送出する。"""
    def __init__(self, message: str, state: str = "unavailable"):
        super().__init__(message)
        self.state = state


def _extract_detail_table(soup: BeautifulSoup) -> Dict[str, str]:
    table = soup.select_one("table.item__details")
    out: Dict[str, str] = {}
    if not table:
        return out
    for tr in table.select("tr"):
        th = tr.select_one("th")
        td = tr.select_one("td")
        if not th or not td:
            continue
        key = th.get_text(strip=True)
        val = td.get_text(strip=True)
        out[key] = val
    return out


def _extract_seller_id(soup: BeautifulSoup) -> Optional[str]:
    img = soup.select_one("img.user[data-original*='/user/']")
    if img and img.has_attr("data-original"):
        m = _SELLER_ID_PATTERN.search(img["data-original"])
        if m:
            return m.group(1)
    # フォールバック: ページ全体から /user/<id>/ を探す
    m = _SELLER_ID_PATTERN.search(str(soup))
    return m.group(1) if m else None


def _extract_seller_name(soup: BeautifulSoup) -> Optional[str]:
    el = soup.select_one("p.header-shopinfo__shop-name span")
    if el:
        return el.get_text(strip=True)
    el = soup.select_one("p.header-shopinfo__user-name")
    return el.get_text(strip=True) if el else None


def _extract_rating_count(soup: BeautifulSoup) -> int:
    """
    「取引の評価」の良い(icon_review_sun)件数をMercariのrating_count相当として使う。
    レビュー欄が存在しない（取引実績なし）場合は0。
    """
    block = soup.select_one("div.header-shopinfo__review_counts")
    if not block:
        return 0
    good = block.select_one("li i.icon_review_sun")
    if not good:
        return 0
    span = good.find_next_sibling("span")
    if not span:
        return 0
    text = span.get_text(strip=True).replace(",", "")
    return int(text) if text.isdigit() else 0


def _extract_num_likes(html: str) -> int:
    m = re.search(r"いいね\s*([\d,]+)\s*件", html)
    if not m:
        return 0
    return int(m.group(1).replace(",", ""))


def _extract_images(soup: BeautifulSoup) -> List[str]:
    urls: List[str] = []
    for img in soup.select("img.sp-image"):
        src = img.get("src")
        if src and src.startswith("http") and src not in urls:
            urls.append(src)
    return urls


def _extract_ld_json_product(soup: BeautifulSoup) -> Optional[dict]:
    for script in soup.select("script[type='application/ld+json']"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type") == "Product":
            return data
    return None


def fetch_rakuma_item_html(url: str) -> str:
    resp = requests.get(
        url,
        headers={"User-Agent": RAKUMA_USER_AGENT},
        timeout=FETCH_TIMEOUT_SEC,
    )
    if resp.status_code == 404:
        raise RakumaItemUnavailableError(f"item not found (404): {url}", state="deleted")
    resp.raise_for_status()
    return resp.text


def check_rakuma_item_status(url: str) -> "tuple[str, Optional[int]]":
    """
    check_remaining_ebay.py 用の在庫確認。
    Mercari側の get_status()/detect_status_from_mercari() と同じ契約:
    戻り値は (status, price_jpy)。status は既存の Status 語彙
    （"販売中" / "売り切れ" / "削除" / "判定不可"）に合わせる。

    実際に確認した仕様:
    - 削除済み商品は 404 になる → RakumaItemUnavailableError を送出（呼び出し側で処理）
    - 売り切れ商品も詳細ページ自体は200で残るが、購入ボタン(a.btn_buy)が無くなり、
      代わりに <span class="type-modal__contents--button--sold">SOLD OUT</span> が表示される
      （JSON-LDのofferisAvailability等は更新されず当てにならないため使わない）
    - <span class="item__icon request-required">すぐに購入可</span> の有無（購入申請仕様）は
      売り切れ判定とは独立した別要素。存在しない(=購入申請あり)場合は、
      即時購入できない状態のため、check_remaining上も「売り切れ」と同様に
      eBay出品を終了させる対象として扱う（既存の終了系ステータス集合を変更せずに済むように
      既存の"売り切れ"へ寄せている）。
    """
    html = fetch_rakuma_item_html(url)  # 404ならRakumaItemUnavailableErrorが送出される
    soup = BeautifulSoup(html, "html.parser")

    sold_marker = soup.select_one("span.type-modal__contents--button--sold")
    buy_button = soup.select_one("a.btn_buy")
    if sold_marker is not None or buy_button is None:
        return "売り切れ", None

    purchase_ok_span = soup.select_one("span.item__icon.request-required")
    if purchase_ok_span is None:
        # 購入申請あり = 今は即時購入できない
        return "売り切れ", None

    price = None
    ld = _extract_ld_json_product(soup) or {}
    offers = ld.get("offers") if isinstance(ld.get("offers"), dict) else None
    if offers and offers.get("price") is not None:
        try:
            price = int(offers["price"])
        except (TypeError, ValueError):
            price = None
    if price is None:
        price_el = soup.select_one("p.item__value_area span.item__price")
        if price_el:
            digits = re.sub(r"[^\d]", "", price_el.get_text())
            price = int(digits) if digits else None

    return "販売中", price


def parse_detail_rakuma(url: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """
    parse_detail_shops / parse_detail_personal と同じ形の rec dict を返す。
    購入申請ありの商品も例外にはせず、rec["purchase_request_required"]=True として返す
    （NG判定自体は heavy_check_detail 側の他のNGチェックと同じ場所で行う）。
    """
    html = fetch_rakuma_item_html(url)
    soup = BeautifulSoup(html, "html.parser")

    ld = _extract_ld_json_product(soup) or {}

    title_el = soup.select_one("h1.item__name")
    title_jp = title_el.get_text(strip=True) if title_el else (ld.get("name") or None)

    description = ld.get("description")
    if description:
        description = description.strip()
    else:
        desc_el = soup.select_one("div.item__description__line-limited")
        description = desc_el.get_text("\n", strip=True) if desc_el else None

    price = None
    offers = ld.get("offers") if isinstance(ld.get("offers"), dict) else None
    if offers and offers.get("price") is not None:
        try:
            price = int(offers["price"])
        except (TypeError, ValueError):
            price = None
    if price is None:
        price_el = soup.select_one("p.item__value_area span.item__price")
        if price_el:
            digits = re.sub(r"[^\d]", "", price_el.get_text())
            price = int(digits) if digits else None

    detail = _extract_detail_table(soup)
    condition_text = detail.get("商品の状態")
    item_condition_id = RAKUMA_ITEM_CONDITION_MAP.get(condition_text)

    shipping_region = detail.get("発送元の地域")
    shipping_days = detail.get("発送日の目安")

    # 購入申請判定（仕様どおり: 存在=購入申請なし、不在=購入申請あり）
    purchase_ok_span = soup.select_one("span.item__icon.request-required")
    purchase_request_required = purchase_ok_span is None

    # 「N日前」等の相対表記のみ取得可能（正確な日時は取得不可のため
    # vendor_created_at/vendor_updated_at はNoneのまま呼び出し元に委ねる）
    time_ago_el = soup.select_one("div.time_ago")
    last_updated_str = time_ago_el.get_text(strip=True) if time_ago_el else None

    rec: Dict[str, Any] = {
        "vendor_name": vendor_name,
        "title_jp": title_jp,
        "title_en": "",
        "price": price,
        "vendor_created_at": None,
        "vendor_updated_at": None,
        "last_updated_str": last_updated_str,
        "shipping_region": shipping_region,
        "shipping_days": shipping_days,
        "seller_id": _extract_seller_id(soup),
        "seller_name": _extract_seller_name(soup),
        "rating_count": _extract_rating_count(soup),
        "num_likes": _extract_num_likes(html),
        "images": _extract_images(soup),
        "preset": preset,
        "description": description,
        "description_en": "",
        "item_attributes": [],
        "item_condition_id": item_condition_id,
        "purchase_request_required": purchase_request_required,
    }
    return rec
