# -*- coding: utf-8 -*-
"""
rakuma_search.py — ラクマ(fril.jp)検索結果ページ取得・パース

- Playwright/Seleniumを使わず、検索結果ページをプレーンHTTP GETで取得する。
- 商品詳細ページへはアクセスしない（一覧ページから取得できる情報のみ扱う）。
- 一覧ページからは vendor_item_id / title_jp / price のみ取得可能で、
  vendor_created_at / vendor_updated_at / item_condition_id は取得できない
  （これらは publish 時の商品詳細取得で判定する想定）。
"""

from __future__ import annotations
import re
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

RAKUMA_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

FETCH_TIMEOUT_SEC = 20

# ラクマへの HTTP アクセスの結果は rakuma_outage が記録し、障害中（5xx は全台、429 はその台）はアクセスしない。
# RakumaHttpError・TRANSIENT_HTTP_STATUSES・ERROR_BODY_HEAD_CHARS は以前ここにあった（呼び出し側のため名前を残す）。
from apps.adapters.rakuma_outage import (  # noqa: E402
    ERROR_BODY_HEAD_CHARS,
    TRANSIENT_HTTP_STATUSES,
    RakumaHttpError,
    RakumaPausedError,
    check_response,
    raise_if_paused,
)

# https://item.fril.jp/<32桁hex> から末尾のIDを取り出す
_ITEM_ID_PATTERN = re.compile(r"item\.fril\.jp/([0-9a-f]+)", re.IGNORECASE)


def rakuma_page_url(base_url: str, page_idx_zero: int) -> str:
    """fril.jpのページ送りは page=N（1始まり）。1ページ目はpage省略でも同じ結果。"""
    if page_idx_zero <= 0:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}page={page_idx_zero + 1}"


def fetch_rakuma_search_html(url: str) -> Optional[str]:
    """
    fril.jpは結果件数を超えたページ番号を要求すると404を返す
    （Mercari側のAPIのように空配列を返さない）。
    ページ範囲外＝「これ以上商品なし」として None を返す。
    それ以外の失敗は RakumaHttpError（レスポンス情報をログに出してから送出）。
    ログには応答の情報だけを出す（リクエストヘッダー・Cookie は出さない）。
    """
    raise_if_paused(url)  # 障害中は送らずに RakumaPausedError（RakumaHttpError の一種・再試行対象）
    resp = requests.get(
        url,
        headers={"User-Agent": RAKUMA_USER_AGENT},
        timeout=FETCH_TIMEOUT_SEC,
    )
    check_response(resp, url)  # 2xx / 404 以外は RakumaHttpError（5xx・429 は障害として記録）
    if resp.status_code == 404:
        return None
    return resp.text


def extract_items_from_rakuma_html(html: str) -> List[Dict[str, Any]]:
    """
    検索結果一覧のHTMLから商品情報を抽出する。
    戻り値の各要素: {vendor_item_id, title_jp, price}
    （created/updated/conditionは一覧からは取得不可のためキーを持たない）
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, Any]] = []

    for box in soup.select("div.item-box"):
        a = box.select_one("p.item-box__item-name a.link_search_title")
        if a is None:
            a = box.select_one("a.link_search_image")
        href = a["href"] if a and a.has_attr("href") else None
        if not href:
            continue

        m = _ITEM_ID_PATTERN.search(href)
        if not m:
            continue
        vendor_item_id = m.group(1)

        title_span = box.select_one("p.item-box__item-name a span")
        title_jp = title_span.get_text(strip=True) if title_span else None

        price: Optional[int] = None
        for sp in box.select("p.item-box__item-price span[data-content]"):
            dc = sp.get("data-content")
            if dc and dc.isdigit():
                price = int(dc)

        rows.append({
            "vendor_item_id": vendor_item_id,
            "title_jp": title_jp,
            "price": price,
        })

    return rows
