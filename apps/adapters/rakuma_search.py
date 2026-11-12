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

# 時間をおけば回復する見込みのあるHTTPステータス。scrape_worker はこれらのときジョブを再試行待ちにする。
# 2026-10-28 01:06〜02:00 に fril.jp が全ワーカーへ 503 を返し、fetch_active の318ジョブが error のまま残った。
TRANSIENT_HTTP_STATUSES = {429, 500, 502, 503, 504}

# エラーログに残すレスポンス本文の先頭の文字数
ERROR_BODY_HEAD_CHARS = 300

# https://item.fril.jp/<32桁hex> から末尾のIDを取り出す
_ITEM_ID_PATTERN = re.compile(r"item\.fril\.jp/([0-9a-f]+)", re.IGNORECASE)


class RakumaHttpError(Exception):
    """検索結果ページの取得が 2xx / 404 以外で失敗した。原因調査用にレスポンスの情報を持つ。"""

    def __init__(self, url: str, status_code: int, retry_after: Optional[str],
                 server: Optional[str], content_type: Optional[str], body_head: str):
        self.url = url
        self.status_code = status_code
        self.retry_after = retry_after
        self.server = server
        self.content_type = content_type
        self.body_head = body_head
        super().__init__(
            f"RAKUMA_HTTP status={status_code} retry_after={retry_after} server={server} "
            f"content_type={content_type} url={url} body_head={body_head!r}"
        )

    @property
    def transient(self) -> bool:
        return self.status_code in TRANSIENT_HTTP_STATUSES


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
    resp = requests.get(
        url,
        headers={"User-Agent": RAKUMA_USER_AGENT},
        timeout=FETCH_TIMEOUT_SEC,
    )
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        err = RakumaHttpError(
            url=url,
            status_code=resp.status_code,
            retry_after=resp.headers.get("Retry-After"),
            server=resp.headers.get("Server"),
            content_type=resp.headers.get("Content-Type"),
            body_head=" ".join(resp.text[:ERROR_BODY_HEAD_CHARS].split()),
        )
        print(f"[RAKUMA HTTP ERROR] {err}", flush=True)
        raise err
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
