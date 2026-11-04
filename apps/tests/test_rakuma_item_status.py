# -*- coding: utf-8 -*-
"""
ラクマの在庫確認 check_rakuma_item_status のテスト（2026-11-04: 購入申請の有無を在庫判定に使わない）。

  - 購入申請なし・購入申請あり → 販売中（価格つき）
  - SOLD OUT → 売り切れ／404 → RakumaItemUnavailableError（呼び出し側で削除）
  - HTML異常（商品名・商品データ・購入ボタン・価格のどれかが無い）→ 判定不可（販売中にしない）
  - 404以外のHTTPエラー・通信エラー → 例外（販売中にしない）
HTMLは 2026-11-04 に取得した実ページの該当部分を縮めたもの。HTTP はモック。
実行: python -m unittest apps.tests.test_rakuma_item_status -v
"""

import unittest
from unittest import mock

import requests

from apps.adapters import rakuma_item_status as r

NAME = '<h1 class="item__name notranslate-otft">CITIZEN Eco-Drive ソーラー</h1>'
LD = ('<script type="application/ld+json">{"@context":"http://schema.org/","@type":"Product",'
      '"name":"CITIZEN Eco-Drive ソーラー","offers":{"@type":"Offer","price":"10600","priceCurrency":"JPY"}}</script>')
PRICE = ('<p class="item__value_area"><span class="item__price notranslate-otft">'
         '<span class="item__currency-symbol">¥</span>10,600</span></p>')
ICONS_OK = ('<div class="item-info__icons"><div class="item__icon">送料込</div>'
            '<div class="item__icon request-required">すぐに購入可</div></div>')
ICONS_REQUEST = '<div class="item-info__icons"><div class="item__icon">送料込</div></div>'
BUY = '<a class="ga-class ga-item-buybutton btn btn-primary btn-large btn_buy" href="https://fril.jp/users/sign_up">購入に進む</a>'
SOLD = '<span class="type-modal__contents--button--sold">SOLD OUT</span>'


def _page(*parts):
    return "<html><body>" + "".join(parts) + "</body></html>"


def _status(html):
    with mock.patch.object(r, "fetch_rakuma_item_html", return_value=html):
        return r.check_rakuma_item_status("https://item.fril.jp/x")


def _resp(status):
    resp = mock.Mock()
    resp.status_code = status
    resp.text = "<html></html>"
    resp.raise_for_status.side_effect = (
        requests.HTTPError(f"{status}") if status >= 400 else None
    )
    return resp


class CheckRakumaItemStatusTest(unittest.TestCase):
    def test_no_purchase_request_is_on_sale(self):
        self.assertEqual(_status(_page(NAME, LD, PRICE, ICONS_OK, BUY)), ("販売中", 10600))

    def test_purchase_request_is_on_sale(self):
        self.assertEqual(_status(_page(NAME, LD, PRICE, ICONS_REQUEST, BUY)), ("販売中", 10600))

    def test_price_from_page_when_ld_has_no_price(self):
        ld = LD.replace(',"offers":{"@type":"Offer","price":"10600","priceCurrency":"JPY"}', "")
        self.assertEqual(_status(_page(NAME, ld, PRICE, ICONS_REQUEST, BUY)), ("販売中", 10600))

    def test_sold_out(self):
        self.assertEqual(_status(_page(NAME, LD, PRICE, ICONS_OK, SOLD)), ("売り切れ", None))

    def test_sold_out_with_purchase_request(self):
        self.assertEqual(_status(_page(NAME, LD, PRICE, ICONS_REQUEST, SOLD)), ("売り切れ", None))

    def test_404_raises_unavailable(self):
        with mock.patch.object(r.requests, "get", return_value=_resp(404)):
            with self.assertRaises(r.RakumaItemUnavailableError) as cm:
                r.check_rakuma_item_status("https://item.fril.jp/x")
        self.assertEqual(cm.exception.state, "削除")

    def test_http_error_and_network_error_raise(self):
        for side_effect in ({"return_value": _resp(503)}, {"side_effect": requests.ConnectionError("x")},
                            {"side_effect": requests.Timeout("x")}):
            with mock.patch.object(r.requests, "get", **side_effect):
                with self.assertRaises(requests.RequestException):
                    r.check_rakuma_item_status("https://item.fril.jp/x")

    def test_html_anomalies_are_undetermined(self):
        cases = {
            "空のページ": "<html><body></body></html>",
            "商品名なし": _page(LD, PRICE, ICONS_OK, BUY),
            "商品データなし": _page(NAME, PRICE, ICONS_OK, BUY),
            "購入ボタンなし(バリエーション等)": _page(NAME, LD, PRICE, ICONS_OK),
            "価格なし": _page(NAME, LD.replace('"price":"10600",', ""), ICONS_OK, BUY),
            "メンテナンス等の別ページ": "<html><body><h1>ただいまメンテナンス中です</h1></body></html>",
        }
        for label, html in cases.items():
            with self.subTest(label):
                self.assertEqual(_status(html), ("判定不可", None))


if __name__ == "__main__":
    unittest.main()
