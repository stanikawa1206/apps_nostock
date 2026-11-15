# -*- coding: utf-8 -*-
"""
メルカリShops の商品ページの状態判定（mercari_item_status.judge_shops_product_html / detect_status_from_mercari_shops）のテスト。

2026-11-15: 「購入手続きへ」のボタンが見つからないと無条件に売り切れにしていたため、
数量を選べる商品の「購入へ」ボタン（2JUgYuUVZySYRbdUCEPW8K、在庫残り3点）を売り切れと誤判定していた。
check_remaining では売り切れ = eBay の出品終了なので、根拠の無い売り切れは「判定不可」（出品維持）にする。

fixtures/mercari_shops/*.html は 2026-11-15 に実際のページ（Selenium の page_source）から #main だけを残し、
script・画像・判定に使わない属性を除いたもの。
ネットワーク・DB・eBay には接続しない。

実行: python -m unittest apps.tests.test_mercari_shops_status -v
"""

import sys
import unittest
from pathlib import Path
from unittest import mock

from apps.adapters import mercari_item_status as mis

FIX = Path(__file__).resolve().parent / "fixtures" / "mercari_shops"


def fixture(name):
    (path,) = FIX.glob(f"{name}_*.html")
    return path.read_text(encoding="utf-8")


def judge(html):
    with mock.patch("builtins.print"):
        return mis.judge_shops_product_html(html, url="https://jp.mercari.com/shops/product/x")


def page(*parts):
    return ('<html><body><main id="main"><div data-testid="product-detail-container">'
            '<div data-testid="product-price">¥3,980</div>' + "".join(parts) + "</div></main></body></html>")


BUY = "<button type='button'>購入手続きへ</button>"
BUY_HE = "<button type='button'>購入へ</button>"
BUY_DISABLED = "<button type='button' disabled data-testid='disabled-purchase-button'>購入手続きへ</button>"
SOLD = "<p data-testid='out-of-stock'>売り切れ</p>"
OUT_OF_STOCK = "<p data-testid='out-of-stock'>在庫切れ</p>"


class RealPagesTest(unittest.TestCase):
    """2026-11-15 に取得した実際のページ"""

    def test_buy_he_is_on_sale(self):
        # 今回の不具合の商品（数量選択あり・「購入へ」）。変更前は売り切れになっていた（実ページで再現済み）
        status, price = judge(fixture("buy_he"))
        self.assertEqual(status, "販売中")
        self.assertIsInstance(price, int)

    def test_buy_tetsuzuki_is_on_sale(self):
        self.assertEqual(judge(fixture("buy_tetsuzuki")), ("販売中", 4000))

    def test_sold_out(self):
        self.assertEqual(judge(fixture("sold1")), ("売り切れ", None))  # 「売り切れ」表示・ボタン無効
        self.assertEqual(judge(fixture("sold2")), ("売り切れ", None))  # 「在庫切れ」表示・ボタン無効

    def test_deleted(self):
        self.assertEqual(judge(fixture("deleted")), ("削除", None))


class RulesTest(unittest.TestCase):
    def test_buttons(self):
        self.assertEqual(judge(page(BUY)), ("販売中", 3980))
        self.assertEqual(judge(page(BUY_HE)), ("販売中", 3980))
        self.assertEqual(judge(page("<button> 購入へ </button>")), ("販売中", 3980))  # 空白の違い

    def test_sold_needs_explicit_mark(self):
        self.assertEqual(judge(page(BUY_DISABLED, SOLD)), ("売り切れ", None))
        self.assertEqual(judge(page(BUY_DISABLED, OUT_OF_STOCK)), ("売り切れ", None))
        self.assertEqual(judge(page(SOLD)), ("売り切れ", None))  # ボタンが無くても売り切れ表示があれば売り切れ

    def test_no_evidence_is_undetermined_not_sold(self):
        cases = {
            "ボタンも売り切れ表示も無い（表示の変更・読み込み途中）": page(),
            "知らない文言のボタンだけ": page("<button>カートに入れる</button>"),
            "ボタンが無効なだけ（売り切れ表示なし）": page(BUY_DISABLED),
            "aria-disabled のボタンだけ": page("<button aria-disabled='true'>購入へ</button>"),
            "有効なボタンと売り切れ表示の両方（矛盾）": page(BUY, SOLD),
            "out-of-stock の要素だが文言が違う": page(BUY_DISABLED, "<p data-testid='out-of-stock'>お知らせ</p>"),
        }
        for label, html in cases.items():
            with self.subTest(label):
                self.assertEqual(judge(html), ("判定不可", None))

    def test_other_items_are_ignored(self):
        # 「他の商品」欄は商品詳細の枠の中にあるが、別の商品の売り切れ表示・ボタンで判定しない
        other_sold = "<div data-testid='shopOtherItem-0'><p data-testid='out-of-stock'>売り切れ</p></div>"
        other_buy = "<div data-testid='shopOtherItem-1'><button>購入へ</button></div>"
        self.assertEqual(judge(page(BUY, other_sold)), ("販売中", 3980))
        self.assertEqual(judge(page(other_buy)), ("判定不可", None))
        # 実ページ（「購入へ」）の「他の商品」欄に売り切れの商品が並んでいても販売中のまま
        real = fixture("buy_he").replace("</main>", other_sold.replace("shopOtherItem-0", "shopOtherItem-9") + "</main>")
        self.assertEqual(judge(real)[0], "販売中")

    def test_load_failure_is_not_sold(self):
        # 読み込み失敗（空・別のページ）は従来どおり例外（売り切れにしない）
        for html in ("", "<html><body></body></html>", "<html><body><main id='main'>エラーが発生しました</main></body></html>"):
            with self.subTest(html=html[:30]):
                with self.assertRaises(RuntimeError):
                    judge(html)

    def test_deleted_texts(self):
        self.assertEqual(judge("<main id='main'><p>該当する商品は削除されています。</p></main>"), ("削除", None))


class DetectWithDriverTest(unittest.TestCase):
    """Selenium の driver から page_source を読んで判定する（待機はこのテストでは省く）"""

    def detect(self, html):
        driver = mock.MagicMock()
        driver.page_source = html
        driver.current_url = "https://jp.mercari.com/shops/product/x"
        with mock.patch.object(mis, "WebDriverWait"), mock.patch.object(mis, "_wait_product_price_ready"), \
                mock.patch.object(mis.time, "sleep"), mock.patch("builtins.print"):
            return mis.detect_status_from_mercari_shops(driver)

    def test_uses_page_source(self):
        self.assertEqual(self.detect(fixture("buy_he"))[0], "販売中")
        self.assertEqual(self.detect(fixture("sold2")), ("売り切れ", None))
        self.assertEqual(self.detect(page()), ("判定不可", None))


# ============================================================
# 呼び出し側（出品前の確認・在庫確認）
# ============================================================
class PublishCallerTest(unittest.TestCase):
    def test_parse_detail_shops(self):
        from apps.publish import publish_ebay as pe
        driver = mock.MagicMock()
        for status in ("判定不可", "売り切れ", "削除"):
            with self.subTest(status), mock.patch.object(pe, "detect_status_from_mercari_shops", return_value=(status, None)):
                with self.assertRaises(pe.MercariItemUnavailableError) as cm:
                    pe.parse_detail_shops(mock.MagicMock(), "u", "p", "メルカリshops", driver)
                self.assertEqual(cm.exception.state, status)  # 出品しない（判定不可は status=判定不可 で候補から外れ、検索で再び見えたら戻る）


class CheckRemainingCallerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # check_remaining_ebay は import 時に stdout をログへ差し替えるので、元に戻す
        _o, _e = sys.stdout, sys.stderr
        from apps.inventory import check_remaining_ebay as cr
        sys.stdout, sys.stderr = _o, _e
        cls.cr = cr

    def run_one(self, status):
        cr = self.cr
        row = {"vendor_name": "メルカリshops", "vendor_item_id": "2JUgYuUVZySYRbdUCEPW8K", "account": "貴文",
               "listing_id": "1", "preset": "p", "mode": None, "low_usd_target": 1.0, "high_usd_target": 2.0,
               "has_preset": True}
        with mock.patch.object(cr, "get_status", return_value=(status, None)), \
                mock.patch.object(cr, "get_vendor_item_price", return_value=3000), \
                mock.patch.object(cr, "handle_listing_delete") as delete, \
                mock.patch.object(cr, "update_vendor_item_price_and_status"), \
                mock.patch("builtins.print"):
            try:
                cr.process_status_and_sync(mock.MagicMock(), mock.MagicMock(), mock.MagicMock(), row, "w")
            except Exception:
                pass  # 価格改定など、ここで見ない後続の処理の失敗は問わない
        return delete

    def test_undetermined_keeps_listing(self):
        self.run_one("判定不可").assert_not_called()

    def test_sold_out_ends_listing(self):
        self.run_one("売り切れ").assert_called_once()

    def test_get_status_routes_shops_url_to_detect(self):
        cr = self.cr
        driver = mock.MagicMock()
        with mock.patch.object(cr, "detect_status_from_mercari_shops", return_value=("判定不可", None)) as d:
            self.assertEqual(cr.get_status(None, driver, "https://jp.mercari.com/shops/product/x"), ("判定不可", None))
        driver.get.assert_called_once_with("https://jp.mercari.com/shops/product/x")
        d.assert_called_once()


if __name__ == "__main__":
    unittest.main()
