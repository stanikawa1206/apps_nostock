# -*- coding: utf-8 -*-
"""
2026-11-16 の改修のテスト。
  1. 縦横とも500pxに足りない写真を、縦横比を保って拡大して使う（listing_images）。
     拡大した画像は R2 に置き、その URL を eBay に渡す（build_pic_urls。NORMAL・CDN の両方）。
  2. 画像不足などで解析失敗にしても、API から取れたショップID（seller_id）を trx.vendor_item に残す。

DB を使うテストは、テスト専用DB（nostock_test_imgup）に本番と同じ列の trx.vendor_item を作って行う
（本番DBからは列の定義を読むだけ）。eBay・R2 には接続しない（R2 は put_object を受け取る偽物）。

実行: python -m unittest apps.tests.test_listing_images -v
"""
import contextlib
import os
import unittest
from io import BytesIO
from unittest import mock

from PIL import Image, ImageDraw

from apps.common.utils import get_sql_server_connection
from apps.publish import listing_images as li
from apps.publish import publish_ebay as pe

TEST_DB = "nostock_test_imgup"


def jpeg(w, h):
    img = Image.new("RGB", (w, h), (240, 240, 240))
    d = ImageDraw.Draw(img)
    d.ellipse((w * 0.1, h * 0.1, w * 0.9, h * 0.5), outline=(200, 0, 0), width=3)  # 歪めば楕円の縦横比が変わる
    buf = BytesIO()
    img.save(buf, "JPEG", quality=95)
    return buf.getvalue()


class FakeResp:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


def fake_get(sizes):
    """URL → 画像の大きさ。None の URL は取得エラー。"""
    def _get(url, timeout=None):
        if sizes[url] is None:
            raise OSError("connection reset")
        return FakeResp(jpeg(*sizes[url]))
    return _get


class FakeR2:
    def __init__(self):
        self.put = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.put[Key] = Body


# ============================================================
# 1. 拡大の大きさ・選別・拡大した画像
# ============================================================
class UpscaleRulesTest(unittest.TestCase):
    def test_upscaled_size(self):
        self.assertEqual(li.upscaled_size(429, 600), (500, 699))
        self.assertEqual(li.upscaled_size(438, 600), (500, 685))
        self.assertEqual(li.upscaled_size(600, 450), (667, 500))
        self.assertEqual(li.upscaled_size(480, 480), (500, 500))
        self.assertEqual(li.upscaled_size(499, 1080), (500, 1082))

    def test_select_keeps_order_and_marks_upscale(self):
        sizes = {"big": (600, 600), "magi": (429, 600), "edge": (400, 600), "tiny": (399, 600),
                 "err": None, "wide": (640, 480)}
        with mock.patch.object(li.requests, "get", side_effect=fake_get(sizes)), mock.patch("builtins.print"):
            images, upscale = li.select_listing_images(list(sizes))
        self.assertEqual(images, ["big", "magi", "edge", "wide"])           # 元の順。tiny(1.25倍超)・err は使わない
        self.assertEqual(upscale, {"magi": (429, 600), "edge": (400, 600), "wide": (640, 480)})

    def test_all_too_small_is_empty(self):
        with mock.patch.object(li.requests, "get", side_effect=fake_get({"a": (300, 300)})), mock.patch("builtins.print"):
            self.assertEqual(li.select_listing_images(["a"]), ([], {}))

    def test_upscale_image_keeps_aspect(self):
        for w, h in ((429, 600), (438, 600), (600, 450), (400, 600)):
            out = Image.open(BytesIO(li.upscale_image(jpeg(w, h))))
            self.assertEqual(out.format, "JPEG")
            self.assertGreaterEqual(min(out.size), 500)
            self.assertEqual(out.size, li.upscaled_size(w, h))
            self.assertAlmostEqual(out.size[0] / out.size[1], w / h, delta=0.002)  # 切り抜き・余白・歪みなし

    def test_upscale_image_refuses_over_limit(self):
        with self.assertRaises(ValueError):
            li.upscale_image(jpeg(399, 600))

    def test_upload_puts_upscaled_bytes(self):
        r2 = FakeR2()
        with mock.patch.object(li.requests, "get", side_effect=fake_get({"u": (429, 600)})), mock.patch("builtins.print"):
            url = li.upload_upscaled_image(r2, "b", "https://pub.example/", "u", "SKU/upscaled_1.jpg")
        self.assertEqual(url, "https://pub.example/SKU/upscaled_1.jpg")
        self.assertEqual(Image.open(BytesIO(r2.put["SKU/upscaled_1.jpg"])).size, (500, 699))


# ============================================================
# 2. eBay に渡す URL（build_pic_urls）
# ============================================================
class BuildPicUrlsTest(unittest.TestCase):
    SIZES = {"https://m/big.jpg?1": (600, 800), "https://m/small.jpg?2": (429, 600)}

    def build(self, rec, mode, r2, cdn_cache=None):
        with mock.patch.object(li.requests, "get", side_effect=fake_get(self.SIZES)), \
                mock.patch.object(pe.requests, "get", side_effect=fake_get(self.SIZES)), \
                mock.patch("builtins.print"):
            return pe.build_pic_urls(rec=rec, sku="SKU", image_mode=mode, r2=r2, r2_bucket="b",
                                     r2_public_base="https://pub.example", cdn_cache={} if cdn_cache is None else cdn_cache)

    def rec(self):
        with mock.patch.object(li.requests, "get", side_effect=fake_get(self.SIZES)), mock.patch("builtins.print"):
            images, upscale = li.select_listing_images(list(self.SIZES))
        return {"images": images, "upscale_images": upscale}

    def test_normal_sends_upscaled_image_for_small_only(self):
        r2, rec = FakeR2(), self.rec()
        urls = self.build(rec, "NORMAL", r2)
        self.assertEqual(urls, ["https://m/big.jpg?1", "https://pub.example/SKU/upscaled_2.jpg"])
        self.assertEqual(list(r2.put), ["SKU/upscaled_2.jpg"])                      # 500px以上の写真は置かない
        self.assertEqual(Image.open(BytesIO(r2.put["SKU/upscaled_2.jpg"])).size, (500, 699))
        self.assertEqual(self.build(rec, "NORMAL", r2), urls)                      # やり直しでは置き直さない
        self.assertEqual(len(r2.put), 1)

    def test_cdn_uses_upscaled_image(self):
        r2, rec = FakeR2(), self.rec()
        urls = self.build(rec, "CDN", r2)
        self.assertEqual(urls, ["https://pub.example/SKU/1.jpg", "https://pub.example/SKU/upscaled_2.jpg"])
        self.assertEqual(Image.open(BytesIO(r2.put["SKU/1.jpg"])).size, (600, 800))        # 元のまま
        self.assertEqual(Image.open(BytesIO(r2.put["SKU/upscaled_2.jpg"])).size, (500, 699))  # 小さい元画像ではない

    def test_no_upscale_unchanged(self):
        r2 = FakeR2()
        rec = {"images": ["https://m/big.jpg?1"]}  # 変更前と同じ rec（upscale_images なし）
        self.assertEqual(self.build(rec, "NORMAL", r2), ["https://m/big.jpg?1"])
        self.assertEqual(r2.put, {})

    def test_r2_failure_raises_instead_of_sending_small_image(self):
        r2 = mock.MagicMock()
        r2.put_object.side_effect = RuntimeError("R2 down")
        with self.assertRaises(RuntimeError):
            self.build(self.rec(), "NORMAL", r2)


# ============================================================
# 3. 解析失敗でも seller_id を残す（テスト専用DBの trx.vendor_item）
# ============================================================
def _conn(db):
    with mock.patch.dict(os.environ, {"DB_NAME": db}):
        return get_sql_server_connection()


class SellerIdOnParseFailureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prod_db = os.environ.get("DB_NAME") or _conn_db_name()
        assert cls.prod_db != TEST_DB
        with contextlib.closing(_conn("master")) as c:
            c.autocommit = True
            c.cursor().execute(f"IF DB_ID('{TEST_DB}') IS NULL CREATE DATABASE {TEST_DB}")
        with contextlib.closing(_conn(TEST_DB)) as c:
            c.autocommit = True
            cur = c.cursor()
            cur.execute("SELECT DB_NAME()")
            assert cur.fetchone()[0] == TEST_DB  # 本番DBで実行しないことの確認
            cur.execute("IF SCHEMA_ID('trx') IS NULL EXEC('CREATE SCHEMA trx')")
            cur.execute("IF OBJECT_ID('trx.vendor_item') IS NOT NULL DROP TABLE trx.vendor_item")
            cur.execute(f"SELECT TOP 0 * INTO trx.vendor_item FROM [{cls.prod_db}].trx.vendor_item")  # 列の定義だけ写す
        cls._old_db = os.environ.get("DB_NAME")
        os.environ["DB_NAME"] = TEST_DB  # setUp の mock.patch.stopall で戻らないよう、patch を使わない

    @classmethod
    def tearDownClass(cls):
        if cls._old_db is None:
            os.environ.pop("DB_NAME", None)
        else:
            os.environ["DB_NAME"] = cls._old_db
        with contextlib.closing(_conn("master")) as c:
            c.autocommit = True
            c.cursor().execute(f"IF DB_ID('{TEST_DB}') IS NOT NULL BEGIN "
                               f"ALTER DATABASE {TEST_DB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {TEST_DB}; END")

    def setUp(self):
        self.conn = get_sql_server_connection()
        self.addCleanup(self.conn.close)
        cur = self.conn.cursor()
        cur.execute("SELECT DB_NAME()")
        self.assertEqual(cur.fetchone()[0], TEST_DB)
        cur.execute("DELETE FROM trx.vendor_item")
        self.conn.commit()
        mock.patch("builtins.print").start()
        mock.patch.object(pe, "detect_status_from_mercari_shops", return_value=("販売中", 1000)).start()
        self.addCleanup(mock.patch.stopall)

    def add_item(self, vendor, sku, seller_id=None):
        self.conn.cursor().execute(
            "INSERT INTO trx.vendor_item (vendor_name, vendor_item_id, seller_id, status, created_at, num_likes) "
            "VALUES (?, ?, ?, N'販売中', SYSDATETIME(), 0)", vendor, sku, seller_id)
        self.conn.commit()

    def row(self, sku):
        r = self.conn.cursor().execute(
            "SELECT seller_id, status, [出品状況], [出品状況詳細] FROM trx.vendor_item WHERE vendor_item_id = ?", sku).fetchone()
        self.conn.commit()
        return tuple(r)

    def heavy(self, vendor, sku):
        return pe.heavy_check_detail(
            self.conn, mock.MagicMock(), f"https://example/{sku}", sku, "preset", vendor, "mode",
            "brand", "1", "d", "t", "トレカ", {}, 0, 1, 2, mock.MagicMock(), 1,
        )

    def shops_api(self, shop_name, photos):
        res = {"productDetail": {"shop": {"name": shop_name, "displayName": "x"}, "photos": photos},
               "displayName": "t", "price": 1000}
        return mock.patch.object(pe, "fetch_shops_api_data", return_value=(res, None))

    def test_shops_image_shortage_keeps_seller_id(self):
        self.add_item("メルカリshops", "S1")
        with self.shops_api("SmmzwZhunXwzCqAPNfRRNb", ["p1"]), \
                mock.patch.object(li.requests, "get", side_effect=fake_get({"p1": (300, 400)})):
            res = self.heavy("メルカリshops", "S1")
        self.assertIsNone(res[0])
        self.assertEqual(self.row("S1"), ("SmmzwZhunXwzCqAPNfRRNb", "販売中", "解析失敗",
                                          "no valid images (all <500px or error)"))  # 出品状況・詳細は従来どおり

    def test_existing_seller_id_not_overwritten_when_not_found(self):
        self.add_item("メルカリshops", "S2", seller_id="OLDSHOP")
        with mock.patch.object(pe, "fetch_shops_api_data", return_value=(None, None)):  # API が取れない＝ショップ不明
            self.heavy("メルカリshops", "S2")
        self.assertEqual(self.row("S2")[:3], ("OLDSHOP", "販売中", "解析失敗"))

    def test_not_found_stays_null(self):
        self.add_item("メルカリshops", "S3")
        with self.shops_api("", ["p1"]), \
                mock.patch.object(li.requests, "get", side_effect=fake_get({"p1": (300, 400)})):
            self.heavy("メルカリshops", "S3")
        self.assertEqual(self.row("S3")[0], None)  # 空のショップIDを推測で埋めない

    def test_mercari_image_shortage_keeps_seller_id(self):
        self.add_item("メルカリ", "m1")
        res = {"result": "OK", "data": {"seller": {"id": 123456789}, "photos": ["p1"], "status": "on_sale"}}
        with mock.patch.object(pe, "fetch_mercari_api_data", return_value=(res, None)), \
                mock.patch.object(pe, "_parse_status_from_res", return_value=("販売中", 1000)), \
                mock.patch.object(li.requests, "get", side_effect=fake_get({"p1": (200, 200)})):
            self.heavy("メルカリ", "m1")
        self.assertEqual(self.row("m1")[:3], ("123456789", "販売中", "解析失敗"))


def _conn_db_name():
    with contextlib.closing(get_sql_server_connection()) as c:
        return c.cursor().execute("SELECT DB_NAME()").fetchone()[0]


if __name__ == "__main__":
    unittest.main()
