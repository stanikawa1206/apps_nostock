# -*- coding: utf-8 -*-
"""
Yahoo!フリマ（apps/adapters/yahoo_fleamarket.py）のテスト（モックのみ。DB・Yahoo!フリマ・eBay には接続しない）。
  - 商品のブランドから preset を決める（子ブランドへの付け替え・タイトルでの付け替え・NGブランド）
  - 商品詳細の rec への変換、在庫確認の状態

実行: python -m unittest apps.tests.test_yahoo_fleamarket -v
"""
import json
import os
import unittest
from io import BytesIO
from unittest import mock

from PIL import Image

from apps.adapters import yahoo_fleamarket as yf

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

SEIKO, GS, CREDOR, ALBA_SEIKO, PROSPEX, GS_44 = 101058, 102478, 102541, 102475, 102488, 154435
FURLA, ALBA_UNDER_FURLA = 101720, 124244

PATHS = {
    SEIKO: [SEIKO],
    GS: [SEIKO, GS],
    GS_44: [SEIKO, GS, GS_44],
    CREDOR: [SEIKO, CREDOR],
    ALBA_SEIKO: [SEIKO, ALBA_SEIKO],
    PROSPEX: [SEIKO, PROSPEX],
    FURLA: [FURLA],
    ALBA_UNDER_FURLA: [FURLA, ALBA_UNDER_FURLA],
    999999: [999999],
}

MEN, WOMEN = 1647, 1645
PRESETS = {
    (SEIKO, MEN): "SEIKOメンズ腕時計menY",
    (GS, MEN): "グランドセイコーメンズ腕時計menY",
    (CREDOR, MEN): "クレドールメンズ腕時計menY",
    (ALBA_SEIKO, MEN): "アルバメンズ腕時計menY",
    (FURLA, MEN): "フルラメンズ腕時計menY",
    (SEIKO, WOMEN): "SEIKOレディース腕時計Y",
    # レディースに CREDOR の preset が無い場合を試すため (CREDOR, WOMEN) は入れない
}


class ResolvePresetTest(unittest.TestCase):
    def setUp(self):
        p = mock.patch.object(yf, "get_brand_path", side_effect=lambda b, root: PATHS[b])
        p.start()
        self.addCleanup(p.stop)

    def resolve(self, brand, title="", cat=MEN):
        return yf.resolve_item_preset(brand, title, cat, PRESETS, SEIKO)[0]

    def test_same_brand(self):
        self.assertEqual(self.resolve(SEIKO, "SEIKO 5 自動巻き"), "SEIKOメンズ腕時計menY")

    def test_child_brand_id(self):
        self.assertEqual(self.resolve(GS), "グランドセイコーメンズ腕時計menY")
        self.assertEqual(self.resolve(ALBA_SEIKO), "アルバメンズ腕時計menY")

    def test_grandchild_brand_id(self):
        # 44GS（未登録）→ 階層を上がって Grand Seiko
        self.assertEqual(self.resolve(GS_44), "グランドセイコーメンズ腕時計menY")

    def test_unregistered_sub_line_stays_parent(self):
        self.assertEqual(self.resolve(PROSPEX, "PROSPEX ダイバー"), "SEIKOメンズ腕時計menY")

    def test_title_grand_seiko(self):
        self.assertEqual(self.resolve(SEIKO, "Grand Seiko 9F クォーツ"), "グランドセイコーメンズ腕時計menY")
        self.assertEqual(self.resolve(SEIKO, "グランドセイコー SBGX"), "グランドセイコーメンズ腕時計menY")
        # 全角英字・全角スペース（NFKC で正規化）
        self.assertEqual(self.resolve(SEIKO, "ＧＲＡＮＤ　ＳＥＩＫＯ"), "グランドセイコーメンズ腕時計menY")

    def test_title_gs_alone_is_not_grand_seiko(self):
        self.assertEqual(self.resolve(SEIKO, "SEIKO GS 風 腕時計"), "SEIKOメンズ腕時計menY")

    def test_title_credor(self):
        self.assertEqual(self.resolve(SEIKO, "SEIKO CREDOR 5A70"), "クレドールメンズ腕時計menY")
        self.assertEqual(self.resolve(SEIKO, "セイコー クレドール レディース"), "クレドールメンズ腕時計menY")

    def test_title_both_first_wins(self):
        self.assertEqual(self.resolve(SEIKO, "クレドール 箱 グランドセイコー"), "クレドールメンズ腕時計menY")

    def test_title_rule_needs_child_preset_in_category(self):
        # レディースに CREDOR の preset が無い → SEIKO のまま
        self.assertEqual(self.resolve(SEIKO, "クレドール", cat=WOMEN), "SEIKOレディース腕時計Y")

    def test_title_rule_only_for_parent(self):
        # ALBA の商品はタイトルに CREDOR とあっても付け替えない（規則は SEIKO だけ）
        self.assertEqual(self.resolve(ALBA_SEIKO, "クレドール 風"), "アルバメンズ腕時計menY")

    def test_ng_brand_under_furla(self):
        preset, reason = yf.resolve_item_preset(ALBA_UNDER_FURLA, "ALBA 腕時計", MEN, PRESETS, FURLA)
        self.assertIsNone(preset)
        self.assertIn("124244", reason)

    def test_no_preset_in_path(self):
        self.assertIsNone(self.resolve(999999))

    def test_no_brand(self):
        self.assertIsNone(self.resolve(None))


class BrandTreeTest(unittest.TestCase):
    """子ブランドの一覧API は子の無いブランドで 404 になる。子の階層は親の階層から作る。"""

    API = {
        SEIKO: {"brandList": [{"id": SEIKO}], "brands": [
            {"id": GS, "hasChildren": True}, {"id": PROSPEX, "hasChildren": False}]},
        GS: {"brandList": [{"id": SEIKO}, {"id": GS}], "brands": [{"id": GS_44, "hasChildren": False}]},
    }

    def setUp(self):
        yf._brand_path_cache.clear()
        yf._crawled_roots.clear()
        self.calls = []

        def fake_get_json(url, params=None):
            bid = int(url.rstrip("/").split("/")[-2])
            self.calls.append(bid)
            return self.API.get(bid)  # 子の無いブランドは None（404）

        p = mock.patch.object(yf, "_get_json", side_effect=fake_get_json)
        p.start()
        self.addCleanup(p.stop)

    def test_paths_from_root(self):
        self.assertEqual(yf.get_brand_path(GS_44, SEIKO), [SEIKO, GS, GS_44])
        self.assertEqual(yf.get_brand_path(PROSPEX, SEIKO), [SEIKO, PROSPEX])
        self.assertEqual(yf.get_brand_path(SEIKO, SEIKO), [SEIKO])
        self.assertEqual(self.calls, [SEIKO, GS])  # 子のあるブランドだけ・1回ずつ

    def test_unknown_brand_after_crawl(self):
        self.assertEqual(yf.get_brand_path(888888, SEIKO), [888888])
        self.assertEqual(yf.get_brand_path(777777, SEIKO), [777777])
        self.assertEqual(self.calls, [SEIKO, GS])  # 同じ検索ブランドは2回たどらない

    def test_leaf_root(self):
        self.assertEqual(yf.get_brand_path(PROSPEX, PROSPEX), [PROSPEX])


def _fixture(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return json.load(f)


def _jpeg(w, h):
    buf = BytesIO()
    Image.new("RGB", (w, h)).save(buf, "JPEG")
    return buf.getvalue()


class FakeResp:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


class DetailTest(unittest.TestCase):
    def test_parse_auction_origin(self):
        js = _fixture("yahoo_item_auction.json")
        with mock.patch.object(yf, "_get_json", return_value=js):
            rec = yf.parse_detail_yahoo("h1245036394", "SEIKOメンズ腕時計menY", yf.VENDOR_NAME)
        self.assertEqual(rec["price"], 15700)
        self.assertEqual(rec["seller_id"], "p5073780")
        self.assertEqual(rec["rating_count"], 2194)
        self.assertEqual(rec["item_condition_id"], 3)
        self.assertEqual(rec["shipping_days"], "1〜2日で発送")
        self.assertIsNone(rec["vendor_updated_at"])          # ヤフオク由来は更新日時なし
        self.assertIsNotNone(rec["vendor_created_at"])
        self.assertEqual(len(rec["images"]), 10)             # サイズが応答にある（1200px等）
        self.assertTrue(rec["description"])

    def test_parse_sparkle_checks_unknown_image_size(self):
        js = _fixture("yahoo_item_z_sparkle.json")             # width/height = -1
        sizes = iter([(600, 600), (400, 400)] + [(800, 800)] * 30)
        with mock.patch.object(yf, "_get_json", return_value=js), \
                mock.patch.object(yf._session, "get", side_effect=lambda url, timeout: FakeResp(_jpeg(*next(sizes)))):
            rec = yf.parse_detail_yahoo("z717343356", "p", yf.VENDOR_NAME)
        self.assertEqual(len(rec["images"]), len(js["images"]) - 1)  # 400px の1枚を除く
        self.assertIsNotNone(rec["vendor_updated_at"])

    def test_sold_and_other_status(self):
        js = dict(_fixture("yahoo_item_auction.json"), status="SOLD")
        with mock.patch.object(yf, "_get_json", return_value=js):
            with self.assertRaises(yf.YahooItemUnavailableError) as cm:
                yf.parse_detail_yahoo("x", "p", yf.VENDOR_NAME)
        self.assertEqual(cm.exception.state, "売り切れ")
        js = dict(js, status="HOLD")
        with mock.patch.object(yf, "_get_json", return_value=js):
            with self.assertRaises(yf.YahooItemUnavailableError) as cm:
                yf.parse_detail_yahoo("x", "p", yf.VENDOR_NAME)
        self.assertEqual(cm.exception.state, "判定不可")

    def test_check_status(self):
        base = _fixture("yahoo_item_auction.json")
        with mock.patch.object(yf, "_get_json", return_value=dict(base, status="OPEN", price=12000)):
            self.assertEqual(yf.check_yahoo_item_status("x"), ("販売中", 12000))
        with mock.patch.object(yf, "_get_json", return_value=dict(base, status="SOLD")):
            self.assertEqual(yf.check_yahoo_item_status("x"), ("売り切れ", None))
        with mock.patch.object(yf, "_get_json", return_value=dict(base, status="CLOSE")):
            self.assertEqual(yf.check_yahoo_item_status("x"), ("判定不可", None))
        with mock.patch.object(yf, "_get_json", return_value=None):
            with self.assertRaises(yf.YahooItemUnavailableError) as cm:
                yf.check_yahoo_item_status("x")
        self.assertEqual(cm.exception.state, "削除")


class SearchParamsTest(unittest.TestCase):
    def test_params(self):
        p = yf.search_params(brand_id=101058, category_id=1647, status="open", min_price=5690, max_price=23670, offset=200)
        self.assertEqual(p["brandIds"], 101058)
        self.assertEqual(p["genreCategoryIds"], 1647)
        self.assertEqual(p["itemConditions"], "NEW,USED10,USED20,USED40")
        self.assertEqual((p["minPrice"], p["maxPrice"], p["offset"]), (5690, 23670, 200))
        self.assertEqual((p["sort"], p["order"]), ("openTime", "DESC"))


if __name__ == "__main__":
    unittest.main()
