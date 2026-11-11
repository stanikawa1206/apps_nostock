# -*- coding: utf-8 -*-
"""
Yahoo!フリマ（apps/adapters/yahoo_fleamarket.py）のテスト（モックのみ。DB・Yahoo!フリマ・eBay には接続しない）。
  - 商品のブランドから preset を決める（子ブランドへの付け替え・タイトルでの付け替え・NGブランド）
  - 商品詳細の rec への変換、在庫確認の状態

実行: python -m unittest apps.tests.test_yahoo_fleamarket -v
"""
import json
import os
import sys
import unittest
from io import BytesIO
from unittest import mock

import requests
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
        self.assertEqual(self.resolve(SEIKO, "クレドール 腕時計 グランドセイコー"), "クレドールメンズ腕時計menY")

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
        with mock.patch.object(yf, "fetch_item_json", return_value=js):
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
        with mock.patch.object(yf, "fetch_item_json", return_value=js), \
                mock.patch.object(yf._session, "get", side_effect=lambda url, timeout: FakeResp(_jpeg(*next(sizes)))):
            rec = yf.parse_detail_yahoo("z717343356", "p", yf.VENDOR_NAME)
        self.assertEqual(len(rec["images"]), len(js["images"]) - 1)  # 400px の1枚を除く
        self.assertIsNotNone(rec["vendor_updated_at"])

    def test_sold_and_other_status(self):
        js = dict(_fixture("yahoo_item_auction.json"), status="SOLD")
        with mock.patch.object(yf, "fetch_item_json", return_value=js):
            with self.assertRaises(yf.YahooItemUnavailableError) as cm:
                yf.parse_detail_yahoo("x", "p", yf.VENDOR_NAME)
        self.assertEqual(cm.exception.state, "売り切れ")
        js = dict(js, status="HOLD")
        with mock.patch.object(yf, "fetch_item_json", return_value=js):
            with self.assertRaises(yf.YahooItemUnavailableError) as cm:
                yf.parse_detail_yahoo("x", "p", yf.VENDOR_NAME)
        self.assertEqual(cm.exception.state, "判定不可")

    def test_check_status(self):
        base = _fixture("yahoo_item_auction.json")
        with mock.patch.object(yf, "fetch_item_json", return_value=dict(base, status="OPEN", price=12000)):
            self.assertEqual(yf.check_yahoo_item_status("x"), ("販売中", 12000))
        with mock.patch.object(yf, "fetch_item_json", return_value=dict(base, status="SOLD")):
            self.assertEqual(yf.check_yahoo_item_status("x"), ("売り切れ", None))
        with mock.patch.object(yf, "fetch_item_json", return_value=dict(base, status="CLOSE")):
            self.assertEqual(yf.check_yahoo_item_status("x"), ("判定不可", None))
        with mock.patch.object(yf, "fetch_item_json", side_effect=yf.YahooItemUnavailableError("x", state="削除")):
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


class NonWatchTest(unittest.TestCase):
    """腕時計以外の判定。タイトルは 2026-11-11 の実商品（一部を縮めた）。"""

    NG = [
        "送料無料★56KS キングセイコー・GSグランドセイコー★金属製揺動レバー★カレンダー早送り修理用純正部品★parts number 981560",
        "HERMES エルメス 箱 BOX ボックス 時計用　時計ケース ケース　空箱　 O-N",
        "◆オメガ◆シーマスター◆コマ◆約２０ｍｍ幅◆1503/825◆300シリーズ◆F-S",
        "送料無料　美品　正規品　シャネル CHANEL J12 38mm メンズ腕時計　ベルト　パーツ　 黒ラバー　 コマ 駒　17mm",
        "◆オメガ◆約１６ｍｍ幅◆尾錠◆純正◆金色◆O-N",
        "◆オメガ◆ギャランティー◆カード◆2512.30◆シーマスター◆アクアテラ◆2512.30◆M-S",
        "TAGHeuer タグホイヤー ギャランティカードINTERNATIONAL GUARANTEE CARD",
        "CHANEL ギャラ J12 H0683 シャネル 時計 国際 保証書 ギャランティ カード 017 送料込み",
        "IWC 純正 アイダブリュシー 正規 ポートフィノ オートマチック 37 取扱説明書 冊子 Ref.3565 / 4581 PORTOFINO",
        "PANERAI 純正 オフィチーネ パネライ 正規 44mm 時計 保護 ケース カバー ベゼル ガード シリコン ルミノール マリーナ",
        "Cartier カルティエ ホワイトレザー製 ウォッチスタンド スタンダード",
        "◆オメガ◆スピードマスター◆自動巻◆3510.50◆風防◆ジェネリックパーツ◆063PW5274◎２F-S",
        "未使用　IWC　Pilot´s Watches インターナショナル パイロットウォッチ用　純正リューズ 7.1mm",
        "【送料無料・ベルトコマのみ】★BREITLING/ブライトリング・純正プロフェッショナルブレス/ステンレスベルト",
        "オメガ　ジュネーブダイナミック専用レザーベルト　メンズサイズ　ネイビー",
        "ROLEX ロレックス 時計ケース 携帯用 ウォッチケース 箱付き",
        "ブルガリ　Cal.712-MBBT クォーツムーブメント　回路稼動品　部品取り用",
    ]
    OK = [
        "稼働品 SEIKO メンズ 腕時計 ソーラー式 V176-0AF0",
        "DIESEL ディーゼル DZ-1730 自動巻き 替えベルト3本・工具付",
        "agnes b. アニエスベー 腕時計 本体 替えベルト2本付き",
        "【中古】Wicca ソーラーテック腕時計（箱・説明書・保証書・予備コマ付き）",
        "FOSSIL BIGTIC [JR-8122] 人気希少モデル アンティーク ウォッチ 箱無し",
        "RICOH COMMANDER リコー コマンダー ムーンフェイズ メンズ腕時計",
        "SEIKO セイコー スポーチュラ キネティック GMT　5M85-0AE0",
        "新品 タグ付 GA-2100HC-2AJF パープル 腕時計 ジーショック カーボンコアガード構造 G-SHOCK Gショック",
        "新品　J.HARRISON ジョンハリソン 腕時計 セラミック 4石 天然サファイア 18K金張りリューズ 紳士用 時計",
        "Invicta インビクタ 47967　47mm プロダイバー バスティオン オート スライドバックル式ゴールドメッシュ",
        "【極美品】CASIO G-SHOCK GM-S2110 メタルカバード パープルピンク×ホワイト",
        "COACH コーチ メンズ腕時計 ネイビー文字盤 レザーベルト 替えベルト付き 箱付き",
        "GUCCI グッチ 5500M メンズ　腕時計 ヴィンテージ 稼働　バックル/ベルト付属",
        "Burberry レクタングル時計 シルバー　シェル　14000 G 純正ベルト",
        "Paul Smith ポールスミス スクエア クロノグラフ メンズ腕時計 シルバー　取扱説明書",
        "稼動品GUCCI メンズ腕時計 本体のみ クォーツ デイト 9000G ベルトなし",
        "送料無料 特価 新品★SEIKO セイコー ALBA アルバ Riki リキ AKPK424 アラビア数字 レザ－バンド バックルタイプ メンズ腕時計",
    ]
    SMARTWATCH_OK = [
        "GARMIN　vivosport + 充電器",
        "HUAWEI Band ファーウェイバンド 11 Aluminum Edition バンド5本+フィルム・ケース 複数セット",
        "【美品】BT94% SE2 保護カバー＋バンド＋充電器セット 2019",
        "Apple Watch Nike Series 3 本体（充電器無し）",
    ]

    def test_ng(self):
        for t in self.NG:
            self.assertIsNotNone(yf.non_watch_reason(t, MEN), t)

    def test_ok(self):
        for t in self.OK:
            self.assertIsNone(yf.non_watch_reason(t, MEN), t)

    def test_smartwatch_ok(self):
        for t in self.SMARTWATCH_OK:
            self.assertIsNone(yf.non_watch_reason(t, yf.SMARTWATCH_CATEGORY_ID), t)

    def test_description(self):
        self.assertIsNotNone(yf.non_watch_reason_desc("こちらはスチック風防部分のみの出品になります。"))
        self.assertIsNotNone(yf.non_watch_reason_desc("写真のコマのみです。時計本体は付属しません。"))
        self.assertIsNotNone(yf.non_watch_reason_desc("腕時計型のディスプレイです。時計ではありません"))
        self.assertIsNone(yf.non_watch_reason_desc("付属品は箱のみです。稼働確認済み。"))
        self.assertIsNone(yf.non_watch_reason_desc("ベルトは社外品に交換済みです。"))

    def test_resolve_skips_non_watch(self):
        with mock.patch.object(yf, "get_brand_path", side_effect=lambda b, root: PATHS[b]):
            preset, reason = yf.resolve_item_preset(SEIKO, "SEIKO 純正部品 時計ベルト 22mm", MEN, PRESETS, SEIKO)
        self.assertIsNone(preset)
        self.assertIn("腕時計以外", reason)


class DepartmentTest(unittest.TestCase):
    def test_correct(self):
        d = yf.department_from_title
        self.assertEqual(d("SEIKO SPIRIT ladies wristwatch gold 5E31", "Men"), "Women")
        self.assertEqual(d("新品★SEIKO Riki AKQK463 レザーバンド レディース腕時計", "Men"), "Women")
        self.assertEqual(d("【稼働品】Paul Smith ポールスミス 1005 メンズクォーツ腕時計", "Women"), "Men")
        self.assertEqual(d("CASIO 腕時計 Men's ブラック", "Women"), "Men")

    def test_keep(self):
        d = yf.department_from_title
        self.assertEqual(d("SEIKO 腕時計 クォーツ", "Men"), "Men")                       # 記載なし
        self.assertEqual(d("アニエスベー 腕時計 メンズ レディース ユニセックス", "Men"), "Men")  # 兼用
        self.assertEqual(d("メンズ レディース 腕時計 2本セット", "Women"), "Women")        # 両方
        self.assertEqual(d("WOMEN'S watch", "Men"), "Women")                             # women's を men's と数えない
        self.assertEqual(d("レディース 腕時計", "Unisex Adults"), "Unisex Adults")       # ペア・スマートウォッチは補正しない


class FakeHttp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)

    def json(self):
        if isinstance(self._body, str):
            raise ValueError("not json")
        return self._body


class FetchItemJsonTest(unittest.TestCase):
    """削除と取得エラーの区別。応答は 2026-11-11 に実際に返った形。"""

    def get(self, resp):
        with mock.patch.object(yf._session, "get", return_value=resp):
            return yf.fetch_item_json("z1")

    def test_ok(self):
        self.assertEqual(self.get(FakeHttp(200, {"status": "OPEN"}))["status"], "OPEN")

    def test_deleted(self):
        body = {"error": {"code": "spbfi-0212-03-1000", "codeV2": "0212-03-1000", "name": "NotFoundError",
                          "statusCode": 404, "message": "商品情報の取得に失敗しました"}}
        with self.assertRaises(yf.YahooItemUnavailableError) as cm:
            self.get(FakeHttp(404, body))
        self.assertEqual(cm.exception.state, "削除")

    def test_auction_ended(self):
        body = {"error": {"code": "spbfi-0212-01-0119", "codeV2": "0212-01-0119", "name": "NotFoundError",
                          "statusCode": 404, "message": "商品が存在していません"}}
        with self.assertRaises(yf.YahooItemUnavailableError) as cm:
            self.get(FakeHttp(404, body))
        self.assertEqual(cm.exception.state, "削除")

    def test_other_404_is_error_not_deleted(self):
        for body in ({"statusCode": 404, "message": "指定されたリソースは見つかりませんでした", "error": "Not Found"},
                     {"error": {"codeV2": "0212-04-2001", "name": "NotFoundError"}},
                     "<html>Not Found</html>"):
            with self.assertRaises(yf.YahooHttpError):
                self.get(FakeHttp(404, body))

    def test_5xx_is_error(self):
        with self.assertRaises(yf.YahooHttpError):
            self.get(FakeHttp(503, "Service Unavailable"))


# check_remaining_ebay は import 時に stdout をログへ差し替えるので、元に戻す
_o, _e = sys.stdout, sys.stderr
from apps.inventory import check_remaining_ebay as cr  # noqa: E402
sys.stdout, sys.stderr = _o, _e


class CheckRemainingYahooTest(unittest.TestCase):
    URL = "https://paypayfleamarket.yahoo.co.jp/item/z123"

    def status(self, **kw):
        with mock.patch.object(yf, "check_yahoo_item_status", **kw):
            return cr.get_status(None, None, self.URL)

    def test_get_status(self):
        self.assertEqual(self.status(return_value=("販売中", 9000)), ("販売中", 9000))
        self.assertEqual(self.status(return_value=("売り切れ", None)), ("売り切れ", None))
        self.assertEqual(self.status(side_effect=yf.YahooItemUnavailableError("x", state="削除")), ("削除", None))
        self.assertEqual(self.status(side_effect=yf.YahooHttpError(self.URL, 503, "")), ("判定不可", None))
        self.assertEqual(self.status(side_effect=requests.ConnectionError("down")), ("判定不可", None))

    def run_sync(self, status, price, old_price=9000):
        row = {"vendor_name": yf.VENDOR_NAME, "vendor_item_id": "z123", "account": "谷川④", "listing_id": "1",
               "preset": "SEIKOメンズ腕時計menY", "mode": "DDP", "low_usd_target": 100.0, "high_usd_target": 300.0,
               "has_preset": True}
        conn = mock.MagicMock()
        page = mock.MagicMock()
        with mock.patch.object(cr, "get_status", return_value=(status, price)),                 mock.patch.object(cr, "get_vendor_item_price", return_value=old_price),                 mock.patch.object(cr, "update_vendor_item_price_and_status") as upd,                 mock.patch.object(cr, "handle_listing_delete") as dele,                 mock.patch.object(cr, "handle_listing_price_update") as price_up:
            cr.process_status_and_sync(conn, page, None, row, "test")
        return dele, price_up, upd

    def test_sold_and_deleted_end_listing(self):
        for st in ("売り切れ", "削除"):
            dele, _, _ = self.run_sync(st, None)
            dele.assert_called_once()
            self.assertEqual(dele.call_args.args[3], st)

    def test_undetermined_keeps_listing(self):
        dele, price_up, upd = self.run_sync("判定不可", None)
        dele.assert_not_called()
        price_up.assert_not_called()
        self.assertEqual(upd.call_args.args[4], "判定不可")

    def test_on_sale_same_price_keeps_listing(self):
        dele, price_up, _ = self.run_sync("販売中", 9000)
        dele.assert_not_called()
        price_up.assert_not_called()

    def test_on_sale_price_change_updates_ebay(self):
        dele, price_up, _ = self.run_sync("販売中", 12000)
        dele.assert_not_called()
        price_up.assert_called_once()


if __name__ == "__main__":
    unittest.main()
