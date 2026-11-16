# -*- coding: utf-8 -*-
"""
Yahoo!フリマを腕時計以外のグループ（谷川② デジカメ・ビデオカメラ・レンズ・ペン・製図用品）に広げる対応のテスト。
モックのみ（DB・Yahoo!フリマ・eBay には接続しない）。タイトルは 2026-11-16 の Yahoo!フリマの実商品（一部を縮めた）。
"""
import inspect
import unittest
from unittest import mock

from apps.adapters import yahoo_fleamarket as yf

WATCH, LENS, DRAFT, PEN, CAMERA = "腕時計", "レンズ", "製図用品", "ペン", "デジカメ"
MEN = 1647
BALLPOINT, FOUNTAIN, LENS_CAT, RULER, OTHER_DRAFT = 47303, 47315, 2465, 47521, 47520
PILOT, PELIKAN, NAMIKI_UNDER_PILOT, CANON = 102632, 101780, 999001, 100614

PRESETS = {
    (PILOT, BALLPOINT): "PILOTボールペンY",
    (PILOT, FOUNTAIN): "PILOT万年筆Y",
    (PELIKAN, FOUNTAIN): "ペリカン万年筆Y",
    (CANON, LENS_CAT): "キヤノン交換レンズY",
    (yf.NO_BRAND_ID, RULER): "直線、方眼定規Y",
    (yf.NO_BRAND_ID, OTHER_DRAFT): "その他製図用品Y",
}
PATHS = {PILOT: [PILOT], PELIKAN: [PELIKAN], NAMIKI_UNDER_PILOT: [PILOT, NAMIKI_UNDER_PILOT], CANON: [CANON]}


class WatchRulesOnlyForWatchTest(unittest.TestCase):
    """腕時計以外の判定は腕時計グループだけ（ペンの「ギフトボックス入り」等を除外しない）"""

    PEN_TITLES = [
        "クロス(cross) クラシックセンチュリー クラシックブラック プレミアムギフトボックス入り 高級",
        "o1174 希少な箱付新品同様品　MONTBLANC　モンブラン　ノブレス　ボールペン　ゴールド",
    ]

    def test_pen_titles_are_watch_ng_but_not_pen_ng(self):
        for t in self.PEN_TITLES:
            self.assertIsNotNone(yf.non_watch_reason(t, BALLPOINT), t)        # 腕時計の判定なら NG になる語
            self.assertIsNone(yf.listing_ng_reason(t, BALLPOINT, PEN), t)     # ペンでは判定しない

    def test_camera_titles_not_ng(self):
        for t in ["FUJIFILM X-T10 (+バッテリ+充電器,ほぼ新品)",
                  "Panasonic LUMIX DMC-GX1 ブラック レンズキットミラーレス一眼 元箱 説明書 ムック付"]:
            self.assertIsNone(yf.listing_ng_reason(t, 47734, CAMERA), t)

    def test_watch_still_ng(self):
        r = yf.listing_ng_reason("SEIKO 純正部品 時計ベルト 22mm", MEN, WATCH)
        self.assertTrue(r.startswith("腕時計以外"), r)

    def test_rec_description_only_for_watch(self):
        rec = {"title_jp": "PILOT カスタム74 万年筆", "description": "本体なし、箱のみの出品です", "yahoo_category_id": FOUNTAIN}
        self.assertIsNone(yf.listing_ng_reason_rec(rec, PEN))
        rec_w = {"title_jp": "SEIKO 腕時計", "description": "写真のコマのみです。時計本体は付属しません。", "yahoo_category_id": MEN}
        self.assertIn("腕時計以外", yf.listing_ng_reason_rec(rec_w, WATCH))

    def test_unknown_group_no_ng(self):
        self.assertIsNone(yf.listing_ng_reason("空箱のみ", 1, None))


class LensTypeTest(unittest.TestCase):
    ZOOM = [
        "OM SYSTEM Olympus M.Zuiko Digital 14-42mm F3.5-5.6 EZ ブラック",
        "11月11日限定♪Canon EF-M 55-200mm IS STM シルバー",
        "【限定値下・希少シルバー】パナソニックLUMIX G VARIO 14-140㎜F3.5-5.6ASPH.POWER O.I.S.",   # ㎜（NFKC で mm）
        "EF17-40mm F4L USM EF17-404L",
        "OLYMPUS 電動式パンケーキズームレンズ M.ZUIKO DIGITAL ED 14-42mm F3.5-5.6 EZ",          # 範囲があればズーム
        "Nikon NIKKOR ズームレンズ Z DX50-250mm 1:4.5-6.3 VR フィルター径62mm",                 # フィルター径は付属品ではない
        "新品同　Canon RF-S18-150mm F3.5-6.3 IS STM　フード付き",
        "SONY FE 24-70mm F2.8 GM SEL2470GM",
        "SONY SEL2470GM 美品",
        "OLYMPUS AF ZOOM 7-70mm 1:2.8-3.5",
    ]
    PRIME = [
        "Canon EF200mm F2.8L USM 初代 1.4倍テレコン付",                                        # テレコン付 → レンズ本体
        "CANON(キヤノン) レンズ　EF28mm F1.8 USM",
        "フジノン XF35mmF1.4 Xマウント 富士フイルム 単焦点",
        "Aランク極美品 FUJIFILM フジノンレンズ XF23mm f2.8 R WR 単焦点レンズ",
        "Nikon AF-S Micro NIKKOR 85mm f/3.5G ED VR 交換レンズ",
        "ZEISS Touit 1.8/32 Planar（SONY Eマウント）",
        "日本光学 W-ニッコール.C 3.5cm F=3.5 Sマウント",
        "【新品】 SIRUI シルイ Night Walker 16mm T1.2 Cine Eマウント シネマレンズ",
        "SONY E 35mm F1.8 OSS SEL35F18",
        "SONY SEL50F18F",
        "【付属品完備】HD PENTAX-DA 35mmF2.8 Macro Limited",
        "Voigtlander フォクトレンダー HELIAR 40mm f/2.8 と VM-E Close Focus Adapter のセット品",
    ]
    UNKNOWN = [
        "FUJIFILM フジノン テレコンバーター XF2X TC WR",
        "Canon EXTENDER EF1.4X III",
        "Nikon ニコン Fマウントアダプター 4x5大判カメラ用",
        "★SIGMA マウントコンバーター MC-11 キヤノン / sony Eマウント★",
        "キヤノン レンズフード ET-63（EF-S55-250mm用）",
        "LENSBABY Velvet 56mm レンズベビー",                                                  # F値なし
        "新商品！SONYミラーレス対応！超望遠レンズ！遠くの被写体にオススメ！",
        "Canon レンズ2本",
    ]

    def test_zoom(self):
        for t in self.ZOOM:
            self.assertEqual(yf.lens_type_from_title(t)[0], yf.LENS_TYPE_ZOOM, t)

    def test_prime(self):
        for t in self.PRIME:
            self.assertEqual(yf.lens_type_from_title(t)[0], yf.LENS_TYPE_PRIME, t)

    def test_unknown(self):
        for t in self.UNKNOWN:
            self.assertIsNone(yf.lens_type_from_title(t)[0], t)

    def test_values_match_existing_categories(self):
        # メルカリ・ラクマの presets_categories.type_ebay（846/1620=Prime Lenses、1255/1621=Zoom Lenses）と同じ値
        self.assertEqual((yf.LENS_TYPE_PRIME, yf.LENS_TYPE_ZOOM), ("Prime Lenses", "Zoom Lenses"))

    def test_ng_reason(self):
        self.assertIsNone(yf.listing_ng_reason("Canon EF 24-105mm F4L IS USM", LENS_CAT, LENS))
        self.assertTrue(yf.listing_ng_reason("Canon EXTENDER EF1.4X III", LENS_CAT, LENS).startswith("レンズ種別不明"))


class DraftingBoardTest(unittest.TestCase):
    NG = [
        "DRAPAS ドラパス ベニヤ製図板 簡易平行定規",
        "MUTOH UT-06 ライナーボード 製図板 平行定規 建築士試験対応 ケース",
        "製図版　平行定規 建築士",                    # 「版」の誤記
        "UCHIDA ウチダ 製図台 DB-3 平行定規 製図スタンド",
        "MUTOH Liner Board UM-06NB",
        "ステッドラー 平行定規　製図道具　建築士試験対策",
        "MAX 製図ボード A2",
        "武藤工業 ドラフター MH-2",
    ]
    OK = [
        "未使用・昭和レトロ MAXマグネットプレート10本フルセット平行定規用当時物",   # 製図板の部品（小物）
        "STAEDTLER マルス⚫︎方眼三角定規⚫︎30cm型 966 30",
        "TISUR チタン 定規 12cm 直尺 両目盛 レーザー刻印 測定ツール 8mm 六角レンチ 高級",
        "ステッドラー 製図用シャープペンシル 925 35",
    ]

    def test_ng(self):
        for t in self.NG:
            self.assertIsNotNone(yf.drafting_board_reason(t), t)
            self.assertIsNotNone(yf.listing_ng_reason(t, OTHER_DRAFT, DRAFT), t)

    def test_ok(self):
        for t in self.OK:
            self.assertIsNone(yf.listing_ng_reason(t, OTHER_DRAFT, DRAFT), t)

    def test_only_drafting_group(self):
        self.assertIsNone(yf.listing_ng_reason("製図板 A2", BALLPOINT, PEN))


class ResolveGroupsTest(unittest.TestCase):
    def setUp(self):
        p = mock.patch.object(yf, "get_brand_path", side_effect=lambda b, root: PATHS[b])
        p.start()
        self.addCleanup(p.stop)

    def test_pen_brand_and_child(self):
        self.assertEqual(yf.resolve_item_preset(PILOT, "PILOT カスタム", BALLPOINT, PRESETS, PILOT, PEN)[0], "PILOTボールペンY")
        # 子ブランド（preset なし）は親の preset（＝親の eBay カテゴリー）
        self.assertEqual(yf.resolve_item_preset(NAMIKI_UNDER_PILOT, "x", FOUNTAIN, PRESETS, PILOT, PEN)[0], "PILOT万年筆Y")

    def test_pen_no_preset_for_category(self):
        # ペリカンはボールペンの preset が無い → 保存しない（他ブランドの eBay カテゴリーに入れない）
        preset, reason = yf.resolve_item_preset(PELIKAN, "Pelikan ボールペン", BALLPOINT, PRESETS, PELIKAN, PEN)
        self.assertIsNone(preset)

    def test_gift_box_pen_is_kept(self):
        self.assertEqual(
            yf.resolve_item_preset(PILOT, "PILOT ボールペン ギフトボックス入り", BALLPOINT, PRESETS, PILOT, PEN)[0],
            "PILOTボールペンY")

    def test_lens(self):
        self.assertEqual(yf.resolve_item_preset(CANON, "Canon EF50mm F1.8 STM", LENS_CAT, PRESETS, CANON, LENS)[0], "キヤノン交換レンズY")
        preset, reason = yf.resolve_item_preset(CANON, "Canon EXTENDER EF1.4X III", LENS_CAT, PRESETS, CANON, LENS)
        self.assertIsNone(preset)
        self.assertIn("レンズ種別不明", reason)

    def test_no_brand_preset(self):
        # ブランドなしの preset: 商品のブランド（有無・未登録）に関係なくカテゴリーの preset
        for brand in (None, 12345, PILOT):
            self.assertEqual(
                yf.resolve_item_preset(brand, "STAEDTLER 方眼定規 30cm", RULER, PRESETS, yf.NO_BRAND_ID, DRAFT)[0],
                "直線、方眼定規Y")

    def test_no_brand_drafting_board_skipped(self):
        preset, reason = yf.resolve_item_preset(None, "MUTOH 製図板セット UM-09N", OTHER_DRAFT, PRESETS, yf.NO_BRAND_ID, DRAFT)
        self.assertIsNone(preset)
        self.assertIn("製図板", reason)

    def test_no_brand_preset_missing(self):
        self.assertIsNone(yf.resolve_item_preset(None, "x", 47522, PRESETS, yf.NO_BRAND_ID, DRAFT)[0])

    def test_brand_search_still_needs_brand(self):
        self.assertIsNone(yf.resolve_item_preset(None, "x", BALLPOINT, PRESETS, PILOT, PEN)[0])


class SearchParamsNoBrandTest(unittest.TestCase):
    def test_no_brand(self):
        p = yf.search_params(brand_id=yf.NO_BRAND_ID, category_id=RULER, status="open", min_price=5690, max_price=131550, offset=0)
        self.assertNotIn("brandIds", p)
        self.assertEqual(p["genreCategoryIds"], RULER)

    def test_brand(self):
        p = yf.search_params(brand_id=PILOT, category_id=BALLPOINT, status="sold", min_price=None, max_price=None, offset=0)
        self.assertEqual(p["brandIds"], PILOT)


class ScrapeWorkerGroupTest(unittest.TestCase):
    def test_load_preset_map_and_group(self):
        from apps.inventory import scrape_worker as sw
        conn = mock.MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [
            (101058, MEN, "SEIKOメンズ腕時計menY", "腕時計"),
            (PILOT, BALLPOINT, "PILOTボールペンY", "ペン"),
            (0, RULER, "直線、方眼定規Y", "製図用品"),
        ]
        presets, groups = sw.load_yahoo_preset_map(conn)
        self.assertEqual(presets[(0, RULER)], "直線、方眼定規Y")
        self.assertEqual(groups, {MEN: "腕時計", BALLPOINT: "ペン", RULER: "製図用品"})

    def test_resolve_page_passes_group(self):
        from apps.inventory import scrape_worker as sw
        items = [{"vendor_item_id": "z1", "brand_id": PILOT, "brand_name": "PILOT",
                  "title_jp": "PILOT ボールペン プレミアムギフトボックス入り"}]
        with mock.patch.object(yf, "get_brand_path", side_effect=lambda b, root: PATHS[b]):
            kept, moved, skipped = sw._yahoo_resolve_page(items, BALLPOINT, PRESETS, "PILOTボールペンY", PILOT, "ペン")
        self.assertEqual((len(kept), moved, skipped), (1, 0, 0))


class PublishYahooNgTest(unittest.TestCase):
    """出品前の NG: グループごとの listing_head と出品不可flg=1。腕時計の見出しは従来どおり。"""

    def run_heavy(self, rec, category_group):
        from apps.publish import publish_ebay as pe
        conn = mock.MagicMock()
        with mock.patch.object(pe.yahoo, "parse_detail_yahoo", return_value=dict(rec)), \
                mock.patch.object(pe, "upsert_vendor_item") as up, \
                mock.patch.object(pe, "_maybe_commit", side_effect=lambda c, n, b: n):
            out = pe.heavy_check_detail(
                conn, None, "url", "z1", "p", yf.VENDOR_NAME, "DDP", "", "3323", None, "", category_group,
                None, 0, 0, 999999, None, 3)
        return out, up, conn

    def test_lens_unknown(self):
        out, up, conn = self.run_heavy({"title_jp": "Canon EXTENDER EF1.4X III", "description": "", "yahoo_category_id": LENS_CAT}, LENS)
        self.assertIsNone(out[0])
        self.assertEqual(up.call_args[0][1]["listing_head"], "NG(レンズ種別不明)")
        sql = conn.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
        self.assertIn("出品不可flg = 1", sql)

    def test_drafting_board(self):
        out, up, _ = self.run_heavy({"title_jp": "MUTOH 製図板 A2", "description": "", "yahoo_category_id": OTHER_DRAFT}, DRAFT)
        self.assertEqual(up.call_args[0][1]["listing_head"], "NG(製図板)")

    def test_watch_head_unchanged(self):
        out, up, _ = self.run_heavy({"title_jp": "SEIKO 純正部品 時計ベルト 22mm", "description": "", "yahoo_category_id": MEN}, WATCH)
        self.assertEqual(up.call_args[0][1]["listing_head"], "NG(腕時計以外)")

    def test_lens_type_set_before_heavy_result(self):
        from apps.publish import publish_ebay as pe
        src = inspect.getsource(pe.heavy_check_detail)
        i_type = src.index("type_ebay, lens_why = yahoo.lens_type_from_title")
        self.assertLess(src.index("listing_ng_reason_rec"), i_type)
        self.assertLess(i_type, src.index('"type_ebay": type_ebay'))


if __name__ == "__main__":
    unittest.main()
