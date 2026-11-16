# -*- coding: utf-8 -*-
"""
過去の製図板データの整理（apps/publish/sql/drafting_board_cleanup_20261116.py）の対象判定のテスト。
タイトルは 2026-11-16 の trx.vendor_item の実商品。DB・eBay には接続しない。
"""
import unittest

from apps.publish.sql import drafting_board_cleanup_20261116 as c


class ClassifyTest(unittest.TestCase):
    TARGET = [
        ("m46684542538", "美品 MUTOH ムトー UT-06 A2 製図板 平行定規 ケース付"),
        ("m74326300202", "MUTOH Liner Board UM-06N3 製図板"),
        ("m21495989486", "TAKEDA A2平行定規 TPM-600N 製図用品セット"),              # その他M（5317）
        ("m69176169953", "STAEDTLER MARS 製図板 A4 ステッドラー　図面　定規　ドラフター"),
        ("m14913558377", "【中古】MUTOH製A2製図台・専用ケース"),
        ("m27406305559", "MUTOH ドラフター SAF-85 製図機器"),
        ("m35426766632", "【未使用に近い】⑪MUTOH UT-06 すぐ書けるA2平行定規ライナーボート"),
        ("m21355624564", "製図版ライナーノート MUTOH US-06A 並行定規 ケース付 建築士試験用"),
        ("2JG48NmhKRQNyTEdpyaK8m", "製図板　ビニアル　A0判　014-0025"),
        ("m59458129646", "【rotring ロットリング】新品未使用 rapid A3 製図板"),
        # 付属品の語があるが本体付き（FORCE_INCLUDE）
        ("m62019285348", "MAX ドラフティングマシンα MP-400FL 製図+A2製図用紙14枚付き"),
        ("m40781952894", "如*水様 MUTOH アーム式ドラフター & ドラフタースケール"),
        ("2JGgJEa6avAumkQDAFGo7e", "Kurflo マグネット製図板(A2-改良型) 高精度A2平行定規 600mm 透明アクリル製 32cm大型三角定規付き"),
    ]
    NOT_TARGET = [
        ("m38113223535", "MUTOH 製図用スケール 4本セット （ドラフター用）"),
        ("m72670331147", "MUTOH ドラフタースケール JL-12"),
        ("m51986138137", "ドラパス A2平行定規 専用携帯バッグ ブラック 黒"),
        ("m94056179506", "平行定規用バッグ　縦の肩掛けタイプ"),
        ("m69901423858", "未使用・昭和レトロ MAXマグネットプレート10本フルセット平行定規用当時物"),
        ("m96093983519", "ドラフティングテープ 11個まとめ売り"),
        ("m68145648880", "【英国王室御用達】CALLAGHAN LONDON 航海用平行定規 アンティーク"),
        ("m81222734207", "多機能 ドラフティングブラシ"),                              # 実カテゴリー 5314
        ("x1", "STAEDTLER MARS 方眼三角定規 30cm 966 30"),
        ("x2", "ステッドラー 製図用シャープペンシル 925 35"),
    ]

    def test_target(self):
        for iid, t in self.TARGET:
            self.assertTrue(c.classify(t, "", iid)[0], t)

    def test_not_target(self):
        for iid, t in self.NOT_TARGET:
            self.assertFalse(c.classify(t, "", iid)[0], t)

    def test_mercari_5314_is_not_target(self):
        self.assertEqual(c.classify("製図板 A2", "5314", "m1"), (False, "実カテゴリー5314（製図用ブラシ）"))

    def test_shops_url(self):
        self.assertEqual(c.item_url("メルカリshops", "2JG48NmhKRQNyTEdpyaK8m"),
                         "https://jp.mercari.com/shops/product/2JG48NmhKRQNyTEdpyaK8m")
        self.assertEqual(c.item_url("メルカリ", "m1"), "https://jp.mercari.com/item/m1")


class ChunkedInsertTest(unittest.TestCase):
    def test_split_under_1000_rows(self):
        sql = c.chunked_insert("@t (a)", [f"({i})" for i in range(1262)])
        stmts = [s for s in sql.split("INSERT INTO") if s.strip()]
        self.assertEqual(len(stmts), 3)
        self.assertTrue(all(s.count("\n    (") <= 500 for s in stmts))
        self.assertEqual(sql.count("(") - sql.count("@t (a)"), 1262)


if __name__ == "__main__":
    unittest.main()
