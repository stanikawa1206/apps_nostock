# -*- coding: utf-8 -*-
"""
トレカの出品前判定（単品以外・鑑定品）のテスト（2026-11-24）。DB・ネットワークには接続しない。
例は 11/24 に実際の商品詳細で確認したタイトル・属性の形。

実行: python -m unittest apps.tests.test_card_listing_check -v
"""
import unittest

from apps.publish import card_listing_check as cc
from apps.publish.publish_ebay import CARD_GRADE_VALUE_IDS, CARD_GRADER_VALUE_IDS, parse_card_grade_text


def attrs(**kv):
    return [{"text": k, "values": [{"text": v}]} for k, v in kv.items()]


def ng(vendor, title, a=None):
    return cc.card_listing_ng(vendor, title, a, parse_card_grade_text, CARD_GRADER_VALUE_IDS, CARD_GRADE_VALUE_IDS)


class NotSingleTest(unittest.TestCase):

    def test_kind_attr_single_passes_even_with_box_in_title(self):
        # 「BOX購入キャンペーン PROMO」は種別=シングルカード 1枚の単品プロモ
        self.assertIsNone(ng("メルカリ", "ピッピ：強化拡張パック「ドリームリーグ」BOX購入キャンペーン PROMO",
                             attrs(種別="シングルカード 1枚")))

    def test_kind_attr_not_single_is_ng(self):
        head, why = ng("メルカリ", "ポケモンカード 未開封", attrs(種別="BOX・デッキ・パック"))
        self.assertEqual(head, cc.NG_HEAD_NOT_SINGLE)
        self.assertIn("種別=", why)

    def test_title_without_kind_attr(self):
        for t in ["【まとめ売り】ラティオス AR SV7a 楽園ドラゴーナ 070/064 20枚",
                  "大会向け　センチュリオン　構築済みデッキ　遊戯王　まとめ売り",
                  "ポケモンカード 151 BOX シュリンク付き",
                  "Eevee and Umbreon 2枚セット プレミアムファイル"]:
            self.assertEqual(ng("メルカリ", t)[0], cc.NG_HEAD_NOT_SINGLE, t)

    def test_title_single_cards_without_kind_attr(self):
        for t in ["ピカチュウex SR スカーレット&バイオレット 拡張パック 超電ブレイカー",
                  "ヨコハマのピカチュウ：スペシャルBOX ポケモンセンターヨコハマ PROMO",
                  "リザードンVMAX SSR S4a シャイニースター 308/190",
                  "Mew PROMO 085/PCG-P マクドナルド"]:
            self.assertIsNone(ng("メルカリ", t), t)


class GradeTest(unittest.TestCase):

    def test_mercari_parsable_grade_passes(self):
        self.assertIsNone(ng("メルカリ", "リザードンVMAX SSR S4a psa10 シャイニースター",
                             attrs(種別="シングルカード 1枚", グレード="PSA10")))

    def test_mercari_grade_in_title_without_attr_is_ng(self):
        self.assertEqual(ng("メルカリ", "迷宮城の白銀姫 プリシク 遊戯王 PSA10",
                            attrs(種別="シングルカード 1枚"))[0], cc.NG_HEAD_GRADE)

    def test_mercari_unknown_grader_is_ng(self):
        # ARS は eBay の鑑定機関の値に無い
        self.assertEqual(ng("メルカリ", "ピカチュウ ARS10", attrs(種別="シングルカード 1枚", グレード="ARS10"))[0],
                         cc.NG_HEAD_GRADE)

    def test_shops_graded_is_ng_even_with_attr(self):
        a = attrs(種別="シングルカード 1枚", グレード="PSA8以下")
        self.assertEqual(ng("メルカリshops", "【PSA7】 ポケモンカード 1996 カメックス 旧裏", a)[0], cc.NG_HEAD_GRADE)
        self.assertEqual(ng("メルカリshops", "【鑑定品PCG10】いちげきウーラオスVMAX【HR】",
                            attrs(種別="シングルカード 1枚"))[0], cc.NG_HEAD_GRADE)

    def test_shops_ungraded_single_passes(self):
        self.assertIsNone(ng("メルカリshops", "いちげきウーラオスVMAX HR 085/070", attrs(種別="シングルカード 1枚")))

    def test_anshin_kantei_is_not_grade(self):
        self.assertIsNone(ng("メルカリ", "リザードン あんしん鑑定", attrs(種別="シングルカード 1枚")))


class ShopsAttributesTest(unittest.TestCase):

    def test_convert(self):
        detail = {"variants": [{"attributes": [
            {"id": "a", "title": "種別", "values": {"x": {"text": "シングルカード 1枚", "legacyId": "0"}}},
            {"id": "b", "title": "グレード", "values": {"y": {"text": "PSA8以下"}}},
        ]}]}
        self.assertEqual(cc.shops_attributes_to_item_attributes(detail), [
            {"text": "種別", "values": [{"text": "シングルカード 1枚"}]},
            {"text": "グレード", "values": [{"text": "PSA8以下"}]},
        ])
        self.assertEqual(cc.shops_attributes_to_item_attributes({}), [])


if __name__ == "__main__":
    unittest.main()
