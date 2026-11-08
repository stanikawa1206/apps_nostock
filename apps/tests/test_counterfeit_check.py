# -*- coding: utf-8 -*-
"""
偽物ポリシー対策（2026-11-08 apps/publish/counterfeit_check.py）のテスト。
DB・eBay・OpenAI には接続しない。
実行: python -m unittest apps.tests.test_counterfeit_check -v
"""

import inspect
import unittest
from unittest import mock

from apps.publish import counterfeit_check as cc
from apps.publish import publish_ebay as pe


class FauxMaterialTest(unittest.TestCase):
    def check(self, before, after, jp=""):
        self.assertEqual(cc.fix_faux_material_en(before, jp), after)

    def test_material_fake_to_faux(self):
        self.check("Dior Fake Pearl CD Logo Necklace", "Dior Faux Pearl CD Logo Necklace")
        self.check("Dior GP×Fake Pearl/Flower/Earrings", "Dior GP×Faux Pearl/Flower/Earrings")
        self.check("LV Dynasty Bracelet M1497L Metal/Fake Pearl Gold", "LV Dynasty Bracelet M1497L Metal/Faux Pearl Gold")
        self.check("adorned with luxurious fake pearls", "adorned with luxurious faux pearls")
        self.check("Album Fake Leather Brown", "Album Faux Leather Brown")
        self.check("FAKE PEARL Necklace", "FAUX PEARL Necklace")
        self.check("Coat fake-fur collar", "Coat faux fur collar")
        self.check("Ring Fake Diamond Silver", "Ring Simulated Diamond Silver")

    def test_truncated_title_uses_japanese_material(self):
        self.check("Vivienne Westwood Necklace Jewelry Metal Fake", "Vivienne Westwood Necklace Jewelry Metal Faux Pearl",
                   jp="ネックレス ジュエリー メタル フェイクパール レディース")
        # 日本語に素材が無ければ変えない（後の判定で偽物疑いになる）
        self.check("Gucci Wallet Fake", "Gucci Wallet Fake", jp="グッチ 財布")

    def test_unrelated_words_unchanged(self):
        for t in ["Mistake Proof Wallet", "Fakey Bag", "Imitation Pearl Earrings", "Gucci 636167 FAKE NOT GG Supreme"]:
            self.check(t, t, jp="x")


class CounterfeitReasonTest(unittest.TestCase):
    def test_fake_not(self):
        self.assertIn("FAKE/NOT", cc.counterfeit_reason("良品 グッチ 636167 FAKE NOT GGスプリーム 二つ折り", "", ""))
        self.assertIn("FAKE/NOT", cc.counterfeit_reason("グッチ 財布", "FAKE/NOTの黄色のレタリング", "Gucci FAKE/NOT Wallet"))

    def test_japanese_title(self):
        self.assertIsNotNone(cc.counterfeit_reason("ルイヴィトン 2つ折り財布メンズN級品", "", ""))
        self.assertIsNotNone(cc.counterfeit_reason("ハイブランド風 バッグ", "", ""))
        self.assertIsNone(cc.counterfeit_reason("和風 レトロ風 巾着", "", ""))  # 「〇〇風」一般は対象外

    def test_admission_in_description(self):
        self.assertIsNotNone(cc.counterfeit_reason("財布", "本物かどうかわかりません。", ""))
        self.assertIsNotNone(cc.counterfeit_reason("財布", "鑑定はしておりません。真贋不明です。", ""))
        self.assertIsNotNone(cc.counterfeit_reason("財布", "鑑定していないので本物か分かりません", ""))
        self.assertIsNotNone(cc.counterfeit_reason("財布", "偽物の可能性もあります。", ""))
        self.assertIsNotNone(cc.counterfeit_reason("財布", "正規品ではありません。", ""))

    def test_not_appraised_alone_is_ok(self):
        # 「鑑定はしておりません」だけでは NG にしない（2026-11-08 miumiu カードケース）
        d = ("miumiuのカードケースです。 購入先は、プレミアムアウトレットのmiumiuの店舗です。 ⚠️鑑定はしておりません "
             "※販売証明書なし、箱なし、簡易包装です。")
        self.assertIsNone(cc.counterfeit_reason("miumiu カードケース", d, "miumiu card case"))
        self.assertIsNone(cc.counterfeit_reason("財布", "鑑定は受けていません。", ""))
        self.assertIsNotNone(cc.counterfeit_reason("財布", "真贋は保証できません。", ""))

    def test_conditional_boilerplate_is_ok(self):
        for d in [
            "※万が一偽物の場合は全額返金致します。",
            "万一コピー品であった場合、返品返金対応をさせていただきます。",
            "万が一、正規品ではないなど不具合があった場合は、返品・返金に対応",
            "製造元または第三者鑑定機関により正規品ではないと\n判断された場合や",
            "コピー品や真贋不明品は一切取り扱いません。",
            "全て真贋済みのお品物を出品させて頂いております。鑑定済みの確実な正規品（本物）です。",
            "1970年代のモデルにインスパイアされたピアス",
        ]:
            self.assertIsNone(cc.counterfeit_reason("ディオール ピアス", d, "Dior Earrings"), d)

    def test_english_title_after_faux(self):
        self.assertIsNone(cc.counterfeit_reason("ディオール フェイクパール", "", "Dior Fake Pearl Earrings"))
        self.assertIsNone(cc.counterfeit_reason("メタル フェイクパール", "", "Vivienne Westwood Necklace Metal Fake"))
        self.assertIn("Fake", cc.counterfeit_reason("グッチ 財布", "", "Fake Gucci Wallet"))
        self.assertIn("Replica", cc.counterfeit_reason("ロレックス", "", "Rolex Replica Watch"))


class PublishIntegrationTest(unittest.TestCase):
    def test_finalize_title_converts_faux(self):
        with mock.patch.object(pe, "TITLE_RULES", []):
            self.assertEqual(pe.finalize_title_en("Chanel Fake Pearl Earrings ★"), "Chanel Faux Pearl Earrings")
            self.assertEqual(pe.finalize_title_en("Necklace Jewelry Metal Fake", "メタル フェイクパール"),
                             "Necklace Jewelry Metal Faux Pearl")

    def test_heavy_check_has_counterfeit_step(self):
        src = inspect.getsource(pe.heavy_check_detail)
        self.assertIn('cf_reason = counterfeit_reason(jp_title, desc_jp, rec.get("title_en") or "")', src)
        self.assertIn('rec["listing_head"] = "NG(偽物疑い)"', src)
        self.assertIn("desc_en = fix_faux_material_en(clean_for_ebay(desc_en_raw))", src)
        # 偽物判定は英語タイトル確定の後、説明文生成の前
        self.assertLess(src.index("cf_reason = counterfeit_reason"), src.index("generate_ebay_description("))
        self.assertLess(src.index('rec["title_en"] = clean_for_ebay(ensure_title_labels(title_en'), src.index("cf_reason = counterfeit_reason"))


if __name__ == "__main__":
    unittest.main()
