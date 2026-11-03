# -*- coding: utf-8 -*-
"""
英語タイトルの最終整形（2026-11-03 復活）のテスト。

  - mst.title_replace_rules（TITLE_RULES）を main() の起動時に読み込む（2026-02-20 に読み込みが抜けていた）
  - 置換は大文字小文字を区別しない文字どおりの置換。英数字の側は単語の途中で置換しない
  - "like new" 系 → "Excellent"、★・絵文字などの装飾記号を削除。×・【】・° などは残す。Ivory は変えない
  - 新しく翻訳したタイトルにも、DB に保存済みのタイトルにも適用する

DB・eBay・OpenAI には接続しない。
実行: python -m unittest apps.tests.test_title_replace_rules -v
"""

import unittest
from unittest import mock

from apps.publish import publish_ebay as pe

# 2026-11-03 時点の mst.title_replace_rules（rule_id 順）
DB_RULES = [
    ("★", ""), ("Regular price: 120,000", ""), ("Super beautiful condition", "Excellent"),
    ("Extremely beautiful condition", "Excellent"), ("Like new", "Excellent"), ("[Like new]", "Excellent"),
    ("[Almost New]", "Excellent"), ("Almost New", "Excellent"), ("Unused grade", "Excellent"),
    ("Current model", ""), ("Authenticity Guaranteed", ""), ("LOUIS VUITTON", "Louis Vuitton"),
    ("since1854/", "Since 1854"), ("Emprent", "Empreinte"), ("Emprete", "Empreinte"), ("Emprene", "Empreinte"),
    ("Emplant", "Empreinte"), ("Portefeuille Sara", "Portefeuille Sarah"), ("IC installed", ""),
    ("IC*Vuitton", "Louis Vuitton"),
]


class ReplaceLiteralTest(unittest.TestCase):
    def test_word_boundary_on_alnum_edge(self):
        f = pe._replace_literal_ignorecase
        self.assertEqual(f("Portefeuille Sarah Wallet", "Portefeuille Sara", "Portefeuille Sarah"), "Portefeuille Sarah Wallet")
        self.assertEqual(f("Portefeuille Sara Wallet", "Portefeuille Sara", "Portefeuille Sarah"), "Portefeuille Sarah Wallet")
        self.assertEqual(f("Monogram Emprente", "Emprent", "Empreinte"), "Monogram Emprente")

    def test_case_insensitive_and_symbols(self):
        f = pe._replace_literal_ignorecase
        self.assertEqual(f("LOUIS VUITTON wallet", "LOUIS VUITTON", "Louis Vuitton"), "Louis Vuitton wallet")
        self.assertEqual(f("[like new] Wallet", "[Like new]", "Excellent"), "Excellent Wallet")
        self.assertEqual(f("A★B", "★", ""), "AB")
        self.assertEqual(f("x", "x", r"\1"), r"\1")  # 置換文字列は正規表現として解釈しない


class SanitizePolicyTest(unittest.TestCase):
    def check(self, before, after):
        self.assertEqual(pe.sanitize_title_policy_words(before), after)

    def test_like_new_variants(self):
        self.check("FENDI Zucca Wallet Like New", "FENDI Zucca Wallet Excellent")
        self.check("Miu Miu Wallet like-new Condition", "Miu Miu Wallet Excellent Condition")
        self.check("[Like New] Chanel Long Wallet", "Excellent Chanel Long Wallet")
        self.check("Nikon COOLPIX W300, almost new, 5977", "Nikon COOLPIX W300, Excellent, 5977")
        self.check("CHANEL Wallet Near New Condition", "CHANEL Wallet Excellent Condition")
        self.check("Coach Wallet Nearly New", "Coach Wallet Excellent")
        self.check("Like New Excellent Hermes Twilly", "Excellent Hermes Twilly")

    def test_decorative_symbols_removed_with_space(self):
        self.check("Like New‼️ Gucci Wallet", "Excellent Gucci Wallet")
        self.check("13★Stock Sale/Good Condition♪Damier Wallet", "13 Stock Sale/Good Condition Damier Wallet")
        self.check("✨Like New✨ Louis Vuitton", "Excellent Louis Vuitton")
        self.check("Ibuki's Dragonite ● Pokémon Card ☆ VS 049/141 ⭐", "Ibuki's Dragonite Pokémon Card VS 049/141")
        self.check("♡Excellent♡ CHANEL ❤ Scarf 😊", "Excellent CHANEL Scarf")
        self.check("GUCCI ✖ Disney Wallet", "GUCCI x Disney Wallet")

    def test_meaningful_symbols_kept(self):
        for t in ["Louis Vuitton Brown x Pink M62235", "Étole Scarf【M7250】", "Watch 50m 10° Brown × Black",
                  "Thermos 70℃ Bottle", "Eevee δ Holon 049/086", "SEIKO® Watch", "Gold Ivory Necklace"]:
            self.check(t, t)

    def test_no_change_for_normal_titles(self):
        for t in ["Louis Vuitton Epi Long Wallet", "Canon EOS 60D Body", "Newer Model Wallet New Arrival", "Brand New Chanel"]:
            self.check(t, t)


class FinalizeTitleTest(unittest.TestCase):
    def setUp(self):
        p = mock.patch.object(pe, "TITLE_RULES", list(DB_RULES))
        p.start()
        self.addCleanup(p.stop)

    def test_rules_and_policy(self):
        f = pe.finalize_title_en
        self.assertEqual(f("LOUIS VUITTON Portefeuille Sarah Long Wallet ★"), "Louis Vuitton Portefeuille Sarah Long Wallet")
        self.assertEqual(f("Chanel Wallet Authenticity Guaranteed 2946"), "Chanel Wallet 2946")
        self.assertEqual(f("Final Price Cut★Chanel Chevron Wallet"), "Final Price Cut Chanel Chevron Wallet")
        self.assertEqual(f("GUCCI Wallet Extremely Beautiful Condition‼️"), "GUCCI Wallet Excellent")
        self.assertEqual(f("Unused Grade COACH Long Wallet"), "Excellent COACH Long Wallet")

    def test_truncate80(self):
        out = pe.finalize_title_en("Like New " + "Louis Vuitton Monogram Long Wallet " * 3)
        self.assertLessEqual(len(out), 80)
        self.assertTrue(out.startswith("Excellent Louis Vuitton"))

    def test_without_rules_policy_still_applies(self):
        with mock.patch.object(pe, "TITLE_RULES", []):
            self.assertEqual(pe.finalize_title_en("★Like New★ Wallet"), "Excellent Wallet")


class LoadRulesTest(unittest.TestCase):
    def test_load_title_rules(self):
        cur = mock.MagicMock()
        cur.fetchall.return_value = [(1, " ★ ", None), (5, "Like new", "Excellent"), (9, "  ", "x")]
        conn = mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        self.assertEqual(pe.load_title_rules(conn), [("★", ""), ("Like new", "Excellent")])

    def test_main_loads_rules(self):
        # main() が起動時に TITLE_RULES を読み込むこと（2026-02-20 に抜けていた行）
        import inspect
        src = inspect.getsource(pe.main)
        self.assertIn("TITLE_RULES = load_title_rules(conn)", src)


class CallSitesTest(unittest.TestCase):
    """保存済みタイトル・新規翻訳の両方の経路で finalize_title_en を通ること。"""

    def test_both_paths_use_finalize(self):
        import inspect
        src = inspect.getsource(pe)
        self.assertIn('rec["title_en"] = clean_for_ebay(finalize_title_en(existing_en))', src)
        self.assertIn("title_en = finalize_title_en(sanitize_title_dangerous_words(title_en_post))", src)


if __name__ == "__main__":
    unittest.main()
