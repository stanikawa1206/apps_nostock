# -*- coding: utf-8 -*-
"""
危険素材・規制素材判定（apps/publish/material_check.py、2026-11-04）のテスト。
例文は 2026-11-04 に調べた出品中の商品の説明文から取った。
DB・OpenAI には接続しない。
実行: python -m unittest apps.tests.test_material_check -v
"""

import inspect
import unittest
from unittest import mock

from apps.publish import material_check as mc


def s1(title, desc="", attrs=None):
    return mc.judge_stage1(mc.detect_hits(title, desc, attrs))[0]


class Stage1Test(unittest.TestCase):
    def test_real_material_is_ng(self):
        self.assertEqual(s1("エルメス ケリー ドゥブルトゥール", "素材：アリゲーター"), "NG")
        self.assertEqual(s1("HERMES バングル", "素材：メタル×リザードレザー"), "NG")
        self.assertEqual(s1("ウォーターマン カレン アリゲーターレザー"), "NG")
        self.assertEqual(s1("エルメス シェーヌダンクル リアルクロコダイルレザー"), "NG")
        self.assertEqual(s1("サンローラン カサンドラ ブレスレット（パイソンレザー）"), "NG")
        self.assertEqual(s1("CITIZEN EXCEED", "ベルト素材: レザー（純正のクロコダイル）"), "NG")
        self.assertEqual(s1("アガット ネックレス", "素材 K10×ムーンストーン×珊瑚×オパール"), "NG")

    def test_hermes_leather_name_in_color_field_is_ng(self):
        # 素材欄は「メッキ/合金」、色欄に「リザード」（エルメス オランプ。AI は対象外と誤判定した）
        self.assertEqual(s1("HERMES オランプPM イヤーカフ", "◆カラー : イエローゴールド×グレー(リザード、オンブレ)\n素材...メッキ/合金"), "NG")
        self.assertEqual(s1("美品 HERMES エルメス ケリー カデナ バングル クロコ ゴールド"), "NG")

    def test_pattern_is_ok(self):
        self.assertEqual(s1("SEIKO DOLCE", "クロコ型押しベルト"), "OK")
        self.assertEqual(s1("パイロット カスタムレガンス", "リザード調（トカゲ革風）型押し"), "OK")
        self.assertEqual(s1("NIXON 腕時計 本体 ベッコウ柄"), "OK")
        self.assertEqual(s1("ロエベ ブレスレット べっ甲モチーフ", "本べっ甲ではなく、べっ甲調の樹脂"), "OK")
        self.assertEqual(s1("グッチ スネーク柄 シルクストール"), "OK")

    def test_unknown_goes_to_ai(self):
        self.assertEqual(s1("[PILOT] パイロット CUSTOM 鼈甲 ボディ ペン芯のみ"), "AI")
        self.assertEqual(s1("GUCCI 二つ折り財布 スネーク GGスプリーム"), "AI")
        self.assertEqual(s1("エルメス アニマル バッグチャーム キーホルダー ヘビ 巳"), "AI")

    def test_conflict_is_hold(self):
        # タイトルは型押し、説明文はクロコのレザー（フランクミュラーの交換ベルト）
        self.assertEqual(s1("フランクミュラー レザー 型押しクロコ ベルト", "購入した時のクロコのレザーベルトです"), "HOLD")

    def test_allow_materials_do_not_stop(self):
        self.assertEqual(s1("FENDI ポンポンチャーム", "素材：リアルファー（フォックスファー）"), "OK")
        self.assertEqual(s1("BOTTEGA VENETA エイ革 ガルーシャ 長財布"), "OK")
        self.assertEqual(s1("エルメス メドール バングル オーストリッチ"), "OK")

    def test_no_false_hits(self):
        for t in ["ヘビーユーズ品 蛇腹 カメラ", "蛇の目傘", "象牙色 ネックレス", "珊瑚礁 柄 スカーフ",
                  "クロス バッグ", "ワニスで仕上げ", "#クロコ #パイソン 財布", "ミンクオイル 手入れ済み"]:
            self.assertEqual(mc.detect_hits(t, ""), [], t)

    def test_keyword_list_line_ignored(self):
        seo = " ".join(["メンズ", "レディース", "ビジネス", "アナログ", "デジタル", "バッグ", "長財布", "バックル",
                        "ベルト", "バック", "レザー", "革", "本革", "クロコ", "シルバー", "結婚式", "パーティ",
                        "ドレス", "時計", "腕時計", "ギフト", "プレゼント", "人気", "定番", "おしゃれ"])
        self.assertEqual(s1("CITIZEN エコドライブ 腕時計", "美品です。\n" + seo), "OK")
        # 【項目】の行は羅列とみなさない
        self.assertEqual(s1("シチズン xC", "【商品名】シチズン CITIZEN XC 腕時計 ウォッチ ソーラー カレンダー付き デイト クロコダイル レザーベルト ゴールド色"), "NG")

    def test_attributes_are_used(self):
        self.assertEqual(s1("バングル", "", [{"text": "素材", "value": "素材：パイソン"}]), "NG")


class AiJudgeTest(unittest.TestCase):
    def test_judge_ai(self):
        j = mc.judge_ai
        self.assertEqual(j({"materials": [{"material": "べっ甲", "verdict": "not_real", "confidence": 0.95}]}, ["べっ甲"])[0], "OK")
        self.assertEqual(j({"materials": [{"material": "べっ甲", "verdict": "real", "confidence": 0.9}]}, ["べっ甲"])[0], "NG")
        self.assertEqual(j({"materials": [{"material": "べっ甲", "verdict": "unknown", "confidence": 0.3}]}, ["べっ甲"])[0], "HOLD")
        self.assertEqual(j({"materials": [{"material": "べっ甲", "verdict": "not_real", "confidence": 0.6}]}, ["べっ甲"])[0], "HOLD")
        self.assertEqual(j({"materials": []}, ["ヘビ"])[0], "HOLD")
        self.assertEqual(j({"materials": [{"material": "ヘビ", "verdict": "not_real", "confidence": 0.9},
                                          {"material": "ワニ", "verdict": "real", "confidence": 0.9}]}, ["ヘビ"])[0], "NG")

    def test_ai_error_is_hold(self):
        client = mock.MagicMock()
        client.responses.create.side_effect = RuntimeError("timeout")
        r = mc.check_material("GUCCI 財布 スネーク", "", None, [], client_factory=lambda: client)
        self.assertEqual((r.result, r.stage), ("HOLD", 2))
        self.assertIn("AIエラー", r.reason)
        self.assertEqual(client.responses.create.call_count, 2)  # 1回だけ再試行

    def test_ai_ok(self):
        resp = mock.MagicMock(output_text='{"materials":[{"material":"ヘビ","verdict":"not_real","evidence":"print","confidence":0.95}]}')
        resp.usage.input_tokens, resp.usage.output_tokens = 4000, 60
        client = mock.MagicMock()
        client.responses.create.return_value = resp
        r = mc.check_material("GUCCI 財布 スネーク", "", None, [], client_factory=lambda: client)
        self.assertEqual((r.result, r.stage, r.ai_input_tokens), ("OK", 2, 4000))

    def test_stage1_real_does_not_call_ai(self):
        client = mock.MagicMock()
        r = mc.check_material("エルメス ブレスレット", "素材：アリゲーター", None, [], client_factory=lambda: client)
        self.assertEqual(r.result, "NG")
        client.responses.create.assert_not_called()


class CacheTest(unittest.TestCase):
    def _conn(self, row):
        cur = mock.MagicMock()
        cur.fetchone.return_value = row
        conn = mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        return conn, cur

    def test_no_hit_does_not_touch_db(self):
        conn = mock.MagicMock()
        r = mc.check_material_with_cache(conn, "メルカリ", "m1", "Canon EOS 60D", "動作品")
        self.assertEqual((r.result, r.stage), ("OK", 0))
        conn.cursor.assert_not_called()

    def test_same_text_uses_cache(self):
        title, desc = "GUCCI 財布 スネーク", ""
        h = mc.text_hash(mc.build_text(title, desc))
        conn, cur = self._conn((h, "OK", 2, "AI: 対象外"))
        with mock.patch.object(mc, "check_material") as cm:
            r = mc.check_material_with_cache(conn, "メルカリ", "m1", title, desc)
        cm.assert_not_called()
        self.assertTrue(r.cached)
        self.assertEqual(r.result, "OK")

    def test_changed_text_rejudged_but_ng_sticks(self):
        conn, cur = self._conn(("x" * 64, "NG", 1, "ワニ: 本物"))
        with mock.patch.object(mc, "check_material") as cm:
            r = mc.check_material_with_cache(conn, "メルカリ", "m1", "GUCCI 財布 スネーク", "")
        cm.assert_not_called()
        self.assertEqual(r.result, "NG")

        conn, cur = self._conn(("x" * 64, "HOLD", 2, "AI: 判断不能"))
        with mock.patch.object(mc, "check_material", return_value=mc.MaterialCheckResult("OK", 2, "AI")) as cm:
            r = mc.check_material_with_cache(conn, "メルカリ", "m1", "GUCCI 財布 スネーク", "")
        cm.assert_called_once()
        self.assertEqual(r.result, "OK")

    def test_manual_result_wins(self):
        conn, cur = self._conn(("x" * 64, "OK", 9, "手動確認"))
        with mock.patch.object(mc, "check_material") as cm:
            r = mc.check_material_with_cache(conn, "メルカリ", "m1", "エルメス 素材：アリゲーター", "")
        cm.assert_not_called()
        self.assertEqual(r.result, "OK")


class TitleTest(unittest.TestCase):
    def r(self, title, desc=""):
        hits = mc.detect_hits(title, desc)
        return mc.MaterialCheckResult(mc.judge_stage1(hits)[0], 1, "", hits)

    def test_pattern_word_marked(self):
        r = self.r("SEIKO DOLCE", "クロコ型押しベルト")
        self.assertEqual(mc.adjust_title_materials("Seiko Dolce Crocodile Leather Strap Watch", r),
                         "Seiko Dolce Crocodile Pattern Leather Strap Watch")
        self.assertEqual(mc.adjust_title_materials("Seiko Croc Embossed Strap", r), "Seiko Croc Embossed Strap")

    def test_mistranslation_removed(self):
        r = self.r("エルメス プティアッシュ ゾウ チャーム")
        self.assertEqual(mc.adjust_title_materials("Hermes Petit h Elephant Leather Charm", r), "Hermes Petit h Leather Charm")
        self.assertEqual(mc.adjust_title_materials("Hermes Petit h Elephant Charm", r), "Hermes Petit h Elephant Charm")

    def test_allowed_real_material_kept(self):
        r = self.r("エルメス メドール バングル オーストリッチ")
        self.assertEqual(mc.adjust_title_materials("Hermes Medor Bangle Ostrich Leather", r), "Hermes Medor Bangle Ostrich Leather")

    def test_ivory_untouched(self):
        self.assertEqual(mc.adjust_title_materials("Hermes Natural Ivory Bangle", None), "Hermes Natural Ivory Bangle")


class PublishIntegrationTest(unittest.TestCase):
    def test_publish_uses_material_check(self):
        from apps.publish import publish_ebay as pe
        src = inspect.getsource(pe.heavy_check_detail)
        self.assertIn("check_material_with_cache(", src)
        self.assertIn('"保留(素材確認)"', src)
        self.assertNotIn("RISKY_MATERIAL_TARGET_CATEGORY_GROUPS", inspect.getsource(pe))
        self.assertNotIn("sanitize_title_dangerous_words", inspect.getsource(pe))

    def test_candidates_exclude_hold(self):
        import os
        p = os.path.join(os.path.dirname(__file__), "..", "publish", "sql", "fn_take_one_candidates_base.sql")
        with open(p, encoding="utf-8-sig") as f:
            self.assertIn("N'保留(素材確認)'", f.read())


if __name__ == "__main__":
    unittest.main()
