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

    def test_declaration_materials_hold(self):
        # 米国輸入時に USFWS 申告・指定港通関が必要（出品者側で対応できない）→ 保留
        self.assertEqual(s1("BOTTEGA VENETA エイ革 ガルーシャ 長財布"), "HOLD")
        self.assertEqual(s1("エルメス メドール バングル オーストリッチ"), "HOLD")
        self.assertEqual(s1("ボッテガヴェネタ 財布", "素材: アイヤーズスネーク / ラム"), "HOLD")
        self.assertEqual(s1("オーストリッチ型押し 財布"), "OK")

    def test_declaration_switch(self):
        with mock.patch.object(mc, "HOLD_WILDLIFE_DECLARATION", False):
            self.assertEqual(s1("エルメス メドール バングル オーストリッチ"), "OK")

    def test_no_false_hits(self):
        for t in ["ヘビーユーズ品 蛇腹 カメラ", "蛇の目傘", "象牙色 ネックレス", "珊瑚礁 柄 スカーフ",
                  "クロス バッグ", "ワニスで仕上げ", "#クロコ #パイソン 財布", "ミンクオイル 手入れ済み",
                  "シェーファー 万年筆", "ファーバーカステル 鉛筆", "タイムグラファー 計測", "ファースナー金具",
                  "ピーターラビット キーホルダー", "起毛皮革のケース"]:
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


class FurTest(unittest.TestCase):
    def check(self, title, desc="", cond=None):
        return mc.check_material(title, desc, None, [], client_factory=mock.MagicMock(side_effect=AssertionError("AI")),
                                 item_condition_id=cond).result

    def test_unused_real_fur_is_ng(self):
        self.assertEqual(self.check("【未使用品】ルイヴィトン ポルトクレ ミンクファー", cond=1), "NG")
        self.assertEqual(self.check("FENDI FOXストール", "未使用でございます。素材：フォックスファー"), "NG")

    def test_unknown_usage_is_hold(self):
        self.assertEqual(self.check("FENDI ポンポンチャーム リアルファー フォックスファー", cond=2), "HOLD")
        self.assertEqual(self.check("CHANEL ラビットファー ストール", "素材：ラビットファー100%"), "HOLD")

    def test_used_rabbit_ok_other_species_hold(self):
        self.assertEqual(self.check("プラダ ファーバッグチャーム ラビット 尻尾", "【素材】ラビット レザー", cond=3), "OK")
        self.assertEqual(self.check("LV ポルトクレ ミンクファー", "使用感があります", cond=4), "HOLD")

    def test_rank_table_ignored(self):
        desc = "\n".join(["S…未使用品(弊社の検品基準による)", "SA…極美品…数回使用した程度のUSED品", "●素材 ラビットファー100%"])
        # ランク基準表（S…未使用品 など）は使用歴として扱わない（未使用とも中古とも決めない）
        self.assertEqual(mc.usage_state("CHANEL ストール", desc, None), "UNKNOWN")
        self.assertEqual(mc.usage_state("新品で購入しました", "数回使用", None), "USED")

    def test_fake_fur_ok(self):
        self.assertEqual(self.check("フェイクファー ティペット", cond=1), "OK")
        self.assertEqual(self.check("エコファー バッグチャーム", cond=1), "OK")
        self.assertEqual(self.check("ムートン ファー ポーチ", cond=1), "OK")

    def test_generic_fur_goes_to_ai_and_ai_decides(self):
        resp = mock.MagicMock(output_text='{"materials":[{"material":"毛皮","verdict":"real","species":"rabbit","confidence":0.9}]}')
        resp.usage.input_tokens, resp.usage.output_tokens = 4000, 60
        client = mock.MagicMock()
        client.responses.create.return_value = resp
        r = mc.check_material("FENDI ファー チャーム", "", None, [], client_factory=lambda: client, item_condition_id=1)
        self.assertEqual((r.result, r.stage), ("NG", 2))
        r = mc.check_material("FENDI ファー チャーム", "", None, [], client_factory=lambda: client, item_condition_id=4)
        self.assertEqual(r.result, "OK")
        self.assertEqual(mc.material_labels(r), ["ラビット"])
        resp.output_text = '{"materials":[{"material":"毛皮","verdict":"unknown","confidence":0.3}]}'
        r = mc.check_material("FENDI ファー チャーム", "", None, [], client_factory=lambda: client, item_condition_id=4)
        self.assertEqual(r.result, "HOLD")


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

    def test_no_image_not_real_is_hold(self):
        j = mc.judge_ai
        ai = {"materials": [{"material": "サンゴ", "verdict": "not_real", "confidence": 0.9}], "_images": 0}
        self.assertEqual(j(ai, ["サンゴ"])[0], "HOLD")
        ai["_images"] = 3
        self.assertEqual(j(ai, ["サンゴ"])[0], "OK")

    def test_coral_jewelry_is_real(self):
        self.assertEqual(s1("美品 ミキモト 珊瑚 リーフ ブローチ ベビーパール シルバー"), "NG")
        self.assertEqual(s1("セーラー 万年筆 うさぎや 限定 珊瑚"), "AI")
        self.assertEqual(s1("珊瑚色 スカーフ"), "OK")

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
        with mock.patch.object(mc, "_image_b64", return_value="eA=="):
            r = mc.check_material("GUCCI 財布 スネーク", "", None, ["https://example/1.jpg"], client_factory=lambda: client)
        self.assertEqual((r.result, r.stage, r.ai_input_tokens), ("OK", 2, 4000))
        # 写真が無い時は、対象外の回答でも保留
        r = mc.check_material("GUCCI 財布 スネーク", "", None, [], client_factory=lambda: client)
        self.assertEqual(r.result, "HOLD")

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
        conn, cur = self._conn((h, "OK", 2, "AI: 対象外", None))
        with mock.patch.object(mc, "check_material") as cm:
            r = mc.check_material_with_cache(conn, "メルカリ", "m1", title, desc)
        cm.assert_not_called()
        self.assertTrue(r.cached)
        self.assertEqual(r.result, "OK")

    def test_changed_text_rejudged_but_ng_sticks(self):
        conn, cur = self._conn(("x" * 64, "NG", 1, "ワニ: 本物", None))
        with mock.patch.object(mc, "check_material") as cm:
            r = mc.check_material_with_cache(conn, "メルカリ", "m1", "GUCCI 財布 スネーク", "")
        cm.assert_not_called()
        self.assertEqual(r.result, "NG")

        conn, cur = self._conn(("x" * 64, "HOLD", 2, "AI: 判断不能", None))
        with mock.patch.object(mc, "check_material", return_value=mc.MaterialCheckResult("OK", 2, "AI")) as cm:
            r = mc.check_material_with_cache(conn, "メルカリ", "m1", "GUCCI 財布 スネーク", "")
        cm.assert_called_once()
        self.assertEqual(r.result, "OK")

    def test_manual_result_wins(self):
        conn, cur = self._conn(("x" * 64, "OK", 9, "手動確認", None))
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

    def test_labels_for_real_allowed_material(self):
        r = mc.check_material("プラダ ファーバッグチャーム ラビット 尻尾", "【素材】ラビット レザー", item_condition_id=3)
        keys = mc.material_labels(r)
        self.assertEqual(keys, ["ラビット"])
        self.assertEqual(mc.ensure_title_labels("Prada Fur Bag Charm Key Holder Tail Border Black Pink", keys),
                         "Prada Fur Bag Charm Key Holder Tail Border Black Pink Rabbit Fur")
        long = "Prada Fur Bag Charm Key Holder Tail Border Triangle Logo Plate Gold Black Pink Extra"
        out = mc.ensure_title_labels(long, keys)
        self.assertLessEqual(len(out), 80)
        self.assertTrue(out.endswith("Rabbit Fur"))
        self.assertEqual(mc.ensure_title_labels("PRADA Rabbit Fur Charm", keys), "PRADA Rabbit Fur Charm")
        self.assertIn("Oryctolagus cuniculus", mc.description_material_note(keys))
        self.assertIn("Fur origin", mc.description_material_note(keys))
        self.assertEqual(mc.material_labels(mc.check_material("SEIKO", "クロコ型押しベルト")), [])

    def test_ivory_untouched(self):
        self.assertEqual(mc.adjust_title_materials("Hermes Natural Ivory Bangle", None), "Hermes Natural Ivory Bangle")


class PublishIntegrationTest(unittest.TestCase):
    def test_publish_uses_material_check(self):
        from apps.publish import publish_ebay as pe
        src = inspect.getsource(pe.heavy_check_detail)
        self.assertIn("check_material_with_cache(", src)
        self.assertIn('"保留(素材確認)"', src)
        self.assertIn("item_condition_id=rec.get(\"item_condition_id\")", src)
        self.assertIn("description_material_note(material_label_keys)", src)
        self.assertNotIn("RISKY_MATERIAL_TARGET_CATEGORY_GROUPS", inspect.getsource(pe))
        self.assertNotIn("sanitize_title_dangerous_words", inspect.getsource(pe))

    def test_candidates_exclude_hold(self):
        import os
        p = os.path.join(os.path.dirname(__file__), "..", "publish", "sql", "fn_take_one_candidates_base.sql")
        with open(p, encoding="utf-8-sig") as f:
            self.assertIn("N'保留(素材確認)'", f.read())


if __name__ == "__main__":
    unittest.main()
