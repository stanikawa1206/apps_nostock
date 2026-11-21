# -*- coding: utf-8 -*-
"""
publish_manager 完了メール（テキスト版＋HTML版の表）のテスト（2026-11-21）。

DB とメールには触れない（接続はフェイク、send_mail・run_script はモック）。
"""
import re
import unittest
from datetime import datetime, timedelta
from html.parser import HTMLParser
from unittest import mock

from apps.inventory import daily_check

# account, close_reason, post_target, close_detail, close_reason_collectibles, post_target_collectibles, posted, posted_c
ROWS = [
    ("BUZZ", "DONE", 500, None, "EMPTY", 333, 500, 0),
    ("BUZZ②", "DONE", 332, None, "EMPTY", 333, 329, 0),
    ("川島", "DONE", 333, None, "EMPTY", 333, 331, 21),
    ("谷川②", "DONE", 500, None, "DONE", 333, 500, 333),
    ("谷川③", "DONE", 333, None, "EMPTY", 333, 333, 0),
    ("谷川④", "LIMIT", 333, None, "NO_SPACE", 333, 329, 0),
    ("貴文", "DONE", 333, None, None, 333, 333, 33),
    ("貴文③", "EMPTY", 100, None, "BLOCKED", 100, 0, 98),
]


class FakeCursor:
    def __init__(self, rows, limit_handled):
        self.rows, self.limit_handled, self.last = rows, limit_handled, ""

    def execute(self, sql, *a):
        self.last = sql

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        assert "定期削除(watch0)" in self.last
        return (self.limit_handled,)


class FakeConn:
    def __init__(self, rows=ROWS, limit_handled=3):
        self.c = FakeCursor(rows, limit_handled)

    def cursor(self):
        return self.c


class BrokenConn:
    def cursor(self):
        raise RuntimeError("db down")


class TableParser(HTMLParser):
    """最初の <table border="1"> の各行のセルの文字（<br> は '|'）を取り出す。"""

    def __init__(self):
        super().__init__()
        self.rows, self.in_main, self.cell, self.depth = [], False, None, 0

    def handle_starttag(self, tag, attrs):
        if tag == "table" and dict(attrs).get("border") == "1":
            self.in_main = True
        if not self.in_main:
            return
        if tag == "tr":
            self.rows.append([])
        elif tag in ("td", "th"):
            self.cell = ""
        elif tag == "br" and self.cell is not None:
            self.cell += "|"

    def handle_endtag(self, tag):
        if not self.in_main:
            return
        if tag in ("td", "th"):
            self.rows[-1].append(self.cell.strip())
            self.cell = None
        elif tag == "table":
            self.in_main = False

    def handle_data(self, data):
        if self.in_main and self.cell is not None:
            self.cell += data


HEADER = [("結果", "正常終了"), ("開始時刻", "2026-11-21 03:07:03"), ("終了時刻", "2026-11-21 08:01:55"),
          ("処理時間", "294分52秒"), ("returncode", "0")]


class MailTest(unittest.TestCase):
    def build(self, conn=None):
        return daily_check.build_publish_manager_mail(conn or FakeConn(), HEADER)

    def test_html_table_shape_and_values(self):
        _, html = self.build()
        p = TableParser()
        p.feed(html)
        self.assertEqual(p.rows[0], ["アカウント", "通常出品数", "通常目標", "Collectibles出品数", "Collectibles目標", "合計出品数"])
        body = p.rows[1:-1]
        self.assertEqual(len(body), 8)
        self.assertTrue(all(len(r) == 6 for r in p.rows))
        self.assertEqual(body[3], ["谷川②", "500|DONE", "500", "333|DONE", "333", "833"])
        self.assertEqual(body[5], ["谷川④", "329|LIMIT", "333", "0|NO_SPACE", "333", "329"])
        self.assertEqual(body[6][3], "33|未終了")       # 終了理由が NULL
        self.assertEqual(body[7][3], "98|BLOCKED")
        # 合計行: 通常 2655 / 目標 2764、Collectibles 485 / 目標 2431、合計 3140
        self.assertEqual(p.rows[-1], ["合計", "2655", "2764", "485", "2431", "3140"])
        for r in body:  # 合計出品数 = 通常 + Collectibles
            self.assertEqual(int(r[5]), int(r[1].split("|")[0]) + int(r[3].split("|")[0]))

    def test_html_keeps_header_footer_and_total(self):
        _, html = self.build()
        for k, v in HEADER:
            self.assertIn(f">{k}</td>", html)
            self.assertIn(f">{v}</td>", html)
        self.assertIn("全アカウントの出品合計: 3140件（通常 2655件 / Collectibles 485件）", html)
        self.assertIn("本日LIMIT対応(出品枠確保)が発生したアカウント数: 3", html)
        self.assertIn("現在LIMIT中のアカウント数（正常終了なら0のはず）: 1", html)
        self.assertIn("出品枠不足（NO_SPACE）", html)
        self.assertIn("eBayの新規出品制限で当日の出品を終了したアカウント", html)
        self.assertRegex(html, r'color:#cc0000;">LIMIT<')
        self.assertTrue(html.startswith("<!DOCTYPE html>"))

    def test_text_keeps_old_lines_and_adds_total(self):
        text, _ = self.build()
        self.assertTrue(text.startswith("publish_manager.py 完了通知\n\n結果: 正常終了\n開始時刻: 2026-11-21 03:07:03\n"
                                        "終了時刻: 2026-11-21 08:01:55\n処理時間: 294分52秒\nreturncode: 0\n\n"))
        self.assertIn("- 谷川②: close_reason=DONE / 本日出品=500件 / post_target=500"
                      "  ｜ Collectibles: close_reason=DONE / 本日出品=333件 / post_target=333", text)
        self.assertIn("- 貴文: close_reason=DONE / 本日出品=333件 / post_target=333"
                      "  ｜ Collectibles: close_reason=NULL / 本日出品=33件 / post_target=333", text)
        self.assertIn("全アカウントの出品合計: 3140件（通常 2655件 / Collectibles 485件）", text)
        self.assertTrue(text.rstrip().endswith("現在LIMIT中のアカウント数（正常終了なら0のはず）: 1"))
        # 合計は LIMIT 件数の前
        self.assertLess(text.index("全アカウントの出品合計"), text.index("本日LIMIT対応"))

    def test_summary_text_same_as_mail_text(self):
        text, _ = self.build()
        self.assertTrue(text.endswith(daily_check.build_publish_manager_account_summary(FakeConn())))

    def test_html_escapes_values(self):
        rows = [("<b>x</b>", "BLOCKED", 1, "a<script>", "BLOCKED", 1, 0, 0)]
        _, html = self.build(FakeConn(rows, 0))
        self.assertNotIn("<b>x</b>", html)
        self.assertNotIn("a<script>", html)
        self.assertIn("&lt;b&gt;x&lt;/b&gt;", html)

    def test_db_error_still_returns_mail(self):
        text, html = self.build(BrokenConn())
        self.assertIn("returncode: 0", text)
        self.assertIn("【アカウント別状況】取得失敗: db down", text)
        self.assertIn("取得失敗: db down", html)

    def test_run_publish_manager_sends_text_and_html(self):
        class Conn(FakeConn):
            def run(self, fn, *a):
                return None
        t0 = datetime(2026, 11, 21, 3, 7, 3)
        times = iter([t0, t0 + timedelta(minutes=294, seconds=52)])
        with mock.patch.object(daily_check, "run_script", return_value=(0, "")), \
                mock.patch.object(daily_check, "send_mail", return_value=True) as sm, \
                mock.patch.object(daily_check, "datetime") as dt:
            dt.now.side_effect = lambda: next(times)
            code = daily_check.run_publish_manager(Conn())
        self.assertEqual(code, 0)
        subject, body = sm.call_args.args
        html = sm.call_args.kwargs["html"]
        self.assertEqual(subject, "✅ publish_manager完了通知")
        self.assertIn("処理時間: 294分52秒", body)
        self.assertIn("returncode: 0", body)
        self.assertIn(">合計出品数</th>", html)
        self.assertEqual(len(re.findall(r"<tr>", html)), 5 + 1 + 8 + 1)  # 冒頭5行・見出し・8アカウント・合計


if __name__ == "__main__":
    unittest.main()
