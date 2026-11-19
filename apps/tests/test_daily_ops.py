# -*- coding: utf-8 -*-
"""
daily_ops（日次メールの在庫チェック等の実績）のテスト（2026-11-19）。DB は使わない。
実行: python -m unittest apps.tests.test_daily_ops
"""
import unittest
from collections import defaultdict
from datetime import date, datetime, timedelta

from apps.inventory import daily_ops as do

T = date(2026, 11, 27)
NOW = datetime(2026, 11, 28, 8, 0)


def ops(stats=None, first=datetime(2026, 11, 19, 23, 20), issues=(), outages=None, heartbeat=NOW):
    st = defaultdict(lambda: defaultdict(list))
    for d, kind, rows in (stats or []):
        st[d][kind].extend(rows)
    return {"target": T, "days": [T - timedelta(days=i) for i in range(7, -1, -1)], "stats": st,
            "covered": lambda d: first is not None and first <= datetime.combine(d, datetime.min.time()),
            "issues": list(issues), "price_pending": 0, "normal_ends": defaultdict(lambda: defaultdict(int)),
            "outages": outages or {}, "heartbeat": heartbeat, "now": NOW}


def check_rows(sold=20, deleted=10, sale=1000, und=0, vendor="メルカリ"):
    rows = [{"vendor": vendor, "result": "販売中", "cause": None, "items": sale, "attempts": sale, "retried": 3},
            {"vendor": vendor, "result": "売り切れ", "cause": None, "items": sold, "attempts": sold, "retried": 0},
            {"vendor": vendor, "result": "削除", "cause": None, "items": deleted, "attempts": deleted, "retried": 0}]
    if und:
        rows.append({"vendor": vendor, "result": "判定不可", "cause": "items/get 応答なし", "items": und,
                     "attempts": und * 2, "retried": und})
    return rows


class SummaryTest(unittest.TestCase):
    def test_no_problem(self):
        o = ops([(T - timedelta(days=i), "在庫チェック", check_rows()) for i in range(8)])
        self.assertEqual(do.summary_lines(o), ["重大な未解決の問題はありません。"])

    def test_sudden_zero_sold_is_flagged(self):
        """通常は出る「売り切れ」が突然0件 → 判定の不具合の兆候として載る"""
        stats = [(T - timedelta(days=i), "在庫チェック", check_rows()) for i in range(1, 8)]
        stats.append((T, "在庫チェック", check_rows(sold=0)))
        lines = do.summary_lines(ops(stats))
        self.assertTrue(any("「売り切れ」判定が0件" in l for l in lines), lines)

    def test_high_undetermined_rate(self):
        stats = [(T, "在庫チェック", check_rows(und=30))]
        self.assertTrue(any("判定不可・例外" in l for l in do.summary_lines(ops(stats))))

    def test_unresolved_end_failure_first(self):
        iss = [{"issue_kind": "仕入不可出品中", "vendor_name": "メルカリshops", "vendor_item_id": "x", "account": "a",
                "listing_id": "1", "cause": "仕入先=売り切れ", "first_at": NOW - timedelta(hours=3),
                "last_at": NOW, "occurrences": 18}]
        lines = do.summary_lines(ops(issues=iss))
        self.assertTrue(lines[0].startswith("【重大】仕入先で購入できないのに"))

    def test_monitor_stale(self):
        lines = do.summary_lines(ops(heartbeat=NOW - timedelta(hours=1)))
        self.assertTrue(any("監視（inventory_monitor）の最終実行が古い" in l for l in lines))


class SectionsTest(unittest.TestCase):
    def test_before_recording_is_not_zero(self):
        """記録開始前の日は 0 件ではなく「集計不可」"""
        secs = do.sections(ops(first=None))
        flat = str(secs)
        self.assertIn("集計不可", flat)

    def test_check_table(self):
        stats = [(T, "在庫チェック", check_rows(und=5))]
        secs = dict((s[0], s) for s in do.sections(ops(stats)))
        row = [r for r in secs["在庫チェック（仕入先別）"][3] if r[0] == "M"][0]
        self.assertEqual(row[1], "1,035")       # 件数
        self.assertEqual(row[5], "5")           # 判定不可・例外
        self.assertEqual(row[7], "3")           # retry回復
        self.assertEqual(row[8], "5")           # retry未解決
        self.assertEqual(row[9], "0")           # 前日（11/26）は記録の期間内で行が無い → 0 件
        causes = secs["判定不可・例外の原因"][3]
        self.assertEqual(causes, [["M", "items/get 応答なし", "5"]])


if __name__ == "__main__":
    unittest.main()
