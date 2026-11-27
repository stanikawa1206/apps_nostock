# -*- coding: utf-8 -*-
"""
在庫チェックの判定不可の後回し再取得と、2サイクル連続の時だけのメール（2026-11-27）のテスト。DB・メール・取得はモック。
実行: python -m unittest apps.tests.test_undetermined_retry_notify
"""
import os
import sys
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest import mock

_o, _e = sys.stdout, sys.stderr
from apps.inventory import check_remaining_ebay as cr  # noqa: E402  import 時に stdout をログへ差し替えるので戻す
sys.stdout, sys.stderr = _o, _e

from apps.inventory import daily_check as dc  # noqa: E402
from apps.inventory import ops_alerts as oa  # noqa: E402
from apps.inventory import ops_stats  # noqa: E402

NOW = datetime(2026, 11, 27, 12, 0, 0)


def row(retry_pass=False, vendor="メルカリ", sku="m59411904484"):
    return {"vendor_name": vendor, "vendor_item_id": sku, "account": "川島", "listing_id": "237175828470",
            "preset": "p", "mode": "DDP", "low_usd_target": 100.0, "high_usd_target": 2000.0,
            "has_preset": True, "retry_pass": retry_pass}


class ProcessFirstAndRetryPassTest(unittest.TestCase):
    """process_status_and_sync: 1回目の判定不可は後回し、2回目の判定不可で従来の判定不可、判定できれば通常処理。"""

    def run_sync(self, status, price=None, retry_pass=False, old_price=33000):
        calls = mock.MagicMock()
        conn = mock.MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        with mock.patch.object(cr, "get_status", return_value=(status, price)), \
                mock.patch.object(cr, "get_vendor_item_price", return_value=old_price), \
                mock.patch.object(cr, "update_vendor_item_price_and_status", calls.update), \
                mock.patch.object(cr, "handle_listing_delete", calls.delete), \
                mock.patch.object(cr, "handle_listing_price_update", calls.price_up), \
                mock.patch.object(cr.ops_stats, "issue", calls.issue), \
                mock.patch.object(cr.ops_stats, "count", calls.count), \
                mock.patch.object(cr.ops_stats, "resolve_undetermined_if_rechecked", calls.resolve), \
                mock.patch.object(cr.ops_stats, "take_pending", return_value=("items/get 応答なし", 1)), \
                mock.patch("builtins.print"):
            cr.process_status_and_sync(conn, mock.MagicMock(), None, row(retry_pass), "w")
        sqls = [c.args[0] for c in cur.execute.call_args_list]
        return calls, sqls, cur

    def test_first_undetermined_is_deferred_without_status_or_issue(self):
        calls, sqls, cur = self.run_sync("判定不可")
        calls.update.assert_not_called()           # status・価格は書かない
        calls.delete.assert_not_called()           # 出品終了しない
        calls.price_up.assert_not_called()         # 価格改定しない
        calls.issue.assert_not_called()            # 異常明細にも記録しない（1回目は再取得で直ることが多い）
        calls.resolve.assert_not_called()
        # 後回しの印を付けるだけ（remaining_check_at は入れない）
        self.assertEqual(len(sqls), 1)
        self.assertIn("remaining_check_by = ?", sqls[0])
        self.assertNotIn("remaining_check_at", sqls[0])
        self.assertEqual(cur.execute.call_args.args[1][0], cr.RETRY_WAIT_MARK)
        # 実績は在庫チェックではなく「再取得待ち」に数える
        kinds = [(c.args[0], c.args[2]) for c in calls.count.call_args_list]
        self.assertEqual(kinds, [(ops_stats.CHECK_RETRY_WAIT_KIND, "再取得待ち")])

    def test_second_undetermined_records_issue_after_vendor_item(self):
        calls, sqls, _ = self.run_sync("判定不可", retry_pass=True)
        calls.delete.assert_not_called()
        calls.price_up.assert_not_called()
        self.assertEqual(calls.update.call_args.args[4], "判定不可")
        self.assertTrue(any("remaining_check_at = SYSDATETIME()" in s for s in sqls))  # この回は完了
        # 順序: 前回の明細の解決確認 → vendor_item を書く → 明細を記録（last_at が last_checked_at より後になる）
        names = [c[0] for c in calls.mock_calls if c[0] in ("resolve", "update", "issue")]
        self.assertEqual(names, ["resolve", "update", "issue"])
        self.assertEqual(calls.issue.call_args.args[:4], ("判定不可", "メルカリ", "m59411904484", "items/get 応答なし"))
        cnt = calls.count.call_args
        self.assertEqual((cnt.args[0], cnt.args[2], cnt.kwargs["retried"]), ("在庫チェック", "判定不可", 1))

    def test_second_pass_on_sale_is_processed_normally(self):
        calls, sqls, _ = self.run_sync("販売中", price=35000, retry_pass=True)
        calls.price_up.assert_called_once()        # 価格が変わっていれば通常どおり価格改定
        calls.issue.assert_not_called()
        calls.resolve.assert_not_called()
        self.assertEqual(calls.update.call_args.args[3:], (35000, "販売中"))
        self.assertTrue(any("remaining_check_at = SYSDATETIME()" in s for s in sqls))
        cnt = calls.count.call_args
        self.assertEqual((cnt.args[2], cnt.kwargs["retried"]), ("販売中", 1))  # 日次の「retry回復」に入る

    def test_second_pass_sold_out_ends_listing(self):
        calls, _, _ = self.run_sync("売り切れ", retry_pass=True)
        calls.delete.assert_called_once()
        self.assertEqual(calls.delete.call_args.args[3], "売り切れ")

    def test_first_pass_determined_is_unchanged(self):
        calls, _, _ = self.run_sync("削除")
        calls.delete.assert_called_once()
        calls.issue.assert_not_called()
        self.assertEqual(calls.count.call_args.args[:3], ("在庫チェック", "メルカリ", "削除"))

    def test_other_vendors_are_deferred_too(self):
        for vendor, sku in (("ラクマ", "abc"), ("ＰａｙＰａｙフリマ", "z1"), ("メルカリshops", "2JX")):
            with self.subTest(vendor):
                calls = mock.MagicMock()
                conn = mock.MagicMock()
                with mock.patch.object(cr, "get_status", return_value=("判定不可", None)), \
                        mock.patch.object(cr, "update_vendor_item_price_and_status", calls.update), \
                        mock.patch.object(cr, "handle_listing_delete", calls.delete), \
                        mock.patch.object(cr.ops_stats, "issue", calls.issue), \
                        mock.patch.object(cr.ops_stats, "count"), \
                        mock.patch("builtins.print"):
                    cr.process_status_and_sync(conn, mock.MagicMock(), mock.MagicMock(), row(False, vendor, sku), "w")
                calls.update.assert_not_called()
                calls.delete.assert_not_called()
                calls.issue.assert_not_called()


class PullRemainingTargetsTest(unittest.TestCase):
    def pull(self, prev_by):
        conn = mock.MagicMock()
        cur = conn.cursor.return_value
        cur.fetchall.return_value = [("メルカリ", "m1", "川島", "237", "p", "DDP", 100, 2000, "p", prev_by)]
        rows = cr.pull_remaining_targets(conn, "w", batch_size=1)
        return rows, cur.execute.call_args.args[0]

    def test_retry_pass_flag_from_previous_marker(self):
        self.assertTrue(self.pull(cr.RETRY_WAIT_MARK)[0][0]["retry_pass"])
        self.assertFalse(self.pull(None)[0][0]["retry_pass"])
        self.assertFalse(self.pull("x162-43-15-160")[0][0]["retry_pass"])  # ロック切れの取り直しは1回目扱い

    def test_sql_defers_marked_rows(self):
        sql = self.pull(None)[1]
        self.assertIn(f"v.remaining_check_by = N'{cr.RETRY_WAIT_MARK}'", sql)
        self.assertIn(f"DATEADD(SECOND, -{cr.RETRY_WAIT_SEC}, SYSDATETIME())", sql)
        self.assertIn(f"ORDER BY CASE WHEN v.remaining_check_by = N'{cr.RETRY_WAIT_MARK}' THEN 1 ELSE 0 END", sql)
        self.assertIn("deleted.remaining_check_by", sql)
        self.assertEqual(cr.RETRY_WAIT_SEC, 60)

    def test_count_total_still_counts_deferred_rows(self):
        # 後回しの行は status 空欄・remaining_check_at NULL のままなので、待機（inventory_loop）は終わらない
        conn = mock.MagicMock()
        cr.count_total_remaining(conn)
        sql = conn.cursor.return_value.__enter__.return_value.execute.call_args.args[0]
        self.assertNotIn("remaining_check_by", sql)
        self.assertIn("v.remaining_check_at IS NULL", sql)


def issue_row(occ, notified_at=None, issue_id=1, sku="m59411904484"):
    return SimpleNamespace(issue_id=issue_id, vendor_name="メルカリ", vendor_item_id=sku, account="川島",
                           listing_id="237175828470", cause="items/get 応答なし", first_at=NOW - timedelta(hours=4),
                           last_at=NOW, occurrences=occ, notified_at=notified_at,
                           vendor_url=f"https://jp.mercari.com/item/{sku}")


class UndeterminedMailTest(unittest.TestCase):
    def test_due_rules(self):
        rows = [issue_row(1, issue_id=1),
                issue_row(2, issue_id=2),
                issue_row(3, NOW - timedelta(hours=3), issue_id=3),
                issue_row(7, NOW - timedelta(hours=25), issue_id=4)]
        due, first, waiting = dc.undetermined_due(rows, NOW)
        self.assertEqual([r.issue_id for r in due], [2, 4])
        self.assertEqual([r.issue_id for r in first], [1])     # 1サイクルだけ → メールしない
        self.assertEqual([r.issue_id for r in waiting], [3])   # 24時間以内に通知済み

    def notify(self, rows, mail_ok=True):
        conn = mock.MagicMock()
        cur = conn.cursor.return_value
        cur.execute.return_value.fetchall.return_value = rows
        with mock.patch.object(dc, "send_mail", return_value=mail_ok) as sm, mock.patch("builtins.print"):
            total = dc.notify_undetermined_remaining(conn, NOW - timedelta(minutes=40))
        return total, sm, [c.args[0] for c in cur.execute.call_args_list]

    def test_single_cycle_is_not_mailed(self):
        total, sm, sqls = self.notify([issue_row(1)])
        self.assertEqual(total, 1)
        sm.assert_not_called()
        self.assertFalse(any("notified_at = SYSDATETIME()" in s for s in sqls))

    def test_consecutive_is_mailed_and_marked(self):
        total, sm, sqls = self.notify([issue_row(1, issue_id=10, sku="a"), issue_row(2, issue_id=11, sku="b")])
        self.assertEqual(total, 2)
        subject, body = sm.call_args.args
        self.assertIn("1件", subject)
        self.assertIn("b", body)
        self.assertNotIn("item/a", body)
        self.assertIn("連続 2回", body)
        upd = [s for s in sqls if "notified_at = SYSDATETIME()" in s]
        self.assertEqual(len(upd), 1)
        self.assertIn("IN (11)", upd[0])

    def test_mail_failure_does_not_mark_notified(self):
        _, sm, sqls = self.notify([issue_row(2)], mail_ok=False)
        sm.assert_called_once()
        self.assertFalse(any("notified_at = SYSDATETIME()" in s for s in sqls))

    def test_query_limits_to_open_issues_of_this_round(self):
        sql = dc.SQL_UNDETERMINED_THIS_ROUND
        for part in ("i.issue_kind = N'判定不可'", "i.resolved_at IS NULL", "i.last_at >= ?", "l.is_deleted = 0"):
            self.assertIn(part, sql)


class ResolveRuleTest(unittest.TestCase):
    def test_no_time_based_resolution_for_undetermined(self):
        sql = oa.SQL_RESOLVE_ISSUES
        self.assertFalse(hasattr(oa, "JUDGE_STALE_HOURS"))
        self.assertIn(ops_stats.UNDETERMINED_RECHECKED_SQL.strip(), sql)
        self.assertNotIn("N'判定不可' AND i.last_at <", sql)
        # 出品終了失敗の24時間・eBay 終了での解決は従来どおり
        self.assertIn("N'出品終了失敗' AND i.last_at < DATEADD(HOUR, -24", sql)
        self.assertIn("NOT EXISTS (SELECT 1 FROM trx.listings l", sql)

    def test_rechecked_condition(self):
        s = ops_stats.UNDETERMINED_RECHECKED_SQL
        self.assertIn("v.last_checked_at > DATEADD(SECOND, 1, i.last_at)", s)
        self.assertIn("ISNULL(v.status, N'') <> N'判定不可'", s)

    def test_resolve_helper_is_noop_in_tests_and_scoped_to_item(self):
        with mock.patch.object(ops_stats, "get_sql_server_connection") as g:
            ops_stats.resolve_undetermined_if_rechecked("メルカリ", "m1")
            g.assert_not_called()   # OPS_STATS_DISABLED=1（apps/tests/__init__.py）
            with mock.patch.dict(os.environ, {"OPS_STATS_DISABLED": "0"}):
                ops_stats.resolve_undetermined_if_rechecked("メルカリ", "m1")
            cur = g.return_value.cursor.return_value
            sql, params = cur.execute.call_args.args
            self.assertIn("i.vendor_name = ? AND i.vendor_item_id = ?", sql)
            self.assertEqual(params, ("メルカリ", "m1"))
            g.return_value.commit.assert_called_once()

    def test_resolve_helper_failure_is_logged_only(self):
        with mock.patch.dict(os.environ, {"OPS_STATS_DISABLED": "0"}), \
                mock.patch.object(ops_stats, "get_sql_server_connection", side_effect=RuntimeError("db down")), \
                mock.patch("builtins.print") as p:
            ops_stats.resolve_undetermined_if_rechecked("メルカリ", "m1")
        self.assertIn("処理は続ける", p.call_args.args[0])


class OutageStatsTest(unittest.TestCase):
    def test_retry_wait_counts_as_failure_for_wide_outage(self):
        sql = oa.SQL_CHECK_STATS
        self.assertIn(f"N'{ops_stats.CHECK_RETRY_WAIT_KIND}'", sql)
        self.assertIn("N'再取得待ち'", sql)


if __name__ == "__main__":
    unittest.main()
