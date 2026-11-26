# -*- coding: utf-8 -*-
"""
出品終了後の DB 反映の再試行（案2）と、通知前の eBay 実態確認（案1）のテスト（2026-11-26）。
eBay・DB・メールには接続しない（すべてモック）。
実行: python -m unittest apps.tests.test_listing_end_db_retry -v
"""
import contextlib
import io
import unittest
from datetime import datetime, timedelta
from unittest import mock

import pyodbc

from apps.adapters import mercari_item_status as mi
from apps.inventory import ops_alerts as oa

ACC, LID, SKU, VN = "谷川④", "137953368991", "z729874026", "ＰａｙＰａｙフリマ"
BUSY = pyodbc.Error("HY000", "[HY000] Connection is busy with results for another command (0) (SQLExecDirectW)")
LOGIN = pyodbc.OperationalError("HYT00", "[HYT00] Login timeout expired")
PUBLISHED = {"success": True, "offer_id": "1", "offer_status": "PUBLISHED", "offer_listing_id": LID}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        if self.conn.errors:
            raise self.conn.errors.pop(0)
        self.conn.updates.append(params)


class FakeConn:
    """errors: execute のたびに先頭から投げる例外"""
    def __init__(self, errors=()):
        self.errors = list(errors)
        self.updates = []
        self.closed = False
        self.rolled_back = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed = True


class RecordListingEndedTest(unittest.TestCase):
    def run_record(self, caller_errors=(), new_conns=()):
        caller = FakeConn(caller_errors)
        connect = mock.MagicMock(side_effect=list(new_conns))   # 例外のインスタンスはその呼び出しで投げられる
        out = io.StringIO()
        with mock.patch.object(mi, "get_sql_server_connection", connect), \
                mock.patch.object(mi, "log_listings_change") as lc, \
                mock.patch.object(mi.ops_stats, "count") as cnt, \
                mock.patch.object(mi.ops_stats, "issue") as iss, \
                mock.patch.object(mi.time, "sleep") as sl, \
                contextlib.redirect_stdout(out):
            ok = mi.record_listing_ended(caller, ACC, LID, SKU, VN, "削除", 1)
        return dict(ok=ok, caller=caller, connect=connect, lc=lc, cnt=cnt, iss=iss,
                    sleep=[c.args[0] for c in sl.call_args_list], log=out.getvalue())

    def test_normal_uses_caller_conn_once(self):
        r = self.run_record()
        self.assertTrue(r["ok"])
        self.assertEqual(len(r["caller"].updates), 1)
        r["connect"].assert_not_called()
        r["lc"].assert_called_once()
        self.assertEqual(r["cnt"].call_args.args[2], "成功")
        r["iss"].assert_not_called()

    def test_busy_then_new_connection_succeeds(self):
        """11/25 の事例: 呼び出し元の接続で Connection is busy → 新しい接続で反映できる"""
        fresh = FakeConn()
        r = self.run_record(caller_errors=[BUSY], new_conns=[fresh])
        self.assertTrue(r["ok"])
        self.assertEqual(r["caller"].rolled_back, 1)
        self.assertEqual(len(fresh.updates), 1)
        self.assertTrue(fresh.closed)
        self.assertFalse(r["caller"].closed)            # 呼び出し元の接続は閉じない
        self.assertEqual(r["sleep"], [2])
        self.assertIn("[DELETE_DB_RETRY]", r["log"])
        r["lc"].assert_called_once()
        self.assertEqual(r["cnt"].call_args.kwargs["retried"], 1)
        r["iss"].assert_not_called()

    def test_same_deleted_at_on_every_attempt(self):
        fresh = FakeConn()
        r = self.run_record(caller_errors=[BUSY], new_conns=[LOGIN, fresh])
        self.assertTrue(r["ok"])
        self.assertEqual(r["sleep"], [2, 5])
        self.assertEqual(fresh.updates[0][1:], ("削除", LID, ACC))

    def test_all_fail_records_issue_and_does_not_raise(self):
        n = len(mi.END_DB_RETRY_WAITS)
        r = self.run_record(caller_errors=[BUSY], new_conns=[LOGIN] * (n - 1))
        self.assertFalse(r["ok"])
        self.assertEqual(r["connect"].call_count, n - 1)
        self.assertEqual(r["sleep"], [2, 5, 15, 30, 60])
        self.assertIn("[DELETE_DB_FAILED]", r["log"])
        r["lc"].assert_not_called()
        self.assertEqual(r["cnt"].call_args.args[2], "失敗")
        self.assertEqual(r["cnt"].call_args.kwargs["cause"], mi.END_DB_FAILED_CAUSE)
        kind, vn, sku, detail = r["iss"].call_args.args
        self.assertEqual((kind, vn, sku), ("出品終了失敗", VN, SKU))
        self.assertIn("eBay終了済み・DB反映失敗", detail)
        self.assertEqual(r["iss"].call_args.kwargs, {"account": ACC, "listing_id": LID})


class HandleDeleteNoRepeatTest(unittest.TestCase):
    def test_ebay_end_is_not_repeated_on_db_failure(self):
        """eBay の終了は1回だけ。DB の反映を再試行しても offer 削除・EndItems は繰り返さない"""
        caller = FakeConn([BUSY])
        fresh = FakeConn()
        with mock.patch.object(mi, "get_active_listing_for_action", return_value=(LID, ACC)), \
                mock.patch.object(mi, "delete_item_from_ebay", return_value=PUBLISHED) as dl, \
                mock.patch.object(mi, "delete_items_from_ebay_batch") as ei, \
                mock.patch.object(mi, "end_listing_by_id_verified") as fb, \
                mock.patch.object(mi, "get_sql_server_connection", return_value=fresh), \
                mock.patch.object(mi, "log_listings_change"), \
                mock.patch.object(mi.ops_stats, "count"), \
                mock.patch.object(mi.ops_stats, "issue") as iss, \
                mock.patch.object(mi.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            mi.handle_listing_delete(caller, SKU, VN, "削除")      # 例外にならない
        dl.assert_called_once()
        ei.assert_not_called()
        fb.assert_not_called()
        self.assertEqual(len(fresh.updates), 1)
        iss.assert_not_called()

    def test_all_db_fail_does_not_raise(self):
        caller = FakeConn([BUSY])
        with mock.patch.object(mi, "get_active_listing_for_action", return_value=(LID, ACC)), \
                mock.patch.object(mi, "delete_item_from_ebay", return_value=PUBLISHED) as dl, \
                mock.patch.object(mi, "get_sql_server_connection", side_effect=LOGIN), \
                mock.patch.object(mi, "log_listings_change"), \
                mock.patch.object(mi.ops_stats, "count"), \
                mock.patch.object(mi.ops_stats, "issue") as iss, \
                mock.patch.object(mi.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            mi.handle_listing_delete(caller, SKU, VN, "削除")
        dl.assert_called_once()
        iss.assert_called_once()


class VerifyOnEbayTest(unittest.TestCase):
    def run_verify(self, status, mark=True, active_rows=1):
        conn = mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (active_rows,)
        gi = mock.MagicMock(side_effect=status) if isinstance(status, Exception) else mock.MagicMock(return_value=status)
        mk = mock.MagicMock(side_effect=mark) if isinstance(mark, Exception) else mock.MagicMock(return_value=mark)
        with mock.patch("apps.adapters.ebay_api.get_listing_status", gi), \
                mock.patch("apps.adapters.ebay_api.mark_listing_deleted_if_ended", mk):
            verdict, note = oa.verify_on_ebay(conn, ACC, LID, SKU)
        return verdict, note, mk

    def test_ended_and_fixed(self):
        v, note, mk = self.run_verify({"ok": True, "ended": True, "listing_status": "Completed",
                                       "end_time": "2026-11-25T14:53:03.000Z"})
        self.assertEqual(v, oa.VERIFY_FIXED)
        mk.assert_called_once_with(ACC, LID, SKU, oa.VERIFIED_END_REASON)

    def test_active(self):
        v, note, mk = self.run_verify({"ok": True, "ended": False, "listing_status": "Active"})
        self.assertEqual(v, oa.VERIFY_ACTIVE)
        mk.assert_not_called()

    def test_getitem_failed(self):
        v, note, mk = self.run_verify({"ok": False, "ended": False, "error": "http_status:503"})
        self.assertEqual(v, oa.VERIFY_UNKNOWN)
        self.assertIn("確認できません", note)
        mk.assert_not_called()

    def test_getitem_exception(self):
        v, note, _ = self.run_verify(RuntimeError("token"))
        self.assertEqual(v, oa.VERIFY_UNKNOWN)

    def test_ended_but_db_error(self):
        v, note, _ = self.run_verify({"ok": True, "ended": True, "listing_status": "Completed"}, mark=LOGIN)
        self.assertEqual(v, oa.VERIFY_DB_FAILED)
        self.assertIn("終了済み", note)
        self.assertIn("DB への反映に失敗", note)

    def test_ended_mark_zero_rows_but_already_ended_in_db(self):
        """同じ時に他の経路が終了済みにした → 解決（通知しない）"""
        v, _, _ = self.run_verify({"ok": True, "ended": True, "listing_status": "Completed"}, mark=False, active_rows=0)
        self.assertEqual(v, oa.VERIFY_FIXED)

    def test_ended_mark_zero_rows_still_active_in_db(self):
        v, _, _ = self.run_verify({"ok": True, "ended": True, "listing_status": "Completed"}, mark=False, active_rows=1)
        self.assertEqual(v, oa.VERIFY_DB_FAILED)

    def test_no_listing_id(self):
        conn = mock.MagicMock()
        v, _ = oa.verify_on_ebay(conn, ACC, None, SKU)
        self.assertEqual(v, oa.VERIFY_UNKNOWN)


class CheckListingEndMailTest(unittest.TestCase):
    COLS = ["issue_id", "issue_kind", "vendor_name", "vendor_item_id", "account", "listing_id", "cause",
            "first_at", "last_at", "occurrences", "notified_at", "age_min"]

    def row(self, issue_id, sku, lid, age_min=130, kind="仕入不可出品中", notified_at=None):
        now = datetime(2026, 11, 26, 2, 0)
        return (issue_id, kind, VN, sku, ACC, lid, "仕入先=削除", now - timedelta(minutes=age_min), now,
                8, notified_at, age_min)

    def run_check(self, rows, verdicts):
        conn = mock.MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.description = [(c,) for c in self.COLS]
        cur.fetchall.return_value = rows
        vf = mock.MagicMock(side_effect=[(v, f"note-{v}") for v in verdicts])
        with mock.patch.object(oa, "scan_unavailable_active"), \
                mock.patch.object(oa, "verify_on_ebay", vf), \
                mock.patch.object(oa.ops_stats, "count") as cnt, \
                mock.patch.object(oa.ops_stats, "flush"), \
                mock.patch.object(oa, "send_mail", return_value=True) as sm:
            oa.check_listing_end(conn, datetime(2026, 11, 26, 2, 0))
        sqls = [c.args[0] for c in cur.execute.call_args_list]
        return sm, sqls, vf, cnt

    def test_fixed_is_not_mailed_and_resolved(self):
        sm, sqls, vf, cnt = self.run_check([self.row(13, SKU, LID)], [oa.VERIFY_FIXED])
        sm.assert_not_called()
        self.assertEqual(sqls.count(oa.SQL_RESOLVE_ISSUES), 2)    # 一次の解決 + 是正後の解決
        self.assertFalse(any("SET notified_at" in s for s in sqls))
        self.assertEqual(cnt.call_args.args[:3], (oa.VERIFY_KIND, VN, oa.VERIFY_FIXED))

    def test_active_is_mailed_as_before(self):
        sm, sqls, _, _ = self.run_check([self.row(13, SKU, LID)], [oa.VERIFY_ACTIVE])
        subject, body = sm.call_args.args
        self.assertIn("【重要】仕入先で購入できないのに eBay で出品中の商品があります（1件）", subject)
        self.assertIn(f"[{oa.VERIFY_ACTIVE}]", body)
        self.assertTrue(any("SET notified_at" in s and "(13)" in s for s in sqls))

    def test_unknown_and_db_failed_are_mailed_with_state(self):
        rows = [self.row(1, "a", "11"), self.row(2, "b", "22")]
        sm, _, _, _ = self.run_check(rows, [oa.VERIFY_UNKNOWN, oa.VERIFY_DB_FAILED])
        subject, body = sm.call_args.args
        self.assertIn("【要確認】", subject)
        self.assertIn("（2件）", subject)
        self.assertIn(f"[{oa.VERIFY_UNKNOWN}]", body)
        self.assertIn(f"[{oa.VERIFY_DB_FAILED}]", body)

    def test_mixed_only_unfixed_are_mailed_and_marked(self):
        rows = [self.row(1, "a", "11"), self.row(2, "b", "22")]
        sm, sqls, _, _ = self.run_check(rows, [oa.VERIFY_FIXED, oa.VERIFY_ACTIVE])
        subject, body = sm.call_args.args
        self.assertIn("（1件）", subject)
        self.assertNotIn(" a ", body)
        upd = [s for s in sqls if "SET notified_at" in s][0]
        self.assertIn("(2)", upd)

    def test_not_due_does_not_call_getitem(self):
        """2時間未満・24時間以内に通知済みの明細は GetItem もしない（通知条件は変えない）"""
        rows = [self.row(1, "a", "11", age_min=60),
                self.row(2, "b", "22", notified_at=datetime(2026, 11, 26, 1, 0))]
        sm, _, vf, _ = self.run_check(rows, [])
        vf.assert_not_called()
        sm.assert_not_called()



class RemainingWorkerConnResetTest(unittest.TestCase):
    """1商品の DB 書き込みで接続が使えなくなっても、接続を作り直して後続の商品を処理する"""

    def test_next_item_is_processed_with_new_connection(self):
        from apps.inventory import check_remaining_ebay as cr
        rows = [[{"vendor_name": VN, "vendor_item_id": "z1"}], [{"vendor_name": VN, "vendor_item_id": "z2"}], []]
        conns = [mock.MagicMock(name=f"conn{i}") for i in range(3)]     # pull / work / 作り直した work
        seen = []

        def proc(conn, page, driver, row, worker_name):
            seen.append((row["vendor_item_id"], conn))
            if row["vendor_item_id"] == "z1":
                raise BUSY

        with mock.patch.object(cr, "sync_playwright"), \
                mock.patch.object(cr, "playwright_driver_monitor"), \
                mock.patch.object(cr, "start_watchdog") as wd, \
                mock.patch.object(cr, "apply_default_route_filter"), \
                mock.patch.object(cr, "get_sql_server_connection", side_effect=conns), \
                mock.patch.object(cr, "pull_remaining_targets", side_effect=rows), \
                mock.patch.object(cr, "count_total_remaining", return_value=0), \
                mock.patch.object(cr, "process_status_and_sync", side_effect=proc), \
                mock.patch.object(cr, "safe_quit"), \
                mock.patch.object(cr.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(SystemExit) as ex:
                cr.run_remaining_worker("w")
        self.assertEqual(ex.exception.code, 0)
        self.assertEqual(seen, [("z1", conns[1]), ("z2", conns[2])])
        conns[1].close.assert_called()
        self.assertIn("[WORK_CONN_RESET] sku=z1", out.getvalue())
        self.assertEqual(wd.return_value.cancel.call_count, 2)      # 1件ごとに監視を解除（残った監視で自己終了しない）

    def test_reconnect_failure_still_exits_as_before(self):
        from apps.inventory import check_remaining_ebay as cr
        with mock.patch.object(cr, "sync_playwright"), \
                mock.patch.object(cr, "playwright_driver_monitor"), \
                mock.patch.object(cr, "start_watchdog"), \
                mock.patch.object(cr, "apply_default_route_filter"), \
                mock.patch.object(cr, "get_sql_server_connection", side_effect=[mock.MagicMock(), mock.MagicMock(), LOGIN]), \
                mock.patch.object(cr, "pull_remaining_targets", return_value=[{"vendor_name": VN, "vendor_item_id": "z1"}]), \
                mock.patch.object(cr, "process_status_and_sync", side_effect=BUSY), \
                mock.patch.object(cr, "safe_quit"), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(pyodbc.OperationalError):
                cr.run_remaining_worker("w")


if __name__ == "__main__":
    unittest.main()
