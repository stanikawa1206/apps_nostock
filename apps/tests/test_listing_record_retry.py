# -*- coding: utf-8 -*-
"""
出品成功後の DB 記録の再試行と、キャンセル済み注文の出品の是正のテスト（2026-11-25）。
DB・eBay・メールには接続しない（すべてモック）。
実行: python -m unittest apps.tests.test_listing_record_retry -v
"""
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyodbc

from apps.publish import publish_ebay as pe
from apps.adapters.ebay_api import ListingStateUnknownError

CONN_ERR = pyodbc.OperationalError("08001", "[08001] TCP Provider: Timeout error [258]. Login timeout expired")
LINK_ERR = pyodbc.OperationalError("08S01", "[08S01] Communication link failure")
DEADLOCK = pyodbc.Error("40001", "deadlock (1205)")
PK_ERR = pyodbc.IntegrityError("23000", "Violation of PRIMARY KEY constraint (2627)")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.last = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *params):
        self.conn.db.sql.append(sql)
        if "SELECT account, vendor_item_id FROM trx.listings" in sql:
            self.last = self.conn.db.existing
        elif "INSERT INTO [trx].[listings]" in sql:
            if self.conn.db.insert_errors:
                raise self.conn.db.insert_errors.pop(0)
            self.conn.pending.append("insert")
        elif "SET listing_left" in sql or "listing_left_collectibles - 1" in sql:
            self.conn.pending.append("decrement")
            self.last = ("listing_left", 99)
        return self

    def fetchone(self):
        return self.last


class FakeConn:
    def __init__(self, db):
        self.db = db
        self.pending = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        if self.db.commit_errors:
            e = self.db.commit_errors.pop(0)
            if self.db.commit_lands:          # commit は届いたが応答が失われた
                self.db.committed += self.pending
                self.db.existing = ("谷川②", "m50264903150")
            self.pending = []
            raise e
        self.db.committed += self.pending
        self.pending = []

    def rollback(self):
        self.pending = []

    def close(self):
        pass


class FakeDb:
    def __init__(self, connect_errors=(), insert_errors=(), commit_errors=(), existing=None, commit_lands=False):
        self.connect_errors = list(connect_errors)
        self.insert_errors = list(insert_errors)
        self.commit_errors = list(commit_errors)
        self.commit_lands = commit_lands
        self.existing = existing
        self.committed = []
        self.sql = []
        self.connects = 0

    def connect(self):
        self.connects += 1
        if self.connect_errors:
            raise self.connect_errors.pop(0)
        return FakeConn(self)


class RecordListingRetryTest(unittest.TestCase):
    def run_record(self, db):
        out = io.StringIO()
        with mock.patch.object(pe, "get_sql_server_connection", db.connect), \
                mock.patch.object(pe, "log_listings_change") as lc, \
                mock.patch.object(pe.time, "sleep") as sl, \
                contextlib.redirect_stdout(out):
            raised = None
            try:
                pe.record_ebay_listing("227616250122", "谷川②", "m50264903150", "メルカリ", 269.99, True)
            except Exception as e:
                raised = e
        return dict(raised=raised, lc=lc, sleep=[c.args[0] for c in sl.call_args_list], log=out.getvalue())

    def test_connection_timeout_then_success(self):
        """11/16 の事例: 接続の Login timeout → 2秒後の再試行で登録・残数の減算が1回ずつ"""
        db = FakeDb(connect_errors=[CONN_ERR])
        r = self.run_record(db)
        self.assertIsNone(r["raised"])
        self.assertEqual(db.committed, ["insert", "decrement"])
        self.assertEqual(r["sleep"], [2])
        r["lc"].assert_called_once()
        self.assertIn("[RECORD_LISTING_RETRY]", r["log"])

    def test_commit_landed_but_reply_lost_is_not_doubled(self):
        """commit が実は成功していた（通信断で応答だけ失われた）→ 再試行で登録済みと分かり、二重登録・二重減算しない"""
        db = FakeDb(commit_errors=[LINK_ERR], commit_lands=True)
        r = self.run_record(db)
        self.assertIsNone(r["raised"])
        self.assertEqual(db.committed, ["insert", "decrement"])
        self.assertIn("[RECORD_LISTING_EXISTS]", r["log"])
        r["lc"].assert_not_called()

    def test_existing_row_for_other_item_is_state_unknown(self):
        """同じ listing_id で別のアカウント・商品が登録済み → 記録せず出品確認不可"""
        db = FakeDb(existing=("谷川④", "OTHER"))
        r = self.run_record(db)
        self.assertIsInstance(r["raised"], ListingStateUnknownError)
        self.assertEqual(db.committed, [])

    def test_all_retries_fail_is_state_unknown(self):
        db = FakeDb(connect_errors=[CONN_ERR] * len(pe.RECORD_LISTING_RETRY_WAITS))
        r = self.run_record(db)
        self.assertIsInstance(r["raised"], ListingStateUnknownError)
        self.assertEqual(db.connects, 6)
        self.assertEqual(r["sleep"], [2, 5, 15, 30, 60])

    def test_deadlock_still_retried(self):
        db = FakeDb(insert_errors=[DEADLOCK])
        r = self.run_record(db)
        self.assertIsNone(r["raised"])
        self.assertEqual(db.committed, ["insert", "decrement"])

    def test_other_db_error_is_state_unknown_not_plain_failure(self):
        """再試行しないエラー（主キー違反など）も、出品失敗ではなく出品確認不可（再出品されない）"""
        db = FakeDb(insert_errors=[PK_ERR])
        r = self.run_record(db)
        self.assertIsInstance(r["raised"], ListingStateUnknownError)
        self.assertEqual(db.connects, 1)

    def test_normal_single_attempt(self):
        db = FakeDb()
        r = self.run_record(db)
        self.assertIsNone(r["raised"])
        self.assertEqual(db.connects, 1)
        self.assertEqual(r["sleep"], [])


class StateUnknownFallbackTest(unittest.TestCase):
    """全再試行に失敗 → 出品不可にする。出品不可の更新にも失敗 → メールは先に送り、ファイルに残して次回起動時に出品不可"""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        p = mock.patch.object(pe, "STATE_UNKNOWN_PENDING_PATH", Path(tmp.name) / "pending.jsonl")
        p.start()
        self.addCleanup(p.stop)
        self.err = ListingStateUnknownError(-1, "出品成功(listing_id=1)だがDB記録・残数減算が6回とも失敗")
        self.rec = {"vendor_item_id": "SKU1", "vendor_name": "メルカリ"}

    def run_mark(self, upsert_ok, flag_results):
        conn = mock.MagicMock()
        order = []
        mail = mock.MagicMock(side_effect=lambda *a, **k: order.append("mail"))
        flag = mock.MagicMock(side_effect=lambda *a: (order.append("flag"), flag_results.pop(0))[1])
        upsert = mock.MagicMock(side_effect=None if upsert_ok else pyodbc.OperationalError("08S01", "link"))
        with mock.patch.object(pe, "send_mail", mail), \
                mock.patch.object(pe, "upsert_vendor_item", upsert), \
                mock.patch.object(pe, "_set_listing_state_unknown_flag", flag), \
                mock.patch.object(pe.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            pe.mark_listing_state_unknown(conn, self.rec, "谷川②", "SKU1", self.err, True)
        return order, flag, conn

    def test_normal_flag(self):
        order, flag, conn = self.run_mark(True, [])
        self.assertEqual(order, ["mail"])
        flag.assert_not_called()
        conn.commit.assert_called()

    def test_main_conn_broken_then_flag_with_new_connection(self):
        order, flag, _ = self.run_mark(False, [False, True])
        self.assertEqual(order, ["mail", "flag", "flag"])
        self.assertFalse(pe.STATE_UNKNOWN_PENDING_PATH.exists())

    def test_flag_never_succeeds_goes_to_pending_file_then_applied(self):
        order, flag, _ = self.run_mark(False, [False] * len(pe.RECORD_LISTING_RETRY_WAITS))
        self.assertEqual(order[0], "mail")   # メールは DB に触れる前
        self.assertTrue(pe.STATE_UNKNOWN_PENDING_PATH.exists())
        # 次回起動時: DB に書ければファイルから消える。書けなければ残る
        with mock.patch.object(pe, "_set_listing_state_unknown_flag", return_value=False), \
                contextlib.redirect_stdout(io.StringIO()):
            pe.apply_state_unknown_pending()
        self.assertTrue(pe.STATE_UNKNOWN_PENDING_PATH.exists())
        with mock.patch.object(pe, "_set_listing_state_unknown_flag", return_value=True) as f, \
                contextlib.redirect_stdout(io.StringIO()):
            pe.apply_state_unknown_pending()
        f.assert_called_once_with("SKU1", str(self.err))
        self.assertFalse(pe.STATE_UNKNOWN_PENDING_PATH.exists())

    def test_apply_without_file_does_nothing(self):
        with mock.patch.object(pe, "_set_listing_state_unknown_flag") as f:
            pe.apply_state_unknown_pending()
        f.assert_not_called()


class CancelledListingsTest(unittest.TestCase):
    """キャンセル済みの注文で DB が出品中のままの出品を、eBay の終了を確かめて終了済みにする"""

    def setUp(self):
        from apps.inventory import fetch_orders_ebay as fo
        self.fo = fo
        p = mock.patch.object(fo, "log")
        p.start()
        self.addCleanup(p.stop)

    def run_close(self, rows, ended, dry_run=False):
        cn = mock.MagicMock()
        cn.cursor.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = rows
        with mock.patch.object(self.fo, "get_sql_server_connection", return_value=cn), \
                mock.patch("apps.adapters.ebay_api.mark_listing_deleted_if_ended", side_effect=ended) as mk:
            n = self.fo.close_cancelled_listings(dry_run=dry_run)
        return n, mk

    def test_only_confirmed_cancel_and_active(self):
        sql = self.fo.SQL_CANCELLED_ACTIVE_LISTINGS
        self.assertIn("o.status = N'キャンセル済み'", sql)
        self.assertNotIn("申請中", sql)
        self.assertIn("l.is_deleted = 0", sql)
        self.assertNotIn("ebay_accounts", sql)     # 出品残数・当日の目標は変えない

    def test_closes_when_ebay_ended(self):
        n, mk = self.run_close([("谷川④", "137881105541", "327a")], [True])
        self.assertEqual(n, 1)
        mk.assert_called_once_with("谷川④", "137881105541", "327a", "キャンセル(eBay終了確認)")

    def test_active_on_ebay_keeps_db(self):
        n, mk = self.run_close([("谷川④", "1", "a")], [False])
        self.assertEqual(n, 0)

    def test_dry_run_does_not_touch(self):
        n, mk = self.run_close([("谷川④", "1", "a")], [True], dry_run=True)
        mk.assert_not_called()


if __name__ == "__main__":
    unittest.main()
