# -*- coding: utf-8 -*-
"""
publishOffer がエラーでも出品されていて、offer が UNPUBLISHED のまま残った出品の自動復旧のテスト
（ebay_api._recover_orphan_listing / _confirm_published_listing。eBay・メール・待機はモック）。

  - offer が既に PUBLISHED なら何もせずその出品番号を返す
  - 通常: 終了 → 終了確認 → offer再公開 → 結び付き確認 で新しい出品番号を返す（終了確認の前に再公開しない）
  - 終了を確認できない: 再公開せず元の出品番号を返し、通知する
  - 再公開に失敗: 送り直さず OrphanRepublishPendingError（一時的な失敗は retryable=True）
  - 保留からの再処理 retry_orphan_republish: 元の出品の終了・同じSKUの出品の有無を確認してから再公開する
  - 保留の記録と再処理（publish_ebay、テスト専用DB nostock_test_unpub）: 再試行の間隔・上限・通知・出品不可flgの解除
  - _confirm_published_listing が GetSellerList で見つけた出品は復旧に回す
実行: python -m unittest apps.tests.test_unpublished_recovery -v
"""

import contextlib
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from apps.adapters import ebay_api
from apps.adapters.ebay_api import ApiHandledError, OrphanRepublishPendingError
from apps.common.utils import get_sql_server_connection
from apps.publish import publish_ebay as pe

UNPUB = {"status": "UNPUBLISHED"}
ENDED = {"ok": True, "ended": True, "listing_status": "Ended", "quantity_sold": "0"}
ACTIVE = {"ok": True, "ended": False, "listing_status": "Active", "quantity_sold": "0"}


def _published(listing_id):
    return {"status": "PUBLISHED", "listing": {"listingId": listing_id, "listingStatus": "ACTIVE"}}


def _resp(status, body):
    r = mock.Mock()
    r.status_code = status
    r.json.return_value = body
    return r


class RecoverOrphanListingTest(unittest.TestCase):
    def setUp(self):
        self.calls = []
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log_path = Path(tmp.name) / "unpublished_recovery.log"
        self.end = mock.Mock(side_effect=lambda *a, **k: self.calls.append("end") or {"results": [{"success": True}]})
        self.mail = mock.Mock()
        for p in (mock.patch.object(ebay_api, "ORPHAN_RECOVERY_LOG_PATH", self.log_path),
                  mock.patch.object(ebay_api.time, "sleep"),
                  mock.patch.object(ebay_api, "delete_items_from_ebay_batch", self.end),
                  mock.patch.object(ebay_api, "send_mail", self.mail),
                  mock.patch("builtins.print")):
            p.start()
            self.addCleanup(p.stop)

    def _run(self, offers, statuses, publish_resp=None, found=(True, None)):
        get_offer = mock.Mock(side_effect=offers)
        status = mock.Mock(side_effect=statuses)
        post = mock.Mock(side_effect=lambda *a, **k: self.calls.append("publish") or publish_resp)
        with mock.patch.object(ebay_api, "_inventory_get_offer", get_offer), \
                mock.patch.object(ebay_api, "get_listing_status", status), \
                mock.patch.object(ebay_api.requests, "post", post), \
                mock.patch.object(ebay_api, "_find_listing_by_sku_started_since", return_value=found):
            result = ebay_api._recover_orphan_listing("BUZZ②", "T", "SKU1", "O1", "OLD1")
        return result, post

    def test_already_linked_does_nothing(self):
        result, post = self._run([_published("L9")], [])
        self.assertEqual(result, "L9")
        self.end.assert_not_called()
        post.assert_not_called()

    def test_end_then_republish(self):
        result, post = self._run([UNPUB, UNPUB, _published("NEW1")], [ACTIVE, ENDED],
                                 publish_resp=_resp(200, {"listingId": "NEW1"}))
        self.assertEqual(result, "NEW1")
        self.assertEqual(self.calls, ["end", "publish"])
        self.mail.assert_not_called()
        self.assertIn("[ORPHAN_RECOVERY][DONE]", self.log_path.read_text(encoding="utf-8"))

    def test_end_not_confirmed_keeps_orphan(self):
        result, post = self._run([UNPUB], [ACTIVE] * len(ebay_api.ORPHAN_END_CHECK_WAITS))
        self.assertEqual(result, "OLD1")
        post.assert_not_called()
        self.mail.assert_called_once()

    def test_sold_listing_is_not_republished(self):
        with self.assertRaises(OrphanRepublishPendingError) as cm:
            self._run([UNPUB], [dict(ENDED, quantity_sold="1")])
        self.assertFalse(cm.exception.retryable)
        self.assertEqual(self.calls, ["end"])

    def test_offer_not_unpublished_after_end_is_not_republished(self):
        with self.assertRaises(OrphanRepublishPendingError) as cm:
            self._run([UNPUB, {"status": None}], [ENDED])
        self.assertFalse(cm.exception.retryable)
        self.assertEqual(self.calls, ["end"])

    def test_offer_fetch_error_after_end_is_retryable(self):
        with self.assertRaises(OrphanRepublishPendingError) as cm:
            self._run([UNPUB, ebay_api.requests.exceptions.ConnectionError("x")], [ENDED])
        self.assertTrue(cm.exception.retryable)
        self.assertEqual(self.calls, ["end"])

    def test_transient_republish_failure_is_retryable_without_resend(self):
        with self.assertRaises(OrphanRepublishPendingError) as cm:
            self._run([UNPUB, UNPUB], [ENDED], publish_resp=_resp(500, {"errors": [{"errorId": 25001}]}),
                      found=(True, None))
        self.assertTrue(cm.exception.retryable)
        self.assertEqual((cm.exception.offer_id, cm.exception.orphan_id), ("O1", "OLD1"))
        self.assertEqual(self.calls, ["end", "publish"])

    def test_definite_republish_error_is_not_retryable(self):
        body = {"errors": [{"errorId": 25007, "message": "Invalid fulfillment policy"}]}
        with self.assertRaises(OrphanRepublishPendingError) as cm:
            self._run([UNPUB, UNPUB], [ENDED], publish_resp=_resp(400, body), found=(True, None))
        self.assertFalse(cm.exception.retryable)

    def test_republish_error_but_listed_returns_found_id(self):
        result, post = self._run([UNPUB, UNPUB], [ENDED], publish_resp=_resp(500, {}), found=(True, "NEW2"))
        self.assertEqual(result, "NEW2")
        self.assertEqual(post.call_count, 1)
        self.mail.assert_called_once()


class ConfirmPublishedListingTest(unittest.TestCase):
    def test_found_by_seller_list_goes_to_recovery(self):
        err = ApiHandledError(25002, "System error")
        with mock.patch.object(ebay_api.time, "sleep"), mock.patch("builtins.print"), \
                mock.patch.object(ebay_api, "_inventory_get_offer", return_value=UNPUB), \
                mock.patch.object(ebay_api, "_find_listing_by_sku_started_since", return_value=(True, "OLD1")), \
                mock.patch.object(ebay_api, "_recover_orphan_listing", return_value="NEW1") as rec:
            result = ebay_api._confirm_published_listing("BUZZ②", "T", "SKU1", "O1", None, err)
        self.assertEqual(result, "NEW1")
        rec.assert_called_once_with("BUZZ②", "T", "SKU1", "O1", "OLD1")

    def test_published_offer_is_returned_without_recovery(self):
        err = ApiHandledError(25002, "System error")
        with mock.patch.object(ebay_api.time, "sleep"), mock.patch("builtins.print"), \
                mock.patch.object(ebay_api, "_inventory_get_offer", return_value=_published("L1")), \
                mock.patch.object(ebay_api, "_recover_orphan_listing") as rec:
            result = ebay_api._confirm_published_listing("BUZZ②", "T", "SKU1", "O1", None, err)
        self.assertEqual(result, "L1")
        rec.assert_not_called()


class RetryOrphanRepublishTest(unittest.TestCase):
    """保留からの再処理（ebay_api.retry_orphan_republish）。"""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.post = mock.Mock(return_value=_resp(200, {"listingId": "NEW1"}))
        for p in (mock.patch.object(ebay_api, "ORPHAN_RECOVERY_LOG_PATH", Path(tmp.name) / "r.log"),
                  mock.patch.object(ebay_api.time, "sleep"),
                  mock.patch.object(ebay_api, "get_access_token_new", return_value="T"),
                  mock.patch.object(ebay_api.requests, "post", self.post),
                  mock.patch("builtins.print")):
            p.start()
            self.addCleanup(p.stop)

    def _run(self, status, found=(True, None), offers=None):
        priced = dict(_published("NEW1"), pricingSummary={"price": {"value": "139.99"}})
        offers = offers or [UNPUB, priced, priced]
        with mock.patch.object(ebay_api, "get_listing_status", return_value=status), \
                mock.patch.object(ebay_api, "_find_listing_by_sku_started_since", return_value=found), \
                mock.patch.object(ebay_api, "_inventory_get_offer", side_effect=offers):
            return ebay_api.retry_orphan_republish("BUZZ②", "SKU1", "O1", "OLD1", None)

    def test_republish_after_checks(self):
        res = self._run(ENDED)
        self.assertEqual((res["result"], res["listing_id"], res["price"]), ("listed", "NEW1", 139.99))
        self.post.assert_called_once()

    def test_orphan_not_ended_is_stopped_without_publish(self):
        self.assertEqual(self._run(ACTIVE)["result"], "stop")
        self.post.assert_not_called()

    def test_orphan_sold_is_stopped(self):
        self.assertEqual(self._run(dict(ENDED, quantity_sold="1"))["result"], "stop")
        self.post.assert_not_called()

    def test_getitem_failure_is_retry(self):
        self.assertEqual(self._run({"ok": False, "ended": False, "error": "x"})["result"], "retry")
        self.post.assert_not_called()

    def test_seller_list_unknown_is_retry(self):
        self.assertEqual(self._run(ENDED, found=(None, None))["result"], "retry")
        self.post.assert_not_called()

    def test_existing_listing_found_is_registered_without_publish(self):
        res = self._run(ENDED, found=(True, "NEW9"), offers=[UNPUB])
        self.assertEqual((res["result"], res["listing_id"]), ("listed", "NEW9"))
        self.assertTrue(res["warn"])
        self.post.assert_not_called()


# ============================================================
# publish_ebay: 保留の記録と再処理（テスト専用DB。本番DBには触れない）
# ============================================================
TEST_DB = "nostock_test_unpub"
SQL_FILE = Path(__file__).resolve().parents[1] / "publish" / "sql" / "unpublished_recovery_20261104.sql"


def _master_conn():
    conn = get_sql_server_connection()
    conn.autocommit = True
    return conn


class OrphanPendingDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._env = mock.patch.dict(os.environ, {"DB_NAME": "master"})
        cls._env.start()
        with contextlib.closing(_master_conn()) as c:
            c.cursor().execute(f"IF DB_ID('{TEST_DB}') IS NULL CREATE DATABASE {TEST_DB}")
        os.environ["DB_NAME"] = TEST_DB
        with contextlib.closing(_master_conn()) as c:
            cur = c.cursor()
            cur.execute("SELECT DB_NAME()")
            assert cur.fetchone()[0] == TEST_DB  # 本番DBで実行しないことの確認
            cur.execute("IF SCHEMA_ID('trx') IS NULL EXEC('CREATE SCHEMA trx')")
            cur.execute("IF OBJECT_ID('trx.vendor_item') IS NULL CREATE TABLE trx.vendor_item ("
                        "vendor_item_id NVARCHAR(200) PRIMARY KEY, [出品状況] NVARCHAR(100), "
                        "[出品状況詳細] NVARCHAR(1000), 出品不可flg BIT)")
            cur.execute(SQL_FILE.read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls):
        os.environ["DB_NAME"] = "master"
        with contextlib.closing(_master_conn()) as c:
            c.cursor().execute(
                f"IF DB_ID('{TEST_DB}') IS NOT NULL BEGIN "
                f"ALTER DATABASE {TEST_DB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {TEST_DB}; END")
        cls._env.stop()

    def setUp(self):
        self.conn = get_sql_server_connection()
        self.addCleanup(self.conn.close)
        cur = self.conn.cursor()
        cur.execute("SELECT DB_NAME()")
        self.assertEqual(cur.fetchone()[0], TEST_DB)
        cur.execute("DELETE FROM trx.unpublished_recovery; DELETE FROM trx.vendor_item;")
        cur.execute("INSERT INTO trx.vendor_item (vendor_item_id, 出品不可flg) VALUES ('SKU1', 0)")
        self.conn.commit()
        self.mail = mock.Mock()
        self.record = mock.Mock()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for p in (mock.patch.object(pe, "send_mail", self.mail),
                  mock.patch.object(pe, "record_ebay_listing", self.record),
                  mock.patch.object(pe, "upsert_vendor_item", self._fake_upsert),
                  mock.patch.object(ebay_api, "ORPHAN_RECOVERY_LOG_PATH", Path(tmp.name) / "r.log"),
                  mock.patch.dict(pe._orphan_retry_checked_at, clear=True),
                  mock.patch("builtins.print")):
            p.start()
            self.addCleanup(p.stop)

    def _fake_upsert(self, conn, rec):
        conn.cursor().execute("UPDATE trx.vendor_item SET [出品状況] = ?, [出品状況詳細] = ? WHERE vendor_item_id = ?",
                              rec["listing_head"], rec["listing_detail"], rec["vendor_item_id"])

    def _pending(self, retryable=True):
        err = OrphanRepublishPendingError("再公開に失敗", offer_id="O1", orphan_id="OLD1",
                                          since_utc=datetime(2026, 11, 4, 4, 0), retryable=retryable)
        rec = {"vendor_item_id": "SKU1", "vendor_name": "メルカリ"}
        pe.mark_listing_state_unknown(self.conn, rec, "BUZZ②", "SKU1", err, True)

    def _row(self):
        cur = self.conn.cursor()
        cur.execute("SELECT status, attempts, new_listing_id, DATEDIFF(MINUTE, SYSDATETIME(), next_at) "
                    "FROM trx.unpublished_recovery WHERE vendor_item_id = 'SKU1'")
        r = cur.fetchone()
        cur.execute("SELECT [出品状況], 出品不可flg FROM trx.vendor_item WHERE vendor_item_id = 'SKU1'")
        v = cur.fetchone()
        self.conn.commit()
        return tuple(r), tuple(v)

    def _due(self):
        self.conn.cursor().execute("UPDATE trx.unpublished_recovery SET next_at = DATEADD(SECOND, -1, SYSDATETIME())")
        self.conn.commit()
        pe._orphan_retry_checked_at.clear()

    def _retry(self, *results):
        with mock.patch.object(pe, "retry_orphan_republish", side_effect=list(results)) as r:
            pe.retry_pending_orphan_republishes(self.conn, "BUZZ②")
        return r

    def test_pending_is_held_not_listed_permanently(self):
        self._pending()
        (status, attempts, _, wait), (head, flg) = self._row()
        self.assertEqual((status, attempts, head, flg), ("pending", 0, pe.ORPHAN_PENDING_HEAD, True))
        self.assertGreaterEqual(wait, pe.ORPHAN_RETRY_BACKOFF_MIN[0] - 1)
        self.mail.assert_not_called()

    def test_not_due_is_not_retried(self):
        self._pending()
        self._retry().assert_not_called()

    def test_retry_success_registers_listing_and_clears_flag(self):
        self._pending()
        self._due()
        r = self._retry({"result": "listed", "listing_id": "NEW1", "warn": None, "price": 139.99})
        r.assert_called_once()
        self.assertEqual(r.call_args.args[:4], ("BUZZ②", "SKU1", "O1", "OLD1"))
        self.record.assert_called_once_with("NEW1", "BUZZ②", "SKU1", "メルカリ", 139.99, True)
        (status, attempts, new_id, _), (head, flg) = self._row()
        self.assertEqual((status, attempts, new_id, head, flg), ("done", 1, "NEW1", "出品", False))
        self.mail.assert_not_called()

    def test_transient_failures_back_off_then_give_up_with_mail(self):
        self._pending()
        for i, wait in enumerate(pe.ORPHAN_RETRY_BACKOFF_MIN, start=1):
            self._due()
            self._retry({"result": "retry", "error": "25001"})
            (status, attempts, _, w), _ = self._row()
            self.assertEqual((status, attempts), ("pending", i))
            self.assertGreaterEqual(w, wait - 1)
        self._due()
        self._retry({"result": "retry", "error": "25001"})
        (status, attempts, _, _), (head, flg) = self._row()
        self.assertEqual((status, attempts, head, flg),
                         ("gave_up", pe.ORPHAN_RETRY_MAX_ATTEMPTS, pe.ORPHAN_PENDING_HEAD, True))
        self.mail.assert_called_once()
        self._due()
        self._retry().assert_not_called()  # 上限後は自動で再処理しない

    def test_stop_result_is_not_retried_and_mailed(self):
        self._pending()
        self._due()
        self._retry({"result": "stop", "error": "元の出品が終了していない"})
        (status, _, _, _), _ = self._row()
        self.assertEqual(status, "stopped")
        self.mail.assert_called_once()

    def test_non_retryable_pending_is_stopped_and_mailed(self):
        self._pending(retryable=False)
        (status, _, _, _), (_, flg) = self._row()
        self.assertEqual((status, flg), ("stopped", True))
        self.mail.assert_called_once()
        self._due()
        self._retry().assert_not_called()

    def test_retry_error_does_not_break_publish(self):
        self._pending()
        self._due()
        with mock.patch.object(pe, "retry_orphan_republish", side_effect=RuntimeError("boom")):
            pe.retry_pending_orphan_republishes(self.conn, "BUZZ②")  # 例外を外へ出さない

    def test_stale_running_row_is_reclaimed(self):
        self._pending()
        self.conn.cursor().execute("UPDATE trx.unpublished_recovery SET status = 'running', "
                                   "updated_at = DATEADD(MINUTE, -31, SYSDATETIME())")
        self.conn.commit()
        self._retry({"result": "listed", "listing_id": "NEW1", "warn": None, "price": 1.0}).assert_called_once()

if __name__ == "__main__":
    unittest.main()
