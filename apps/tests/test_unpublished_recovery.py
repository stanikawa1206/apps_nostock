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


class RecreateOfferTest(unittest.TestCase):
    """retry_orphan_republish(recreate_payload=...): offer を作り直して再公開する（eBay はモック）"""

    PAYLOAD = {"sku": "SKU1", "marketplaceId": "EBAY_US", "categoryId": "45258"}

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.calls = []
        for p in (mock.patch.object(ebay_api, "ORPHAN_RECOVERY_LOG_PATH", Path(tmp.name) / "r.log"),
                  mock.patch.object(ebay_api.time, "sleep"),
                  mock.patch.object(ebay_api, "get_access_token_new", return_value="T"),
                  mock.patch.object(ebay_api, "get_listing_status", return_value=ENDED),
                  mock.patch.object(ebay_api, "_find_listing_by_sku_started_since", return_value=(True, None)),
                  mock.patch("builtins.print")):
            p.start()
            self.addCleanup(p.stop)

    def _run(self, listed, *, delete_status=204, create=(201, {"offerId": "O2"}), publish=(200, {"listingId": "NEW1"}),
             offers_after=None):
        def get(url, **k):
            self.calls.append(("list", k.get("params", {}).get("sku")))
            return listed if isinstance(listed, mock.Mock) else _resp(200, {"offers": listed})

        def delete(url, **k):
            self.calls.append(("delete", url.rsplit("/", 1)[-1]))
            r = _resp(delete_status, {})
            r.text = ""
            return r

        def post(url, **k):
            if url.endswith("/offer"):
                self.calls.append(("create", k["json"]))
                return _resp(*create)
            self.calls.append(("publish", url.split("/offer/")[1].split("/")[0]))
            return _resp(*publish)

        priced = dict(_published("NEW1"), offerId="O2", pricingSummary={"price": {"value": "749.99"}})
        offers_after = offers_after or [dict(UNPUB, offerId="O2"), priced, priced]
        with mock.patch.object(ebay_api.requests, "get", side_effect=get), \
                mock.patch.object(ebay_api.requests, "delete", side_effect=delete), \
                mock.patch.object(ebay_api.requests, "post", side_effect=post), \
                mock.patch.object(ebay_api, "_inventory_get_offer", side_effect=offers_after):
            return ebay_api.retry_orphan_republish("貴文", "SKU1", "O1", "OLD1", None, recreate_payload=self.PAYLOAD)

    def test_delete_create_publish(self):
        res = self._run([dict(UNPUB, offerId="O1")])
        self.assertEqual((res["result"], res["listing_id"], res["offer_id"], res["price"]), ("listed", "NEW1", "O2", 749.99))
        self.assertEqual([c[0] for c in self.calls], ["list", "delete", "create", "publish"])
        self.assertEqual(self.calls[1][1], "O1")
        self.assertEqual(self.calls[2][1], self.PAYLOAD)
        self.assertEqual(self.calls[3][1], "O2")

    def test_offer_missing_is_created_from_payload(self):
        """前回 削除の後に作成が失敗した: 保存した内容で作る"""
        missing = mock.Mock(status_code=404)
        res = self._run(missing)
        self.assertEqual(res["result"], "listed")
        self.assertEqual([c[0] for c in self.calls], ["list", "create", "publish"])

    def test_already_recreated_offer_is_reused(self):
        res = self._run([dict(UNPUB, offerId="O2")])
        self.assertEqual((res["result"], res["offer_id"]), ("listed", "O2"))
        self.assertEqual([c[0] for c in self.calls], ["list", "publish"])

    def test_multiple_offers_stop(self):
        res = self._run([dict(UNPUB, offerId="O1"), dict(UNPUB, offerId="O3")])
        self.assertEqual(res["result"], "stop")
        self.assertEqual([c[0] for c in self.calls], ["list"])

    def test_published_offer_with_listing_is_registered(self):
        res = self._run([dict(_published("LIVE1"), offerId="O1")])
        self.assertEqual((res["result"], res["listing_id"]), ("listed", "LIVE1"))
        self.assertEqual([c[0] for c in self.calls], ["list"])

    def test_list_failure_is_retry_without_delete(self):
        res = self._run(mock.Mock(status_code=500))
        self.assertEqual(res["result"], "retry")
        self.assertEqual([c[0] for c in self.calls], ["list"])

    def test_delete_failure_is_retry_without_create(self):
        res = self._run([dict(UNPUB, offerId="O1")], delete_status=500)
        self.assertEqual(res["result"], "retry")
        self.assertEqual([c[0] for c in self.calls], ["list", "delete"])

    def test_create_failure_is_retry(self):
        res = self._run([dict(UNPUB, offerId="O1")], create=(500, {"errors": [{"errorId": 25001, "message": "x"}]}))
        self.assertEqual(res["result"], "retry")
        self.assertIsNone(res.get("offer_id"))
        self.assertEqual([c[0] for c in self.calls], ["list", "delete", "create"])

    def test_payload_excludes_status_fields(self):
        offer = {"offerId": "O1", "sku": "SKU1", "status": "UNPUBLISHED", "categoryId": "1", "tax": {"applyTax": False}}
        with mock.patch.object(ebay_api, "_inventory_get_offer", return_value=offer):
            self.assertEqual(ebay_api.get_orphan_offer_payload("貴文", "O1"),
                             {"sku": "SKU1", "categoryId": "1", "tax": {"applyTax": False}})
        with mock.patch.object(ebay_api, "_inventory_get_offer", return_value=_published("L1")):
            self.assertIsNone(ebay_api.get_orphan_offer_payload("貴文", "O1"))


# ============================================================
# publish_ebay: 保留の記録と再処理（テスト専用DB。本番DBには触れない）
# ============================================================
TEST_DB = "nostock_test_unpub"
SQL_FILE = Path(__file__).resolve().parents[1] / "publish" / "sql" / "unpublished_recovery_20261104.sql"
SQL_FILE_RECREATE = SQL_FILE.with_name("unpublished_recovery_20261109_offer_recreate.sql")


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
            cur.execute(SQL_FILE_RECREATE.read_text(encoding="utf-8"))

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
        self.payload = mock.Mock(return_value={"sku": "SKU1", "categoryId": "1"})
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for p in (mock.patch.object(pe, "send_mail", self.mail),
                  mock.patch.object(pe, "get_orphan_offer_payload", self.payload),
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
                                          since_utc=datetime.utcnow(), retryable=retryable)
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

    def test_transient_failures_back_off_then_daily_without_mail(self):
        self._pending()
        waits = pe.ORPHAN_RETRY_BACKOFF_MIN + [pe.ORPHAN_RETRY_DAILY_MIN, pe.ORPHAN_RETRY_DAILY_MIN]
        for i, wait in enumerate(waits, start=1):
            self._due()
            r = self._retry({"result": "retry", "error": "25001"})
            self.assertIsNone(r.call_args.kwargs["recreate_payload"])   # 2日未満は同じ offer で再公開
            (status, attempts, _, w), (head, flg) = self._row()
            self.assertEqual((status, attempts, head, flg), ("pending", i, pe.ORPHAN_PENDING_HEAD, True))
            self.assertGreaterEqual(w, wait - 1)
            self.assertLessEqual(w, wait)
        self.mail.assert_not_called()   # 一時的な失敗の個別メールは送らない

    def test_gives_up_after_max_days_without_mail(self):
        self._pending()
        self._set_since(days_ago=pe.ORPHAN_RETRY_MAX_DAYS + 1)
        self._due()
        self._retry({"result": "retry", "error": "25001"})
        (status, _, _, _), (head, flg) = self._row()
        self.assertEqual((status, head, flg), ("gave_up", pe.ORPHAN_PENDING_HEAD, True))
        self.mail.assert_not_called()   # inventory_monitor の集約通知に出す
        self._due()
        self._retry().assert_not_called()  # 上限後は自動で再処理しない

    def _set_since(self, days_ago, recreated_days_ago=None):
        self.conn.cursor().execute(
            "UPDATE trx.unpublished_recovery SET since_utc = DATEADD(MINUTE, ?, SYSUTCDATETIME()), "
            "offer_recreated_utc = CASE WHEN ? IS NULL THEN NULL ELSE DATEADD(MINUTE, ?, SYSUTCDATETIME()) END",
            -int(days_ago * 1440), recreated_days_ago, -int((recreated_days_ago or 0) * 1440))
        self.conn.commit()

    def _offer_cols(self):
        cur = self.conn.cursor()
        cur.execute("SELECT offer_id, offer_payload, offer_recreated_utc FROM trx.unpublished_recovery")
        r = cur.fetchone()
        self.conn.commit()
        return tuple(r)

    def test_recreate_after_two_days_saves_payload_first(self):
        self._pending()
        self._set_since(days_ago=pe.ORPHAN_RECREATE_AFTER_DAYS + 0.1)
        self._due()
        seen = {}

        def retry(*a, recreate_payload=None):
            seen["payload"] = recreate_payload
            seen["db_before"] = self._offer_cols()      # eBay で削除する前に DB へ保存済み
            return {"result": "retry", "error": "作り直し: offerを作成できない", "offer_id": None}

        with mock.patch.object(pe, "retry_orphan_republish", side_effect=retry):
            pe.retry_pending_orphan_republishes(self.conn, "BUZZ②")
        self.assertEqual(seen["payload"], {"sku": "SKU1", "categoryId": "1"})
        self.assertIn('"categoryId": "1"', seen["db_before"][1])
        offer_id, payload, recreated = self._offer_cols()
        self.assertEqual((offer_id, recreated), ("O1", None))   # 作成できなければ元のまま・内容は残す
        self.assertIsNotNone(payload)
        # 次回は保存した内容を使う（getOffer しない）
        self.payload.reset_mock()
        self._due()
        r = self._retry({"result": "retry", "error": "x"})
        self.assertEqual(r.call_args.kwargs["recreate_payload"], {"sku": "SKU1", "categoryId": "1"})
        self.payload.assert_not_called()

    def test_recreated_offer_is_recorded_and_not_recreated_again_soon(self):
        self._pending()
        self._set_since(days_ago=3)
        self._due()
        self._retry({"result": "retry", "error": "再公開に失敗 http=500", "offer_id": "O2"})
        offer_id, payload, recreated = self._offer_cols()
        self.assertEqual(offer_id, "O2")
        self.assertIsNone(payload)
        self.assertIsNotNone(recreated)
        self._due()
        r = self._retry({"result": "listed", "listing_id": "NEW2", "warn": None, "price": 1.0})
        self.assertEqual(r.call_args.args[2], "O2")
        self.assertIsNone(r.call_args.kwargs["recreate_payload"])   # 作り直してから2日未満
        (status, _, new_id, _), (head, flg) = self._row()
        self.assertEqual((status, new_id, head, flg), ("done", "NEW2", "出品", False))

    def test_recreate_again_two_days_after_previous_recreate(self):
        self._pending()
        self._set_since(days_ago=6, recreated_days_ago=pe.ORPHAN_RECREATE_AFTER_DAYS + 0.1)
        self._due()
        r = self._retry({"result": "retry", "error": "x"})
        self.assertIsNotNone(r.call_args.kwargs["recreate_payload"])

    def test_payload_unavailable_falls_back_to_same_offer(self):
        self._pending()
        self._set_since(days_ago=3)
        self._due()
        self.payload.return_value = None
        r = self._retry({"result": "retry", "error": "x"})
        self.assertIsNone(r.call_args.kwargs["recreate_payload"])

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

    # ----- inventory_monitor（出品時間外の再処理と集約通知） -----
    def _monitor(self):
        from apps.inventory import inventory_monitor as im
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for p in (mock.patch.object(im, "_logger"),
                  mock.patch.object(im, "ORPHAN_NOTIFY_STATE_PATH", Path(tmp.name) / "notify.json")):
            p.start()
            self.addCleanup(p.stop)
        return im

    def test_monitor_retries_due_rows(self):
        im = self._monitor()
        self._pending()
        with mock.patch.object(pe, "retry_orphan_republish") as r:
            im.retry_due_orphan_republishes(self.conn)
        r.assert_not_called()   # まだ時刻が来ていない
        self._due()
        r = None
        with mock.patch.object(pe, "retry_orphan_republish",
                               return_value={"result": "listed", "listing_id": "NEW1", "warn": None, "price": 1.0}) as r:
            im.retry_due_orphan_republishes(self.conn)
        r.assert_called_once()
        (status, _, new_id, _), _ = self._row()
        self.assertEqual((status, new_id), ("done", "NEW1"))

    def _add_row(self, account, sku, status="pending", days_ago=0.0):
        self.conn.cursor().execute(
            "INSERT INTO trx.unpublished_recovery (account, vendor_item_id, offer_id, orphan_listing_id, since_utc, "
            "status, attempts, next_at) VALUES (?, ?, 'O', 'L', DATEADD(MINUTE, ?, SYSUTCDATETIME()), ?, 1, "
            "DATEADD(HOUR, 1, SYSDATETIME()))", account, sku, -int(days_ago * 1440), status)
        self.conn.commit()

    def test_monitor_single_fresh_row_is_silent(self):
        im = self._monitor()
        self._add_row("貴文", "A1", days_ago=1)
        with mock.patch.object(im, "send_mail") as mail:
            im.notify_orphan_alert(self.conn, "2026-11-09")
        mail.assert_not_called()

    def test_monitor_alerts_once_per_day(self):
        im = self._monitor()
        self._add_row("貴文", "A1", days_ago=1)
        self._add_row("BUZZ②", "B1", days_ago=1)
        with mock.patch.object(im, "send_mail", return_value=True) as mail:
            im.notify_orphan_alert(self.conn, "2026-11-09")
            im.notify_orphan_alert(self.conn, "2026-11-09")
            self.assertEqual(mail.call_count, 1)
            self.assertIn("複数アカウント", mail.call_args[0][1])
            im.notify_orphan_alert(self.conn, "2026-11-10")
            self.assertEqual(mail.call_count, 2)

    def test_monitor_alert_reasons(self):
        im = self._monitor()
        self._add_row("貴文", "A1", days_ago=im.ORPHAN_NOTIFY_LONG_DAYS + 0.5)
        self._add_row("貴文", "A2", status="gave_up", days_ago=1)
        self._add_row("貴文", "A3", status="done", days_ago=9)   # 復旧済みは数えない
        with mock.patch.object(im, "send_mail", return_value=True) as mail:
            im.notify_orphan_alert(self.conn, "2026-11-09")
        body = mail.call_args[0][1]
        self.assertIn("3日以上 未復旧が1件", body)
        self.assertIn("gave_up)が1件", body)
        self.assertNotIn("A3", body)
        rows = [{"account": "貴文", "status": "pending", "days": 0.1}] * im.ORPHAN_NOTIFY_MIN_ROWS
        self.assertTrue(any("件以上" in x for x in im.orphan_alert(rows)))


if __name__ == "__main__":
    unittest.main()
