# -*- coding: utf-8 -*-
"""
出品終了の「offer が無い出品は出品番号を指定して終了する」修正のテスト（2026-11-19）。
eBay・DB には接続しない（すべてモック）。
実行: python -m unittest apps.tests.test_listing_delete_fallback
"""
import unittest
from unittest import mock

import requests

from apps.adapters import ebay_api as ea
from apps.adapters import mercari_item_status as mi
from apps.inventory import ops_stats


def setUpModule():
    # 集計・異常明細の記録（本番DBへの書き込み）はすべて止める
    global _patches
    _patches = [mock.patch.object(ops_stats, name) for name in ("count", "issue", "flush")]
    for p in _patches:
        p.start()


def tearDownModule():
    for p in _patches:
        p.stop()

ACC, LID, SKU = "谷川④", "137892055692", "2JT6buMgYDpQhiooQqHamj"
NO_OFFER = {"success": False, "error_code": 404,
            "raw_response": '{"errors":[{"errorId":25713,"message":"This Offer is not available."}]}'}
ACTIVE = {"ok": True, "ended": False, "listing_status": "Active", "sku": SKU}
ENDED = {"ok": True, "ended": True, "listing_status": "Ended", "sku": SKU}
FAILED = {"ok": False, "ended": False, "error": "http_error:timeout"}
END_OK = {"success": True, "results": [{"item_id": LID, "success": True, "error_code": None}]}
END_NG = {"success": True, "results": [{"item_id": LID, "success": False, "error_code": 1047}]}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        if self.conn.fail_update:
            raise RuntimeError("DB更新失敗（テスト）")
        self.conn.updates.append(params)


class FakeConn:
    def __init__(self, fail_update=False):
        self.updates, self.commits, self.fail_update = [], 0, fail_update

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class EndListingByIdTest(unittest.TestCase):
    """ebay_api.end_listing_by_id_verified"""

    def run_case(self, statuses, end=END_OK, end_exc=None):
        with mock.patch.object(ea, "get_listing_status", side_effect=list(statuses)) as gi, \
             mock.patch.object(ea, "delete_items_from_ebay_batch",
                               side_effect=end_exc, return_value=end) as ei:
            return ea.end_listing_by_id_verified(ACC, LID, SKU), gi, ei

    def test_active_is_ended_by_id(self):
        r, gi, ei = self.run_case([ACTIVE, ENDED])
        self.assertTrue(r["ended"])
        self.assertEqual(r["how"], "ended_by_id")
        ei.assert_called_once_with(ACC, [LID])

    def test_already_ended_is_not_ended_again(self):
        r, gi, ei = self.run_case([ENDED])
        self.assertTrue(r["ended"])
        self.assertEqual(r["how"], "already_ended")
        ei.assert_not_called()

    def test_getitem_failed_before_end(self):
        r, gi, ei = self.run_case([FAILED])
        self.assertFalse(r["ended"])
        ei.assert_not_called()

    def test_sku_mismatch_is_not_ended(self):
        r, gi, ei = self.run_case([dict(ACTIVE, sku="OTHER")])
        self.assertFalse(r["ended"])
        self.assertTrue(r["how"].startswith("sku_mismatch"))
        ei.assert_not_called()

    def test_sku_missing_is_not_ended(self):
        r, gi, ei = self.run_case([dict(ACTIVE, sku="")])
        self.assertFalse(r["ended"])
        ei.assert_not_called()

    def test_enditems_request_error(self):
        r, gi, ei = self.run_case([ACTIVE, ACTIVE], end_exc=requests.exceptions.ConnectionError("down"))
        self.assertFalse(r["ended"])
        self.assertTrue(r["how"].startswith("enditems_request_error"))
        self.assertEqual(gi.call_count, 2)   # 終了要求の後のエラーでも GetItem で確かめる（2026-11-24）

    def test_enditems_request_error_but_ended(self):
        """終了要求を送った後にタイムアウトしたが、eBay では終了していた → 終了扱い（2026-11-24）"""
        r, gi, ei = self.run_case([ACTIVE, ENDED], end_exc=requests.exceptions.ReadTimeout("t"))
        self.assertTrue(r["ended"])
        self.assertEqual(r["how"], "ended_after_request_error")

    def test_enditems_failed_and_still_active(self):
        r, gi, ei = self.run_case([ACTIVE, ACTIVE], end=END_NG)
        self.assertFalse(r["ended"])
        self.assertEqual(r["how"], "end_unconfirmed")

    def test_check_after_end_failed(self):
        r, gi, ei = self.run_case([ACTIVE, FAILED])
        self.assertFalse(r["ended"])
        self.assertEqual(r["how"], "end_unconfirmed")


class HandleListingDeleteTest(unittest.TestCase):
    """mercari_item_status.handle_listing_delete"""

    def run_delete(self, offer_res, statuses, conn=None, end=END_OK, offer_exc=None):
        conn = conn or FakeConn()
        with mock.patch.object(mi, "get_active_listing_for_action", return_value=(LID, ACC)), \
             mock.patch.object(mi, "delete_item_from_ebay", side_effect=offer_exc, return_value=offer_res), \
             mock.patch.object(mi, "log_listings_change"), \
             mock.patch.object(ea, "get_listing_status", side_effect=list(statuses)), \
             mock.patch.object(mi, "get_listing_status", side_effect=list(statuses)), \
             mock.patch.object(ea, "delete_items_from_ebay_batch", return_value=end) as ei_fb, \
             mock.patch.object(mi, "delete_items_from_ebay_batch", return_value=end) as ei_main:
            mi.handle_listing_delete(conn, SKU, "メルカリshops", "売り切れ")
        return conn, ei_fb, ei_main

    def test_normal_offer(self):
        """offer があり公開済み（この出品）なら、offer 削除で終了して DB を削除済みにする（従来どおり）"""
        res = {"success": True, "offer_id": "1", "offer_status": "PUBLISHED", "offer_listing_id": LID}
        conn, ei_fb, ei_main = self.run_delete(res, [])
        self.assertEqual(len(conn.updates), 1)
        ei_fb.assert_not_called()
        ei_main.assert_not_called()

    def test_no_offer_active_listing_is_ended_by_id(self):
        """137892055692 と同じ: offer が無く eBay で出品中 → 出品番号で終了 → 確認 → DB 削除済み"""
        conn, ei_fb, _ = self.run_delete(NO_OFFER, [ACTIVE, ENDED])
        ei_fb.assert_called_once_with(ACC, [LID])
        self.assertEqual(len(conn.updates), 1)
        self.assertEqual(conn.updates[0][1], "売り切れ")

    def test_no_offer_already_ended(self):
        conn, ei_fb, _ = self.run_delete(NO_OFFER, [ENDED])
        ei_fb.assert_not_called()
        self.assertEqual(len(conn.updates), 1)

    def test_end_failed_keeps_db(self):
        conn, ei_fb, _ = self.run_delete(NO_OFFER, [ACTIVE, ACTIVE], end=END_NG)
        self.assertEqual(conn.updates, [])

    def test_check_failed_keeps_db(self):
        """終了後の確認が通信エラー → その場で再試行（2026-11-24）。4回とも確認できなければ DB 変更なし"""
        with mock.patch.object(mi.time, "sleep"):
            conn, ei_fb, _ = self.run_delete(NO_OFFER, [ACTIVE, FAILED] * 4)
        self.assertEqual(conn.updates, [])
        self.assertEqual(ei_fb.call_count, 4)

    def test_unknown_correspondence_keeps_db(self):
        conn, ei_fb, _ = self.run_delete(NO_OFFER, [dict(ACTIVE, sku="OTHER")])
        ei_fb.assert_not_called()
        self.assertEqual(conn.updates, [])

    def test_temporary_api_error_is_retried_next_time(self):
        """offer 取得の通信エラーが4回続けば従来どおり例外（DB 変更なし）→ 次回の終了処理で再試行（2026-11-24 からその場で4回）"""
        conn = FakeConn()
        with mock.patch.object(mi.time, "sleep"), self.assertRaises(requests.exceptions.ConnectionError):
            self.run_delete(None, [ACTIVE], conn=conn, offer_exc=requests.exceptions.ConnectionError("down"))
        self.assertEqual(conn.updates, [])

    def test_db_update_failure_recovers_next_time(self):
        """eBay は終了したが DB 更新に失敗 → 次回は GetItem で終了済みを確認して DB を直す（二重終了しない）"""
        with self.assertRaises(RuntimeError):
            self.run_delete(NO_OFFER, [ACTIVE, ENDED], conn=FakeConn(fail_update=True))
        conn, ei_fb, _ = self.run_delete(NO_OFFER, [ENDED])
        ei_fb.assert_not_called()
        self.assertEqual(len(conn.updates), 1)


class PriceFailureDeleteTest(unittest.TestCase):
    """ebay_api._delete_listing_after_price_failure: offer が無い時も出品番号で終了を試みる"""

    def test_no_offer_uses_end_by_id(self):
        fake = mock.MagicMock()
        fake.cursor.return_value.fetchall.return_value = [(LID, "メルカリshops")]
        with mock.patch.object(ea, "delete_item_from_ebay", return_value=NO_OFFER), \
             mock.patch.object(ea, "get_sql_server_connection", return_value=fake), \
             mock.patch.object(ea, "end_listing_by_id_verified", return_value={"ended": True, "how": "ended_by_id"}) as fb, \
             mock.patch.object(ea, "mark_listing_deleted_if_ended", return_value=True) as mk:
            self.assertTrue(ea._delete_listing_after_price_failure(ACC, SKU, "err", "価格更新失敗"))
        fb.assert_called_once_with(ACC, LID, SKU)
        mk.assert_called_once()

    def test_offer_deleted_does_not_use_end_by_id(self):
        fake = mock.MagicMock()
        fake.cursor.return_value.fetchall.return_value = [(LID, "メルカリshops")]
        with mock.patch.object(ea, "delete_item_from_ebay", return_value={"success": True, "offer_id": "1"}), \
             mock.patch.object(ea, "get_sql_server_connection", return_value=fake), \
             mock.patch.object(ea, "end_listing_by_id_verified") as fb, \
             mock.patch.object(ea, "mark_listing_deleted_if_ended", return_value=True):
            ea._delete_listing_after_price_failure(ACC, SKU, "err", "価格更新失敗")
        fb.assert_not_called()


if __name__ == "__main__":
    unittest.main()
