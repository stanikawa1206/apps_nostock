# -*- coding: utf-8 -*-
"""
出品終了のその場での再試行（案1）と「仕入不可出品中」の解決条件（案3）のテスト（2026-11-24）。
eBay・DB には接続しない（すべてモック）。ops_stats.count / issue もモックにする。
実行: python -m unittest apps.tests.test_listing_delete_retry -v
"""
import contextlib
import io
import unittest
from unittest import mock

import requests

from apps.adapters import ebay_api as ea
from apps.adapters import mercari_item_status as mi
from apps.inventory import ops_alerts as oa

ACC, LID, SKU = "BUZZ②", "800909223838", "2JZSWBJ7AMuQLqf8Lk3eJ7"
PUBLISHED = {"success": True, "offer_id": "1", "offer_status": "PUBLISHED", "offer_listing_id": LID}
UNPUB = {"success": True, "offer_id": "1", "offer_status": "UNPUBLISHED", "offer_listing_id": None}
NO_OFFER = {"success": False, "error_code": "offer_not_found", "raw_response": {}}
ACTIVE = {"ok": True, "ended": False, "listing_status": "Active", "sku": SKU}
ENDED = {"ok": True, "ended": True, "listing_status": "Completed", "sku": SKU}
NET_FAIL = {"ok": False, "ended": False, "error": "http_error:ReadTimeout"}
HTTP500 = {"ok": False, "ended": False, "error": "http_status:503"}
SYS10007 = {"ok": False, "ended": False, "error": "ack=Failure 10007 System error"}
RATE = {"ok": False, "ended": False, "rate_limited": True, "error": "getitem_rate_limited(skip)"}
END_OK = {"success": True, "results": [{"item_id": LID, "success": True, "error_code": None}]}
TIMEOUT = requests.exceptions.ReadTimeout("Read timed out. (read timeout=20)")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        self.conn.updates.append(params)


class FakeConn:
    def __init__(self):
        self.updates = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass


class DeleteRetryTest(unittest.TestCase):
    def run_delete(self, offer_seq, statuses=(), end=END_OK, end_exc=None):
        """offer_seq: delete_item_from_ebay の呼び出しごとの結果（例外ならその例外を投げる）。
        statuses: GetItem（ea・mi 共通の1本の並び）。"""
        conn = FakeConn()
        gi = mock.MagicMock(side_effect=list(statuses))
        out = io.StringIO()
        with mock.patch.object(mi, "get_active_listing_for_action", return_value=(LID, ACC)), \
                mock.patch.object(mi, "delete_item_from_ebay", side_effect=list(offer_seq)) as dl, \
                mock.patch.object(mi, "log_listings_change"), \
                mock.patch.object(ea, "get_listing_status", gi), \
                mock.patch.object(mi, "get_listing_status", gi), \
                mock.patch.object(ea, "delete_items_from_ebay_batch", side_effect=end_exc, return_value=end) as ei_fb, \
                mock.patch.object(mi, "delete_items_from_ebay_batch", side_effect=end_exc, return_value=end) as ei_main, \
                mock.patch.object(mi.ops_stats, "count") as cnt, \
                mock.patch.object(mi.ops_stats, "issue") as iss, \
                mock.patch.object(mi.time, "sleep") as sl, \
                contextlib.redirect_stdout(out):
            raised = None
            try:
                mi.handle_listing_delete(conn, SKU, "メルカリshops", "削除")
            except Exception as e:
                raised = e
        return dict(conn=conn, dl=dl, gi=gi, ei=ei_fb.call_count + ei_main.call_count, cnt=cnt, iss=iss,
                    sleep=[c.args[0] for c in sl.call_args_list], raised=raised, log=out.getvalue())

    def test_token_timeout_once_then_ended(self):
        """今回の事例: トークン更新の ReadTimeout 1回 → 2秒後の再試行で終了 → DB 削除済み・未解決記録なし"""
        r = self.run_delete([TIMEOUT, PUBLISHED])
        self.assertIsNone(r["raised"])
        self.assertEqual(r["dl"].call_count, 2)
        self.assertEqual(r["sleep"], [2])
        self.assertEqual(len(r["conn"].updates), 1)
        r["iss"].assert_not_called()
        self.assertEqual(r["cnt"].call_args.args[2], "成功")
        self.assertEqual(r["cnt"].call_args.kwargs["attempts"], 2)
        self.assertEqual(r["cnt"].call_args.kwargs["retried"], 1)
        self.assertIn("[DELETE_RETRY]", r["log"])

    def test_all_timeouts_keep_existing_failure_path(self):
        """4回すべて通信エラー・確認でも出品中 → 従来どおり例外・未解決記録（次回サイクルで再試行）、DB 変更なし"""
        r = self.run_delete([TIMEOUT] * 4, [ACTIVE])
        self.assertIsInstance(r["raised"], requests.exceptions.ReadTimeout)
        self.assertEqual(r["dl"].call_count, 4)
        self.assertEqual(r["sleep"], [2, 6, 15])
        self.assertEqual(r["conn"].updates, [])
        r["iss"].assert_called_once()
        self.assertEqual(r["iss"].call_args.args[0], "出品終了失敗")
        self.assertIn("ReadTimeout", r["iss"].call_args.args[3])
        self.assertEqual(r["cnt"].call_args.args[2], "失敗")
        self.assertEqual(r["cnt"].call_args.kwargs["cause"], "ReadTimeout")   # eBay 障害の集計(ops_alerts C)と同じ原因名
        self.assertEqual(r["cnt"].call_args.kwargs["attempts"], 4)

    def test_all_timeouts_but_ebay_already_ended(self):
        """終了要求の送信後にタイムアウトし、eBay では終了していた → 最後の確認で終了扱い（例外にしない）"""
        r = self.run_delete([TIMEOUT] * 4, [ENDED])
        self.assertIsNone(r["raised"])
        self.assertEqual(len(r["conn"].updates), 1)
        r["iss"].assert_not_called()

    def test_final_check_also_fails(self):
        """最後の確認もトークン更新のタイムアウト → 終了扱いにせず従来どおり例外"""
        r = self.run_delete([TIMEOUT] * 4, [TIMEOUT])
        self.assertIsInstance(r["raised"], requests.exceptions.ReadTimeout)
        self.assertEqual(r["conn"].updates, [])
        r["iss"].assert_called_once()

    def test_offer_delete_timeout_then_offer_gone_is_not_ended_twice(self):
        """offer 削除の要求後にタイムアウト（eBay では削除済み）→ 再試行で offer なし → GetItem で終了確認、EndItems は送らない"""
        r = self.run_delete([TIMEOUT, NO_OFFER], [ENDED])
        self.assertIsNone(r["raised"])
        self.assertEqual(r["ei"], 0)
        self.assertEqual(len(r["conn"].updates), 1)

    def test_getitem_transient_errors_are_retried(self):
        for fail in (NET_FAIL, HTTP500, SYS10007):
            r = self.run_delete([NO_OFFER, NO_OFFER], [fail, ACTIVE, ENDED])
            self.assertEqual(r["dl"].call_count, 2, fail)
            self.assertEqual(len(r["conn"].updates), 1, fail)
            r["iss"].assert_not_called()

    def test_enditems_timeout_then_checked(self):
        """UNPUBLISHED の offer 削除後の EndItems がタイムアウト → GetItem で終了を確かめて終了扱い（1回で済む）"""
        r = self.run_delete([UNPUB], [ENDED], end_exc=TIMEOUT)
        self.assertIsNone(r["raised"])
        self.assertEqual(r["dl"].call_count, 1)
        self.assertEqual(len(r["conn"].updates), 1)

    def test_enditems_timeout_still_active_is_retried(self):
        r = self.run_delete([UNPUB, NO_OFFER], [ACTIVE, ENDED], end_exc=TIMEOUT)
        self.assertEqual(r["dl"].call_count, 2)
        self.assertEqual(len(r["conn"].updates), 1)

    def test_non_transient_failures_are_not_retried(self):
        """SKU 不一致・GetItem 利用上限・終了したのに出品中のまま（eBay の個別エラー）は従来どおり1回で失敗記録"""
        end_ng = {"success": True, "results": [{"item_id": LID, "success": False, "error_code": 1047}]}
        for offer, statuses, end in ((NO_OFFER, [dict(ACTIVE, sku="OTHER")], END_OK),
                                     (NO_OFFER, [RATE], END_OK),
                                     (NO_OFFER, [ACTIVE, ACTIVE], end_ng)):
            r = self.run_delete([offer], statuses, end=end)
            self.assertIsNone(r["raised"])
            self.assertEqual(r["dl"].call_count, 1)
            self.assertEqual(r["sleep"], [])
            self.assertEqual(r["conn"].updates, [])
            r["iss"].assert_called_once()

    def test_unexpected_exception_not_retried(self):
        r = self.run_delete([KeyError("x")])
        self.assertIsInstance(r["raised"], KeyError)
        self.assertEqual(r["dl"].call_count, 1)
        r["iss"].assert_called_once()

    def test_normal_end_single_call(self):
        r = self.run_delete([PUBLISHED])
        self.assertEqual(r["dl"].call_count, 1)
        self.assertEqual(r["sleep"], [])
        self.assertEqual(r["cnt"].call_args.kwargs["attempts"], 1)
        self.assertEqual(r["cnt"].call_args.kwargs["retried"], 0)

    def test_price_update_uses_same_waits(self):
        self.assertEqual(mi.EBAY_RETRY_WAITS, (0, 2, 6, 15))


class ResolveSqlTest(unittest.TestCase):
    """案3: 「仕入不可出品中」は eBay で終了 or 仕入先で販売中の時だけ解決。空欄に戻っただけでは解決しない。"""

    def test_resolve_condition(self):
        sql = oa.SQL_RESOLVE_ISSUES
        part = sql[sql.index("N'仕入不可出品中'"):]
        self.assertIn("v.status = N'販売中'", part)
        self.assertNotIn("NOT EXISTS", part)
        self.assertIn("l.is_deleted = 0", sql)   # eBay で終了した時の解決は従来どおり


if __name__ == "__main__":
    unittest.main()
