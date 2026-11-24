# -*- coding: utf-8 -*-
"""
値上げ失敗のうち eBay 側のシステム障害と明確に判断できるものは初回メールを送らず、
最初の失敗から3時間以上たっても失敗した時に1通だけ送る（2026-11-23）。
モックのみ。eBay・DB には接続しない（ops_stats.count もモックにし、終了時の本番 DB への書き込みを起こさない）。

実行: python -m unittest apps.tests.test_price_up_outage_notify -v
"""

import contextlib
import io
import unittest
from datetime import datetime, timedelta
from unittest import mock

from apps.adapters import mercari_item_status as mis

OUTAGE = mis.PRICE_UP_OUTAGE_CATEGORY


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        if sql.lstrip().upper().startswith("SELECT"):
            self.row = self.db["row"]
        else:
            self.db["updates"].append(params)
            self.db["row"] = (params[0], params[1])

    def fetchone(self):
        return self.row


class FakeConn:
    def __init__(self, row):
        self.db = {"row": row, "updates": []}

    def cursor(self):
        return FakeCursor(self.db)

    def commit(self):
        pass


def pending(tag, hours_ago):
    return (f"{mis.PRICE_PENDING_PREFIX}[{tag}]:getoffers_http_500:25001:x", datetime.now() - timedelta(hours=hours_ago))


class IsEbaySystemErrorTest(unittest.TestCase):
    def test_clear_system_errors(self):
        for e in ("getoffers_http_500:25001:A system error has occurred.",
                  "getoffers_http_503:None:None",
                  "getoffers_request_error:ReadTimeout",
                  "inventory_put_error_25001:A system error has occurred.",
                  "publish_error_25001:A system error has occurred.",
                  "inventory_put_error_25002:System error. Unable to process your request.",
                  "publish_error_25002:System error. Unable to process your request.",
                  "inventory_put_failed",
                  "publish_failed"):
            self.assertTrue(mis._is_ebay_system_error(e), e)

    def test_not_clear_errors_keep_notification(self):
        for e in ("getoffers_http_429:None:Too many",
                  "getoffers_http_401:1001:Invalid access token",
                  "offer_get_failed",
                  "offer_not_found_but_listing_active:getoffers_http_200:offers=0 total=0",
                  "offer_not_found_getitem_failed:getoffers_http_404:x getitem=518",
                  "price_verify_failed:listing_id=1 expected=139.99 actual=129.99 status=Active",
                  "inventory_put_error_25604:Availability not found",
                  "inventory_put_error_25713:Offer is not available",
                  "inventory_put_error_25002:A user error has occurred. identical items from the same seller",
                  "inventory_put_error_25002:You've reached the amount you can list",
                  "inventory_put_failed / eBayの出品終了を確認できずDB削除なし",
                  "exception",
                  None):
            self.assertFalse(mis._is_ebay_system_error(e), e)


class RecordPendingTest(unittest.TestCase):
    def record(self, row, category=OUTAGE):
        conn = FakeConn(row)
        with mock.patch.object(mis, "send_mail") as mail, contextlib.redirect_stdout(io.StringIO()):
            mis._record_price_update_pending(conn, "BUZZ②", "L1", "SKU1", "getoffers_http_500:25001:x",
                                             category=category, mail_info="")
        return mail, conn.db

    def test_first_failure_no_mail(self):
        mail, db = self.record((None, None))
        mail.assert_not_called()
        self.assertTrue(db["updates"][0][0].startswith(f"{mis.PRICE_PENDING_PREFIX}[未通知:{OUTAGE}]"))

    def test_retry_within_3h_no_mail_and_keeps_first_at(self):
        row = pending(f"未通知:{OUTAGE}", 2.5)
        mail, db = self.record(row)
        mail.assert_not_called()
        self.assertEqual(db["updates"][0][1], row[1])

    def test_retry_after_3h_one_mail_then_silent(self):
        mail, db = self.record(pending(f"未通知:{OUTAGE}", 3.1))
        mail.assert_called_once()
        self.assertIn(OUTAGE, mail.call_args[0][0])
        self.assertTrue(db["updates"][0][0].startswith(f"{mis.PRICE_PENDING_PREFIX}[通知済:{OUTAGE}]"))
        mail2, _ = self.record(pending(f"通知済:{OUTAGE}", 10))
        mail2.assert_not_called()

    def test_already_notified_as_price_up_not_resent(self):
        for tag in ("通知済:値上げ・一時的な障害", "通知済:値上げ・要確認", "通知済:旧形式"):
            mail, _ = self.record(pending(tag, 5))
            mail.assert_not_called()

    def test_other_categories_unchanged(self):
        mail, _ = self.record((None, None), category="値上げ・一時的な障害")
        mail.assert_called_once()
        mail, _ = self.record((None, None), category="値上げ・要確認")
        mail.assert_called_once()
        mail, _ = self.record((None, None), category="値下げ")
        mail.assert_not_called()
        mail, _ = self.record(pending("未通知:値下げ", 5), category="値下げ")
        mail.assert_not_called()
        mail, _ = self.record(pending("未通知:値下げ", 6.1), category="値下げ")
        mail.assert_called_once()


class HandleCategoryTest(unittest.TestCase):
    def run_handle(self, resp, usd=139.99):
        conn = mock.MagicMock()
        with mock.patch.object(mis, "get_active_listing_for_action", return_value=("L1", "BUZZ②")), \
                mock.patch.object(mis, "update_ebay_price_rest", return_value=resp) as upd, \
                mock.patch.object(mis, "_record_price_update_pending") as rec, \
                mock.patch.object(mis, "_delete_listing_after_price_failure") as delete, \
                mock.patch.object(mis, "mark_listing_deleted_if_ended", return_value=False), \
                mock.patch.object(mis, "_get_listing_start_price", return_value=mis.Decimal("129.99")), \
                mock.patch.object(mis.ops_stats, "count"), \
                mock.patch.object(mis, "send_mail") as mail, \
                mock.patch.object(mis.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError):
                mis.handle_listing_price_update(conn, "SKU1", "メルカリ", usd)
        delete.assert_not_called()
        mail.assert_not_called()
        return rec.call_args.kwargs["category"], upd.call_count

    def test_getoffers_500_is_outage_and_retries_unchanged(self):
        resp = {"success": False, "error": "getoffers_http_500:25001:A system error has occurred.", "transient": True}
        category, calls = self.run_handle(resp)
        self.assertEqual(category, OUTAGE)
        self.assertEqual(calls, 4)   # その場の再試行（0・2・6・15秒）は変わらない

    def test_put_25001_is_outage(self):
        resp = {"success": False, "error": "inventory_put_error_25001:A system error has occurred.",
                "transient": True, "old_price": "129.99"}
        self.assertEqual(self.run_handle(resp)[0], OUTAGE)

    def test_unclear_transient_keeps_immediate_category(self):
        for err in ("offer_get_failed", "price_verify_failed:listing_id=L1 expected=139.99 actual=129.99",
                    "offer_not_found_but_listing_active:getoffers_http_200:offers=0",
                    "inventory_put_error_25713:Offer is not available", "getoffers_http_429:None:x"):
            resp = {"success": False, "error": err, "transient": True, "old_price": "129.99"}
            self.assertEqual(self.run_handle(resp)[0], "値上げ・一時的な障害", err)

    def test_non_transient_keeps_requires_check(self):
        resp = {"success": False, "error": "inventory_put_error_25019:policy", "old_price": "129.99"}
        self.assertEqual(self.run_handle(resp)[0], "値上げ・要確認")

    def test_price_down_outage_stays_price_down(self):
        resp = {"success": False, "error": "getoffers_http_500:25001:x", "transient": True, "old_price": "149.99"}
        self.assertEqual(self.run_handle(resp)[0], "値下げ")


class DuplicateEndedNoMailTest(unittest.TestCase):
    """重複出品(25002 identical item)で終了・GetItem確認・DB反映まで成功したら通知しない（2026-11-24）。"""
    DUP = {"success": False, "duplicate_listing": True, "old_price": "129.99",
           "error": "inventory_put_error_25002:A user error has occurred. identical items from the same seller"}

    def run_handle(self, resp, deleted, usd=139.99):
        conn = mock.MagicMock()
        out = io.StringIO()
        with mock.patch.object(mis, "get_active_listing_for_action", return_value=("L1", "貴文")), \
                mock.patch.object(mis, "update_ebay_price_rest", return_value=dict(resp)) as upd, \
                mock.patch.object(mis, "_record_price_update_pending") as rec, \
                mock.patch.object(mis, "_delete_listing_after_price_failure", return_value=deleted) as delete, \
                mock.patch.object(mis.ops_stats, "count") as stats, \
                mock.patch.object(mis, "send_mail") as mail, \
                mock.patch.object(mis.time, "sleep"), \
                contextlib.redirect_stdout(out):
            raised = False
            try:
                mis.handle_listing_price_update(conn, "SKU1", "メルカリ", usd)
            except RuntimeError:
                raised = True
        return dict(upd=upd, rec=rec, delete=delete, stats=stats, mail=mail, raised=raised, log=out.getvalue())

    def test_duplicate_ended_ok_no_mail_but_log_and_stats(self):
        for usd in (139.99, 119.99):   # 値上げ・値下げとも
            r = self.run_handle(self.DUP, True, usd)
            r["mail"].assert_not_called()
            r["rec"].assert_not_called()
            self.assertFalse(r["raised"])
            r["delete"].assert_called_once_with("貴文", "SKU1", self.DUP["error"], "価格更新失敗")
            self.assertEqual(r["upd"].call_count, 1)       # 個別エラーのため再試行しない（従来どおり）
            self.assertIn("[PRICE_DUPLICATE_ENDED]", r["log"])
            self.assertEqual(r["stats"].call_args.args[:3], ("価格更新", "メルカリ", "失敗"))

    def test_duplicate_end_not_confirmed_still_notifies(self):
        r = self.run_handle(self.DUP, False)
        self.assertTrue(r["raised"])
        r["mail"].assert_not_called()   # 通知は _record_price_update_pending（出品終了未確認）が送る
        self.assertEqual(r["rec"].call_args.kwargs["category"], "出品終了未確認")
        self.assertIn("重複出品", r["rec"].call_args.kwargs["mail_info"])

    def test_other_end_reasons_still_mail(self):
        limit = {"success": False, "selling_limit": True, "old_price": "129.99",
                 "error": "inventory_put_error_25002:You've reached the amount you can list"}
        space = {"resp": dict(limit), "ended": [], "attempts": 0, "waited_sec": 0, "note": "候補なし"}
        with mock.patch("apps.publish.make_listing_space.free_listing_space_for_price_up", return_value=space):
            r = self.run_handle(limit, True)
        r["mail"].assert_called_once()
        self.assertIn("販売上限", r["mail"].call_args.args[0])
        ip = {"success": False, "db_deleted": True, "end_reason": "IP違反", "old_price": "129.99",
              "error": "inventory_put_error_25097:x"}
        r = self.run_handle(ip, True)
        r["mail"].assert_called_once()
        self.assertIn("IP違反", r["mail"].call_args.args[0])


if __name__ == "__main__":
    unittest.main()
