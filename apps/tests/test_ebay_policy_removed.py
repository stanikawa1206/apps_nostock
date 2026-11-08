# -*- coding: utf-8 -*-
"""
eBay がポリシー違反で削除した出品の判定（2026-11-08）のテスト。
GetItem の Failure 21920397 は、本文が「ポリシー違反で削除」と明示している時だけ削除扱いにする。
eBay・DB には接続しない（requests.post・DB接続はモック）。
実行: python -m unittest apps.tests.test_ebay_policy_removed -v
"""

import unittest
from unittest import mock

from apps.adapters import ebay_api


def _xml_failure(code, msg):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<GetItemResponse xmlns="urn:ebay:apis:eBLBaseComponents"><Ack>Failure</Ack>'
        f'<Errors><ErrorCode>{code}</ErrorCode><LongMessage>{msg}</LongMessage></Errors></GetItemResponse>'
    )


ACTIVE_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<GetItemResponse xmlns="urn:ebay:apis:eBLBaseComponents"><Ack>Success</Ack><Item>'
    '<ListingDetails><EndTime>2026-12-01T00:00:00.000Z</EndTime></ListingDetails>'
    '<SellingStatus><ListingStatus>Active</ListingStatus><QuantitySold>0</QuantitySold>'
    '<CurrentPrice currencyID="USD">100.0</CurrentPrice></SellingStatus></Item></GetItemResponse>'
)
COUNTERFEIT_MSG = "This listing was removed for violating our Counterfeit items policy."


def _resp(text):
    r = mock.MagicMock()
    r.status_code = 200
    r.text = text
    return r


class GetListingStatusTest(unittest.TestCase):
    def setUp(self):
        for p in (mock.patch.object(ebay_api, "get_access_token_new", return_value="T"),
                  mock.patch.object(ebay_api, "_getitem_rate_limited_until", 0.0)):
            p.start()
            self.addCleanup(p.stop)

    def status(self, text):
        with mock.patch.object(ebay_api.requests, "post", return_value=_resp(text)):
            return ebay_api.get_listing_status("BUZZ", "820289330073")

    def test_counterfeit_removal_is_ended(self):
        st = self.status(_xml_failure("21920397", COUNTERFEIT_MSG))
        self.assertTrue(st["ok"])
        self.assertTrue(st["ended"])
        self.assertEqual(st["listing_status"], "RemovedByEbay")
        self.assertEqual(st["removed_by_ebay"], "Counterfeit items")
        self.assertEqual(st["quantity_sold"], "")

    def test_other_policy_name(self):
        st = self.status(_xml_failure("21920397", "This listing was removed for violating our Prohibited and restricted items policy."))
        self.assertEqual(st["removed_by_ebay"], "Prohibited and restricted items")

    def test_same_code_other_message_is_not_ended(self):
        # 21920397 の番号だけでは削除と断定しない
        for msg in ["", "Item cannot be accessed.", "This listing is not available."]:
            st = self.status(_xml_failure("21920397", msg))
            self.assertFalse(st["ok"], msg)
            self.assertFalse(st["ended"], msg)
            self.assertNotIn("removed_by_ebay", st)

    def test_same_message_other_code_is_not_ended(self):
        st = self.status(_xml_failure("17", COUNTERFEIT_MSG))
        self.assertFalse(st["ended"])

    def test_active_unchanged(self):
        st = self.status(ACTIVE_XML)
        self.assertEqual(st["listing_status"], "Active")
        self.assertFalse(st["ended"])
        self.assertNotIn("removed_by_ebay", st)


class MarkDeletedTest(unittest.TestCase):
    def run_mark(self, status):
        conn = mock.MagicMock()
        cur = conn.cursor.return_value
        cur.fetchall.return_value = [("820289330073",)]
        with mock.patch.object(ebay_api, "get_listing_status", return_value=status), \
                mock.patch.object(ebay_api, "get_sql_server_connection", return_value=conn), \
                mock.patch.object(ebay_api, "log_listings_change") as log, \
                mock.patch("builtins.print"):
            ok = ebay_api.mark_listing_deleted_if_ended("BUZZ", "820289330073", "SKU1", "eBay終了確認", "x")
        return ok, cur.execute.call_args_list, log

    def test_policy_removed_marks_listing_and_vendor_item(self):
        ok, calls, log = self.run_mark({
            "ok": True, "ended": True, "listing_status": "RemovedByEbay", "removed_by_ebay": "Counterfeit items",
            "error": "ack=Failure 21920397 " + COUNTERFEIT_MSG, "end_time": "",
        })
        self.assertTrue(ok)
        self.assertEqual(len(calls), 2)
        vi_sql, vi_args = calls[0][0][0], calls[0][0][1:]
        self.assertIn("出品不可flg = 1", vi_sql)
        self.assertIn("NG(eBay削除)", vi_sql)
        self.assertEqual(vi_args[1:], ("BUZZ", "820289330073"))
        l_args = calls[1][0][1:]
        self.assertEqual(l_args[1], "eBay削除(Counterfeit items)")
        self.assertIn("21920397", l_args[2])
        log.assert_called_once_with("DELETE", "BUZZ", "820289330073", "SKU1", "eBay削除(Counterfeit items)")

    def test_normal_end_does_not_touch_vendor_item(self):
        ok, calls, _ = self.run_mark({
            "ok": True, "ended": True, "listing_status": "Completed", "end_time": "2026-11-01T00:00:00.000Z",
        })
        self.assertTrue(ok)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0][2], "eBay終了確認")

    def test_unknown_error_no_change(self):
        ok, calls, _ = self.run_mark({"ok": False, "ended": False, "error": "ack=Failure 21920397 other"})
        self.assertFalse(ok)
        self.assertEqual(calls, [])


class PriceUpdatePathTest(unittest.TestCase):
    """価格更新で offer が無く GetItem が eBay削除 → offer_not_found_for_sku（呼び出し元が削除済みにする）。"""

    def test_removed_listing_is_not_transient(self):
        removed = {"ok": True, "ended": True, "listing_status": "RemovedByEbay", "removed_by_ebay": "Counterfeit items"}
        with mock.patch.object(ebay_api, "get_access_token_new", return_value="T"), \
                mock.patch.object(ebay_api, "_inventory_find_offer_by_sku",
                                  return_value={"kind": "not_found", "error": "getoffers_http_404:25713"}), \
                mock.patch.object(ebay_api, "get_listing_status", return_value=removed), \
                mock.patch("builtins.print"):
            out = ebay_api.update_ebay_price_rest("BUZZ", "SKU1", 100, expected_listing_id="820289330073")
        self.assertEqual(out["error"], "offer_not_found_for_sku")
        self.assertNotIn("transient", out)


class OrphanRetryTest(unittest.TestCase):
    def test_retry_stops_for_removed_listing(self):
        removed = {"ok": True, "ended": True, "listing_status": "RemovedByEbay", "removed_by_ebay": "Counterfeit items",
                   "quantity_sold": ""}
        with mock.patch.object(ebay_api, "get_access_token_new", return_value="T"), \
                mock.patch.object(ebay_api, "get_listing_status", return_value=removed), \
                mock.patch.object(ebay_api, "_republish_ended_orphan") as rep, \
                mock.patch.object(ebay_api, "_orphan_log"):
            res = ebay_api.retry_orphan_republish("BUZZ", "SKU1", "O1", "OLD1", ebay_api.datetime.utcnow())
        self.assertEqual(res["result"], "stop")
        rep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
