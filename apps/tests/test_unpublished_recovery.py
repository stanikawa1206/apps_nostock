# -*- coding: utf-8 -*-
"""
publishOffer がエラーでも出品されていて、offer が UNPUBLISHED のまま残った出品の自動復旧のテスト
（ebay_api._recover_orphan_listing / _confirm_published_listing。eBay・メール・待機はモック）。

  - offer が既に PUBLISHED なら何もせずその出品番号を返す
  - 通常: 終了 → 終了確認 → offer再公開 → 結び付き確認 で新しい出品番号を返す（終了確認の前に再公開しない）
  - 終了を確認できない: 再公開せず元の出品番号を返し、通知する
  - 再公開に失敗: 送り直さず ListingStateUnknownError、通知する
  - _confirm_published_listing が GetSellerList で見つけた出品は復旧に回す
実行: python -m unittest apps.tests.test_unpublished_recovery -v
"""

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from apps.adapters import ebay_api
from apps.adapters.ebay_api import ApiHandledError, ListingStateUnknownError

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
        with self.assertRaises(ListingStateUnknownError):
            self._run([UNPUB], [dict(ENDED, quantity_sold="1")])
        self.assertEqual(self.calls, ["end"])
        self.mail.assert_called_once()

    def test_offer_changed_after_end_is_not_republished(self):
        with self.assertRaises(ListingStateUnknownError):
            self._run([UNPUB, {"status": None}], [ENDED])
        self.assertEqual(self.calls, ["end"])

    def test_republish_failure_stops_without_retry(self):
        with self.assertRaises(ListingStateUnknownError):
            self._run([UNPUB, UNPUB], [ENDED], publish_resp=_resp(500, {"errors": [{"errorId": 25001}]}),
                      found=(True, None))
        self.assertEqual(self.calls, ["end", "publish"])
        self.mail.assert_called_once()

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


if __name__ == "__main__":
    unittest.main()
