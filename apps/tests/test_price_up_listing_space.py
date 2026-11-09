# -*- coding: utf-8 -*-
"""
値上げ時の販売上限（25002）対応 make_listing_space.free_listing_space_for_price_up と
mercari_item_status.handle_listing_price_update の組み込みのテスト。
モックのみ。eBay・DB には接続しない。

実行: python -m unittest apps.tests.test_price_up_listing_space -v
"""

import contextlib
import io
import unittest
from unittest import mock

from apps.publish import make_listing_space as mls
from apps.adapters import mercari_item_status as mis

LIMIT = {"success": False, "selling_limit": True, "error": "inventory_put_error_25002:amount you can list",
         "old_price": "100.00"}
OK = {"success": True}


@contextlib.contextmanager
def fake_lock(rc):
    yield rc


class FreeSpaceTest(unittest.TestCase):
    def run_free(self, *, rc=0, candidates=(), end_ok=None, price_results=()):
        """candidates: 順に返す候補。end_ok: listing_id → 終了できたか。price_results: 値上げ再試行の結果の順番"""
        cands = list(candidates)
        prices = list(price_results)
        retry = mock.Mock(side_effect=lambda: prices.pop(0))
        picked_skips = []

        def pick(account, listing_id, sku, skip_ids):
            picked_skips.append(list(skip_ids))
            return cands.pop(0) if cands else None

        with mock.patch.object(mls, "account_listing_space_lock", lambda a, t: fake_lock(rc)), \
                mock.patch.object(mls, "_pick_price_up_candidate", side_effect=pick), \
                mock.patch.object(mls, "_end_listing_for_price_up",
                                  side_effect=lambda a, lid, sku: (end_ok or {}).get(lid, True)), \
                mock.patch.object(mls.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            out = mls.free_listing_space_for_price_up("谷川④", "L0", "SKU0", retry)
        return out, retry, picked_skips

    def test_one_end_then_success(self):
        out, retry, _ = self.run_free(candidates=[("L1", "S1")], price_results=[OK])
        self.assertTrue(out["resp"]["success"])
        self.assertEqual(out["ended"], ["L1"])
        self.assertEqual(out["attempts"], 1)
        self.assertEqual(out["waited_sec"], mls.PRICE_UP_RETRY_WAITS[0])

    def test_waits_then_retries_after_delay(self):
        """枠の反映に時間差がある場合: 1回目の再試行は上限のまま、待って2回目で成功（次の出品は終了しない）"""
        out, retry, _ = self.run_free(candidates=[("L1", "S1"), ("L2", "S2")], price_results=[LIMIT, OK])
        self.assertTrue(out["resp"]["success"])
        self.assertEqual(out["ended"], ["L1"])
        self.assertEqual(out["attempts"], 2)
        self.assertEqual(out["waited_sec"], sum(mls.PRICE_UP_RETRY_WAITS))

    def test_ends_next_when_still_limit(self):
        out, retry, skips = self.run_free(candidates=[("L1", "S1"), ("L2", "S2")],
                                          price_results=[LIMIT, LIMIT, OK])
        self.assertTrue(out["resp"]["success"])
        self.assertEqual(out["ended"], ["L1", "L2"])
        self.assertEqual(skips[1], ["L1"])    # 終了した出品は次の候補から外す

    def test_gives_up_at_max_ends(self):
        n = mls.PRICE_UP_MAX_ENDS
        out, retry, _ = self.run_free(candidates=[(f"L{i}", f"S{i}") for i in range(1, n + 3)],
                                      price_results=[LIMIT] * (n * len(mls.PRICE_UP_RETRY_WAITS)))
        self.assertEqual(len(out["ended"]), n)
        self.assertTrue(out["resp"]["selling_limit"])
        self.assertEqual(retry.call_count, n * len(mls.PRICE_UP_RETRY_WAITS))
        self.assertIn("上限", out["note"])

    def test_skips_candidates_that_cannot_be_ended(self):
        out, retry, skips = self.run_free(candidates=[("L1", "S1"), ("L2", "S2")], end_ok={"L1": False},
                                          price_results=[OK])
        self.assertEqual(out["ended"], ["L2"])
        self.assertEqual(skips[1], ["L1"])

    def test_candidate_check_limit(self):
        m = mls.PRICE_UP_MAX_CANDIDATES
        out, retry, _ = self.run_free(candidates=[(f"L{i}", f"S{i}") for i in range(m + 5)],
                                      end_ok={f"L{i}": False for i in range(m + 5)})
        self.assertEqual(out["ended"], [])
        self.assertIsNone(out["resp"])
        retry.assert_not_called()

    def test_no_candidates(self):
        out, retry, _ = self.run_free(candidates=[])
        self.assertEqual(out["ended"], [])
        self.assertIsNone(out["resp"])
        self.assertIn("候補", out["note"])

    def test_lock_timeout_does_nothing(self):
        out, retry, _ = self.run_free(rc=-1, candidates=[("L1", "S1")])
        self.assertEqual(out["ended"], [])
        self.assertIsNone(out["resp"])
        retry.assert_not_called()

    def test_waited_for_lock_retries_first(self):
        """ロックを待った場合は、他の処理が空けた枠で通るかを先に試す（出品を終了しない）"""
        out, retry, _ = self.run_free(rc=1, candidates=[("L1", "S1")], price_results=[OK])
        self.assertTrue(out["resp"]["success"])
        self.assertEqual(out["ended"], [])

    def test_other_error_stops_loop(self):
        other = {"success": False, "transient": True, "error": "inventory_put_error_25001"}
        out, retry, _ = self.run_free(candidates=[("L1", "S1"), ("L2", "S2")], price_results=[other])
        self.assertEqual(out["ended"], ["L1"])
        self.assertIs(out["resp"], other)


class EndListingTest(unittest.TestCase):
    def run_end(self, status, ended_after=1):
        calls = {"n": 0}

        def mark(*a, **k):
            calls["n"] += 1
            return calls["n"] >= ended_after

        conn = mock.MagicMock()
        with mock.patch.object(mls, "get_listing_status", return_value=status), \
                mock.patch.object(mls, "delete_items_from_ebay_batch", return_value={"results": []}) as end, \
                mock.patch.object(mls, "mark_listing_deleted_if_ended", side_effect=mark) as mk, \
                mock.patch.object(mls, "get_sql_server_connection", return_value=conn), \
                mock.patch.object(mls.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            ok = mls._end_listing_for_price_up("谷川④", "L1", "S1")
        return ok, end, mk

    def test_active_unsold_is_ended_and_confirmed(self):
        ok, end, mk = self.run_end({"ok": True, "listing_status": "Active", "quantity_sold": "0"}, ended_after=2)
        self.assertTrue(ok)
        end.assert_called_once_with("谷川④", ["L1"])
        self.assertEqual(mk.call_args[0][3], mls.PRICE_UP_DELETE_REASON)

    def test_sold_or_ended_is_not_ended(self):
        for st in ({"ok": True, "listing_status": "Active", "quantity_sold": "1"},
                   {"ok": True, "listing_status": "Completed", "quantity_sold": "0"},
                   {"ok": False, "error": "518"}):
            ok, end, mk = self.run_end(st)
            self.assertFalse(ok)
            end.assert_not_called()

    def test_unconfirmed_end_keeps_db(self):
        ok, end, mk = self.run_end({"ok": True, "listing_status": "Active", "quantity_sold": "0"}, ended_after=99)
        self.assertFalse(ok)
        self.assertEqual(mk.call_count, len(mls.PRICE_UP_END_CHECK_WAITS))


class HandlePriceUpdateTest(unittest.TestCase):
    """handle_listing_price_update: 販売上限の値上げは枠確保→成功なら終了しない、失敗なら従来どおり終了"""

    def run_handle(self, space_out, usd=150.0):
        conn = mock.MagicMock()
        with mock.patch.object(mis, "get_active_listing_for_action", return_value=("L0", "谷川④")), \
                mock.patch.object(mis, "update_ebay_price_rest", return_value=dict(LIMIT)), \
                mock.patch("apps.publish.make_listing_space.free_listing_space_for_price_up",
                           return_value=space_out) as free, \
                mock.patch.object(mis, "_clear_price_update_pending") as clear, \
                mock.patch.object(mis, "_delete_listing_after_price_failure", return_value=True) as delete, \
                mock.patch.object(mis, "send_mail") as mail, \
                mock.patch.object(mis.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            mis.handle_listing_price_update(conn, "SKU0", "メルカリ", usd)
        return free, clear, delete, mail

    def test_success_after_space_does_not_end_target(self):
        free, clear, delete, mail = self.run_handle({"resp": OK, "ended": ["L1"], "attempts": 1,
                                                     "waited_sec": 5, "note": ""})
        free.assert_called_once()
        clear.assert_called_once()
        delete.assert_not_called()
        mail.assert_not_called()

    def test_still_limit_falls_back_to_ending_target(self):
        free, clear, delete, mail = self.run_handle({"resp": dict(LIMIT), "ended": ["L1", "L2", "L3"],
                                                     "attempts": 6, "waited_sec": 75, "note": "終了の上限3件に達した"})
        delete.assert_called_once()
        self.assertIn("販売上限対応", mail.call_args[0][1])
        self.assertIn("販売上限", mail.call_args[0][0])

    def test_price_down_limit_does_not_free_space(self):
        free, clear, delete, mail = None, None, None, None
        conn = mock.MagicMock()
        down = dict(LIMIT, old_price="200.00")
        with mock.patch.object(mis, "get_active_listing_for_action", return_value=("L0", "谷川④")), \
                mock.patch.object(mis, "update_ebay_price_rest", return_value=down), \
                mock.patch("apps.publish.make_listing_space.free_listing_space_for_price_up") as free, \
                mock.patch.object(mis, "_record_price_update_pending"), \
                mock.patch.object(mis, "_delete_listing_after_price_failure") as delete, \
                mock.patch.object(mis.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError):
                mis.handle_listing_price_update(conn, "SKU0", "メルカリ", 150.0)
        free.assert_not_called()
        delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
