# -*- coding: utf-8 -*-
"""
LIMIT 対応・定期削除・NO_SPACE・Collectibles 優先（2026-11-18）のテスト。

  - LIMIT 対応（make_listing_space）の終了候補: 出品から20日以上・watchers=0・古い順。区分・有在庫/無在庫を区別しない。
    DB で終了済み（trx.listings.is_deleted=1）の出品は選ばない。eBay で終了できなかった出品は DB を変えない
  - 定期削除（delete_ebay_daily）: trx.listings に行が無い手動の出品（有在庫）も29日以上なら対象
  - publish_manager: 確保する件数は優先して出品している区分の不足件数。候補0件なら NO_SPACE で終了し、
    publish_manager の監視も終わる。翌日のリセットで NO_SPACE もクリア
  - Collectibles 優先でも非Collectibles が進む

DB を使うテストは test_account_listing_block と同じテスト専用DB（nostock_test_blocked。最後に削除）を使う。本番DBと eBay には触れない
（eBay の終了 API は mock）。
実行: python -m unittest apps.tests.test_limit_no_space -v
"""

import contextlib
from unittest import mock

from apps.common.utils import get_sql_server_connection
from apps.inventory import daily_check
from apps.publish import delete_ebay_daily as dd
from apps.publish import listing_class as lc
from apps.publish import make_listing_space as mls
from apps.publish import publish_ebay as pe
from apps.publish import publish_manager as pm
from apps.publish.make_listing_space import ListingSpaceResult
from apps.tests.test_collectibles_split import SplitDbBase, NC, C

ACCT = "川島"


def fake_end_items(fail_ids=(), already_ended_ids=()):
    """delete_items_from_ebay_batch の代わり。fail_ids は eBay で終了できない（エラー）、それ以外は終了成功。"""
    calls = []

    def _end(account, item_ids, batch_no=None):
        calls.append(list(item_ids))
        results = []
        for iid in item_ids:
            if iid in fail_ids:
                results.append({"item_id": iid, "success": False, "error_code": 1004, "error_message": "x"})
            else:
                results.append({"item_id": iid, "success": True,
                                "error_code": 1047 if iid in already_ended_ids else None, "error_message": ""})
        return {"success": True, "results": results}

    return _end, calls


class ExtDbBase(SplitDbBase):
    def setUp(self):
        super().setUp()
        for p in (mock.patch.object(pm, "_pm_log"), mock.patch("builtins.print"),
                  mock.patch.object(mls, "_log_delete_debug"), mock.patch.object(mls, "log_listings_change"),
                  mock.patch.object(dd, "log_listings_change")):
            p.start()
            self.addCleanup(p.stop)

    def add_active(self, listing_id, days, watchers=0, sku=None, account=ACCT, in_trx=True, trx_deleted=False,
                   cls=NC):
        """eBay の出品中一覧（start_time は UTC で days 日前）。in_trx なら trx.listings にも出品中（または終了済み）の行を作る。"""
        sku = f"s{listing_id}" if sku is None else sku
        self.sql("INSERT INTO ext.ebay_active_download (account, vendor_item_id, watchers, listing_id, start_time, fetched_at) "
                 "VALUES (?, ?, ?, ?, DATEADD(MINUTE, ?, SYSUTCDATETIME()), GETDATE())",
                 account, sku, watchers, listing_id, int(-days * 1440))
        if in_trx:
            self.add_item(sku, cls)
            self.sql("INSERT INTO trx.listings (listing_id, account, start_time, vendor_name, vendor_item_id, is_deleted) "
                     "VALUES (?, ?, DATEADD(MINUTE, ?, GETDATE()), N'メルカリ', ?, ?)",
                     listing_id, account, int(-days * 1440), sku, 1 if trx_deleted else 0)
        self.conn.commit()

    def trx_deleted(self, listing_id):
        r = self.sql("SELECT is_deleted, delete_reason FROM trx.listings WHERE listing_id = ?", listing_id).fetchone()
        self.conn.commit()
        return None if r is None else (bool(r[0]), r[1])

    def in_ext(self, listing_id):
        n = self.sql("SELECT COUNT(*) FROM ext.ebay_active_download WHERE listing_id = ?", listing_id).fetchone()[0]
        self.conn.commit()
        return n == 1

    def vendor_item_count(self):
        n = self.sql("SELECT COUNT(*) FROM trx.vendor_item").fetchone()[0]
        self.conn.commit()
        return n


# ============================================================
# LIMIT 対応の終了候補（make_listing_space）
# ============================================================
class ListingSpaceCandidateTest(ExtDbBase):
    def test_only_20_days_or_older_watchers0_oldest_first(self):
        self.add_active("new19", 19.9)          # 20日未満は watchers=0 でも終了しない
        self.add_active("old25", 25)
        self.add_active("old21", 21)
        self.add_active("old30w", 30, watchers=2)  # ウォッチャーがいる出品は終了しない
        self.add_active("old40", 40)
        self.add_active("other", 40, account="谷川②")  # 他のアカウント
        self.assertEqual(mls.get_listing_space_candidates(ACCT, 10), ["old40", "old25", "old21"])
        self.assertEqual(mls.get_listing_space_candidates(ACCT, 2), ["old40", "old25"])  # 必要な件数まで

    def test_manual_stock_and_both_classes_are_candidates(self):
        # 有在庫（trx.listings に行が無い手動の出品。短い・空の vendor_item_id）も、区分も区別しない
        self.add_active("manual", 30, sku="M-366", in_trx=False)
        self.add_active("blank", 29, sku="", in_trx=False)
        self.add_active("pen", 28, cls=C)
        self.add_active("cam", 27, cls=NC)
        self.assertEqual(mls.get_listing_space_candidates(ACCT, 10), ["manual", "blank", "pen", "cam"])

    def test_already_ended_in_db_is_not_candidate(self):
        # 一覧の取得後に在庫管理・定期削除で終了済みにした出品（eBay では 1047 を返し、枠は空かない）
        self.add_active("sold", 40, trx_deleted=True)
        self.add_active("live", 30)
        self.assertEqual(mls.get_listing_space_candidates(ACCT, 10), ["live"])

    def test_db_reflects_only_successful_ends(self):
        self.add_active("ok1", 40)
        self.add_active("ng1", 35)
        self.add_active("manual", 30, sku="M-1", in_trx=False)
        items_before = self.vendor_item_count()
        end, calls = fake_end_items(fail_ids={"ng1"})
        with mock.patch.object(mls, "delete_items_from_ebay_batch", side_effect=end):
            res = mls.run_make_listing_space(ACCT, 10)
        self.assertEqual(res, ListingSpaceResult(secured=2, candidates=3))
        self.assertEqual(calls, [["ok1", "ng1", "manual"]])
        self.assertEqual(self.trx_deleted("ok1"), (True, "定期削除(watch0)"))
        self.assertEqual(self.trx_deleted("ng1"), (False, None))   # eBay で終了できなかった出品は出品中のまま
        self.assertTrue(self.in_ext("ng1"))
        self.assertFalse(self.in_ext("ok1"))
        self.assertFalse(self.in_ext("manual"))                     # 有在庫は一覧から外すだけ（trx.listings に行を作らない）
        self.assertIsNone(self.trx_deleted("manual"))
        self.assertEqual(self.vendor_item_count(), items_before)    # trx.vendor_item は変えない

    def test_no_candidates(self):
        self.add_active("new", 5)
        with mock.patch.object(mls, "delete_items_from_ebay_batch") as end:
            res = mls.run_make_listing_space(ACCT, 10)
        self.assertEqual(res, ListingSpaceResult(secured=0, candidates=0))
        end.assert_not_called()

    def test_api_failure_is_not_no_candidates(self):
        self.add_active("ng1", 40)
        end, _ = fake_end_items(fail_ids={"ng1"})
        with mock.patch.object(mls, "delete_items_from_ebay_batch", side_effect=end):
            self.assertEqual(mls.run_make_listing_space(ACCT, 10), ListingSpaceResult(secured=0, candidates=1))


# ============================================================
# 定期削除（delete_ebay_daily。29日以上）
# ============================================================
class DailyDeleteTest(ExtDbBase):
    def setUp(self):
        super().setUp()
        p = mock.patch.object(dd.RATE_LIMITER, "before_call")
        p.start()
        self.addCleanup(p.stop)

    def test_candidates_include_manual_stock(self):
        self.add_active("trx30", 30)                                   # 従来どおり（trx.listings の出品中）
        self.add_active("trx10", 10)
        self.add_active("manual30", 30, sku="M-326", in_trx=False)     # 有在庫も29日以上なら対象
        self.add_active("manual10", 10, sku="M-382", in_trx=False)     # 29日未満は対象外
        self.add_active("ended30", 30, trx_deleted=True)               # 終了済みは対象外
        got = sorted(iid for _, iid in dd.fetch_delete_candidates_30d_all())
        self.assertEqual(got, ["manual30", "trx30"])

    def test_db_reflects_only_successful_ends(self):
        self.add_active("trx30", 30)
        self.add_active("trx31ng", 31)
        self.add_active("manual30", 30, sku="M-326", in_trx=False)
        items_before = self.vendor_item_count()
        end, _ = fake_end_items(fail_ids={"trx31ng"})
        with mock.patch.object(dd, "delete_items_from_ebay_batch", side_effect=end):
            acc, deleted, limited = dd.delete_items_from_ebay_and_sql(ACCT, ["trx31ng", "trx30", "manual30"])
        self.assertEqual((acc, deleted, limited), (ACCT, 2, False))
        self.assertEqual(self.trx_deleted("trx30"), (True, "定期削除"))
        self.assertEqual(self.trx_deleted("trx31ng"), (False, None))
        self.assertTrue(self.in_ext("trx31ng"))
        self.assertFalse(self.in_ext("trx30"))
        self.assertFalse(self.in_ext("manual30"))
        self.assertIsNone(self.trx_deleted("manual30"))
        self.assertEqual(self.vendor_item_count(), items_before)
        # 終了した有在庫は次のサイクルの候補に出てこない（失敗した出品はまた候補になる）
        self.assertEqual(sorted(iid for _, iid in dd.fetch_delete_candidates_30d_all()), ["trx31ng"])


# ============================================================
# publish_manager: 確保する件数・NO_SPACE
# ============================================================
class NoSpaceTest(ExtDbBase):
    PC = "testpc"

    def setUp(self):
        super().setUp()
        self.add_pc(self.PC)

    def limit_account(self, nc=("LIMIT", 10, 0), c=("LIMIT", 6, 0), account=ACCT):
        """(終了理由, 目標, 当日の出品数) を区分ごとに指定して LIMIT のアカウントを作る。"""
        self.add_account(account, post_target=nc[1], post_target_collectibles=c[1], close_reason=nc[0],
                         close_reason_collectibles=c[0])
        for cls, (_, _, n) in ((NC, nc), (C, c)):
            for _ in range(n):
                self.add_class_listing(account, cls)
        return pm.get_limit_accounts()[0]

    def test_remaining_is_collectibles_while_collectibles_open(self):
        info = self.limit_account(nc=("LIMIT", 10, 2), c=("LIMIT", 6, 1))
        self.assertEqual(pm.calculate_remaining_count(info), 5)  # 合計の 13 ではなく Collectibles の 6 − 1

    def test_remaining_is_non_collectibles_after_collectibles_finished(self):
        for done in ("DONE", "EMPTY"):
            with self.subTest(done=done):
                self.sql("DELETE FROM mst.ebay_accounts; DELETE FROM trx.listings")
                self.conn.commit()
                info = self.limit_account(nc=("LIMIT", 10, 2), c=(done, 6, 1))
                self.assertEqual(pm.calculate_remaining_count(info), 8)

    def test_collectibles_reached_target_uses_non_collectibles(self):
        info = self.limit_account(nc=("LIMIT", 4, 1), c=("LIMIT", 2, 2))
        self.assertEqual(pm.calculate_remaining_count(info), 3)

    def test_collectibles_phase_then_non_collectibles_phase(self):
        # Collectibles の不足分だけ確保して再開 → 非Collectibles で再び LIMIT → 非Collectibles の不足分を追加で確保（重ねて数えない）
        info = self.limit_account(nc=("LIMIT", 10, 2), c=("LIMIT", 3, 0))
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(3, 3)) as space:
            pm.handle_limit_account(info)
        space.assert_called_once_with(ACCT, 3)
        self.assertEqual(self.cls_state(ACCT), (None, None))
        for _ in range(3):
            self.add_class_listing(ACCT, C)
        self.sql("UPDATE mst.ebay_accounts SET close_reason = 'LIMIT', close_reason_collectibles = 'DONE'")
        self.conn.commit()
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(8, 8)) as space:
            pm.handle_limit_account(pm.get_limit_accounts()[0])
        space.assert_called_once_with(ACCT, 8)
        self.assertEqual(self.cls_state(ACCT), (None, "DONE"))

    def test_waits_for_workers_before_ending_listings(self):
        # 二重削除防止: そのアカウントを処理中の worker がいる間は終了しない（従来どおり）
        info = self.limit_account()
        self.sql("UPDATE mst.execute_pcs SET account = ? WHERE execute_pc = ?", ACCT, self.PC)
        self.conn.commit()
        with mock.patch.object(pm, "run_make_listing_space") as space:
            pm.handle_limit_account(pm.get_limit_accounts()[0])
        space.assert_not_called()
        self.assertEqual(self.cls_state(ACCT), ("LIMIT", "LIMIT"))

    def test_no_candidates_becomes_no_space_and_finishes(self):
        self.add_account("谷川②", post_target=5, post_target_collectibles=5, close_reason="DONE",
                         close_reason_collectibles="EMPTY")
        info = self.limit_account(nc=("LIMIT", 10, 2), c=("LIMIT", 6, 1))
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(0, 0)):
            pm.handle_limit_account(info)
        self.assertEqual(self.cls_state(ACCT), ("NO_SPACE", "NO_SPACE"))
        self.assertEqual(self.cls_state("谷川②"), ("DONE", "EMPTY"))  # 他のアカウントは変えない
        self.assertFalse(pe.has_limit_accounts(self.conn))
        self.assertTrue(pm.is_all_accounts_finished())
        self.assertIsNone(pe.fetch_next_account_and_lock(self.conn, self.PC))  # worker も取りに来ない

    def test_no_space_keeps_finished_classes_and_marks_reached_done(self):
        info = self.limit_account(nc=("LIMIT", 10, 2), c=("EMPTY", 6, 1))
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(0, 0)):
            pm.handle_limit_account(info)
        self.assertEqual(self.cls_state(ACCT), ("NO_SPACE", "EMPTY"))
        self.sql("DELETE FROM mst.ebay_accounts; DELETE FROM trx.listings")
        self.conn.commit()
        # 目標に達している区分（LIMIT のままだった）は NO_SPACE ではなく DONE
        info = self.limit_account(nc=("LIMIT", 10, 2), c=("LIMIT", 1, 1))
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(0, 0)):
            pm.handle_limit_account(info)
        self.assertEqual(self.cls_state(ACCT), ("NO_SPACE", "DONE"))

    def test_api_failure_keeps_limit(self):
        info = self.limit_account()
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(0, 4)):
            pm.handle_limit_account(info)
        self.assertEqual(self.cls_state(ACCT), ("LIMIT", "LIMIT"))  # 候補はあるので従来どおり LIMIT のまま再試行
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(0, None)):
            pm.handle_limit_account(pm.get_limit_accounts()[0])  # 排他ロックを取れなかった
        self.assertEqual(self.cls_state(ACCT), ("LIMIT", "LIMIT"))
        self.assertFalse(pm.is_all_accounts_finished())

    def test_monitor_loop_ends_with_no_space(self):
        self.limit_account()
        with mock.patch.object(pm, "run_make_listing_space", return_value=ListingSpaceResult(0, 0)) as space, \
                mock.patch.object(pm.time, "sleep") as sleep:
            pm.monitor_limit_accounts()  # 無限に繰り返さず抜ける
        space.assert_called_once()
        self.assertLessEqual(sleep.call_count, 1)
        self.assertEqual(self.cls_state(ACCT), ("NO_SPACE", "NO_SPACE"))

    def test_real_candidates_flow_to_no_space(self):
        # make_listing_space の実際の候補の選び方を通す: 20日未満しか無ければ NO_SPACE
        info = self.limit_account()
        self.add_active("new", 10)
        with mock.patch.object(mls, "delete_items_from_ebay_batch") as end:
            pm.handle_limit_account(info)
        end.assert_not_called()
        self.assertEqual(self.cls_state(ACCT), ("NO_SPACE", "NO_SPACE"))
        self.assertTrue(self.in_ext("new"))

    def test_next_day_reset_clears_no_space(self):
        self.add_account(ACCT, post_target=3, post_target_collectibles=3, close_reason="NO_SPACE",
                         close_reason_collectibles="NO_SPACE")
        pm.reset_close_status()
        self.assertEqual(self.cls_state(ACCT), (None, None))
        acct = pe.fetch_next_account_and_lock(self.conn, self.PC)
        self.assertEqual(acct.account, ACCT)

    def test_auto_fix_and_record_do_not_touch_no_space(self):
        self.add_account(ACCT, post_target=1, post_target_collectibles=1, close_reason="NO_SPACE",
                         close_reason_collectibles="NO_SPACE")
        self.add_class_listing(ACCT, NC)
        self.assertEqual(pm.auto_fix_done_accounts(), [])
        self.assertEqual(self.cls_state(ACCT), ("NO_SPACE", "NO_SPACE"))

    def test_mail_lists_no_space(self):
        self.add_account(ACCT, post_target=3, post_target_collectibles=3, close_reason="NO_SPACE",
                         close_reason_collectibles="NO_SPACE")
        text = daily_check.build_publish_manager_account_summary(self.conn)
        self.assertIn("出品枠不足（NO_SPACE）", text)
        self.assertIn(f"- {ACCT}", text)


# ============================================================
# Collectibles 優先（区分の選び方）
# ============================================================
class PickOrderTest(SplitDbBase):
    def test_order_two_to_one(self):
        picks = {}
        order = []
        for _ in range(9):
            c = lc.class_pick_order([NC, C], picks)[0]
            picks[c] = picks.get(c, 0) + 1
            order.append(c)
        self.assertEqual(order, [C, C, NC] * 3)
        self.assertEqual(lc.class_pick_order([NC], picks), [NC])  # 片方だけなら、その区分
        self.assertEqual(lc.class_pick_order([C], {}), [C])

    def test_full_slot_falls_back_to_other_class(self):
        # Collectibles の処理枠が空いていなければ（他 worker が目標まで処理中）、非Collectibles を取る
        self.add_account(ACCT, post_target=5, post_target_collectibles=5)
        acct = pe.Account(ACCT, ACCT, 5, 5)
        reserve = lc.reserve_class_slot

        def _reserve(conn, account, cls, n):
            return (None, {}) if cls == C else reserve(conn, account, cls, n)

        row = {"vendor_item_id": "x", "is_collectibles": False}
        with mock.patch.object(lc, "reserve_class_slot", side_effect=_reserve), \
                mock.patch.object(pe, "take_one_vendor_item", return_value=row) as take, \
                mock.patch("builtins.print"):
            kind, got, slot = pe.pick_next_item(self.conn, acct, "w", {}, picks={})
        self.assertEqual(kind, "ROW")
        self.assertFalse(take.call_args.kwargs["is_collectibles"])
        lc.release_class_slot(self.conn, slot)
