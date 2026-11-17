# -*- coding: utf-8 -*-
"""
eBay 出品の Collectibles／非Collectibles の区分ごとの管理（2026-11-17）のテスト。

  - 当日の目標の自動計算（非Collectibles: 月 10,000件・1日 500件まで / Collectibles: 月 50,000件・1日 333件まで）
  - 出品ループ: 区分ごとに DONE・EMPTY。片方が終わってももう片方は続ける。両方終わったらアカウントを終える
  - 進み具合（当日の出品数 ÷ 目標）が低い区分から取る（偏らない）
  - 途中から再開しても、当日の出品済みの件数（trx.listings から区分ごとに数える）を引き継ぐ
  - 月間無料出品残数は商品の区分の列から減らし、区分の目標に達したらその区分を DONE
  - LIMIT は未終了の両区分、BLOCKED は両区分。publish_manager の LIMIT 対応・DONE補正・終了判定・翌日のリセット
  - 同時実行: 区分の処理枠で、何台が同時に取りに来ても目標を超えない

DB を使うテストは test_account_listing_block と同じテスト専用DB（nostock_test_blocked。最後に削除）を使う。本番DBには触れない。
実行: python -m unittest apps.tests.test_collectibles_split -v
"""

import contextlib
import itertools
import threading
import unittest
from datetime import date, datetime
from unittest import mock

from apps.common.utils import get_sql_server_connection
from apps.publish import listing_class as lc
from apps.publish import publish_ebay as pe
from apps.publish import publish_manager as pm
from apps.tests.test_account_listing_block import DbTestBase, _fake_sync_playwright

NC, C = lc.NON_COLLECTIBLES, lc.COLLECTIBLES
PRESET = {NC: "p_camera", C: "p_pen"}


# ============================================================
# 当日の目標の自動計算（DB なし）
# ============================================================
class TargetCalcTest(unittest.TestCase):
    NOV17 = date(2026, 11, 17)  # 30日の月の17日目（10,000件なら 1日 333.3件の目安）

    def non_col(self, left, d=NOV17):
        return pm.calculate_post_target(left, d)["calculated_post_target"]

    def col(self, left, d=NOV17):
        return pm.calculate_post_target(left, d, monthly_target=pm.MONTHLY_TARGET_COLLECTIBLES,
                                        max_daily_post_target=pm.MAX_DAILY_POST_TARGET_COLLECTIBLES)["calculated_post_target"]

    def test_non_collectibles_follows_progress(self):
        # 16日目までの予定は 5,333件。ちょうどなら 334件、遅れていれば多く、進んでいれば少なく
        self.assertEqual(self.non_col(10000 - 5333), 334)
        self.assertEqual(self.non_col(10000 - 5200), 467)   # 133件遅れ → 333件より多い
        self.assertEqual(self.non_col(10000 - 5500), 167)   # 167件進んでいる → 333件より少ない
        self.assertEqual(self.non_col(10000 - 5800), 0)     # 大きく進んでいる → 0件

    def test_non_collectibles_cap_500(self):
        self.assertEqual(self.non_col(6831), 500)  # 谷川② 11/17 の値（2,498件遅れ）
        self.assertEqual(pm.MAX_DAILY_POST_TARGET, 500)

    def test_collectibles_cap_333(self):
        for left in (50000, 45284, 20000, 1000):
            self.assertLessEqual(self.col(left), 333)
        self.assertEqual(self.col(45284), 333)
        self.assertEqual(pm.MAX_DAILY_POST_TARGET_COLLECTIBLES, 333)

    def test_last_day_and_zero_left(self):
        last = date(2026, 11, 30)
        self.assertEqual(self.col(120, last), 120)   # 最終日は残数まで
        self.assertEqual(self.col(0), 0)             # 残数0なら0件
        self.assertEqual(self.non_col(0), 0)

    def test_never_exceeds_left(self):
        for d in (date(2026, 11, 1), self.NOV17, date(2026, 11, 29), date(2026, 11, 30)):
            for left in (0, 1, 50, 333, 499, 9999):
                self.assertLessEqual(self.non_col(left, d), left)
                self.assertLessEqual(self.col(left, d), left)

    def test_validate_uses_class_monthly_target(self):
        self.assertTrue(pm.validate_listing_left(45284, pm.MONTHLY_TARGET_COLLECTIBLES)[0])
        self.assertFalse(pm.validate_listing_left(45284)[0])  # 非Collectibles の枠（10,000件）では異常値
        self.assertFalse(pm.validate_listing_left(None, pm.MONTHLY_TARGET_COLLECTIBLES, "listing_left_collectibles")[0])


# ============================================================
# DB を使うテストの共通部分
# ============================================================
class SplitDbBase(DbTestBase):
    def setUp(self):
        super().setUp()
        # 区分を数えるためのマスター: メルカリ（vendor_group M）のカテゴリー 1 = カメラ（非Collectibles）、2 = ペン（Collectibles）
        self.sql("INSERT INTO mst.vendors (vendor_code, vendor_name, vendor_group) VALUES ('M', N'メルカリ', N'M')")
        self.sql("INSERT INTO mst.presets_categories (category_id, vendor_group, brand_id, is_collectibles) VALUES "
                 "(1, N'M', 0, 0), (2, N'M', 0, 1), (2, N'M', 7, 1)")  # ペンはブランド別の行もある（重複して数えない）
        self.sql("INSERT INTO mst.presets_lookup (preset, category_id) VALUES (?, 1), (?, 2)", PRESET[NC], PRESET[C])
        self.conn.commit()
        self.seq = itertools.count(1)

    def add_item(self, sku, cls):
        c = get_sql_server_connection()
        try:
            c.cursor().execute("INSERT INTO trx.vendor_item (vendor_name, vendor_item_id, preset) VALUES (N'メルカリ', ?, ?)",
                               sku, PRESET[cls])
            c.commit()
        finally:
            c.close()

    def add_class_listing(self, account, cls, sku=None):
        """区分 cls の商品を当日1件出品した状態にする（他worker相当の別接続で確定）。"""
        sku = sku or f"{account}-{cls}-{next(self.seq)}"
        self.add_item(sku, cls)
        c = get_sql_server_connection()
        try:
            c.cursor().execute("INSERT INTO trx.listings (listing_id, account, start_time, vendor_name, vendor_item_id, is_deleted) "
                               "VALUES (?, ?, GETDATE(), N'メルカリ', ?, 0)", f"L{sku}", account, sku)
            c.commit()
        finally:
            c.close()
        return sku

    def cls_state(self, account):
        r = self.sql("SELECT close_reason, close_reason_collectibles FROM mst.ebay_accounts WHERE account = ?",
                     account).fetchone()
        self.conn.commit()
        return tuple(r)

    def posted(self, account):
        st = lc.read_class_state(self.conn, account)
        self.conn.commit()
        return st[NC]["posted"], st[C]["posted"]


# ============================================================
# publish_ebay.main（1 worker 分）
# ============================================================
class MainLoopTest(SplitDbBase):
    PC = "testpc"

    def run_main(self, take_one, post=None):
        """take_one(account, cls) -> row|None|TAKE_RAKUMA_PAUSED。post(account, sku, cls) -> "OK"|"NG"。"""
        calls = {"take": [], "post": []}
        post = post or (lambda a, s, c: "OK")

        def _take(conn, preset_group, processing_by, account, is_collectibles=False):
            cls = C if is_collectibles else NC
            calls["take"].append((account, cls))
            return take_one(account, cls)

        def _heavy_check(conn, page, url, sku, *a, **k):
            return ({"sku": sku}, None, 0, None, None)

        def _post(**kw):
            sku = kw["heavy"]["sku"]
            cls = lc.class_of(kw["is_collectibles"])
            calls["post"].append((kw["acct"], cls))
            if post(kw["acct"], sku, cls) == "OK":
                self.add_class_listing(kw["acct"], cls, sku=f"{sku}-done")
                kw["acct_success"][kw["acct"]] += 1
            return (kw["acct_targets"], kw["acct_success"], kw["total_listings"], False, 0,
                    0, "NORMAL", 0, None, False)

        patches = [
            mock.patch.object(pe.socket, "gethostname", return_value=self.PC),
            mock.patch.object(pe, "set_exit_report"),
            mock.patch.object(pe, "set_exit_context"),
            mock.patch.object(pe, "start_watchdog", return_value=mock.Mock()),
            mock.patch.object(pe.boto3, "client"),
            mock.patch.object(pe, "fetch_active_presets", return_value=[]),
            mock.patch.object(pe, "sync_playwright", _fake_sync_playwright),
            mock.patch.object(pe, "playwright_driver_monitor", contextlib.nullcontext),
            mock.patch.object(pe, "retry_pending_orphan_republishes"),
            mock.patch.object(pe, "take_one_vendor_item", side_effect=_take),
            mock.patch.object(pe, "heavy_check_detail", side_effect=_heavy_check),
            mock.patch.object(pe, "post_to_ebay", side_effect=_post),
            mock.patch("builtins.print"),
        ]
        with contextlib.ExitStack() as st:
            for p in patches:
                st.enter_context(p)
            with self.assertRaises(SystemExit) as cm:
                pe.main()
        self.assertEqual(cm.exception.code, 10)  # 正常終了
        return calls

    def row(self, account, cls):
        sku = f"{account}-{'C' if cls == C else 'N'}-{next(self.seq)}"
        return {"vendor_item_id": sku, "price": 1, "shipping_days": None, "vendor_name": "メルカリ",
                "preset": PRESET[cls], "mode": None, "default_brand_en": "b", "category_id_ebay": "1",
                "department": "d", "type_ebay": "t", "category_group": "g", "low_jpy_target": 1,
                "high_jpy_target": 2, "item_condition_id": 1, "is_collectibles": cls == C}

    def test_both_classes_to_done_alternating(self):
        self.add_account("谷川②", post_target=3, post_target_collectibles=3)
        self.add_pc(self.PC)
        calls = self.run_main(self.row)
        self.assertEqual(self.posted("谷川②"), (3, 3))
        self.assertEqual(self.cls_state("谷川②"), ("DONE", "DONE"))
        # 進み具合の低い区分から取る（同じなら非Collectibles）→ 交互になる
        self.assertEqual([c for _, c in calls["post"]], [NC, C, NC, C, NC, C])

    def test_uneven_targets_stay_proportional(self):
        self.add_account("谷川②", post_target=2, post_target_collectibles=4)
        self.add_pc(self.PC)
        calls = self.run_main(self.row)
        self.assertEqual(self.posted("谷川②"), (2, 4))
        order = [c for _, c in calls["post"]]
        self.assertEqual(order, [NC, C, C, NC, C, C])  # 目標の比率（1:2）どおりに混ざる

    def test_collectibles_empty_other_continues(self):
        # BUZZ② のように Collectibles の候補が無いアカウント: Collectibles だけ EMPTY、非Collectibles は目標まで
        self.add_account("BUZZ②", post_target=3, post_target_collectibles=333)
        self.add_pc(self.PC)
        calls = self.run_main(lambda a, c: self.row(a, c) if c == NC else None)
        self.assertEqual(self.cls_state("BUZZ②"), ("DONE", "EMPTY"))
        self.assertEqual(self.posted("BUZZ②"), (3, 0))
        self.assertEqual(sum(1 for _, c in calls["take"] if c == C), 1)  # EMPTY の後は取りに行かない

    def test_non_collectibles_empty_collectibles_continues(self):
        self.add_account("貴文③", post_target=100, post_target_collectibles=2)
        self.add_pc(self.PC)
        self.run_main(lambda a, c: self.row(a, c) if c == C else None)
        self.assertEqual(self.cls_state("貴文③"), ("EMPTY", "DONE"))
        self.assertEqual(self.posted("貴文③"), (0, 2))

    def test_failed_items_do_not_count(self):
        self.add_account("川島", post_target=2, post_target_collectibles=0)
        self.add_pc(self.PC)
        results = iter(["NG", "OK", "NG", "OK"])
        calls = self.run_main(self.row, post=lambda a, s, c: next(results))
        self.assertEqual(len(calls["post"]), 4)
        self.assertEqual(self.posted("川島"), (2, 0))
        self.assertEqual(self.cls_state("川島"), ("DONE", "DONE"))  # 目標0件の Collectibles は出品せずに DONE

    def test_restart_midday_keeps_today_counts(self):
        # 途中停止・再起動・変更当日: 当日すでに出品した件数（区分ごと）を引き継ぎ、残りだけ出品する
        self.add_account("谷川②", post_target=3, post_target_collectibles=2)
        self.add_pc(self.PC)
        self.add_class_listing("谷川②", NC)
        self.add_class_listing("谷川②", NC)
        self.add_class_listing("谷川②", C)
        calls = self.run_main(self.row)
        self.assertEqual(sorted(c for _, c in calls["post"]), [NC, C])
        self.assertEqual(self.posted("谷川②"), (3, 2))

    def test_yesterday_listings_do_not_count(self):
        self.add_account("谷川②", post_target=1, post_target_collectibles=1)
        self.add_pc(self.PC)
        sku = self.add_class_listing("谷川②", C)
        self.sql("UPDATE trx.listings SET start_time = DATEADD(DAY, -1, start_time) WHERE vendor_item_id = ?", sku)
        self.conn.commit()
        self.run_main(self.row)
        self.assertEqual(self.posted("谷川②"), (1, 1))

    def test_rakuma_paused_class_waits_other_continues(self):
        self.add_account("谷川②", post_target=2, post_target_collectibles=1)
        self.add_pc(self.PC)
        paused = {"n": 0}

        def take(a, c):
            if c == C and paused["n"] < 2:
                paused["n"] += 1
                return pe.TAKE_RAKUMA_PAUSED
            return self.row(a, c)

        now = [1000.0]
        with mock.patch.object(pe.time, "time", lambda: now[0]), \
                mock.patch.object(pe.time, "sleep", lambda s: now.__setitem__(0, now[0] + max(s, 1))):
            calls = self.run_main(take)
        self.assertEqual(self.posted("谷川②"), (2, 1))
        self.assertEqual(self.cls_state("谷川②"), ("DONE", "DONE"))
        self.assertEqual(calls["post"][0][1], NC)  # ラクマの障害中は非Collectiblesを先に出品

    def test_target_zero_account_is_not_assigned_and_finishes(self):
        self.add_account("BUZZ", post_target=0, post_target_collectibles=0)
        self.add_pc(self.PC)
        self.assertIsNone(pe.fetch_next_account_and_lock(self.conn, self.PC))
        with mock.patch.object(pm, "_pm_log"), mock.patch("builtins.print"):
            self.assertFalse(pm.is_all_accounts_finished())
            self.assertEqual(pm.auto_fix_done_accounts(), ["BUZZ"])
            self.assertTrue(pm.is_all_accounts_finished())
        self.assertEqual(self.cls_state("BUZZ"), ("DONE", "DONE"))

    def test_account_assigned_while_one_class_open(self):
        self.add_account("谷川②", post_target=5, post_target_collectibles=5, close_reason="DONE",
                         close_reason_collectibles=None)
        self.add_pc(self.PC)
        acct = pe.fetch_next_account_and_lock(self.conn, self.PC)
        self.assertEqual((acct.account, acct.post_target, acct.post_target_collectibles), ("谷川②", 5, 5))

    def test_next_day_reset_rechecks_empty_class(self):
        self.add_account("BUZZ②", post_target=1, post_target_collectibles=1, close_reason="DONE",
                         close_reason_collectibles="EMPTY")
        self.add_pc(self.PC)
        with mock.patch.object(pm, "_pm_log"):
            pm.reset_close_status()
        self.assertEqual(self.cls_state("BUZZ②"), (None, None))
        # 翌日（今日の出品は無い状態）に Collectibles の候補ができていれば、特別な設定なしで出品する
        self.run_main(self.row)
        self.assertEqual(self.posted("BUZZ②"), (1, 1))


# ============================================================
# 出品成功の記録（月間無料出品残数・区分の DONE）
# ============================================================
class RecordListingTest(SplitDbBase):
    def record(self, account, cls, sku):
        self.add_item(sku, cls)
        with mock.patch("builtins.print"):
            pe.record_ebay_listing(f"L{sku}", account, sku, "メルカリ", 100.0, cls == C)

    def lefts(self, account):
        r = self.sql("SELECT listing_left, listing_left_collectibles FROM mst.ebay_accounts WHERE account = ?",
                     account).fetchone()
        self.conn.commit()
        return tuple(r)

    def test_decrements_the_class_column(self):
        self.add_account("谷川②", post_target=5, post_target_collectibles=5, listing_left=6831,
                         listing_left_collectibles=45284)
        self.record("谷川②", C, "pen1")
        self.assertEqual(self.lefts("谷川②"), (6831, 45283))
        self.record("谷川②", NC, "cam1")
        self.assertEqual(self.lefts("谷川②"), (6830, 45283))
        self.assertEqual(self.posted("谷川②"), (1, 1))

    def test_class_done_when_target_reached(self):
        self.add_account("谷川②", post_target=1, post_target_collectibles=2, listing_left=10, listing_left_collectibles=10)
        self.record("谷川②", C, "pen1")
        self.assertEqual(self.cls_state("谷川②"), (None, None))
        self.record("谷川②", C, "pen2")
        self.assertEqual(self.cls_state("谷川②"), (None, "DONE"))
        self.record("谷川②", NC, "cam1")
        self.assertEqual(self.cls_state("谷川②"), ("DONE", "DONE"))

    def test_limit_upgraded_to_done(self):
        self.add_account("谷川②", post_target=1, post_target_collectibles=5, close_reason="LIMIT",
                         listing_left=10, listing_left_collectibles=10)
        self.record("谷川②", NC, "cam1")
        self.assertEqual(self.cls_state("谷川②"), ("DONE", "LIMIT"))


# ============================================================
# LIMIT・BLOCKED（publish_ebay の書き込みと publish_manager の対応）
# ============================================================
class LimitBlockedTest(SplitDbBase):
    PC = "testpc"

    def setUp(self):
        super().setUp()
        for p in (mock.patch.object(pm, "_pm_log"), mock.patch("builtins.print")):
            p.start()
            self.addCleanup(p.stop)
        self.add_pc(self.PC)

    def test_limit_written_to_open_classes(self):
        self.add_account("谷川②", post_target=1, post_target_collectibles=5)
        self.add_account("BUZZ②", post_target=5, post_target_collectibles=5, close_reason=None,
                         close_reason_collectibles="EMPTY")
        self.add_class_listing("谷川②", NC)  # 非Collectibles は目標到達済み
        pe.release_pc_and_close_account(self.conn, self.PC, "谷川②", "LIMIT")
        pe.release_pc_and_close_account(self.conn, self.PC, "BUZZ②", "LIMIT")
        self.assertEqual(self.cls_state("谷川②"), ("DONE", "LIMIT"))
        self.assertEqual(self.cls_state("BUZZ②"), ("LIMIT", "EMPTY"))  # EMPTY の区分はそのまま
        self.assertTrue(pe.has_limit_accounts(self.conn))

    def test_manager_handles_limit_of_both_classes(self):
        self.add_account("谷川②", post_target=4, post_target_collectibles=6, close_reason="LIMIT",
                         close_reason_collectibles="LIMIT")
        self.add_class_listing("谷川②", NC)
        self.add_class_listing("谷川②", C)
        accts = pm.get_limit_accounts()
        self.assertEqual([(a["account"], a["post_target"]) for a in accts], [("谷川②", 10)])
        self.assertEqual(pm.calculate_remaining_count(accts[0]), 3 + 5)
        self.assertFalse(pm.is_all_accounts_finished())
        with mock.patch.object(pm, "run_make_listing_space", return_value=8) as space:
            pm.handle_limit_account(accts[0])
        space.assert_called_once_with("谷川②", 8)
        self.assertEqual(self.cls_state("谷川②"), (None, None))  # 両区分とも再開（片方だけ取り残さない）

    def test_manager_limit_only_one_class(self):
        self.add_account("谷川②", post_target=4, post_target_collectibles=6, close_reason="DONE",
                         close_reason_collectibles="LIMIT")
        accts = pm.get_limit_accounts()
        self.assertEqual(pm.calculate_remaining_count(accts[0]), 6)  # DONE の区分の不足は数えない
        with mock.patch.object(pm, "run_make_listing_space", return_value=6):
            pm.handle_limit_account(accts[0])
        self.assertEqual(self.cls_state("谷川②"), ("DONE", None))

    def test_manager_limit_reached_target_becomes_done(self):
        self.add_account("川島", post_target=1, post_target_collectibles=1, close_reason="LIMIT",
                         close_reason_collectibles="LIMIT")
        self.add_class_listing("川島", NC)
        self.add_class_listing("川島", C)
        with mock.patch.object(pm, "run_make_listing_space") as space:
            pm.handle_limit_account(pm.get_limit_accounts()[0])
        space.assert_not_called()
        self.assertEqual(self.cls_state("川島"), ("DONE", "DONE"))
        self.assertTrue(pm.is_all_accounts_finished())

    def test_manager_waits_for_workers(self):
        self.add_account("谷川②", post_target=4, post_target_collectibles=6, close_reason="LIMIT",
                         close_reason_collectibles="LIMIT")
        self.sql("UPDATE mst.execute_pcs SET account = N'谷川②' WHERE execute_pc = ?", self.PC)
        self.conn.commit()
        with mock.patch.object(pm, "run_make_listing_space") as space:
            pm.handle_limit_account(pm.get_limit_accounts()[0])
        space.assert_not_called()
        self.assertEqual(self.cls_state("谷川②"), ("LIMIT", "LIMIT"))

    def test_auto_fix_done_per_class(self):
        self.add_account("谷川②", post_target=1, post_target_collectibles=3, close_reason="LIMIT",
                         close_reason_collectibles=None)
        self.add_class_listing("谷川②", NC)
        self.add_class_listing("谷川②", C)
        self.assertEqual(pm.auto_fix_done_accounts(), ["谷川②"])
        self.assertEqual(self.cls_state("谷川②"), ("DONE", None))  # Collectibles は 1/3 なので続ける

    def test_blocked_both_classes(self):
        self.add_account("BUZZ", post_target=5, post_target_collectibles=5, close_reason=None,
                         close_reason_collectibles="EMPTY")
        pe.release_pc_and_close_account(self.conn, self.PC, "BUZZ", "BLOCKED", "detail")
        self.assertEqual(self.cls_state("BUZZ"), ("BLOCKED", "BLOCKED"))
        self.assertEqual(pm.log_blocked_accounts(), [("BUZZ", "detail")])
        self.assertTrue(pm.is_all_accounts_finished())
        self.assertIsNone(pe.fetch_next_account_and_lock(self.conn, self.PC))

    def test_other_worker_blocked_collectibles_only(self):
        # 片方の区分にだけ BLOCKED が見えても（書き込みの途中など）アカウントを止める
        self.add_account("BUZZ", post_target=5, post_target_collectibles=5, close_reason=None,
                         close_reason_collectibles="BLOCKED")
        acct = pe.Account("BUZZ", "BUZZ", 5, 5)
        self.assertEqual(pe.pick_next_item(self.conn, acct, "w", {})[0], "BLOCKED")

    def test_completion_mail_shows_both_classes(self):
        from apps.inventory import daily_check
        self.add_account("谷川②", post_target=3, post_target_collectibles=4, close_reason="DONE",
                         close_reason_collectibles="EMPTY")
        self.add_class_listing("谷川②", NC)
        self.add_class_listing("谷川②", C)
        body = daily_check.build_publish_manager_account_summary(self.conn)
        self.assertIn("- 谷川②: close_reason=DONE / 本日出品=1件 / post_target=3"
                      "  ｜ Collectibles: close_reason=EMPTY / 本日出品=1件 / post_target=4", body)


# ============================================================
# 同時実行（区分の処理枠）
# ============================================================
class SlotTest(SplitDbBase):
    def conns(self, n):
        cs = [get_sql_server_connection() for _ in range(n)]
        for c in cs:
            self.addCleanup(c.close)
        return cs

    def test_slots_respect_target_and_in_flight(self):
        self.add_account("谷川②", post_target=0, post_target_collectibles=2)
        a, b, c = self.conns(3)
        sa, _ = lc.reserve_class_slot(a, "谷川②", C, 8)
        sb, _ = lc.reserve_class_slot(b, "谷川②", C, 8)
        sc, info = lc.reserve_class_slot(c, "谷川②", C, 8)
        self.assertIsNotNone(sa)
        self.assertIsNotNone(sb)
        self.assertIsNone(sc)                     # 2件が処理中なので3台目は取れない
        self.assertEqual(info["held"], 2)
        self.add_class_listing("谷川②", C)       # a の出品が成功（枠を持ったまま記録）
        lc.release_class_slot(a, sa)
        self.assertIsNone(lc.reserve_class_slot(c, "谷川②", C, 8)[0])  # 1件出品 + b が処理中 = 目標2
        lc.release_class_slot(b, sb)              # b は出品できなかった
        sc, _ = lc.reserve_class_slot(c, "谷川②", C, 8)
        self.assertIsNotNone(sc)                  # 空いた分だけ取れる
        self.assertIsNone(lc.reserve_class_slot(a, "谷川②", NC, 8)[0])  # 目標0件の区分は取れない

    def test_slot_released_when_connection_closes(self):
        self.add_account("谷川②", post_target=1, post_target_collectibles=0)
        a = get_sql_server_connection()
        b, = self.conns(1)
        self.assertIsNotNone(lc.reserve_class_slot(a, "谷川②", NC, 8)[0])
        self.assertIsNone(lc.reserve_class_slot(b, "谷川②", NC, 8)[0])
        # プロセスが落ちた相当（セッションが切れる）。pyodbc は close() した接続を使い回すためセッションが残るので、
        # KILL でセッションを終わらせる（本番の worker はプロセスの終了で接続が切れる）
        spid = a.cursor().execute("SELECT @@SPID").fetchone()[0]
        a.commit()
        with contextlib.closing(get_sql_server_connection()) as k:
            k.autocommit = True  # KILL はトランザクションの外で実行する
            k.cursor().execute(f"KILL {int(spid)}")
        a.close()
        self.assertIsNotNone(lc.reserve_class_slot(b, "谷川②", NC, 8)[0])

    def test_concurrent_workers_never_exceed_target(self):
        # 8台が同時に、枠を取る → 出品（成功を記録）→ 枠を返す、を繰り返しても目標ちょうどで止まる
        target = 13
        self.add_account("谷川②", post_target=target, post_target_collectibles=target)
        errors = []
        barrier = threading.Barrier(8)

        def worker(i):
            conn = get_sql_server_connection()
            try:
                barrier.wait()
                idle = 0
                while idle < 30:
                    progressed = False
                    for cls in (NC, C):
                        slot, _ = lc.reserve_class_slot(conn, "谷川②", cls, 8)
                        if slot is None:
                            continue
                        progressed = True
                        self.add_class_listing("谷川②", cls, sku=f"w{i}-{cls}-{next(self.seq)}")
                        lc.release_class_slot(conn, slot)
                    idle = 0 if progressed else idle + 1
            except Exception as e:  # pragma: no cover - 失敗時の表示用
                errors.append(e)
            finally:
                conn.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(120)
        self.assertEqual(errors, [])
        self.assertEqual(self.posted("谷川②"), (target, target))


if __name__ == "__main__":
    unittest.main()
