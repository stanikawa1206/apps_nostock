# -*- coding: utf-8 -*-
"""
ops_alerts（重大障害の即時通知）と EbayWatchdog の監視の生存確認のテスト（2026-11-19）。DB・メールはモック。
実行: python -m unittest apps.tests.test_ops_alerts
"""
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

from apps.inventory import ops_alerts as oa

NOW = datetime(2026, 11, 20, 10, 0, 0)


def issue(age_min, cause="売り切れ: offerなし", notified=None, kind="出品終了失敗", sku="s1"):
    return {"issue_id": 1, "issue_kind": kind, "vendor_name": "メルカリshops", "vendor_item_id": sku,
            "account": "谷川④", "listing_id": "137892055692", "cause": cause,
            "first_at": NOW - timedelta(minutes=age_min), "last_at": NOW, "occurrences": 3,
            "notified_at": notified, "age_min": age_min}


class EndIssueAlertTest(unittest.TestCase):
    def test_two_hours_or_more_is_notified(self):
        self.assertEqual(len(oa.end_issue_alert([issue(120)], NOW)), 1)

    def test_younger_than_two_hours_waits(self):
        self.assertEqual(oa.end_issue_alert([issue(119)], NOW), [])

    def test_structural_is_notified_at_once(self):
        self.assertEqual(len(oa.end_issue_alert([issue(5, cause="sku_mismatch(eBay='X')")], NOW)), 1)

    def test_renotify_interval(self):
        self.assertEqual(oa.end_issue_alert([issue(600, notified=NOW - timedelta(hours=23))], NOW), [])
        self.assertEqual(len(oa.end_issue_alert([issue(600, notified=NOW - timedelta(hours=24))], NOW)), 1)

    def test_72_times_case_is_detected(self):
        """137892055692: 仕入先は売り切れのまま、終了処理が何度も失敗 → 2時間で通知対象"""
        rows = [issue(130, cause="売り切れ: offerなし・出品番号での終了 end_unconfirmed offer=404"),
                issue(130, kind="仕入不可出品中", cause="仕入先=売り切れ")]
        self.assertEqual(len(oa.end_issue_alert(rows, NOW)), 2)


class VendorOutageTest(unittest.TestCase):
    """B: 仕入先ごと（在庫チェックと検索を分ける・前半/後半の15分とも続く時だけ）、台ごと、検索の0件、検索APIの停止（2026-11-27）"""
    OK = [500, 0, 500, 0]

    def halves(self, check=None, job=None, zero=None, host=None):
        def fill(d):
            out = {v: list(self.OK) for v in oa.VENDORS}
            out.update(d or {})
            return out
        return {"check": fill(check), "job": fill(job), "zero": {v: [100, 10, 100, 10] for v in oa.VENDORS} | (zero or {}),
                "host": host or {}}

    def dead(self, **kw):
        base = {v: (1000, 0) for v in oa.VENDORS}
        if "Y" in kw:
            kw["ＰａｙＰａｙフリマ"] = kw.pop("Y")
        base.update(kw)
        return base

    def test_sustained_check_failure(self):
        r = oa.vendor_outages(self.halves(check={"メルカリ": [100, 12, 100, 15]}), self.dead(), None)
        self.assertEqual(list(r), ["vendor:メルカリ"])
        self.assertIn("個別の在庫チェックの失敗 27/200件", r["vendor:メルカリ"])

    def test_burst_in_one_half_is_not_outage(self):
        # 20件以上・10%以上でも、失敗が後半（または前半）だけに集中した場合は通知しない
        self.assertEqual(oa.vendor_outages(self.halves(check={"メルカリ": [100, 0, 100, 30]}), self.dead(), None), {})
        self.assertEqual(oa.vendor_outages(self.halves(check={"メルカリ": [100, 30, 100, 1]}), self.dead(), None), {})  # 回復済み

    def test_min_count_is_20(self):
        self.assertIn("vendor:ラクマ", oa.vendor_outages(self.halves(check={"ラクマ": [50, 10, 50, 10]}), self.dead(), None))
        self.assertEqual(oa.vendor_outages(self.halves(check={"ラクマ": [50, 10, 50, 9]}), self.dead(), None), {})

    def test_low_rate_is_not_outage(self):
        self.assertEqual(oa.vendor_outages(self.halves(check={"ラクマ": [2000, 30, 2000, 30]}), self.dead(), None), {})

    def test_check_and_search_are_not_mixed(self):
        # 検索の失敗が多数の在庫チェックの成功に埋もれない（以前は仕入先ごとに合計していた）
        r = oa.vendor_outages(self.halves(check={"ラクマ": [2800, 0, 2800, 0]}, job={"ラクマ": [50, 50, 50, 50]}),
                              self.dead(), None)
        self.assertIn("検索ジョブの失敗 100/100件", r["vendor:ラクマ"])

    def test_one_key_per_vendor(self):
        r = oa.vendor_outages(self.halves(check={"メルカリ": [100, 50, 100, 50]}, job={"メルカリ": [40, 40, 40, 40]},
                                          zero={"メルカリ": [20, 20, 20, 20]}), self.dead(), None)
        self.assertEqual(list(r), ["vendor:メルカリ"])
        for part in ("個別の在庫チェック", "検索ジョブ", "検索の結果が0件"):
            self.assertIn(part, r["vendor:メルカリ"])

    def test_search_zero_results(self):
        self.assertIn("vendor:ＰａｙＰａｙフリマ",
                      oa.vendor_outages(self.halves(zero={"ＰａｙＰａｙフリマ": [10, 10, 10, 10]}), self.dead(), None))
        # 実績の最大 61% は通知しない、件数が少ない（30分に20回未満）も通知しない、片方の半分だけも通知しない
        self.assertEqual(oa.vendor_outages(self.halves(zero={"ＰａｙＰａｙフリマ": [100, 61, 100, 61]}), self.dead(), None), {})
        self.assertEqual(oa.vendor_outages(self.halves(zero={"メルカリ": [9, 9, 10, 10]}), self.dead(), None), {})
        self.assertEqual(oa.vendor_outages(self.halves(zero={"メルカリ": [2, 2, 30, 30]}), self.dead(), None), {})

    def test_host_rule(self):
        host = {"x162-43-42-135": {"メルカリ": [10, 6, 10, 6]}, "x162-43-15-160": {"メルカリ": [10, 0, 10, 0]}}
        r = oa.vendor_outages(self.halves(host=host), self.dead(), None)
        self.assertEqual(list(r), ["host:x162-43-42-135"])
        # 実績の最大（8件中1件）や、失敗が前半だけの台は通知しない
        host = {"mouse": {"メルカリshops": [8, 1, 8, 1]}, "x": {"ラクマ": [20, 20, 20, 0]}}
        self.assertEqual(oa.vendor_outages(self.halves(host=host), self.dead(), None), {})

    def test_host_rule_excludes_vendor_outage(self):
        # 仕入先全体の障害では、各台の同じ失敗を台の障害として重ねない（1通に何行も並ばない）
        host = {h: {"メルカリ": [50, 50, 50, 50]} for h in ("a", "b", "c")}
        r = oa.vendor_outages(self.halves(check={"メルカリ": [150, 150, 150, 150]}, host=host), self.dead(), None)
        self.assertEqual(list(r), ["vendor:メルカリ"])

    def test_low_volume_complete_failure(self):
        r = oa.vendor_outages(self.halves(), self.dead(Y=(6, 6)), None)
        self.assertIn("vendor:ＰａｙＰａｙフリマ", r)

    def test_low_volume_partial_failure_is_not_outage(self):
        self.assertEqual(oa.vendor_outages(self.halves(), self.dead(Y=(6, 5)), None), {})

    def test_no_attempts_is_not_outage(self):
        """試行0件（処理していない）は成功扱いにも障害扱いにもしない"""
        zero = {v: [0, 0, 0, 0] for v in oa.VENDORS}
        self.assertEqual(oa.vendor_outages({"check": zero, "job": zero, "zero": zero, "host": {}},
                                           self.dead(ラクマ=(0, 0)), None), {})

    def test_rakuma_pause(self):
        """自動停止が30分以上続いたら通知（10分の停止→自動解除は通知しない）"""
        self.assertIn("rakuma_pause", oa.vendor_outages(self.halves(), self.dead(), 30))
        self.assertNotIn("rakuma_pause", oa.vendor_outages(self.halves(), self.dead(), 29))
        self.assertNotIn("rakuma_pause", oa.vendor_outages(self.halves(), self.dead(), 10))

    def test_search_api_block(self):
        self.assertIn("search_api_block", oa.vendor_outages(self.halves(), self.dead(), None, 30))
        self.assertEqual(oa.vendor_outages(self.halves(), self.dead(), None, 29), {})
        self.assertEqual(oa.vendor_outages(self.halves(), self.dead(), None, None), {})

    def test_half_sql(self):
        self.assertIn("NOT LIKE N'RakumaPausedError%'", oa.SQL_CHECK_HALVES)
        self.assertIn(f"N'{oa.ops_stats.CHECK_RETRY_WAIT_KIND}'", oa.SQL_CHECK_HALVES)
        self.assertIn("job_kind = 'fetch_active_ebay' AND status = 'done' AND fetched_items = 0", oa.SQL_JOB_HALVES)
        self.assertIn("NOT LIKE N'%RAKUMA_PAUSED kind=%'", oa.SQL_JOB_HALVES)

    def test_paused_retry_wait_is_not_counted(self):
        """RAKUMA_PAUSED の再試行待ちは数えない。実際の HTTP エラー（500 等）の再試行待ち・error は従来どおり数える"""
        sql = oa.SQL_JOB_STATS
        self.assertIn("NOT LIKE N'%RAKUMA_PAUSED kind=%'", sql)
        self.assertIn("status IN ('done', 'error')", sql)
        # 11/25 08:26 の形の文言に一致すること（scrape_worker の retry_wait の error_message）
        self.assertIn(oa.RAKUMA_PAUSED_MARK, "[RETRY_WAIT 1/6] RAKUMA_PAUSED kind=OUTAGE paused_until=2026-11-25 08:36:07 url=x")
        self.assertNotIn(oa.RAKUMA_PAUSED_MARK, "[RETRY_WAIT 1/6] RAKUMA_HTTP status=500 retry_after=None url=x")


class EbayOutageTest(unittest.TestCase):
    def test_rate_and_count(self):
        self.assertIn("ebay", oa.ebay_outages([("貴文", 300, 20)]))

    def test_policy_errors_are_not_counted(self):
        """個別商品のエラーは transient に入らない（SQL 側）→ transient 0 なら障害にしない"""
        self.assertEqual(oa.ebay_outages([("貴文", 300, 0)]), {})

    def test_low_rate_is_not_outage(self):
        self.assertEqual(oa.ebay_outages([("貴文", 1000, 20)]), {})

    def test_multi_account(self):
        self.assertIn("ebay", oa.ebay_outages([("貴文", 1000, 15), ("川島", 1000, 15)]))
        self.assertEqual(oa.ebay_outages([("貴文", 1000, 30)]), {})


class OutageStateTest(unittest.TestCase):
    """B・C の通知: 発生時1通、続く間は再通知の間隔ごと、解消したら回復1通で状態を消す"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        oa.STATE_PATH = Path(self.tmp.name) / "state.json"

    def tearDown(self):
        oa.STATE_PATH = None
        self.tmp.cleanup()

    def run_at(self, now, outages):
        conn = mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
        with mock.patch.object(oa, "vendor_outages", return_value=dict(outages)), \
             mock.patch.object(oa, "_vendor_counts", return_value={}), \
             mock.patch.object(oa, "_half_counts", return_value={}), \
             mock.patch.object(oa, "_search_block_minutes", return_value=None), \
             mock.patch.object(oa, "_rakuma_pause_minutes", return_value=None), \
             mock.patch.object(oa, "ebay_outages", return_value={}), \
             mock.patch.object(oa, "send_mail", return_value=True) as sm:
            oa.check_outages(conn, now)
        return [c.args[0] for c in sm.call_args_list]

    def test_lifecycle(self):
        o = {"vendor:ラクマ": "ラクマ: 失敗"}
        self.assertEqual(len(self.run_at(NOW, o)), 1)                              # 発生
        self.assertEqual(self.run_at(NOW + timedelta(minutes=10), o), [])         # 継続中は送らない
        self.assertEqual(len(self.run_at(NOW + timedelta(hours=6), o)), 1)        # 再通知
        sent = self.run_at(NOW + timedelta(hours=7), {})                          # 回復
        self.assertEqual(len(sent), 1)
        self.assertTrue(sent[0].startswith("【回復】"))
        self.assertEqual(json.loads(oa.STATE_PATH.read_text(encoding="utf-8"))["outages"], {})
        self.assertEqual(len(self.run_at(NOW + timedelta(hours=8), o)), 1)        # 新たな障害は再び通知

    def test_unnotified_outage_recovers_silently(self):
        """通知を送れていない（送信失敗）まま解消した障害は回復メールを送らず状態だけ消す"""
        o = {"vendor:ラクマ": "ラクマ: 失敗"}
        with mock.patch.object(oa, "send_mail", return_value=False):
            conn = mock.MagicMock()
            conn.cursor.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
            with mock.patch.object(oa, "vendor_outages", return_value=dict(o)), mock.patch.object(oa, "_vendor_counts", return_value={}), mock.patch.object(oa, "_half_counts", return_value={}), mock.patch.object(oa, "_search_block_minutes", return_value=None), mock.patch.object(oa, "_rakuma_pause_minutes", return_value=None), mock.patch.object(oa, "ebay_outages", return_value={}):
                oa.check_outages(conn, NOW)
        self.assertIn("vendor:ラクマ", json.loads(oa.STATE_PATH.read_text(encoding="utf-8"))["outages"])
        self.assertEqual(self.run_at(NOW + timedelta(minutes=10), {}), [])
        self.assertEqual(json.loads(oa.STATE_PATH.read_text(encoding="utf-8"))["outages"], {})

    def test_notified_and_unnotified_recover_together(self):
        """回復メールには通知済みの障害だけを載せる"""
        self.run_at(NOW, {"vendor:ラクマ": "a"})
        st = json.loads(oa.STATE_PATH.read_text(encoding="utf-8"))
        st["outages"]["ebay"] = {"first": "x", "detail": "b", "notified": None}
        oa.STATE_PATH.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
        sent = self.run_at(NOW + timedelta(minutes=10), {})
        self.assertEqual(sent, ["【回復】在庫管理の障害が解消しました（1件）"])
        self.assertEqual(json.loads(oa.STATE_PATH.read_text(encoding="utf-8"))["outages"], {})

    def test_many_outages_one_mail(self):
        o = {"vendor:ラクマ": "a", "vendor:メルカリ": "b", "ebay": "c"}
        self.assertEqual(len(self.run_at(NOW, o)), 1)


class MonitorAliveTest(unittest.TestCase):
    """EbayWatchdog の inventory_monitor 生存確認"""

    def setUp(self):
        from apps.inventory import ebay_watchdog as w
        self.w = w
        self.tmp = tempfile.TemporaryDirectory()
        d = Path(self.tmp.name)
        self.patches = [mock.patch.object(w, "MONITOR_HEARTBEAT", d / "hb.json"),
                        mock.patch.object(w, "MONITOR_ALERT_STATE", d / "alert.json")]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def beat(self, minutes_ago):
        at = datetime.now() - timedelta(minutes=minutes_ago)
        self.w.MONITOR_HEARTBEAT.write_text(json.dumps({"at": at.isoformat(timespec="seconds")}), encoding="utf-8")

    def test_stale_notifies_once_and_recovers(self):
        with mock.patch("apps.common.utils.send_mail", return_value=True) as sm:
            self.beat(40)
            self.w.check_inventory_monitor_alive()
            self.w.check_inventory_monitor_alive()
            self.assertEqual(sm.call_count, 1)
            self.beat(1)
            self.w.check_inventory_monitor_alive()
            self.assertFalse(self.w.MONITOR_ALERT_STATE.exists())
            self.beat(40)
            self.w.check_inventory_monitor_alive()
            self.assertEqual(sm.call_count, 2)

    def test_fresh_does_not_notify(self):
        with mock.patch("apps.common.utils.send_mail", return_value=True) as sm:
            self.beat(5)
            self.w.check_inventory_monitor_alive()
            sm.assert_not_called()


if __name__ == "__main__":
    unittest.main()
