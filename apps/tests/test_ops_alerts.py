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
    def w(self, **kw):
        # キーワード引数の名前は NFKC で正規化される（ＰａｙＰａｙ→PayPay）ので、仕入先名は Y= で渡す
        base = {v: (1000, 0) for v in oa.VENDORS}
        if "Y" in kw:
            kw["ＰａｙＰａｙフリマ"] = kw.pop("Y")
        base.update(kw)
        return base

    def test_rate_and_count(self):
        r = oa.vendor_outages(self.w(ラクマ=(500, 60)), self.w(), None)
        self.assertIn("vendor:ラクマ", r)

    def test_rate_without_count_is_not_outage(self):
        self.assertEqual(oa.vendor_outages(self.w(ラクマ=(100, 49)), self.w(), None), {})

    def test_count_without_rate_is_not_outage(self):
        self.assertEqual(oa.vendor_outages(self.w(ラクマ=(5000, 60)), self.w(), None), {})

    def test_low_volume_complete_failure(self):
        r = oa.vendor_outages(self.w(), self.w(Y=(6, 6)), None)
        self.assertIn("vendor:ＰａｙＰａｙフリマ", r)

    def test_low_volume_partial_failure_is_not_outage(self):
        self.assertEqual(oa.vendor_outages(self.w(), self.w(Y=(6, 5)), None), {})

    def test_no_attempts_is_not_outage(self):
        """試行0件（処理していない）は成功扱いにも障害扱いにもしない"""
        self.assertEqual(oa.vendor_outages(self.w(ラクマ=(0, 0)), self.w(ラクマ=(0, 0)), None), {})

    def test_rakuma_pause(self):
        self.assertIn("rakuma_pause", oa.vendor_outages(self.w(), self.w(), 60))
        self.assertNotIn("rakuma_pause", oa.vendor_outages(self.w(), self.w(), 59))


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
