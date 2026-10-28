# -*- coding: utf-8 -*-
"""
post_target 自動計算の対象外アカウント（MANUAL_POST_TARGET_ACCOUNTS）のテスト（モックのみ。DB・eBay には接続しない）。

実行: python -m unittest apps.tests.test_post_target_manual -v
"""

import contextlib
import io
import unittest
from datetime import datetime
from unittest import mock

from apps.publish import publish_manager as pm


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, *params):
        if sql == pm.SQL_SELECT_POST_TARGET_ACCOUNTS:
            self.result = self.conn.rows
        else:
            self.conn.updates.append((sql, params))

    def fetchall(self):
        return self.result


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.updates = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def close(self):
        pass


class UpdatePostTargetsTest(unittest.TestCase):
    def run_update(self, rows):
        conn = FakeConn(rows)
        out = io.StringIO()
        with mock.patch.object(pm, "get_sql_server_connection", return_value=conn), contextlib.redirect_stdout(out):
            pm.update_post_targets()
        return {params[1]: params[0] for sql, params in conn.updates if sql == pm.SQL_UPDATE_POST_TARGET}, out.getvalue()

    def test_manual_account_keeps_post_target(self):
        rows = [
            ("BUZZ", 1289, 321),
            ("貴文③", None, 10),     # 現在の状態（listing_left 未設定）
        ]
        updated, out = self.run_update(rows)
        self.assertNotIn("貴文③", updated)
        self.assertIn("BUZZ", updated)
        self.assertIn("貴文③", out)
        self.assertIn("手動設定のため自動計算しない", out)

    def test_manual_account_is_not_recalculated_even_with_listing_left(self):
        # 月次リセットで listing_left=10,000 になっても、手入力の post_target を上書きしない
        updated, _ = self.run_update([("貴文③", 10000, 10), ("谷川②", 4112, 500)])
        self.assertEqual(list(updated), ["谷川②"])

    def test_other_accounts_unchanged_behavior(self):
        # 他アカウントは従来どおり calculate_post_target の結果で更新される
        rows = [("BUZZ", 1289, 0), ("川島", 1311, 0)]
        updated, _ = self.run_update(rows)
        us_date = datetime.now(pm.US_EASTERN_TZ).date()
        for account, left, _ in rows:
            self.assertEqual(updated[account], pm.calculate_post_target(left, us_date)["calculated_post_target"])

    def test_manual_list(self):
        self.assertEqual(pm.MANUAL_POST_TARGET_ACCOUNTS, ("貴文③",))


if __name__ == "__main__":
    unittest.main()
