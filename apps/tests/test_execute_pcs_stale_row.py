# -*- coding: utf-8 -*-
"""
出品の PC 割り当て（publish_ebay.fetch_next_account_and_lock）で、この PC 自身の行に残った前回のアカウントを
並行数に数えないことのテスト（2026-11-23。11/21 39.209 が Renderer 異常→再起動後に「割当なし」で途中離脱）。

専用の一時DB（nostock_test_blocked。test_account_listing_block と同じ）を使い、最後に削除する（本番DBには触れない）。

実行: python -m unittest apps.tests.test_execute_pcs_stale_row -v
"""

import unittest

from apps.publish import publish_ebay as pe
from apps.tests.test_account_listing_block import DbTestBase

MAX = pe.MAX_PARALLEL_PC


class StaleOwnRowTest(DbTestBase):
    PC = "x162-43-39-209"

    def accounts(self):
        r = self.sql("SELECT execute_pc, account FROM mst.execute_pcs ORDER BY execute_pc").fetchall()
        self.conn.commit()
        return {pc: acc for pc, acc in r}

    def test_own_stale_row_not_counted(self):
        # 11/21 の再現: 他の7台が谷川②、自分の行にも異常終了前の谷川②が残っている（8台分に見える）
        self.add_account("谷川②")
        for i in range(MAX - 1):
            self.add_pc(f"other{i}", "谷川②")
        self.add_pc(self.PC, "谷川②")
        acct = pe.fetch_next_account_and_lock(self.conn, self.PC)
        self.assertIsNotNone(acct)
        self.assertEqual(acct.account, "谷川②")
        self.assertEqual(self.accounts()[self.PC], "谷川②")

    def test_other_pcs_at_max_still_refused(self):
        # 他の PC が上限台数に達していれば、従来どおり割り当てない
        self.add_account("谷川②")
        for i in range(MAX):
            self.add_pc(f"other{i}", "谷川②")
        self.add_pc(self.PC, "谷川②")
        self.assertIsNone(pe.fetch_next_account_and_lock(self.conn, self.PC))
        self.assertIsNone(self.accounts()[self.PC])  # 自分の行は従来どおり NULL に戻る

    def test_normal_case_unchanged(self):
        # 自分の行が NULL（通常）: 上限未満なら割り当て、上限なら割り当てない
        self.add_account("A")
        for i in range(MAX - 1):
            self.add_pc(f"other{i}", "A")
        self.add_pc(self.PC)
        self.assertEqual(pe.fetch_next_account_and_lock(self.conn, self.PC).account, "A")
        self.sql("UPDATE mst.execute_pcs SET account = NULL WHERE execute_pc = ?", self.PC)
        self.sql("INSERT INTO mst.execute_pcs (execute_pc, account, is_active) VALUES ('otherX', 'A', 1)")
        self.conn.commit()
        self.assertIsNone(pe.fetch_next_account_and_lock(self.conn, self.PC))

    def test_spread_order_ignores_own_row(self):
        # 分散の並び（稼働の少ない順）にも自分の残り行を数えない: A は他1台、B は他1台＋自分の残り → 名前順で A
        self.add_account("A")
        self.add_account("B")
        self.add_pc("other1", "A")
        self.add_pc("other2", "B")
        self.add_pc(self.PC, "B")
        self.assertEqual(pe.fetch_next_account_and_lock(self.conn, self.PC).account, "A")
        # 他の PC の割り当てには影響しない
        acc = self.accounts()
        self.assertEqual((acc["other1"], acc["other2"]), ("A", "B"))

    def test_inactive_pc_not_assigned(self):
        self.add_account("A")
        self.sql("INSERT INTO mst.execute_pcs (execute_pc, account, is_active) VALUES (?, 'A', 0)", self.PC)
        self.conn.commit()
        self.assertIsNone(pe.fetch_next_account_and_lock(self.conn, self.PC))


if __name__ == "__main__":
    unittest.main()
