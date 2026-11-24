# -*- coding: utf-8 -*-
"""
既存出品の「価格範囲外」判定をアカウント自身の価格行で行うテスト（2026-11-24 keep_* 廃止）。
モックのみ。DB には接続しない。

実行: python -m unittest apps.tests.test_listing_price_range -v
"""

import unittest
from decimal import Decimal as D
from unittest import mock

from apps.inventory import listing_price_range as lpr
from apps.inventory import scrape_worker as sw


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, sql, *params):
        self.executed.append((sql, params))
        return self

    def fetchone(self):
        return self.row

    def close(self):
        pass


class FakeConn:
    def __init__(self, row):
        self.cur = FakeCursor(row)

    def cursor(self):
        return self.cur


class ListingPriceRangeTest(unittest.TestCase):

    def test_own_row_is_used(self):
        conn = FakeConn(("貴文", D("600.00"), D("1000.00")))
        self.assertEqual(lpr.listing_price_range(conn, "ラクマ", "x1", 200.0, 1000.0), (600.0, 1000.0, "貴文"))
        sql, params = conn.cur.executed[0]
        self.assertEqual(params, ("x1", "ラクマ"))
        # アカウントの preset_group × 商品の category_group × 仕入先で引く。delete_flag は見ない
        self.assertIn("r.preset_group = a.preset_group", sql)
        self.assertIn("r.vendor_code = vd.vendor_code", sql)
        self.assertNotIn("delete_flag", sql)
        self.assertNotIn("keep_", sql)

    def test_no_price_row_falls_back_to_collection_range(self):
        conn = FakeConn(("川島", None, None))
        low, high, basis = lpr.listing_price_range(conn, "メルカリ", "m1", 100.0, 1000.0)
        self.assertEqual((low, high), (100.0, 1000.0))
        self.assertIn("価格行なし", basis)

    def test_no_listing_falls_back(self):
        conn = FakeConn(None)
        self.assertEqual(lpr.listing_price_range(conn, "メルカリ", "m1", 1.0, 2.0), (1.0, 2.0, "出品なし"))


class PriceChangeUsesOwnRangeTest(unittest.TestCase):
    """他アカウントの価格帯で既存出品が維持されないこと（BUZZ $200-600 と 貴文 $600-1000 の例）。"""

    def run_change(self, own_range, new_price_jpy, mode="DDP"):
        calls = {}
        with mock.patch.object(sw, "listing_price_range", return_value=own_range + ("貴文",)), \
             mock.patch.object(sw, "handle_listing_delete", side_effect=lambda *a, **k: calls.setdefault("delete", a)), \
             mock.patch.object(sw, "handle_listing_price_update", side_effect=lambda *a, **k: calls.setdefault("update", a)), \
             mock.patch.object(sw, "conn_cursor_row", create=True):
            conn = mock.MagicMock()
            # ① 更新前の状態取得（古い更新ではない）
            conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (None, None, None, None)
            conn.cursor.return_value.fetchone.return_value = (None, None, None, None)
            sw.handle_price_change_side_effects(
                conn, "x1", "ラクマ", 50000, new_price_jpy,
                mode=mode, low_usd_target=200.0, high_usd_target=1000.0,  # 収集範囲（組の MIN〜MAX）
            )
        return calls

    def test_below_own_low_is_ended_even_if_inside_collection_range(self):
        # 約 $570 → 貴文 $600-1000 の範囲外。収集範囲 $200-1000 の中でも終了する
        calls = self.run_change((600.0, 1000.0), 47600)
        self.assertIn("delete", calls)
        self.assertEqual(calls["delete"][3], "価格範囲外")
        self.assertNotIn("update", calls)

    def test_inside_own_range_updates_price(self):
        calls = self.run_change((600.0, 1000.0), 60000)
        self.assertIn("update", calls)
        self.assertNotIn("delete", calls)


if __name__ == "__main__":
    unittest.main()
