# -*- coding: utf-8 -*-
"""
mst.presets_categories.is_active（2026-11-16）: lookup 生成は is_active=1 の行だけを使う
（メルカリのペンのブランド別カテゴリーを is_active=0 で「マスターとして持つが preset を作らない」ため）。
DB には接続しない。
"""
import unittest
from unittest import mock

from apps.inventory import daily_check as dc


class LoadCategoriesTest(unittest.TestCase):
    def test_categories_query_uses_only_active_rows(self):
        cur = mock.MagicMock()
        seen = []
        cur.execute.side_effect = lambda sql, *a: seen.append(sql) or cur
        cur.fetchall.return_value = []
        dc.load_lookup_sources(cur)
        cat_sql = [s for s in seen if "FROM mst.presets_categories" in s]
        self.assertEqual(len(cat_sql), 1)
        self.assertRegex(cat_sql[0], r"WHERE\s+is_active\s*=\s*1")


if __name__ == "__main__":
    unittest.main()
