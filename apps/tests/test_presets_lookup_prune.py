# -*- coding: utf-8 -*-
"""
daily_check.prune_unlisted_lookups（新規出品を止めた仕入先の lookup の漸減）のテスト。
モックのみ。DB には接続しない（SQL の条件そのものは本番DBの読み取りと一時テーブルで確認する）。

実行: python -m unittest apps.tests.test_presets_lookup_prune -v
"""

import unittest
from types import SimpleNamespace as NS
from unittest import mock

from apps.inventory import daily_check as dc


class FakeCursor:
    """SQL の中身で返す行を決める。DELETE の引数を記録する。"""

    def __init__(self, preset_group="谷川④", other_groups=(), scope=(), deleted=None):
        self.preset_group = preset_group
        self.other_groups = list(other_groups)
        self.scope = list(scope)
        self.deleted = deleted or {}
        self.deletes = []
        self.executed = []
        self._rows = []

    def execute(self, sql, *params):
        self.executed.append((sql, params))
        if "FROM mst.ebay_accounts" in sql:
            self._rows = [NS(preset_group=self.preset_group)] if self.preset_group else []
        elif "FROM mst.presets_price_ranges" in sql:
            self._rows = [NS(preset_group=g) for g in self.other_groups]
        elif "FROM mst.vendors AS vd" in sql:
            self._rows = self.scope
        elif sql.lstrip().startswith("DELETE p"):
            self.deletes.append(params)
            self._rows = [NS(preset=x) for x in self.deleted.get(params[1], [])]
        else:
            raise AssertionError(f"想定外のSQL: {sql}")
        return self

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


def scope_row(code, name, lookup, excluded=1, unknown=0):
    return NS(vendor_code=code, vendor_name=name, lookup_cnt=lookup,
              is_excluded=excluded, unknown_listing_cnt=unknown)


SCOPE = [scope_row("M", "メルカリ", 237), scope_row("MS", "メルカリshops", 237)]


class PruneTest(unittest.TestCase):

    def test_deletes_only_lookups_without_listings(self):
        cur = FakeCursor(scope=SCOPE, deleted={
            "メルカリ": [f"m{i}" for i in range(150)],
            "メルカリshops": [f"s{i}" for i in range(174)],
        })
        res = dc.prune_unlisted_lookups(cur)

        # 対象グループ・仕入先ごとに DELETE（出品の有無は DELETE 自身の NOT EXISTS で判定）
        self.assertEqual(cur.deletes, [("腕時計", "メルカリ"), ("腕時計", "メルカリshops")])
        self.assertEqual([(r["vendor_code"], r["deleted"], r["kept"], r["skip"]) for r in res],
                         [("M", 150, 87, None), ("MS", 174, 63, None)])

    def test_scope_query_uses_account_group_and_vendor_codes(self):
        cur = FakeCursor(scope=[])
        dc.prune_unlisted_lookups(cur)
        sql, params = [e for e in cur.executed if "FROM mst.vendors AS vd" in e[0]][0]
        self.assertEqual(params, ("腕時計", "谷川④", "M", "MS"))
        self.assertIn("vd.vendor_code IN (?, ?)", sql)

    def test_delete_sql_checks_unended_listings_of_all_accounts(self):
        sql = dc._LOOKUP_PRUNE_DELETE_SQL
        self.assertIn("l.is_deleted = 0", sql)
        self.assertIn("v.preset = p.preset", sql)
        self.assertNotIn("l.account", sql)
        self.assertIn("p.category_group = ?", sql)
        self.assertIn("p.vendor_name = ?", sql)

    def test_not_excluded_vendor_is_kept(self):
        cur = FakeCursor(scope=[scope_row("M", "メルカリ", 237, excluded=0),
                                scope_row("MS", "メルカリshops", 237)],
                         deleted={"メルカリshops": ["s1"]})
        res = dc.prune_unlisted_lookups(cur)
        self.assertEqual(cur.deletes, [("腕時計", "メルカリshops")])
        self.assertEqual((res[0]["deleted"], res[0]["kept"]), (0, 237))
        self.assertIn("新規出品除外に M が無い", res[0]["skip"])

    def test_group_used_by_other_preset_group_keeps_all(self):
        cur = FakeCursor(scope=SCOPE, other_groups=["BUZZ"])
        res = dc.prune_unlisted_lookups(cur)
        self.assertEqual(cur.deletes, [])
        self.assertEqual([r["kept"] for r in res], [237, 237])
        self.assertTrue(all("BUZZ" in r["skip"] for r in res))

    def test_unknown_listing_keeps_that_vendor(self):
        cur = FakeCursor(scope=[scope_row("M", "メルカリ", 237, unknown=1),
                                scope_row("MS", "メルカリshops", 237)])
        res = dc.prune_unlisted_lookups(cur)
        self.assertEqual(cur.deletes, [("腕時計", "メルカリshops")])
        self.assertIn("preset が分からない未終了の出品 1 件", res[0]["skip"])

    def test_missing_account_keeps_all(self):
        cur = FakeCursor(preset_group=None, scope=SCOPE)
        res = dc.prune_unlisted_lookups(cur)
        self.assertEqual(cur.deletes, [])
        self.assertTrue(all("mst.ebay_accounts に無い" in r["skip"] for r in res))

    def test_no_lookup_rows_skips_delete(self):
        cur = FakeCursor(scope=[scope_row("M", "メルカリ", 0)])
        res = dc.prune_unlisted_lookups(cur)
        self.assertEqual(cur.deletes, [])
        self.assertEqual((res[0]["deleted"], res[0]["kept"]), (0, 0))

    def test_format(self):
        text = dc.format_lookup_prune([
            {"vendor_code": "M", "deleted": 150, "kept": 87, "skip": None},
            {"vendor_code": "MS", "deleted": 0, "kept": 237, "skip": "谷川④ の新規出品除外に MS が無い"},
        ])
        self.assertEqual(
            text,
            "presets_lookup 漸減（谷川④・腕時計）: M 削除 150 / 維持 87、"
            "MS 削除 0 / 維持 237（削除しない: 谷川④ の新規出品除外に MS が無い）",
        )


class RefreshCallsPruneBeforeCommitTest(unittest.TestCase):

    def test_prune_runs_after_insert_and_before_commit(self):
        order = []

        class Cur:
            fast_executemany = False

            def execute(self, sql, *a):
                if "TRUNCATE" in sql:
                    order.append("truncate")
                self._count = "COUNT(*)" in sql
                return self

            def fetchall(self):
                return []

            def fetchone(self):
                return (0,)

            def executemany(self, sql, rows):
                order.append("insert")

        class Conn:
            def cursor(self):
                return Cur()

            def commit(self):
                order.append("commit")

        result = [{"vendor_code": "M", "deleted": 1, "kept": 2, "skip": None}]
        with mock.patch.object(dc, "prune_unlisted_lookups",
                               side_effect=lambda cur: order.append("prune") or result):
            got = dc.refresh_presets_lookup(Conn())
        self.assertEqual(order, ["truncate", "insert", "prune", "commit"])
        self.assertIs(got, result)


if __name__ == "__main__":
    unittest.main()
