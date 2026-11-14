# -*- coding: utf-8 -*-
"""
daily_check の lookup 生成（mst.presets_price_ranges からの収集価格帯・delete_flag・lookup の漸減）のテスト。
モックのみ。DB には接続しない（本番データでの新旧比較は移行時の検証スクリプトで行う）。

実行: python -m unittest apps.tests.test_presets_lookup_prune -v
"""

import unittest
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest import mock

from apps.inventory import daily_check as dc


def pr(pg, cg, vc, low, high, delete_flag=0, keep_low=None, keep_high=None):
    return NS(preset_group=pg, category_group=cg, vendor_code=vc,
              low_usd_target=D(low), high_usd_target=D(high), delete_flag=delete_flag,
              keep_low_usd_target=None if keep_low is None else D(keep_low),
              keep_high_usd_target=None if keep_high is None else D(keep_high))


def ls(account, vc, preset, cnt=1):
    return NS(account=account, vendor_code=vc, preset=preset, cnt=cnt)


ACCOUNTS = [NS(account=a, preset_group=a) for a in ("BUZZ②", "貴文", "谷川③", "谷川④")]

# 財布 M: 3アカウントが新規出品、腕時計 M: 谷川④だけ・新規出品停止
RANGES = [
    pr("BUZZ②", "財布", "M", 100, 200),
    pr("谷川③", "財布", "M", 300, 600),
    pr("貴文", "財布", "M", 600, 1000),
    pr("BUZZ②", "財布", "R", 100, 200, delete_flag=1),
    pr("谷川③", "財布", "R", 300, 600),
    pr("貴文", "財布", "R", 600, 1000),
    pr("谷川④", "腕時計", "M", 100, 300, delete_flag=1),
    pr("谷川④", "腕時計", "R", 100, 300),
]

PAIRS = {
    "グッチ財布M": ("財布", "M"), "グッチ財布R": ("財布", "R"),
    "セイコー腕時計M": ("腕時計", "M"), "シチズン腕時計M": ("腕時計", "M"),
    "セイコー腕時計R": ("腕時計", "R"),
}


class PlanCollectionRangesTest(unittest.TestCase):

    def plan(self, listings, ranges=RANGES):
        return dc.plan_collection_ranges(ranges, ACCOUNTS, listings, PAIRS)

    def test_min_max_over_rows_even_with_gap(self):
        ranges, _, _ = self.plan([])
        # $200〜300 の空白があっても MIN〜MAX の1つの範囲
        self.assertEqual(ranges[("財布", "M")], (D(100), D(1000)))

    def test_delete_flag_row_counted_only_while_its_account_has_listings(self):
        ranges, _, _ = self.plan([])
        self.assertEqual(ranges[("財布", "R")], (D(300), D(1000)))
        ranges, _, _ = self.plan([ls("BUZZ②", "R", "グッチ財布R")])
        self.assertEqual(ranges[("財布", "R")], (D(100), D(1000)))
        # 他アカウントの出品では BUZZ② の行は集約に入らない
        ranges, _, _ = self.plan([ls("貴文", "R", "グッチ財布R")])
        self.assertEqual(ranges[("財布", "R")], (D(300), D(1000)))

    def test_unknown_preset_listing_of_that_account_counts_the_row(self):
        ranges, _, _ = self.plan([ls("BUZZ②", "R", None)])
        self.assertEqual(ranges[("財布", "R")], (D(100), D(1000)))

    def test_pair_with_new_listing_rows_is_not_pruned(self):
        _, prune, _ = self.plan([])
        self.assertNotIn(("財布", "R"), prune)  # BUZZ② は delete_flag=1 でも他アカウントが新規出品
        self.assertNotIn(("財布", "M"), prune)
        self.assertNotIn(("腕時計", "R"), prune)

    def test_keep_range_over_all_rows_null_is_row_range(self):
        ranges = RANGES + [pr("谷川③", "財布", "MS", 300, 600, keep_low=100, keep_high=1500),
                           pr("貴文", "財布", "MS", 600, 1000)]
        _, _, keeps = self.plan([], ranges=ranges)
        self.assertEqual(keeps[("財布", "MS")], (D(100), D(1500)))
        # keep_* が NULL の行は low/high。delete_flag=1 の行も入る（出品の有無に関係なく）
        self.assertEqual(keeps[("財布", "R")], (D(100), D(1000)))
        self.assertEqual(keeps[("腕時計", "M")], (D(100), D(300)))

    def test_stopped_pair_keeps_only_listed_presets(self):
        ranges, prune, _ = self.plan([ls("谷川④", "M", "セイコー腕時計M", 3), ls("谷川④", "R", "セイコー腕時計R")])
        self.assertEqual(ranges[("腕時計", "M")], (D(100), D(300)))
        self.assertEqual(prune[("腕時計", "M")]["keep"], {"セイコー腕時計M"})
        self.assertIsNone(prune[("腕時計", "M")]["skip"])

    def test_stopped_pair_keeps_presets_listed_by_other_accounts(self):
        # 谷川④ の出品が0件でも、別アカウントの出品がある preset は残す（行は集約に入らないが組は残す）
        ranges, prune, _ = self.plan([ls("貴文", "M", "シチズン腕時計M")])
        self.assertEqual(ranges[("腕時計", "M")], (D(100), D(300)))
        self.assertEqual(prune[("腕時計", "M")]["keep"], {"シチズン腕時計M"})
        self.assertEqual(prune[("腕時計", "M")]["rows"], [("谷川④", 1, False)])

    def test_stopped_pair_without_listings_has_no_lookup(self):
        ranges, prune, _ = self.plan([ls("谷川④", "R", "セイコー腕時計R")])
        self.assertNotIn(("腕時計", "M"), ranges)
        self.assertEqual(prune[("腕時計", "M")]["keep"], set())

    def test_unknown_preset_of_vendor_keeps_all(self):
        # どのアカウントの出品でも、preset の分からない M の出品があれば腕時計 M は全件残す
        ranges, prune, _ = self.plan([ls("貴文", "M", None, 2)])
        self.assertIn(("腕時計", "M"), ranges)
        self.assertIsNone(prune[("腕時計", "M")]["keep"])
        self.assertIn("preset が分からない未終了の出品 2 件", prune[("腕時計", "M")]["skip"])
        # preset が lookup の生成対象に無い（どの組か分からない）出品も同じ
        _, prune, _ = self.plan([ls("貴文", "M", "消えたpresetM")])
        self.assertIsNone(prune[("腕時計", "M")]["keep"])


def src_for(price_ranges, listings):
    """generate_preset_rows が腕時計（ブランドに依存しないカテゴリー）の preset を作る最小のマスター。"""
    vendors = [NS(vendor_code="M", vendor_name="メルカリ", vendor_group="メルカリ"),
               NS(vendor_code="R", vendor_name="ラクマ", vendor_group="ラクマ")]
    categories = [
        NS(category_id=1, vendor_group="メルカリ", category_name_ja="腕時計", type_ebay="Watch",
           category_id_ebay="31387", department=None, brand_id=10),
        NS(category_id=2, vendor_group="メルカリ", category_name_ja="置時計", type_ebay="Clock",
           category_id_ebay="1", department=None, brand_id=10),
        NS(category_id=1, vendor_group="ラクマ", category_name_ja="腕時計", type_ebay="Watch",
           category_id_ebay="31387", department=None, brand_id=10),
    ]
    return {
        "vendors": vendors,
        "brands": [],
        "vbm_rows": [],
        "categories": categories,
        "category_groups": [NS(category_group="腕時計", mode="DDP")],
        "cgc_rows": [NS(category_group="腕時計", category_id=1, is_brand_dependent=0, vendor_group="メルカリ"),
                     NS(category_group="腕時計", category_id=2, is_brand_dependent=0, vendor_group="メルカリ"),
                     NS(category_group="腕時計", category_id=1, is_brand_dependent=0, vendor_group="ラクマ")],
        "bcg_rows": [],
        "bcgv_rows": [],
        "price_ranges": price_ranges,
        "accounts": [NS(account="谷川④", preset_group="谷川④"), NS(account="貴文", preset_group="貴文")],
        "listings": listings,
    }


class BuildPresetsLookupTest(unittest.TestCase):

    RANGES = [pr("谷川④", "腕時計", "M", 100, 300, delete_flag=1, keep_low=100, keep_high=300),
              pr("谷川④", "腕時計", "R", 150, 250, keep_low=100, keep_high=300)]

    def build(self, listings, ranges=None):
        rows, prune = dc.build_presets_lookup(src_for(ranges or self.RANGES, listings))
        return {r[0]: dict(zip(dc.LOOKUP_INSERT_COLUMNS, r)) for r in rows}, prune

    def test_ranges_jpy_and_keep_range(self):
        lk, _ = self.build([ls("谷川④", "M", "腕時計M")])
        r = lk["腕時計R"]
        self.assertEqual((r["low_usd_target"], r["high_usd_target"]), (D(150), D(250)))
        self.assertEqual((r["low_jpy_target"], r["high_jpy_target"]),
                         dc.compute_cost_range_jpy_from_usd_range("DDP", 150.0, 250.0))
        # 終了判定は presets_price_ranges の keep_*（100〜300）∪ 収集範囲
        self.assertEqual((r["keep_low_usd_target"], r["keep_high_usd_target"]), (D(100), D(300)))
        self.assertEqual(r["mode"], "DDP")

    def test_keep_range_widens_with_collection_range(self):
        lk, _ = self.build([], ranges=[pr("谷川④", "腕時計", "R", 50, 400, keep_low=100, keep_high=300)])
        r = lk["腕時計R"]
        self.assertEqual((r["keep_low_usd_target"], r["keep_high_usd_target"]), (D(50), D(400)))

    def test_prune_counts_and_kept_rows(self):
        lk, prune = self.build([ls("谷川④", "M", "腕時計M", 2)])
        self.assertIn("腕時計M", lk)
        self.assertNotIn("置時計M", lk)
        self.assertIn("腕時計R", lk)
        self.assertEqual(
            [(p["category_group"], p["vendor_code"], p["lookup"], p["deleted"], p["kept"], p["skip"]) for p in prune],
            [("腕時計", "M", 2, 1, 1, None)],
        )

    def test_no_price_range_row_no_lookup(self):
        lk, prune = self.build([], ranges=[pr("谷川④", "腕時計", "R", 100, 300)])  # keep_* NULL
        self.assertEqual(sorted(lk), ["腕時計R"])
        self.assertEqual(prune, [])

    def test_format(self):
        _, prune = self.build([ls("谷川④", "M", "腕時計M")])
        self.assertEqual(dc.format_lookup_prune(prune), "presets_lookup 漸減: 腕時計 M 削除 1 / 維持 1［谷川④］")
        _, prune = self.build([ls("貴文", "M", None, 3)])
        self.assertEqual(
            dc.format_lookup_prune(prune),
            "presets_lookup 漸減: 腕時計 M 削除 0 / 維持 2（削除しない: preset が分からない未終了の出品 3 件）［谷川④(出品なし)］",
        )
        self.assertEqual(dc.format_lookup_prune([]), "presets_lookup 漸減: 対象なし")


class RefreshOrderTest(unittest.TestCase):

    def test_loads_then_truncate_insert_commit(self):
        order = []

        class Cur:
            fast_executemany = False

            def execute(self, sql, *a):
                if "TRUNCATE" in sql:
                    order.append("truncate")
                return self

            def fetchone(self):
                return (0,)

            def executemany(self, sql, rows):
                order.append(("insert", sql.count("?"), len(rows)))

        class Conn:
            def cursor(self):
                return Cur()

            def commit(self):
                order.append("commit")

        result = [{"category_group": "腕時計", "vendor_code": "M", "deleted": 1, "kept": 2, "skip": None, "rows": []}]
        with mock.patch.object(dc, "load_lookup_sources", side_effect=lambda cur: order.append("load") or {}), \
             mock.patch.object(dc, "build_presets_lookup", return_value=([("x",) * 17], result)):
            got = dc.refresh_presets_lookup(Conn())
        self.assertEqual(order, ["load", "truncate", ("insert", 17, 1), "commit"])
        self.assertIs(got, result)


if __name__ == "__main__":
    unittest.main()
