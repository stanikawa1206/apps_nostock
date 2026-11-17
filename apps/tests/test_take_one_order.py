# -*- coding: utf-8 -*-
"""
publish_ebay.take_one_vendor_item（TAKE_ONE_SQL）の処理順のテスト。

2026-11-15: 同じ優先度の中は「仕入先での出品日時（vendor_created_at）が新しい順」にする（それまでは商品ID順）。
ラクマ・Yahoo!フリマは出品日時が NULL なので、最初に取り込んだ日時（created_at）で代える。同じ日時は商品ID順。
2026-11-17: 優先度は仕入先の publish_priority だけ（presets_price_ranges.category_priority を削除）。

テスト専用DB（test_account_listing_block と同じ nostock_test_blocked。最後に削除）に、
本物の dbo.fn_take_one_candidates_base の代わりの関数（候補表 test_cand の行を返すだけ）と、
TAKE_ONE_SQL が使う列だけの trx.vendor_item を作り、本物の TAKE_ONE_SQL を実行する。本番DBには触れない。

実行: python -m unittest apps.tests.test_take_one_order -v
"""

import contextlib
import threading
import time
import unittest
from datetime import datetime, timedelta
from unittest import mock

from apps.publish import publish_ebay as pe
from apps.tests.test_account_listing_block import DbTestBase

T0 = datetime(2026, 11, 15, 9, 0, 0)

OBJECTS = [
    """IF OBJECT_ID('trx.vendor_item') IS NULL
    CREATE TABLE trx.vendor_item (
        vendor_name NVARCHAR(100) NOT NULL, vendor_item_id NVARCHAR(200) NOT NULL,
        price INT NULL, shipping_region NVARCHAR(100) NULL, shipping_days NVARCHAR(100) NULL,
        item_condition_id INT NULL, processing_by NVARCHAR(100) NULL, processing_at DATETIME2 NULL,
        vendor_created_at DATETIME2 NULL, created_at DATETIME2 NULL,
        CONSTRAINT PK_vendor_item PRIMARY KEY CLUSTERED (vendor_name, vendor_item_id))""",
    """IF OBJECT_ID('trx.test_cand') IS NULL
    CREATE TABLE trx.test_cand (preset_group NVARCHAR(100), vendor_name NVARCHAR(100), vendor_item_id NVARCHAR(200),
        publish_priority INT)""",
    # 本物の base 関数と同じ列・同じく processing_at IS NULL の行だけを返す（重複行も返せる）
    """IF OBJECT_ID('dbo.fn_take_one_candidates_base') IS NULL EXEC('
    CREATE FUNCTION dbo.fn_take_one_candidates_base(@preset_group NVARCHAR(100)) RETURNS TABLE AS RETURN (
        SELECT v.vendor_item_id, v.vendor_name, v.price, v.shipping_region, v.shipping_days,
               N''p'' AS preset, CAST(NULL AS NVARCHAR(50)) AS mode, N''b'' AS default_brand_en, N''1'' AS category_id_ebay,
               N''d'' AS department, N''t'' AS type_ebay, N''cg'' AS category_group, CAST(NULL AS BIT) AS is_collectibles,
               1 AS low_jpy_target, 2 AS high_jpy_target, v.item_condition_id,
               CAST(1 AS BIT) AS is_ok_logic, c.publish_priority
        FROM trx.vendor_item v
        INNER JOIN trx.test_cand c ON c.vendor_name = v.vendor_name AND c.vendor_item_id = v.vendor_item_id
        WHERE c.preset_group = @preset_group AND v.processing_at IS NULL)')""",
]


class TakeOneOrderTest(DbTestBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # 本番DBと同じ READ_COMMITTED_SNAPSHOT（① の候補の読み取りが、他 worker の未 commit 行を待たない前提）
        with contextlib.closing(pe.get_sql_server_connection()) as c:
            c.autocommit = True
            c.cursor().execute("ALTER DATABASE nostock_test_blocked SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE")
        with contextlib.closing(pe.get_sql_server_connection()) as c:
            cur = c.cursor()
            cur.execute("SELECT DB_NAME()")
            assert cur.fetchone()[0] == "nostock_test_blocked"
            for sql in OBJECTS:
                cur.execute(sql)
            # TAKE_ONE_SQL はラクマの障害情報（mst.control）も読む。本番と同じ定義
            cur.execute("IF OBJECT_ID('mst.control') IS NULL CREATE TABLE mst.control ("
                        "param_key NVARCHAR(200) NOT NULL CONSTRAINT PK_control_parameter PRIMARY KEY, param_value INT NULL)")
            c.commit()

    def setUp(self):
        super().setUp()
        self.sql("DELETE FROM trx.vendor_item; DELETE FROM trx.test_cand;")
        self.conn.commit()
        p = mock.patch("builtins.print")
        p.start()
        self.addCleanup(p.stop)

    def add(self, vendor, iid, created, first_seen=T0, prio=3, group="g", dup=False):
        self.sql("INSERT INTO trx.vendor_item (vendor_name, vendor_item_id, price, vendor_created_at, created_at) "
                 "VALUES (?, ?, 1000, ?, ?)", vendor, iid, created, first_seen)
        for _ in range(2 if dup else 1):
            self.sql("INSERT INTO trx.test_cand VALUES (?, ?, ?, ?)", group, vendor, iid, prio)
        self.conn.commit()

    def take_all(self, group="g", worker="w1"):
        conn = pe.get_sql_server_connection()
        try:
            got = []
            while True:
                row = pe.take_one_vendor_item(conn, group, worker, "acct")
                if row is None:
                    return got
                got.append(row["vendor_item_id"])
        finally:
            conn.close()

    def test_newest_first_within_priority(self):
        self.add("メルカリshops", "A_old", T0 - timedelta(days=30))
        self.add("メルカリshops", "B_new", T0 - timedelta(hours=1))
        self.add("メルカリshops", "C_mid", T0 - timedelta(days=3), dup=True)  # base 関数が同じ商品を2行返しても1回だけ
        self.add("メルカリshops", "D_same", T0 - timedelta(days=3))           # 同じ日時は商品ID順
        self.assertEqual(self.take_all(), ["B_new", "C_mid", "D_same", "A_old"])

    def test_vendor_priority_first(self):
        # 仕入先の優先度（R → Y → MS → M）は従来どおり先。新しさはその中だけ
        self.add("ラクマ", "R_old", None, first_seen=T0 - timedelta(days=60), prio=1)
        self.add("ＰａｙＰａｙフリマ", "Y_old", None, first_seen=T0 - timedelta(days=30), prio=2)
        self.add("メルカリ", "M_new", T0, prio=4)
        self.add("メルカリshops", "S_new", T0, prio=3)
        self.add("メルカリshops", "S_old", T0 - timedelta(days=9), prio=3)
        self.assertEqual(self.take_all(), ["R_old", "Y_old", "S_new", "S_old", "M_new"])

    def test_null_created_uses_first_seen(self):
        # ラクマ・Yahoo!フリマ（出品日時なし）は、最初に取り込んだ日時の新しい順
        self.add("ラクマ", "r1", None, first_seen=T0 - timedelta(days=5), prio=1)
        self.add("ラクマ", "r2", None, first_seen=T0 - timedelta(hours=2), prio=1)
        self.add("ラクマ", "r3", None, first_seen=T0 - timedelta(days=1), prio=1)
        self.assertEqual(self.take_all(), ["r2", "r3", "r1"])

    def test_more_than_limit(self):
        # 候補が TAKE_ONE_CANDIDATE_LIMIT より多くても、全体の中で新しい順に取れる
        n = pe.TAKE_ONE_CANDIDATE_LIMIT + 7
        for i in range(n):
            self.add("メルカリshops", f"x{i:03d}", T0 - timedelta(minutes=i))
        self.assertEqual(self.take_all(), [f"x{i:03d}" for i in range(n)])

    def test_concurrent_worker_skips_locked_row(self):
        # 他の worker が先頭の行を確保中（未 commit）でも、待たずに次の行を取る（READPAST・1行ずつの確保が保たれている）
        for i, iid in enumerate(["n1", "n2", "n3"]):
            self.add("メルカリshops", iid, T0 - timedelta(hours=i))
        holder = pe.get_sql_server_connection()
        try:
            hc = holder.cursor()
            hc.execute("UPDATE trx.vendor_item SET processing_by = N'other' WHERE vendor_item_id = N'n1'")  # 未 commit
            conn = pe.get_sql_server_connection()
            try:
                t0 = time.time()
                row = pe.take_one_vendor_item(conn, "g", "w2", "acct")
                self.assertLess(time.time() - t0, 5)
                self.assertEqual(row["vendor_item_id"], "n2")
            finally:
                conn.close()
            holder.rollback()
        finally:
            holder.close()

    def test_rakuma_paused_keeps_newest_first(self):
        # ラクマの障害中（mst.control の全台の停止期限）はラクマの候補を外し、次の優先度を新しい順に取る。期限後はラクマを新しい順に
        self.sql("DELETE FROM mst.control")
        self.add("ラクマ", "r_old", None, first_seen=T0 - timedelta(days=2), prio=1)
        self.add("ラクマ", "r_new", None, first_seen=T0 - timedelta(hours=1), prio=1)
        self.add("メルカリshops", "s_old", T0 - timedelta(days=5))
        self.add("メルカリshops", "s_new", T0 - timedelta(hours=3))
        # 全台の停止（ラクマ障害_停止期限 = 今から10分後）
        self.sql("INSERT INTO mst.control VALUES (N'ラクマ障害_停止期限', DATEDIFF(SECOND, '20000101', SYSDATETIME()) + 600)")
        self.conn.commit()
        conn = pe.get_sql_server_connection()
        try:
            got = [pe.take_one_vendor_item(conn, "g", "w1", "acct")["vendor_item_id"] for _ in range(2)]
        finally:
            conn.close()
        self.assertEqual(got, ["s_new", "s_old"])
        self.sql("UPDATE mst.control SET param_value = DATEDIFF(SECOND, '20000101', SYSDATETIME()) - 1 "
                 "WHERE param_key = N'ラクマ障害_停止期限'")
        self.conn.commit()
        self.assertEqual(self.take_all(), ["r_new", "r_old"])
        self.sql("DELETE FROM mst.control")
        self.conn.commit()

    def test_empty(self):
        self.assertEqual(self.take_all(group="none"), [])


if __name__ == "__main__":
    unittest.main()
