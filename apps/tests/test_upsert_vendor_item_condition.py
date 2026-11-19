# -*- coding: utf-8 -*-
"""
publish_ebay.upsert_vendor_item の商品状態(item_condition_id)保存のテスト（2026-11-19）。

- ラクマは詳細解析で取れた状態を保存し、None（状態欄なし・取得失敗）では既存の値を消さない。
- ラクマ以外は従来どおり保存しない（メルカリ系は検索で保存した値のまま、Yahoo!フリマも書かない）。

upsert_vendor_item は `with conn.cursor()` を使うため、pyodbc が抜けるときに commit する
（rollback では戻らない。2026-11-19 本番DBで実際に残った）。そのため必ず nostock_dev に接続し、
接続先が nostock_dev でなければ何もせずに失敗する。テスト行は TEST_COND_ で始まる ID だけを使い、終わったら消す。

実行: python -m unittest apps.tests.test_upsert_vendor_item_condition
"""
import os
import unittest

from apps.common.utils import get_sql_server_connection
from apps.publish import publish_ebay as pe

TEST_DB = "nostock_dev"
PREFIX = "TEST_COND_"


def connect_dev():
    os.environ["DB_NAME"] = TEST_DB
    conn = get_sql_server_connection()
    db = conn.cursor().execute("SELECT DB_NAME()").fetchone()[0]
    if db != TEST_DB:
        conn.close()
        raise RuntimeError(f"接続先が {db} です（{TEST_DB} 以外では実行しない）")
    return conn


class UpsertVendorItemConditionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connect_dev()

    @classmethod
    def tearDownClass(cls):
        cls.cleanup()
        cls.conn.close()

    @classmethod
    def cleanup(cls):
        cur = cls.conn.cursor()
        cur.execute("DELETE FROM trx.vendor_item WHERE vendor_item_id LIKE ?", PREFIX + "%")
        cls.conn.commit()

    def setUp(self):
        self.cleanup()

    def up(self, vendor_name, sku, cond):
        pe.upsert_vendor_item(self.conn, {"vendor_name": vendor_name, "vendor_item_id": PREFIX + sku,
                                          "item_condition_id": cond})

    def cond(self, vendor_name, sku):
        return self.conn.cursor().execute(
            "SELECT item_condition_id FROM trx.vendor_item WHERE vendor_name = ? AND vendor_item_id = ?",
            vendor_name, PREFIX + sku).fetchone()[0]

    def test_rakuma_saved_on_insert_and_update(self):
        self.up("ラクマ", "R1", 2)
        self.assertEqual(self.cond("ラクマ", "R1"), 2)
        self.up("ラクマ", "R1", 4)  # 出品直前の最新の値で上書き
        self.assertEqual(self.cond("ラクマ", "R1"), 4)

    def test_rakuma_none_keeps_existing(self):
        self.up("ラクマ", "R2", 3)
        self.up("ラクマ", "R2", None)
        self.assertEqual(self.cond("ラクマ", "R2"), 3)

    def test_rakuma_none_on_insert_stays_null(self):
        self.up("ラクマ", "R3", None)
        self.assertIsNone(self.cond("ラクマ", "R3"))

    def test_mercari_value_not_written(self):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO trx.vendor_item (vendor_name, vendor_item_id, item_condition_id, status) "
                    "VALUES (N'メルカリ', ?, 3, N'販売中')", PREFIX + "M1")
        self.conn.commit()
        self.up("メルカリ", "M1", 1)
        self.assertEqual(self.cond("メルカリ", "M1"), 3)

    def test_yahoo_not_written(self):
        self.up("ＰａｙＰａｙフリマ", "Y1", 2)
        self.assertIsNone(self.cond("ＰａｙＰａｙフリマ", "Y1"))


if __name__ == "__main__":
    unittest.main()
