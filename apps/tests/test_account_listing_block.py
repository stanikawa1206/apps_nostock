# -*- coding: utf-8 -*-
"""
eBay の新規出品制限（25019 Tutorial_Block_7D_NR、2026-11-03 に BUZZ で発生）への対応のテスト。

  - ebay_api: 制限の応答だけを AccountListingBlockedError にする（25019 の商品単位エラー・LIMIT は従来どおり）
  - publish_ebay.post_to_ebay: 制限の商品はロックを外して候補に戻し、例外を main へ伝える
  - publish_ebay.main（DB はテスト専用DB、ブラウザ・eBay はモック）:
      制限を検知したアカウントは BLOCKED で当日終了し、他のアカウントは通常どおり続く／
      別workerが BLOCKED にしたアカウントは次の商品へ進まずに終える（記録済みの内容は残る）
  - publish_manager: BLOCKED は終了扱い・出品枠確保の対象外・DONE補正の対象外、翌日のリセットでクリア
  - daily_check: 完了メールに BLOCKED のアカウントとエラー内容が載る
  - LIMIT の書き込み（DONE/LIMIT の判定）は従来どおり

DB を使うテストは、同じ SQL Server に専用DB nostock_test_blocked を作って使い、最後に削除する（本番DBには触れない）。
実行: python -m unittest apps.tests.test_account_listing_block -v
"""

import contextlib
import os
import unittest
from unittest import mock

import pyodbc

from apps.adapters import ebay_api
from apps.adapters.ebay_api import AccountListingBlockedError, ApiHandledError, ListingLimitError
from apps.common.utils import get_sql_server_connection
from apps.inventory import daily_check
from apps.publish import publish_ebay as pe
from apps.publish import publish_manager as pm

TEST_DB = "nostock_test_blocked"

BLOCK_TEXT = (
    "eBay has restricted your ability to list new items for approximately seven days as a result of a "
    "policy violation. You can still access other features on eBay. For more information about this policy "
    "violation, please check your messages in My eBay. Thank you for supporting our efforts to maintain a safe marketplace."
)

# 2026-11-03 の BUZZ の publishOffer 応答（実測そのまま）
BLOCK_RESPONSE = {"errors": [{
    "errorId": 25019, "domain": "API_INVENTORY", "subdomain": "Selling", "category": "Request",
    "message": "Cannot revise listing. The item cannot be listed or modified. The title and/or description may "
               "contain improper words, or the listing or seller may be in violation of eBay policy.",
    "parameters": [
        {"name": "0", "value": "<div>\n        eBay has restricted your ability to list new items for approximately "
                               "seven days as a result of a policy violation. <p></p></div><font color=#757575 size=1>{e301542-1288643x}</font>"},
        {"name": "1", "value": BLOCK_TEXT},
        {"name": "2", "value": "Tutorial_Block_7D_NR"},
        {"name": "3", "value": "1288643"},
    ],
}]}

# 同じ 25019 でも商品単位の失敗（制限名なし）
IMPROPER_WORDS_RESPONSE = {"errors": [{
    "errorId": 25019, "message": "Cannot revise listing. The title and/or description may contain improper words.",
    "parameters": [{"name": "0", "value": "The title contains improper words."}],
}]}


class FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


# ============================================================
# ebay_api
# ============================================================
class EbayApiDetectTest(unittest.TestCase):
    def test_block_response_is_detected(self):
        self.assertEqual(ebay_api._find_account_listing_block(BLOCK_RESPONSE), ("Tutorial_Block_7D_NR", BLOCK_TEXT))

    def test_item_level_25019_is_not_block(self):
        self.assertIsNone(ebay_api._find_account_listing_block(IMPROPER_WORDS_RESPONSE))

    def test_other_errors_are_not_block(self):
        self.assertIsNone(ebay_api._find_account_listing_block({"errors": [{"errorId": 25002, "message": "x"}]}))
        self.assertIsNone(ebay_api._find_account_listing_block({}))

    def _publish(self, status, body):
        with mock.patch.object(ebay_api.requests, "post", return_value=FakeResponse(status, body)), \
                mock.patch("builtins.print"):
            return ebay_api.publish_offer("OFFER1", "TOKEN")

    def test_publish_offer_raises_block(self):
        with self.assertRaises(AccountListingBlockedError) as cm:
            self._publish(400, BLOCK_RESPONSE)
        e = cm.exception
        self.assertEqual((e.code, e.block_name, e.detail), (25019, "Tutorial_Block_7D_NR", BLOCK_TEXT))
        self.assertIsInstance(e, ApiHandledError)  # 個別に扱わない呼び出し元では従来どおり

    def test_publish_offer_item_level_25019_unchanged(self):
        with self.assertRaises(ApiHandledError) as cm:
            self._publish(400, IMPROPER_WORDS_RESPONSE)
        self.assertNotIsInstance(cm.exception, AccountListingBlockedError)
        self.assertEqual(cm.exception.code, 25019)

    def test_publish_offer_listing_limit_unchanged(self):
        body = {"errors": [{"errorId": 21916611, "message": "You have reached the number of items you can list."}]}
        with self.assertRaises(ListingLimitError):
            self._publish(400, body)

    def test_post_one_item_propagates_block(self):
        blocked = AccountListingBlockedError(25019, "m", "Tutorial_Block_7D_NR", BLOCK_TEXT)
        with mock.patch.object(ebay_api, "get_access_token_new", return_value="T"), \
                mock.patch.object(ebay_api.requests, "get"), \
                mock.patch.object(ebay_api, "register_inventory_item"), \
                mock.patch.object(ebay_api, "create_offer", return_value="O"), \
                mock.patch.object(ebay_api, "update_offer"), \
                mock.patch.object(ebay_api, "publish_offer", side_effect=blocked), \
                mock.patch.object(ebay_api, "_confirm_published_listing") as confirm:
            with self.assertRaises(AccountListingBlockedError):
                ebay_api.post_one_item({"CustomLabel": "SKU1"}, "BUZZ", {})
        confirm.assert_not_called()


# ============================================================
# publish_ebay.post_to_ebay
# ============================================================
def _heavy(sku="SKU1"):
    return {
        "vendor_name": "メルカリ", "sku": sku,
        "rec": {"title_en": "t", "processing_by": "pc", "processing_at": "2026-11-03"},
        "start_price_usd": 100, "category_id_ebay": "1", "default_brand_en": "b",
        "department": "d", "type_ebay": "t", "category_group": "財布",
    }


def _call_post_to_ebay(conn, heavy):
    return pe.post_to_ebay(
        conn=conn, p=None, acct="BUZZ", heavy=heavy, acct_targets={"BUZZ": 10}, acct_success={"BUZZ": 0},
        acct_policies_map={"BUZZ": {}}, total_listings=0, MAX_LISTINGS=10**9, stop_all=False,
        writes_since_commit=0, BATCH_COMMIT=10, image_mode="NORMAL", image_error_count=0, cdn_mode_until=None,
        r2=None, r2_bucket="", r2_public_base="", cdn_cache={}, now_dt=None, preset="p",
    )


class PostToEbayTest(unittest.TestCase):
    def setUp(self):
        for p in (mock.patch.object(pe, "build_pic_urls", return_value=[]),
                  mock.patch.object(pe, "upsert_vendor_item"),
                  mock.patch("builtins.print")):
            p.start()
            self.addCleanup(p.stop)

    def test_block_releases_item_and_reraises(self):
        conn = mock.Mock()
        heavy = _heavy()
        blocked = AccountListingBlockedError(25019, "m", "Tutorial_Block_7D_NR", BLOCK_TEXT)
        with mock.patch.object(pe, "post_one_item", side_effect=blocked):
            with self.assertRaises(AccountListingBlockedError):
                _call_post_to_ebay(conn, heavy)
        rec = heavy["rec"]
        self.assertEqual(rec["listing_head"], "出品停止(出品制限)")
        self.assertIsNone(rec["processing_by"])
        self.assertIsNone(rec["processing_at"])
        pe.upsert_vendor_item.assert_called_once()
        conn.commit.assert_called()

    def test_item_level_api_error_unchanged(self):
        heavy = _heavy()
        with mock.patch.object(pe, "post_one_item", side_effect=ApiHandledError(25019, "improper words")):
            result = _call_post_to_ebay(mock.Mock(), heavy)
        self.assertEqual(len(result), 10)
        self.assertFalse(result[9])  # listing_limit_hit
        self.assertEqual(heavy["rec"]["listing_head"], "出品失敗")
        self.assertEqual(heavy["rec"]["processing_at"], "2026-11-03")  # 当日は再取得しない（従来どおり）


# ============================================================
# テスト専用DB
# ============================================================
SCHEMA_SQL = [
    "CREATE SCHEMA mst",
    "CREATE SCHEMA trx",
    """CREATE TABLE mst.ebay_accounts (
        account NVARCHAR(100) NOT NULL PRIMARY KEY, preset_group NVARCHAR(20), post_target INT,
        close_reason NVARCHAR(100), close_detail NVARCHAR(1000), is_excluded BIT,
        fulfillment_policy_id NVARCHAR(100), payment_policy_id NVARCHAR(100), return_policy_id NVARCHAR(100))""",
    "CREATE TABLE mst.execute_pcs (execute_pc NVARCHAR(100) NOT NULL PRIMARY KEY, account NVARCHAR(100), is_active BIT)",
    """CREATE TABLE trx.listings (id INT IDENTITY PRIMARY KEY, account NVARCHAR(100), start_time DATETIME,
        is_deleted BIT, delete_reason NVARCHAR(100), deleted_at DATETIME)""",
]


def _master_conn():
    conn = get_sql_server_connection()
    conn.autocommit = True
    return conn


class DbTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._env = mock.patch.dict(os.environ, {"DB_NAME": "master"})
        cls._env.start()
        with contextlib.closing(_master_conn()) as c:
            c.cursor().execute(f"IF DB_ID('{TEST_DB}') IS NULL CREATE DATABASE {TEST_DB}")
        os.environ["DB_NAME"] = TEST_DB
        with contextlib.closing(_master_conn()) as c:
            cur = c.cursor()
            cur.execute("SELECT DB_NAME()")
            assert cur.fetchone()[0] == TEST_DB  # 本番DBで実行しないことの確認
            cur.execute("SELECT COUNT(*) FROM sys.schemas WHERE name = 'mst'")
            if cur.fetchone()[0] == 0:
                for s in SCHEMA_SQL:
                    cur.execute(s)

    @classmethod
    def tearDownClass(cls):
        os.environ["DB_NAME"] = "master"
        with contextlib.closing(_master_conn()) as c:
            c.cursor().execute(
                f"IF DB_ID('{TEST_DB}') IS NOT NULL BEGIN "
                f"ALTER DATABASE {TEST_DB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {TEST_DB}; END")
        cls._env.stop()

    def setUp(self):
        self.conn = get_sql_server_connection()
        self.addCleanup(self.conn.close)
        self.sql("SELECT DB_NAME()")
        self.assertEqual(self.cur.fetchone()[0], TEST_DB)
        self.sql("DELETE FROM mst.ebay_accounts; DELETE FROM mst.execute_pcs; DELETE FROM trx.listings;")
        self.conn.commit()

    def sql(self, q, *params):
        self.cur = self.conn.cursor()
        self.cur.execute(q, *params)
        return self.cur

    def add_account(self, account, post_target=10, close_reason=None, close_detail=None, is_excluded=0):
        self.sql("INSERT INTO mst.ebay_accounts (account, preset_group, post_target, close_reason, close_detail, "
                 "is_excluded, fulfillment_policy_id, payment_policy_id, return_policy_id) "
                 "VALUES (?, ?, ?, ?, ?, ?, '1', '2', '3')",
                 account, account, post_target, close_reason, close_detail, is_excluded)
        self.conn.commit()

    def add_pc(self, pc, account=None):
        self.sql("INSERT INTO mst.execute_pcs (execute_pc, account, is_active) VALUES (?, ?, 1)", pc, account)
        self.conn.commit()

    def add_listing(self, account):
        c = get_sql_server_connection()  # main が使う接続とは別の接続で確定させる（他worker相当）
        try:
            c.cursor().execute("INSERT INTO trx.listings (account, start_time, is_deleted) VALUES (?, GETDATE(), 0)", account)
            c.commit()
        finally:
            c.close()

    def state(self, account):
        r = self.sql("SELECT close_reason, close_detail FROM mst.ebay_accounts WHERE account = ?", account).fetchone()
        self.conn.commit()
        return tuple(r)


# ============================================================
# publish_ebay.main（1 worker 分）
# ============================================================
class FakeChromium:
    def launch(self, **kw):
        return mock.MagicMock()


class FakePlaywright:
    chromium = FakeChromium()


@contextlib.contextmanager
def _fake_sync_playwright():
    yield FakePlaywright()


class PublishMainTest(DbTestBase):
    PC = "testpc"

    def run_main(self, take_one, post):
        """take_one(account) -> row|None、post(account, sku) -> post_to_ebay の戻り値 or 例外。"""
        calls = {"take": [], "post": []}

        def _take(conn, preset_group, processing_by, account):
            calls["take"].append(account)
            return take_one(account)

        def _heavy_check(conn, page, url, sku, *a, **k):
            return ({"sku": sku}, None, 0, None, None)

        def _post(**kw):
            calls["post"].append((kw["acct"], kw["heavy"]["sku"]))
            res = post(kw["acct"], kw["heavy"]["sku"])
            if res == "OK":
                self.add_listing(kw["acct"])
                kw["acct_success"][kw["acct"]] += 1
            return (kw["acct_targets"], kw["acct_success"], kw["total_listings"], False, 0,
                    0, "NORMAL", 0, None, False)

        patches = [
            mock.patch.object(pe.socket, "gethostname", return_value=self.PC),
            mock.patch.object(pe, "set_exit_report"),
            mock.patch.object(pe, "set_exit_context"),
            mock.patch.object(pe, "start_watchdog", return_value=mock.Mock()),
            mock.patch.object(pe.boto3, "client"),
            mock.patch.object(pe, "fetch_active_presets", return_value=[]),
            mock.patch.object(pe, "sync_playwright", _fake_sync_playwright),
            mock.patch.object(pe, "playwright_driver_monitor", contextlib.nullcontext),
            mock.patch.object(pe, "take_one_vendor_item", side_effect=_take),
            mock.patch.object(pe, "heavy_check_detail", side_effect=_heavy_check),
            mock.patch.object(pe, "post_to_ebay", side_effect=_post),
            mock.patch("builtins.print"),
        ]
        with contextlib.ExitStack() as st:
            for p in patches:
                st.enter_context(p)
            with self.assertRaises(SystemExit) as cm:
                pe.main()
        self.assertEqual(cm.exception.code, 10)  # 正常終了
        return calls

    @staticmethod
    def _row(sku):
        return {"vendor_item_id": sku, "price": 1, "shipping_days": None, "vendor_name": "メルカリ",
                "preset": "p", "mode": None, "default_brand_en": "b", "category_id_ebay": "1", "department": "d",
                "type_ebay": "t", "category_group": "財布", "low_jpy_target": 1, "high_jpy_target": 2,
                "item_condition_id": 1}

    def test_blocked_account_closes_and_others_continue(self):
        self.add_account("BUZZ", post_target=10)
        self.add_account("川島", post_target=2)
        self.add_pc(self.PC)
        blocked = AccountListingBlockedError(25019, "m", "Tutorial_Block_7D_NR", BLOCK_TEXT)

        def post(acct, sku):
            if acct == "BUZZ":
                raise blocked
            return "OK"

        counter = iter(range(1000))
        calls = self.run_main(lambda a: self._row(f"{a}-{next(counter)}"), post)

        # BUZZ は1件目の失敗で終了（2件目は取らない）、川島は目標まで出品して DONE
        self.assertEqual([c for c in calls["post"] if c[0] == "BUZZ"], [("BUZZ", "BUZZ-0")])
        self.assertEqual(sum(1 for c in calls["post"] if c[0] == "川島"), 2)
        reason, detail = self.state("BUZZ")
        self.assertEqual(reason, "BLOCKED")
        self.assertIn("errorId=25019 Tutorial_Block_7D_NR SKU=BUZZ-0 pc=testpc", detail)
        self.assertIn("restricted your ability to list new items", detail)
        self.assertEqual(self.state("川島"), ("DONE", None))
        self.assertIsNone(self.sql("SELECT account FROM mst.execute_pcs WHERE execute_pc = ?", self.PC).fetchone()[0])
        self.conn.commit()

    def test_other_worker_blocked_stops_without_next_item(self):
        self.add_account("BUZZ", post_target=10)
        self.add_pc(self.PC)
        self.add_pc("otherpc")

        def post(acct, sku):
            # このworkerが1件目を処理している間に、別workerが制限を検知して BLOCKED にした
            c = get_sql_server_connection()
            try:
                c.cursor().execute("UPDATE mst.ebay_accounts SET close_reason = 'BLOCKED', close_detail = N'他workerの記録' "
                                   "WHERE account = 'BUZZ'")
                c.commit()
            finally:
                c.close()
            return "NG"  # このworker自身の1件目は通常の失敗

        counter = iter(range(1000))
        calls = self.run_main(lambda a: self._row(f"{a}-{next(counter)}"), post)

        self.assertEqual(calls["take"], ["BUZZ"])  # 次の商品を取らない
        self.assertEqual(self.state("BUZZ"), ("BLOCKED", "他workerの記録"))  # 記録済みの内容が残る

    def test_blocked_account_is_not_assigned(self):
        self.add_account("BUZZ", close_reason="BLOCKED", close_detail="x")
        self.add_pc(self.PC)
        self.assertIsNone(pe.fetch_next_account_and_lock(self.conn, self.PC))
        self.assertFalse(pe.has_limit_accounts(self.conn))  # LIMIT待ちにもならない

    def test_limit_write_unchanged(self):
        self.add_account("川島", post_target=1)
        self.add_account("谷川②", post_target=5)
        self.add_pc(self.PC)
        self.add_listing("川島")
        pe.release_pc_and_close_account(self.conn, self.PC, "川島", "LIMIT")
        pe.release_pc_and_close_account(self.conn, self.PC, "谷川②", "LIMIT")
        self.assertEqual(self.state("川島"), ("DONE", None))  # 目標到達済みは DONE
        self.assertEqual(self.state("谷川②"), ("LIMIT", None))

    def test_blocked_write_keeps_detail_when_none(self):
        self.add_account("BUZZ")
        self.add_pc(self.PC)
        pe.release_pc_and_close_account(self.conn, self.PC, "BUZZ", "BLOCKED", "最初の記録")
        pe.release_pc_and_close_account(self.conn, self.PC, "BUZZ", "BLOCKED", None)
        self.assertEqual(self.state("BUZZ"), ("BLOCKED", "最初の記録"))


# ============================================================
# publish_manager / daily_check
# ============================================================
class ManagerTest(DbTestBase):
    def setUp(self):
        super().setUp()
        p = mock.patch.object(pm, "_pm_log")
        self.pm_log = p.start()
        self.addCleanup(p.stop)
        pp = mock.patch("builtins.print")
        pp.start()
        self.addCleanup(pp.stop)

    def test_blocked_counts_as_finished(self):
        self.add_account("BUZZ", close_reason="BLOCKED", close_detail="x")
        self.add_account("川島", close_reason="DONE")
        self.add_account("谷川②", close_reason="EMPTY")
        self.assertTrue(pm.is_all_accounts_finished())

    def test_limit_and_null_still_unfinished(self):
        self.add_account("BUZZ", close_reason="BLOCKED")
        self.add_account("川島", close_reason="LIMIT")
        self.assertFalse(pm.is_all_accounts_finished())
        self.sql("UPDATE mst.ebay_accounts SET close_reason = NULL WHERE account = N'川島'")
        self.conn.commit()
        self.assertFalse(pm.is_all_accounts_finished())

    def test_blocked_is_not_limit_and_not_auto_done(self):
        self.add_account("BUZZ", post_target=1, close_reason="BLOCKED", close_detail="x")
        self.add_listing("BUZZ")
        self.assertEqual(pm.get_limit_accounts(), [])
        self.assertEqual(pm.auto_fix_done_accounts(), [])
        self.assertEqual(self.state("BUZZ"), ("BLOCKED", "x"))

    def test_reset_clears_blocked_for_next_day(self):
        self.add_account("BUZZ", close_reason="BLOCKED", close_detail="x")
        self.add_account("川島", close_reason="LIMIT")
        self.add_account("貴文", close_detail="残骸")
        self.add_account("谷川②", is_excluded=1, close_reason="BLOCKED", close_detail="y")
        pm.reset_close_status()
        for a in ("BUZZ", "川島", "貴文", "谷川②"):
            self.assertEqual(self.state(a), (None, None))

    def test_log_blocked_accounts(self):
        self.add_account("BUZZ", close_reason="BLOCKED", close_detail="detail-1")
        self.add_account("川島", close_reason="DONE")
        self.assertEqual(pm.log_blocked_accounts(), [("BUZZ", "detail-1")])
        self.assertTrue(any("account=BUZZ detail=detail-1" in str(c) for c in self.pm_log.call_args_list))

    def test_completion_mail_lists_blocked(self):
        self.add_account("BUZZ", close_reason="BLOCKED", close_detail="2026-11-03 03:17:11 errorId=25019 Tutorial_Block_7D_NR")
        self.add_account("川島", close_reason="DONE")
        body = daily_check.build_publish_manager_account_summary(self.conn)
        self.assertIn("eBayの新規出品制限で当日の出品を終了したアカウント", body)
        self.assertIn("- BUZZ: 2026-11-03 03:17:11 errorId=25019 Tutorial_Block_7D_NR", body)
        self.assertIn("- 川島: close_reason=DONE", body)
        self.assertIn("現在LIMIT中のアカウント数（正常終了なら0のはず）: 0", body)

    def test_completion_mail_without_blocked_unchanged(self):
        self.add_account("川島", close_reason="DONE")
        body = daily_check.build_publish_manager_account_summary(self.conn)
        self.assertTrue(body.startswith("【アカウント別状況】"))


if __name__ == "__main__":
    unittest.main()
