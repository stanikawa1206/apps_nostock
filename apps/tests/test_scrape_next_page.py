# -*- coding: utf-8 -*-
"""
scrape_worker のメルカリ検索の終了判定（nextPageToken）のテスト。
モックのみ。メルカリ・DB には接続しない。

実行: python -m unittest apps.tests.test_scrape_next_page -v
"""

import os
import sys
import tempfile
import unittest
from unittest import mock

_TMP = tempfile.TemporaryDirectory()
_stdout, _stderr = sys.stdout, sys.stderr
with mock.patch.dict(os.environ, {"WORKER_STATUS_DIR": _TMP.name, "WORKER_NAME": "test-worker"}):
    from apps.inventory import scrape_worker as sw
sys.stdout, sys.stderr = _stdout, _stderr
sw._log_file.close()

PAYLOAD = {"preset": "P", "vendor_name": "メルカリ", "brand_id": 1, "category_id": 2, "mode": "DDP",
           "low_usd_target": 100.0, "high_usd_target": 200.0}


def page(n_items, start=0, token="__omit__", meta=True):
    """n_items 件のページ。token="__omit__" はキー自体を入れない。meta=False は meta を入れない"""
    js = {"items": [{"id": f"m{start + i}", "name": "t", "price": 10000, "sellerId": "s",
                     "created": 1700000000, "updated": 1700000000, "itemConditionId": 3}
                    for i in range(n_items)]}
    if meta:
        js["meta"] = {} if token == "__omit__" else {"nextPageToken": token}
    return js


class FakeSearch:
    """ページ番号（0始まり）→ 応答。範囲外は0件・最終ページ。例外を入れるとその番号で送出する"""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def __call__(self, page_obj, url, conn, job_id):
        idx = 0 if "page_token=" not in url else int(url.split("page_token=v1%3A")[-1].split("&")[0])
        self.calls.append(idx)
        r = self.pages[idx] if idx < len(self.pages) else page(0, token="")
        if isinstance(r, Exception):
            raise r
        return r


class Base(unittest.TestCase):
    def run_job(self, fn, pages):
        fake = FakeSearch(pages)
        upserted = []
        with mock.patch.object(sw, "fetch_page_json", fake), \
             mock.patch.object(sw, "get_sql_server_connection", return_value=mock.MagicMock()), \
             mock.patch.object(sw, "upsert_vendor_items", lambda conn, rows, now: upserted.extend(r["vendor_item_id"] for r in rows)), \
             mock.patch.object(sw, "get_vendor_item_prices_batch", return_value={}), \
             mock.patch.object(sw, "handle_listing_delete"), \
             mock.patch.object(sw, "write_status"), \
             mock.patch.object(sw.time, "sleep"), \
             mock.patch("builtins.print"):
            ret = fn(None, None, dict(PAYLOAD), 1)
        return fake.calls, upserted, ret


class HasNextTest(unittest.TestCase):
    def test_values(self):
        with mock.patch("builtins.print"):
            self.assertFalse(sw.search_has_next_page({"items": [], "meta": {"nextPageToken": ""}}))
            self.assertTrue(sw.search_has_next_page({"items": [], "meta": {"nextPageToken": "v1:1"}}))
            # 判断できない応答は「次がある」= 従来どおり次ページで確かめる
            self.assertTrue(sw.search_has_next_page({"items": [], "meta": {}}))
            self.assertTrue(sw.search_has_next_page({"items": []}))
            self.assertTrue(sw.search_has_next_page({"items": [], "meta": None}))
            self.assertTrue(sw.search_has_next_page({"items": [], "meta": {"nextPageToken": None}}))
            self.assertTrue(sw.search_has_next_page({"items": [], "meta": {"nextPageToken": 0}}))
            self.assertTrue(sw.search_has_next_page(None))


class ActiveTest(Base):
    fn = staticmethod(sw.run_fetch_active_ebay)

    def test_stop_at_empty_token(self):
        calls, ids, ret = self.run_job(self.fn, [page(120, 0, "v1:1"), page(120, 120, "v1:2"), page(50, 240, "")])
        self.assertEqual(calls, [0, 1, 2])                     # 従来は 0,1,2,3（3は0件）
        self.assertEqual(ids, [f"m{i}" for i in range(290)])
        self.assertEqual(ret, (3, 290))

    def test_partial_page_with_next_continues(self):
        # 件数が120未満でも next があれば続ける（実データで 118件・next=true がある）
        calls, ids, _ = self.run_job(self.fn, [page(118, 0, "v1:1"), page(30, 118, "")])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(len(ids), 148)

    def test_missing_token_falls_back(self):
        calls, ids, _ = self.run_job(self.fn, [page(120, 0, "v1:1"), page(50, 120)])  # キー欠落
        self.assertEqual(calls, [0, 1, 2])                     # 従来どおり0件ページまで
        self.assertEqual(len(ids), 170)

    def test_missing_meta_falls_back(self):
        calls, ids, _ = self.run_job(self.fn, [page(50, 0, meta=False)])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(len(ids), 50)

    def test_abnormal_token_falls_back(self):
        calls, ids, _ = self.run_job(self.fn, [page(50, 0, None)])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(len(ids), 50)

    def test_zero_first_page(self):
        calls, ids, ret = self.run_job(self.fn, [page(0, 0, "")])
        self.assertEqual(calls, [0])
        self.assertEqual((ids, ret), ([], (0, 0)))

    def test_empty_page_with_next_stops(self):
        calls, ids, _ = self.run_job(self.fn, [page(120, 0, "v1:1"), page(0, 120, "v1:2")])
        self.assertEqual(calls, [0, 1])

    def test_100_pages(self):
        # 実データ: 100ページ目は next=true、101ページ目が0件・next 空
        pages = [page(120, i * 120, f"v1:{i + 1}") for i in range(100)]
        calls, ids, ret = self.run_job(self.fn, pages)
        self.assertEqual(calls, list(range(101)))
        self.assertEqual(ret, (100, 12000))

    def test_api_error_is_not_empty(self):
        err = RuntimeError("FETCH_TIMEOUT: SEARCH_API_ERROR x")
        with self.assertRaises(RuntimeError):
            self.run_job(self.fn, [page(120, 0, "v1:1"), err])


class SoldTest(Base):
    fn = staticmethod(sw.run_fetch_sold_ebay)

    def test_stop_at_empty_token(self):
        calls, ids, ret = self.run_job(self.fn, [page(50, 0, "")])
        self.assertEqual(calls, [0])                           # 従来は 0,1（1は0件）
        self.assertEqual(ret, (1, 50))

    def test_two_page_cap(self):
        calls, ids, ret = self.run_job(self.fn, [page(120, 0, "v1:1"), page(120, 120, "v1:2"), page(120, 240, "v1:3")])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(ret, (2, 240))

    def test_second_page_last(self):
        calls, ids, ret = self.run_job(self.fn, [page(120, 0, "v1:1"), page(30, 120, "")])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(ret, (2, 150))

    def test_missing_token_falls_back(self):
        calls, ids, ret = self.run_job(self.fn, [page(50, 0)])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(ret, (2, 50))

    def test_zero_first_page(self):
        calls, ids, ret = self.run_job(self.fn, [page(0, 0, "")])
        self.assertEqual(calls, [0])
        self.assertEqual(ret, (1, 0))

    def test_api_error_is_not_empty(self):
        err = RuntimeError("FETCH_TIMEOUT: SEARCH_API_ERROR x")
        with self.assertRaises(RuntimeError):
            self.run_job(self.fn, [err])

    def test_page_error_does_not_stop_early(self):
        # 1ページ目の想定外の例外は従来どおり警告だけで次ページへ（最終ページ扱いにしない）
        calls, ids, ret = self.run_job(self.fn, [ValueError("x"), page(30, 120, "")])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(len(ids), 30)


if __name__ == "__main__":
    unittest.main()
