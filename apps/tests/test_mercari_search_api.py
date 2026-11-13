# -*- coding: utf-8 -*-
"""
メルカリ検索APIの直接取得（mercari_search_api）と scrape_worker の切り替えのテスト。
モックのみ。メルカリ・DB には接続しない。

実行: python -m unittest apps.tests.test_mercari_search_api -v
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
from unittest import mock

from apps.adapters import mercari_search_api as api

_TMP = tempfile.TemporaryDirectory()
_stdout, _stderr = sys.stdout, sys.stderr
with mock.patch.dict(os.environ, {"WORKER_STATUS_DIR": _TMP.name, "WORKER_NAME": "test-worker"}):
    from apps.inventory import scrape_worker as sw
sys.stdout, sys.stderr = _stdout, _stderr
sw._log_file.close()

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "mercari_search_browser_bodies_20261104.json")
VOLATILE = {"searchSessionId", "laplaceDeviceUuid"}


class FakeResp:
    def __init__(self, status_code, js=None, text=""):
        self.status_code = status_code
        self._js = js
        self.text = text if text else json.dumps(js) if js is not None else ""

    def json(self):
        if self._js is None:
            raise ValueError("no json")
        return self._js


class BuildBodyTest(unittest.TestCase):
    def test_same_as_browser_body(self):
        """2026-11-04 に VPS のブラウザが実際に送った本文と、検索URLから組み立てた本文が一致する"""
        for fx in json.load(open(FIXTURE, encoding="utf-8")):
            got = api.build_search_body(fx["page_url"], "dev-uuid")
            want = {k: v for k, v in fx["body"].items() if k not in VOLATILE}
            self.assertEqual({k: v for k, v in got.items() if k not in VOLATILE}, want)
            self.assertEqual(got["laplaceDeviceUuid"], "dev-uuid")

    def test_page_token(self):
        url = json.load(open(FIXTURE, encoding="utf-8"))[0]["page_url"] + "&page_token=v1%3A1"
        self.assertEqual(api.build_search_body(url, "u")["pageToken"], "v1:1")

    def test_unknown_condition_rejected(self):
        url = json.load(open(FIXTURE, encoding="utf-8"))[0]["page_url"] + "&keyword=abc"
        with self.assertRaises(api.SearchApiError):
            api.build_search_body(url, "u")

    def test_conflicting_price_rejected(self):
        url = json.load(open(FIXTURE, encoding="utf-8"))[0]["page_url"] + "&price_min=1"
        with self.assertRaises(api.SearchApiError):
            api.build_search_body(url, "u")


class FetchTest(unittest.TestCase):
    def setUp(self):
        self.url = json.load(open(FIXTURE, encoding="utf-8"))[0]["page_url"]
        api._client = None
        self.p_interval = mock.patch.object(api, "MIN_INTERVAL_SEC", 0)
        self.p_log = mock.patch.object(api, "_log")
        self.p_interval.start(); self.log = self.p_log.start()

    def tearDown(self):
        self.p_interval.stop(); self.p_log.stop()
        api._client = None

    def _post(self, resp):
        c = api._get_client()
        return mock.patch.object(c.session, "post", return_value=resp)

    def test_ok(self):
        with self._post(FakeResp(200, {"items": [{"id": "m1"}], "meta": {"nextPageToken": ""}})) as post:
            js = api.fetch_search_json(self.url)
        self.assertEqual(js["items"][0]["id"], "m1")
        headers = post.call_args.kwargs["headers"]
        self.assertEqual(headers["x-platform"], "web")
        self.assertTrue(headers["dpop"].count(".") == 2)

    def test_dpop_signature_verifies(self):
        """自前の ES256 署名が公開鍵で検証できる（JWS の r||s 形式）"""
        import base64
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
        c = api._get_client()
        token = c.dpop()
        h, p, sig = token.split(".")
        dec = lambda x: base64.urlsafe_b64decode(x + "=" * (-len(x) % 4))
        header, payload = json.loads(dec(h)), json.loads(dec(p))
        self.assertEqual((header["typ"], header["alg"]), ("dpop+jwt", "ES256"))
        self.assertEqual((payload["htm"], payload["htu"], payload["uuid"]), ("POST", api.API_URL, c.device_uuid))
        raw = dec(sig)
        self.assertEqual(len(raw), 64)
        der = encode_dss_signature(int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big"))
        c.key.public_key().verify(der, f"{h}.{p}".encode(), ec.ECDSA(hashes.SHA256()))  # 失敗なら例外

    def test_zero_items_is_ok(self):
        with self._post(FakeResp(200, {"items": [], "meta": {}})):
            self.assertEqual(api.fetch_search_json(self.url)["items"], [])

    def test_blocked_statuses(self):
        for st in (401, 403, 429):
            with self._post(FakeResp(st, text="denied")):
                with self.assertRaises(api.SearchApiBlocked):
                    api.fetch_search_json(self.url)

    def test_other_errors(self):
        for resp in (FakeResp(500, text="x"), FakeResp(400, text="x"), FakeResp(200, None, text="<html>"),
                     FakeResp(200, {"meta": {}}), FakeResp(200, {"items": []})):
            with self._post(resp):
                with self.assertRaises(api.SearchApiError):
                    api.fetch_search_json(self.url)

    def test_network_error(self):
        c = api._get_client()
        with mock.patch.object(c.session, "post", side_effect=api.requests.ConnectionError("down")):
            with self.assertRaises(api.SearchApiError):
                api.fetch_search_json(self.url)

    def test_min_interval(self):
        with mock.patch.object(api, "MIN_INTERVAL_SEC", 17), mock.patch.object(api.time, "sleep") as sl, \
                self._post(FakeResp(200, {"items": [], "meta": {}})):
            api.fetch_search_json(self.url)
            api.fetch_search_json(self.url)
        waits = [c.args[0] for c in sl.call_args_list]
        self.assertEqual(len(waits), 1)
        self.assertGreater(waits[0], 16)


class WorkerSwitchTest(unittest.TestCase):
    def setUp(self):
        # 本番の状態ファイル（プロジェクト直下）に触れないよう、一時フォルダのファイルに差し替える
        # （以前は本番のファイルを作成・削除しており、稼働中のワーカーの取得方式が変わる恐れがあった）
        self.tmp = tempfile.TemporaryDirectory()
        self.patches = [
            mock.patch.object(sw, "SEARCH_FETCH_MODE", "api"),
            mock.patch.object(sw, "SEARCH_API_BLOCKED_FILE", os.path.join(self.tmp.name, "search_api_blocked.txt")),
            mock.patch.object(sw, "SEARCH_API_FALLBACK_FILE", os.path.join(self.tmp.name, "search_api_fallback.txt")),
        ]
        for p in self.patches:
            p.start()
        sw._search_api_fail_streak = 0

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def _make_old_fallback_file(self, age_sec):
        with open(sw.SEARCH_API_FALLBACK_FILE, "w", encoding="utf-8") as f:
            f.write("old\n")
        t = time.time() - age_sec
        os.utime(sw.SEARCH_API_FALLBACK_FILE, (t, t))

    def test_fallback_returns_to_api_after_interval(self):
        self._make_old_fallback_file(sw.SEARCH_API_PROBE_INTERVAL_SEC + 1)
        page = mock.Mock()
        with mock.patch("apps.adapters.mercari_search_api.fetch_search_json", return_value={"items": [1]}), \
                contextlib.redirect_stdout(io.StringIO()):
            data = sw.fetch_page_json(page, "https://jp.mercari.com/search?x", None, 1)
        self.assertEqual(data, {"items": [1]})
        self.assertFalse(os.path.exists(sw.SEARCH_API_FALLBACK_FILE))
        self.assertTrue(sw.use_search_api())
        page.goto.assert_not_called()  # ブラウザでは取得していない

    def test_fallback_return_failure_keeps_browser_and_delays_next_probe(self):
        self._make_old_fallback_file(sw.SEARCH_API_PROBE_INTERVAL_SEC + 1)
        with mock.patch("apps.adapters.mercari_search_api.fetch_search_json",
                        side_effect=api.SearchApiError("dns")), \
                mock.patch.object(sw, "_fetch_page_json_attempts", return_value={"items": [2]}) as browser_fetch, \
                contextlib.redirect_stdout(io.StringIO()):
            data = sw.fetch_page_json(mock.Mock(), "https://jp.mercari.com/search?x", None, 1)
        self.assertEqual(data, {"items": [2]})   # ブラウザ方式で取得した結果
        browser_fetch.assert_called_once()
        self.assertTrue(os.path.exists(sw.SEARCH_API_FALLBACK_FILE))
        self.assertFalse(sw._search_api_probe_due())   # 更新時刻が進み、次の確認は間隔の後

    def test_fallback_not_probed_before_interval(self):
        self._make_old_fallback_file(60)
        with mock.patch("apps.adapters.mercari_search_api.fetch_search_json") as api_fetch, \
                mock.patch.object(sw, "_fetch_page_json_attempts", return_value={"items": []}), \
                contextlib.redirect_stdout(io.StringIO()):
            sw.fetch_page_json(mock.Mock(), "https://jp.mercari.com/search?x", None, 1)
        api_fetch.assert_not_called()

    def test_fallback_probe_blocked_stops_host(self):
        self._make_old_fallback_file(sw.SEARCH_API_PROBE_INTERVAL_SEC + 1)
        with mock.patch("apps.adapters.mercari_search_api.fetch_search_json",
                        side_effect=api.SearchApiBlocked("status=403")), \
                mock.patch.object(sw, "_fetch_page_json_attempts") as browser_fetch, \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as cm:
                sw.fetch_page_json(mock.Mock(), "https://jp.mercari.com/search?x", None, 1)
        self.assertIn("SEARCH_API_BLOCKED", str(cm.exception))
        self.assertTrue(os.path.exists(sw.SEARCH_API_BLOCKED_FILE))
        browser_fetch.assert_not_called()

    def test_browser_mode_host_never_probes(self):
        self._make_old_fallback_file(sw.SEARCH_API_PROBE_INTERVAL_SEC + 1)
        with mock.patch.object(sw, "SEARCH_FETCH_MODE", "browser"), \
                mock.patch("apps.adapters.mercari_search_api.fetch_search_json") as api_fetch, \
                mock.patch.object(sw, "_fetch_page_json_attempts", return_value={"items": []}), \
                contextlib.redirect_stdout(io.StringIO()):
            sw.fetch_page_json(mock.Mock(), "https://jp.mercari.com/search?x", None, 1)
        api_fetch.assert_not_called()

    def call(self, side_effect):
        with mock.patch("apps.adapters.mercari_search_api.fetch_search_json", side_effect=side_effect), \
                contextlib.redirect_stdout(io.StringIO()):
            return sw.fetch_page_json(None, "https://jp.mercari.com/search?x", None, 1)

    def test_default_mode_is_browser(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SEARCH_FETCH_MODE", None)
            self.assertEqual(os.environ.get("SEARCH_FETCH_MODE", "browser"), "browser")
        with mock.patch.object(sw, "SEARCH_FETCH_MODE", "browser"):
            self.assertFalse(sw.use_search_api())

    def test_ok_returns_json(self):
        self.assertEqual(self.call([{"items": [], "meta": {}}]), {"items": [], "meta": {}})

    def test_blocked_writes_flag_and_releases_job(self):
        with self.assertRaises(RuntimeError) as cm:
            self.call(api.SearchApiBlocked("status=403"))
        self.assertIn("FETCH_TIMEOUT", str(cm.exception))   # → main で release_job（pending へ戻す）
        self.assertTrue(os.path.exists(sw.SEARCH_API_BLOCKED_FILE))

    def test_errors_fall_back_after_streak(self):
        for i in range(sw.SEARCH_API_FALLBACK_AFTER):
            self.assertTrue(sw.use_search_api())
            with self.assertRaises(RuntimeError) as cm:
                self.call(api.SearchApiError("status=500"))
            self.assertIn("FETCH_TIMEOUT", str(cm.exception))
        self.assertTrue(os.path.exists(sw.SEARCH_API_FALLBACK_FILE))
        self.assertFalse(sw.use_search_api())

    def test_success_resets_streak(self):
        for _ in range(sw.SEARCH_API_FALLBACK_AFTER - 1):
            with self.assertRaises(RuntimeError):
                self.call(api.SearchApiError("x"))
        self.call([{"items": [], "meta": {}}])
        self.assertEqual(sw._search_api_fail_streak, 0)
        self.assertFalse(os.path.exists(sw.SEARCH_API_FALLBACK_FILE))

    def test_fallback_without_browser_raises_browser_broken(self):
        open(sw.SEARCH_API_FALLBACK_FILE, "w").close()
        with self.assertRaises(RuntimeError) as cm:
            sw.fetch_page_json(None, "https://jp.mercari.com/search?x", None, 1)
        self.assertIn("BROWSER_BROKEN", str(cm.exception))

    def test_sold_page_error_is_not_swallowed(self):
        """売り切れ取得のページ単位の try で握りつぶされず、ジョブごと pending へ戻る（売り切れ漏れにしない）"""
        payload = {"preset": "p", "vendor_name": "メルカリ", "brand_id": 1, "category_id": 2, "mode": "DDP",
                   "low_usd_target": 100, "high_usd_target": 200, "search_low_usd_target": 100, "search_high_usd_target": 200}
        with mock.patch.object(sw, "get_sql_server_connection"), mock.patch.object(sw, "write_status"), \
                mock.patch.object(sw, "make_search_url", return_value="https://jp.mercari.com/search?x"), \
                mock.patch("apps.adapters.mercari_search_api.fetch_search_json",
                           side_effect=api.SearchApiError("x")), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as cm:
                sw.run_fetch_sold_ebay(None, 0, payload, 1)
        self.assertIn("FETCH_TIMEOUT", str(cm.exception))

    def test_active_page_error_propagates(self):
        payload = {"preset": "p", "vendor_name": "メルカリ", "brand_id": 1, "category_id": 2, "mode": "DDP",
                   "low_usd_target": 100, "high_usd_target": 200, "search_low_usd_target": 100, "search_high_usd_target": 200}
        with mock.patch.object(sw, "get_sql_server_connection"), mock.patch.object(sw, "write_status"), \
                mock.patch.object(sw, "make_search_url", return_value="https://jp.mercari.com/search?x"), \
                mock.patch("apps.adapters.mercari_search_api.fetch_search_json",
                           side_effect=api.SearchApiError("x")), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(RuntimeError) as cm:
                sw.run_fetch_active_ebay(None, 0, payload, 1)
        self.assertIn("FETCH_TIMEOUT", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
