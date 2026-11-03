# -*- coding: utf-8 -*-
"""
eBay の出品失敗を「ポリシーNG」（恒久的に出品候補から除外）に分類する処理のテスト（2026-11-03 修正）。

  - ebay_api._extract_error_details: publishOffer の parameters から説明文1つと規則名だけを取り出す
  - classify_ebay_error: 25019 の商品単位の違反だけ ポリシーNG。アカウント制限の文言を含む 25019 は除く。
    "cannot contain javascript" は従来どおり ポリシーNG。"Cannot revise listing" でも 25019 以外は出品失敗
  - post_to_ebay: 出品状況詳細にエラーコード・メッセージ・説明文を残す。説明文が無いエラーの詳細は従来と同じ
  - 7日間制限（Tutorial_Block_7D_NR）は従来どおり AccountListingBlockedError → 出品停止(出品制限)

DB・eBay には接続しない。
実行: python -m unittest apps.tests.test_policy_ng_classification -v
"""

import copy
import unittest
from unittest import mock

from apps.adapters import ebay_api
from apps.adapters.ebay_api import AccountListingBlockedError, ApiHandledError
from apps.publish import publish_ebay as pe
from apps.tests.test_account_listing_block import BLOCK_RESPONSE, FakeResponse, _call_post_to_ebay, _heavy

REVISE_MSG = ("Cannot revise listing. The item cannot be listed or modified. The title and/or description may "
              "contain improper words, or the listing or seller may be in violation of eBay policy.")

LIKE_NEW_HTML = ("We don&rsquo;t allow phrases such as &quot;like new&quot; in your title or description because the "
                 "listing will show up in search results for new items.<br />\n<br />\nFor more information, see our "
                 "<a href=http://pages.ebay.com/help/policies/search-manipulation.html target=_blank>search and browse "
                 "manipulation policy</a>.<font color=#757575 size=1>{e301542-1246334x}</font>")

# 2026-11-03 のログの応答（説明文は一部省略）
LIKE_NEW_RESPONSE = {"errors": [{"errorId": 25019, "message": REVISE_MSG, "parameters": [
    {"name": "0", "value": LIKE_NEW_HTML},
    {"name": "1", "value": "We don’t allow phrases such as like new in your title ... see our {URL0}."},
    {"name": "2", "value": "LP_KWS_LikeNew"},
    {"name": "3", "value": "1246334"},
    {"name": "4", "value": "http://pages.ebay.com/help/policies/search-manipulation.html"},
    {"name": "5", "value": "search and browse manipulation policy"},
    {"name": "6", "value": "{URL0}"},
    {"name": "7", "value": LIKE_NEW_HTML},
]}]}

LIKE_NEW_TEXT = ("We don’t allow phrases such as \"like new\" in your title or description because the listing will "
                 "show up in search results for new items. For more information, see our search and browse "
                 "manipulation policy .")

IVORY_RESPONSE = {"errors": [{"errorId": 25019, "message": REVISE_MSG, "parameters": [
    {"name": "0", "value": "It appears you&rsquo;re listing an item made from ivory. These items may not be sold on eBay."
                           "<font color=#757575 size=1>{e301542-1262737x}</font>"},
    {"name": "2", "value": "PI_APW_Ivory_ItemSpecifics_Block_102014"},
    {"name": "3", "value": "1262737"},
]}]}

# 7日間以外のアカウント制限（制限名が違う場合。商品のポリシーNGにはしない）
OTHER_BLOCK_RESPONSE = copy.deepcopy(BLOCK_RESPONSE)
for _p in OTHER_BLOCK_RESPONSE["errors"][0]["parameters"]:
    if _p["value"] == "Tutorial_Block_7D_NR":
        _p["value"] = "Tutorial_Block_30D_NR"


def _publish(body):
    with mock.patch.object(ebay_api.requests, "post", return_value=FakeResponse(400, body)), \
            mock.patch("builtins.print"):
        ebay_api.publish_offer("OFFER1", "TOKEN")


def _publish_error(body):
    try:
        _publish(body)
    except ApiHandledError as e:
        return e
    raise AssertionError("例外が出なかった")


class ExtractDetailsTest(unittest.TestCase):
    def test_like_new(self):
        self.assertEqual(ebay_api._extract_error_details(LIKE_NEW_RESPONSE), [LIKE_NEW_TEXT, "LP_KWS_LikeNew"])

    def test_ivory(self):
        self.assertEqual(ebay_api._extract_error_details(IVORY_RESPONSE), [
            "It appears you’re listing an item made from ivory. These items may not be sold on eBay.",
            "PI_APW_Ivory_ItemSpecifics_Block_102014",
        ])

    def test_no_parameters(self):
        self.assertEqual(ebay_api._extract_error_details({"errors": [{"errorId": 25002, "message": "x"}]}), [])
        self.assertEqual(ebay_api._extract_error_details({}), [])

    def test_publish_offer_sets_details(self):
        e = _publish_error(LIKE_NEW_RESPONSE)
        self.assertNotIsInstance(e, AccountListingBlockedError)
        self.assertEqual((e.code, e.details), (25019, [LIKE_NEW_TEXT, "LP_KWS_LikeNew"]))

    def test_7d_block_unchanged(self):
        e = _publish_error(BLOCK_RESPONSE)
        self.assertIsInstance(e, AccountListingBlockedError)
        self.assertEqual(e.block_name, "Tutorial_Block_7D_NR")

    def test_errors_without_details_default_empty(self):
        self.assertEqual(ApiHandledError(25002, "x").details, [])


class ClassifyTest(unittest.TestCase):
    def test_item_level_25019_is_policy_ng(self):
        self.assertEqual(pe.classify_ebay_error(_publish_error(LIKE_NEW_RESPONSE)), "ポリシーNG")
        self.assertEqual(pe.classify_ebay_error(_publish_error(IVORY_RESPONSE)), "ポリシーNG")

    def test_25019_without_details_is_policy_ng(self):
        # register/create など説明文を取らない経路の 25019（従来どおり商品単位とみなす）
        self.assertEqual(pe.classify_ebay_error(ApiHandledError(25019, REVISE_MSG)), "ポリシーNG")

    def test_account_restriction_25019_is_not_policy_ng(self):
        e = _publish_error(OTHER_BLOCK_RESPONSE)
        self.assertNotIsInstance(e, AccountListingBlockedError)  # 自動終了は7日間制限だけ（7da1209 の仕様）
        self.assertIsNone(pe.classify_ebay_error(e))

    def test_javascript_is_policy_ng(self):
        e = ApiHandledError(25002, 'A user error has occurred. Your listing cannot contain javascript (".cookie", "cookie(")')
        self.assertEqual(pe.classify_ebay_error(e), "ポリシーNG")

    def test_cannot_revise_with_other_code_is_not_policy_ng(self):
        self.assertIsNone(pe.classify_ebay_error(ApiHandledError(25002, REVISE_MSG)))

    def test_other_errors(self):
        for code, msg in [(25002, "A user error has occurred. It looks like this listing is for an item you already have"),
                          (25604, "Input error. Product not found."), (25001, "A system error has occurred.")]:
            self.assertIsNone(pe.classify_ebay_error(ApiHandledError(code, msg)))


class PostToEbayDetailTest(unittest.TestCase):
    def setUp(self):
        for p in (mock.patch.object(pe, "build_pic_urls", return_value=[]),
                  mock.patch.object(pe, "upsert_vendor_item"),
                  mock.patch("builtins.print")):
            p.start()
            self.addCleanup(p.stop)

    def _post(self, exc):
        heavy = _heavy()
        with mock.patch.object(pe, "post_one_item", side_effect=exc):
            result = _call_post_to_ebay(mock.Mock(), heavy)
        self.assertEqual(len(result), 10)
        self.assertFalse(result[9])  # listing_limit_hit（LIMIT扱いにならない）
        return heavy["rec"]

    def test_policy_ng_keeps_detail(self):
        rec = self._post(_publish_error(LIKE_NEW_RESPONSE))
        self.assertEqual(rec["listing_head"], "ポリシーNG")
        self.assertEqual(rec["listing_detail"], f"25019: {REVISE_MSG} / {LIKE_NEW_TEXT} / LP_KWS_LikeNew")
        self.assertEqual(rec["processing_at"], "2026-11-03")  # 当日は再取得しない（従来どおり）

    def test_account_restriction_is_failed_not_policy_ng(self):
        rec = self._post(_publish_error(OTHER_BLOCK_RESPONSE))
        self.assertEqual(rec["listing_head"], "出品失敗")
        self.assertIn("Tutorial_Block_30D_NR", rec["listing_detail"])
        self.assertIn("restricted your ability to list new items", rec["listing_detail"])

    def test_other_error_detail_unchanged(self):
        e = ApiHandledError(25002, "A user error has occurred. It looks like this listing is for an item you already have")
        rec = self._post(e)
        self.assertEqual(rec["listing_head"], "出品失敗")
        self.assertEqual(rec["listing_detail"], str(e))  # 説明文の無いエラーは従来と同じ内容

    def test_7d_block_still_blocked(self):
        heavy = _heavy()
        with mock.patch.object(pe, "post_one_item", side_effect=_publish_error(BLOCK_RESPONSE)):
            with self.assertRaises(AccountListingBlockedError):
                _call_post_to_ebay(mock.Mock(), heavy)
        self.assertEqual(heavy["rec"]["listing_head"], "出品停止(出品制限)")


if __name__ == "__main__":
    unittest.main()
