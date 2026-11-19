# -*- coding: utf-8 -*-
"""
mercari_item_status.shops_api_out_of_stock のテスト（2026-11-19）。DB・ネットワークは使わない。
実行: python -m unittest apps.tests.test_shops_api_stock
"""
import unittest

from apps.adapters.mercari_item_status import shops_api_out_of_stock

PID = "2JYbetoqFF5bWJB7bAkiko"


def js(*variants, name=PID):
    return {"name": name, "productDetail": {"variants": [{"quantity": q, "soldOut": s} for q, s in variants]}}


class ShopsApiOutOfStockTest(unittest.TestCase):
    def test_quantity_zero_and_sold_out(self):
        self.assertIs(shops_api_out_of_stock(js(("0", True)), PID), True)

    def test_quantity_zero_without_sold_out_flag(self):
        # 2JTKtfVKV2i5sd9bLXdThE（2026-11-19）: quantity '0'・soldOut false、ページは購入ボタン無効
        self.assertIs(shops_api_out_of_stock(js(("0", False)), PID), True)

    def test_sold_out_flag(self):
        self.assertIs(shops_api_out_of_stock(js(("1", True)), PID), True)

    def test_in_stock(self):
        self.assertIs(shops_api_out_of_stock(js(("1", False)), PID), False)
        # 数量を選べる商品（2JUgYuUVZySYRbdUCEPW8K、在庫3）
        self.assertIs(shops_api_out_of_stock(js(("3", False)), PID), False)

    def test_some_variant_in_stock(self):
        self.assertIs(shops_api_out_of_stock(js(("0", True), ("2", False)), PID), False)

    def test_all_variants_out(self):
        self.assertIs(shops_api_out_of_stock(js(("0", True), ("0", False)), PID), True)

    def test_unknown(self):
        self.assertIsNone(shops_api_out_of_stock(None, PID))
        self.assertIsNone(shops_api_out_of_stock({}, PID))
        self.assertIsNone(shops_api_out_of_stock(js(), PID))
        # 別の商品の応答は使わない
        self.assertIsNone(shops_api_out_of_stock(js(("0", True), name="2JOTHERxxxxxxxxxxxxxxx"), PID))


if __name__ == "__main__":
    unittest.main()
