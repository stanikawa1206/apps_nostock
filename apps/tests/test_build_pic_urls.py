# -*- coding: utf-8 -*-
"""
publish_ebay.build_pic_urls が、画像サイズ確認と同じ ?付きURLを eBay に渡すことのテスト
（2026-10-26 川島 237145561379: ?なしURLで差し替え前の464px画像がeBayに渡った）。
実行: python -m unittest apps.tests.test_build_pic_urls -v
"""

import unittest
from unittest import mock

from apps.publish import publish_ebay as pe

URLS = [
    "https://static.mercdn.net/item/detail/orig/photos/m29554890546_1.jpg?1793019884",
    "https://img.fril.jp/img/862674150/l/2992449461.jpg?1793751393#top",
    "https://assets.mercari-shops-static.com/-/large/plain/2JYxXahu8j9Fi4BAWtxcRo.jpg@jpg",
]
EXPECTED = [
    "https://static.mercdn.net/item/detail/orig/photos/m29554890546_1.jpg?1793019884",
    "https://img.fril.jp/img/862674150/l/2992449461.jpg?1793751393",
    "https://assets.mercari-shops-static.com/-/large/plain/2JYxXahu8j9Fi4BAWtxcRo.jpg@jpg",
]


def _build(mode, cdn_cache=None, **kw):
    return pe.build_pic_urls(rec={"images": URLS}, sku="SKU1", image_mode=mode, r2=None,
                             r2_bucket=kw.get("bucket", ""), r2_public_base=kw.get("base", ""),
                             cdn_cache={} if cdn_cache is None else cdn_cache)


class BuildPicUrlsTest(unittest.TestCase):
    def test_normal_keeps_query(self):
        self.assertEqual(_build("NORMAL"), EXPECTED)

    def test_cdn_downloads_query_url(self):
        with mock.patch.object(pe, "upload_image_to_r2", side_effect=lambda r2, b, base, u, key: f"{base}/{key}") as up:
            urls = _build("CDN", bucket="b", base="https://cdn")
        self.assertEqual([c.args[3] for c in up.call_args_list], EXPECTED)
        self.assertEqual(urls, ["https://cdn/SKU1/1.jpg", "https://cdn/SKU1/2.jpg", "https://cdn/SKU1/3.jpg"])


if __name__ == "__main__":
    unittest.main()
