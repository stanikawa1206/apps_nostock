# -*- coding: utf-8 -*-
"""
テストが本番の logs/ に書かないことの確認（2026-11-20）。仕組みは apps/tests/__init__.py。
実行: python -m unittest apps.tests.test_log_isolation
"""
import logging
import unittest

from apps import tests


class LogIsolationTest(unittest.TestCase):
    def assert_in_test_dir(self, logger):
        files = [h.baseFilename for h in logger.handlers if isinstance(h, logging.FileHandler)]
        self.assertTrue(files)
        for f in files:
            self.assertTrue(f.startswith(str(tests.TEST_LOG_DIR)), f)

    def test_ebay_watchdog_log(self):
        from apps.inventory import ebay_watchdog
        self.assert_in_test_dir(ebay_watchdog.log)

    def test_inventory_monitor_log(self):
        from apps.inventory import inventory_monitor
        self.assert_in_test_dir(inventory_monitor._logger)

    def test_prod_log_path_is_redirected(self):
        h = logging.FileHandler(tests.PROD_LOG_DIR / "sub" / "x.log", encoding="utf-8")
        try:
            self.assertEqual(h.baseFilename, str(tests.TEST_LOG_DIR / "sub" / "x.log"))
        finally:
            h.close()

    def test_check_remaining_log(self):
        from apps.inventory import check_remaining_ebay as cr
        self.assertEqual(cr.LOG_DIR, str(tests.TEST_LOG_DIR))


if __name__ == "__main__":
    unittest.main()
