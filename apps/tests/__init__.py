# apps/tests のテストでは、在庫管理の実績集計（apps/inventory/ops_stats.py）を DB に書かない
# （テストは本番DBに接続する設定のまま動くものがあるため。2026-11-19）。
#
# 本番の logs/ にもテストのログを書かない（2026-11-20）。多くのモジュールは import 時に logs/ のファイルへ
# logging.FileHandler を付けるため、テストで通知の処理を呼ぶと本番ログに「通知しました」等の行が残り、
# 本物の通知と区別できなくなった（11/19 23:20:55 ebay_watchdog.log）。logs/ 配下を指す FileHandler は
# テスト用の一時ディレクトリへ向け直す。logging を使わない check_remaining_ebay は NOSTOCK_LOG_DIR を見る。
import logging
import os
import tempfile
from pathlib import Path

os.environ["OPS_STATS_DISABLED"] = "1"

PROD_LOG_DIR = (Path(__file__).resolve().parents[2] / "logs").resolve()
TEST_LOG_DIR = Path(tempfile.mkdtemp(prefix="nostock_test_logs_"))
os.environ["NOSTOCK_LOG_DIR"] = str(TEST_LOG_DIR)

_file_handler_init = logging.FileHandler.__init__


def _test_file_handler_init(self, filename, *args, **kwargs):
    path = Path(os.fspath(filename)).resolve()
    if path == PROD_LOG_DIR or PROD_LOG_DIR in path.parents:
        path = TEST_LOG_DIR / path.relative_to(PROD_LOG_DIR)
        path.parent.mkdir(parents=True, exist_ok=True)
    _file_handler_init(self, path, *args, **kwargs)


logging.FileHandler.__init__ = _test_file_handler_init
