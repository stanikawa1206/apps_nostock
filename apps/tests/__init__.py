# apps/tests のテストでは、在庫管理の実績集計（apps/inventory/ops_stats.py）を DB に書かない
# （テストは本番DBに接続する設定のまま動くものがあるため。2026-11-19）。
import os

os.environ["OPS_STATS_DISABLED"] = "1"
