-- inventory_ops_tables_20261119.sql の戻し（記録用の表を消す。在庫管理本体の動作には影響しない）
-- 先に apps/inventory/ops_stats.py を使うコードを戻すか、表が無くても書き込み失敗をログに出すだけで処理は続く。
DROP TABLE IF EXISTS trx.inventory_ops_issue;
DROP TABLE IF EXISTS trx.inventory_ops_stats;
