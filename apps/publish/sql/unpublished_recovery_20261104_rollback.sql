-- unpublished_recovery_20261104.sql の取り消し（保留中の行が無いことを確認してから実行する）
-- SELECT * FROM trx.unpublished_recovery WHERE status IN ('pending', 'running');
IF OBJECT_ID('trx.unpublished_recovery', 'U') IS NOT NULL
    DROP TABLE trx.unpublished_recovery;
