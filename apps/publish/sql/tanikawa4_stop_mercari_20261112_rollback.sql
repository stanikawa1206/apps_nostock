-- tanikawa4_stop_mercari_20261112.sql の戻し（谷川④の M・MS 新規出品を再開）
-- 戻すと漸減も止まる。消えた lookup は次のサイクルの再生成で元に戻る。
DELETE FROM mst.ebay_account_excluded_vendors
WHERE account = N'谷川④' AND vendor_code IN ('M', 'MS');
