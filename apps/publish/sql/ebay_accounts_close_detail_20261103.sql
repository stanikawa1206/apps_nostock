-- 2026-11-03 eBayの新規出品制限（25019 Tutorial_Block_7D_NR）対応
-- close_reason='BLOCKED' の時のエラー内容（検知時刻・errorId・制限名・SKU・PC・eBayの説明文）を残す列。
-- publish_manager の起動時リセット（reset_close_status）で close_reason と一緒に NULL へ戻る。
IF COL_LENGTH('mst.ebay_accounts', 'close_detail') IS NULL
    ALTER TABLE mst.ebay_accounts ADD close_detail NVARCHAR(1000) NULL;
