-- ============================================================================
-- ebay_accounts_collectibles_split_20261117.sql の取り消し（2026-11-17）
-- listing_left_collectibles を additional_listing_left に戻し（値はそのまま）、追加した2列を削除する。
-- 先に全台のコードを変更前の版に戻し、出品処理が動いていない時間に実行する。実行後に Access のリンクを更新する。
-- 削除する2列の値（当日の目標・終了理由）は翌日の開始時に作り直す値なので保存しない。
-- ============================================================================
SET XACT_ABORT ON;
BEGIN TRANSACTION;
GO
IF COL_LENGTH('mst.ebay_accounts', 'listing_left_collectibles') IS NULL
    THROW 50000, N'mst.ebay_accounts.listing_left_collectibles が無い（変更前の状態）', 1;
GO

ALTER TABLE mst.ebay_accounts DROP COLUMN post_target_collectibles, close_reason_collectibles;
GO

EXEC sp_rename 'mst.ebay_accounts.listing_left_collectibles', 'additional_listing_left', 'COLUMN';
GO

COMMIT TRANSACTION;
GO
