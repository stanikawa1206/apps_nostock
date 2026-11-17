-- ============================================================================
-- eBay 出品の Collectibles／非Collectibles の区分ごとの管理（2026-11-17）
--
-- mst.ebay_accounts を区分ごとの6列にする:
--                     非Collectibles     Collectibles
--   月間無料出品残数   listing_left       listing_left_collectibles   … additional_listing_left を改名（値はそのまま）
--   1日の出品目標      post_target        post_target_collectibles    … 追加（publish_manager が毎日自動計算。上限 333件）
--   当日の終了理由     close_reason       close_reason_collectibles   … 追加（close_reason と同じ NVARCHAR(50)）
-- post_target・close_reason は今後 非Collectibles 専用（値は publish_manager が翌日の開始時に計算・初期化する）。
--
-- 参照: apps/publish/listing_class.py（列の対応・区分ごとの当日件数）、publish_ebay.py、publish_manager.py、
--       publish_daily.py（月初リセット）、apps/inventory/daily_check.py（完了メール）。
--       SQL Server の関数・ビュー・プロシージャ・トリガーでこれらの列を参照するものは無い（2026-11-17 確認）。
--
-- 適用の順番（出品処理 publish_ebay・publish_manager が動いていない時間に、コードと同時に切り替える）:
--   1. 全台（VPS 7台・mouse）のコードを新しい版にする（git pull。まだ起動はしない）
--   2. このファイルを実行
--   3. Access（ebay在庫管理.accdb・ebay_売上.accdb）のリンクテーブル mst_ebay_accounts を更新
--   旧コードは listing_left_collectibles の改名後に additional_listing_left を書けず、出品の記録で失敗する。
--   新コードは新しい列が無いと動かない。どちらも出品処理の外では使われない（在庫管理は参照しない）。
--
-- 今日の状態の引き継ぎ:
--   close_reason_collectibles … 今日の出品を終えたアカウント（close_reason が DONE/EMPTY/BLOCKED）は同じ値、
--                                それ以外は NULL（翌日の開始時に publish_manager が両区分とも初期化する）
--   post_target_collectibles  … 貴文③（目標を手入力するアカウント）は今の post_target（100）。
--                                それ以外は NULL（翌日の開始時に publish_manager が自動計算する。NULL の間は0件扱い）
-- 戻すとき: ebay_accounts_collectibles_split_20261117_rollback.sql（コードも戻すこと）
-- ============================================================================
SET XACT_ABORT ON;
BEGIN TRANSACTION;
GO
IF COL_LENGTH('mst.ebay_accounts', 'additional_listing_left') IS NULL
   OR COL_LENGTH('mst.ebay_accounts', 'listing_left_collectibles') IS NOT NULL
    THROW 50000, N'mst.ebay_accounts は既に変更済み（additional_listing_left が無い／listing_left_collectibles がある）', 1;
GO

EXEC sp_rename 'mst.ebay_accounts.additional_listing_left', 'listing_left_collectibles', 'COLUMN';
GO

ALTER TABLE mst.ebay_accounts ADD
    post_target_collectibles INT NULL,
    close_reason_collectibles NVARCHAR(50) NULL;
GO

UPDATE mst.ebay_accounts
SET close_reason_collectibles = close_reason
WHERE close_reason IN ('DONE', 'EMPTY', 'BLOCKED');

UPDATE mst.ebay_accounts
SET post_target_collectibles = post_target
WHERE account = N'貴文③';
GO

COMMIT TRANSACTION;
GO
