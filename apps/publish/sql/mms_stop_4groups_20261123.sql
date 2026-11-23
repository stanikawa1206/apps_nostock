-- 2026-11-23 アクセサリー・スカーフ・スカーフメンズ・財布メンズの M・MS（メルカリ・メルカリshops）の新規出品を停止する。
-- Collectibles（キーホルダー）も含めて止める。R・Y の行は変えない。
--
-- delete_flag=1 の行からは出品候補を作らない（dbo.fn_take_one_candidates_base）。
-- 4グループ×M・MS の組は全行が delete_flag=1 になるので、在庫管理の lookup 再生成（daily_check.plan_collection_ranges）で
-- 谷川④・腕時計と同じ漸減の対象になる: 未終了の eBay 出品（全アカウント）がある preset の lookup だけ残し、
-- 出品が無くなった lookup から順に作らなくなる。既存出品は終了させない（収集範囲・keep 範囲は変わらない）。
-- マスター（presets_categories・category_group_categories・ブランド）は変えないので、rollback で元に戻せる。
-- 変更前の全行: E:\backup\sqlserver\mms_stop_4groups_20261123\presets_price_ranges_before.csv

SET XACT_ABORT ON;
BEGIN TRAN;

UPDATE mst.presets_price_ranges
SET delete_flag = 1
WHERE category_group IN (N'アクセサリー', N'スカーフ', N'スカーフメンズ', N'財布メンズ')
  AND vendor_code IN ('M', 'MS')
  AND delete_flag = 0;

IF @@ROWCOUNT <> 12
BEGIN
    RAISERROR(N'更新行数が12行ではありません。ロールバックします。', 16, 1);
    ROLLBACK TRAN;
    RETURN;
END

COMMIT TRAN;
