-- presets_categories_is_active_20261116.sql の戻し。先に is_active=0 の行（pen_brand_categories_20261116_rollback.sql）を消し、
-- lookup 生成のコードを戻してから流す（新しいコードは列が無いと lookup 再生成で失敗する）。
SET XACT_ABORT ON;
IF EXISTS (SELECT 1 FROM mst.presets_categories WHERE is_active = 0) THROW 50001, N'is_active=0 の行が残っている', 1;
ALTER TABLE mst.presets_categories DROP CONSTRAINT DF_presets_categories_is_active;
ALTER TABLE mst.presets_categories DROP COLUMN is_active;
