-- 2026-11-24 keep_* 廃止の移行段階1: presets_lookup の keep_* を NULL 可にする（値は変えない）。
-- 新しい daily_check（keep_* を書かない）と、再起動前の inventory_loop（旧 daily_check。keep_* を書く）が
-- 混在しても lookup の再生成が失敗しないようにする。列の削除は drop_keep_columns_20261124.sql。
ALTER TABLE mst.presets_lookup ALTER COLUMN keep_low_usd_target  DECIMAL(10, 2) NULL;
ALTER TABLE mst.presets_lookup ALTER COLUMN keep_high_usd_target DECIMAL(10, 2) NULL;
