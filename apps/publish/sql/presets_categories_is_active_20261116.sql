-- mst.presets_categories に is_active を足す（2026-11-16）。既存の行はすべて 1（今と同じ）。
-- lookup 生成（daily_check.load_lookup_sources）は is_active=1 の行だけを使う。0 の行は「マスターとして持つが preset を作らない」。
-- 古いコードは列を選ばないので、列を足しても今の動きは変わらない（コードより先に流す）。
SET XACT_ABORT ON;
IF COL_LENGTH('mst.presets_categories', 'is_active') IS NULL
    ALTER TABLE mst.presets_categories ADD is_active BIT NOT NULL CONSTRAINT DF_presets_categories_is_active DEFAULT (1);
