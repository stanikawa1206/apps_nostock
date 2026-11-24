-- presets_lookup_keep_nullable_20261124.sql の取り消し（NULL の行が無いことを確認してから）。
ALTER TABLE mst.presets_lookup ALTER COLUMN keep_low_usd_target  DECIMAL(10, 2) NOT NULL;
ALTER TABLE mst.presets_lookup ALTER COLUMN keep_high_usd_target DECIMAL(10, 2) NOT NULL;
