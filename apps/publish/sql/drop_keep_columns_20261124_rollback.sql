-- drop_keep_columns_20261124.sql の取り消し（列を戻すだけ。旧コードに戻す場合は keep の値も戻すこと）。
-- presets_price_ranges の keep_* の値は E:\backup\sqlserver\keep_removal_20261124\presets_price_ranges_before.csv にある。
-- presets_lookup の keep_* は旧 daily_check の再生成で作り直される（NULL 可で追加しておく）。
ALTER TABLE mst.presets_price_ranges ADD keep_low_usd_target DECIMAL(10, 2) NULL, keep_high_usd_target DECIMAL(10, 2) NULL;
ALTER TABLE mst.presets_lookup ADD keep_low_usd_target DECIMAL(10, 2) NULL, keep_high_usd_target DECIMAL(10, 2) NULL;
