-- 段階2 有効化（2026-11-16）: 谷川② の5グループに Yahoo!フリマ の価格帯の行を追加する。
-- 価格帯・category_priority はメルカリ(M)の行と同じ。keep_* は NULL（Y の既存出品は無い）。
-- 次の inventory_loop サイクル開始時の lookup 再生成から在庫取得が始まり、その後の 3:00 の出品から候補になる。
-- 前提: 段階1（yahoo_tanikawa2_master_20261116.sql）とコードの本番反映。
SET XACT_ABORT ON;
BEGIN TRAN;
INSERT INTO mst.presets_price_ranges (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, high_jpy_target, category_priority, vendor_code, delete_flag, keep_low_usd_target, keep_high_usd_target) VALUES (N'谷川②', N'デジカメ', 300.00, 700.00, 23670, 59630, 2, 'Y', 0, NULL, NULL);
INSERT INTO mst.presets_price_ranges (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, high_jpy_target, category_priority, vendor_code, delete_flag, keep_low_usd_target, keep_high_usd_target) VALUES (N'谷川②', N'ビデオカメラ', 300.00, 700.00, 23670, 59630, 2, 'Y', 0, NULL, NULL);
INSERT INTO mst.presets_price_ranges (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, high_jpy_target, category_priority, vendor_code, delete_flag, keep_low_usd_target, keep_high_usd_target) VALUES (N'谷川②', N'レンズ', 300.00, 700.00, 23670, 59630, 2, 'Y', 0, NULL, NULL);
INSERT INTO mst.presets_price_ranges (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, high_jpy_target, category_priority, vendor_code, delete_flag, keep_low_usd_target, keep_high_usd_target) VALUES (N'谷川②', N'ペン', 100.00, 1500.00, 5690, 131550, 1, 'Y', 0, NULL, NULL);
INSERT INTO mst.presets_price_ranges (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, high_jpy_target, category_priority, vendor_code, delete_flag, keep_low_usd_target, keep_high_usd_target) VALUES (N'谷川②', N'製図用品', 100.00, 1500.00, 5690, 131550, 1, 'Y', 0, NULL, NULL);
COMMIT;
