-- ============================================================================
-- presets_price_ranges_vendor_20261113.sql の戻し（2026-11-13 変更前の状態に戻す）
--
-- 先に在庫管理ループを止め（mst.control『日次処理停止』=1 でサイクル終了を待つ）、出品処理が
-- 動いていない時間に実行する。コードも変更前のコミットに戻すこと（refresh_presets_lookup・
-- check_remaining・scrape_worker が keep_*／vendor_code を使うため）。
-- 関数は fn_take_one_candidates_base_backup_20261113_before_vendor_ranges.sql を続けて適用する。
-- ============================================================================

SET XACT_ABORT ON;
BEGIN TRAN;

ALTER TABLE mst.presets_price_ranges DROP CONSTRAINT FK_presets_price_ranges_vendor_code;
ALTER TABLE mst.presets_price_ranges DROP CONSTRAINT PK_presets_price_ranges;
DELETE FROM mst.presets_price_ranges;
ALTER TABLE mst.presets_price_ranges DROP CONSTRAINT DF_mst_presets_price_ranges_delete_flag;
ALTER TABLE mst.presets_price_ranges DROP COLUMN delete_flag, vendor_code;
ALTER TABLE mst.presets_lookup DROP COLUMN keep_low_usd_target, keep_high_usd_target;
GO

-- 変更前の 17 行（E:\backup\sqlserver\presets_vendor_20261113\ の mst.presets_price_ranges.json と同じ）
INSERT INTO mst.presets_price_ranges (
    preset_group, category_group, low_usd_target, high_usd_target,
    low_jpy_target, high_jpy_target, category_priority
)
VALUES
    (N'BUZZ', N'アクセサリー', 200.00, 600.00, 14680, 50640, 1),
    (N'BUZZ②', N'財布', 100.00, 200.00, 5690, 14680, 1),
    (N'貴文', N'アクセサリー', 600.00, 1000.00, 50640, 86600, 1),
    (N'貴文', N'スカーフ', 400.00, 800.00, 32660, 68620, 1),
    (N'貴文', N'スカーフメンズ', 400.00, 800.00, 32660, 68620, 1),
    (N'貴文', N'財布', 600.00, 1000.00, 67510, 112770, 1),
    (N'貴文③', N'トレカ', 50.00, 100.00, 1195, 5690, 1),
    (N'貴文③', N'遊戯王カード', 50.00, 100.00, 1195, 5690, 1),
    (N'川島', N'スカーフメンズ', 100.00, 400.00, 5690, 32660, 1),
    (N'川島', N'財布メンズ', 100.00, 400.00, 5690, 32660, 1),
    (N'谷川②', N'デジカメ', 300.00, 700.00, 23670, 59630, 2),
    (N'谷川②', N'ビデオカメラ', 300.00, 700.00, 23670, 59630, 2),
    (N'谷川②', N'ペン', 100.00, 1500.00, 5690, 131550, 1),
    (N'谷川②', N'レンズ', 300.00, 700.00, 23670, 59630, 2),
    (N'谷川②', N'製図用品', 100.00, 1500.00, 5690, 131550, 1),
    (N'谷川③', N'財布', 300.00, 600.00, 23670, 67510, 1),
    (N'谷川④', N'腕時計', 100.00, 300.00, 5690, 23670, 1);

ALTER TABLE mst.presets_price_ranges ADD CONSTRAINT PK_presets_price_ranges_new
    PRIMARY KEY CLUSTERED (preset_group, category_group);

COMMIT;
GO
