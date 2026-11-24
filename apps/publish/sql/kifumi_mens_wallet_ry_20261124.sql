-- 2026-11-24 貴文に「財布メンズ」の R（ラクマ）・Y（Yahoo!フリマ）の新規出品を追加する（$400〜1,500）。
--
-- 円の範囲は compute_cost_range_jpy_from_usd_range('GA', 400, 1500) = (32660, 169345)（update_jpy_price_range.py と同じ換算）。
-- keep_* は NULL（その行の low/high と同じ）。財布メンズ×R の終了判定範囲は川島の行（$100〜1,500）のまま変わらない。
-- 財布メンズ×Y は川島の行が keep NULL（$100〜400）なので、終了判定範囲は $100〜1,500 に広がる。
-- lookup の収集範囲は、次のサイクルの再生成（daily_check.plan_collection_ranges）で
-- 財布メンズ×R・Y が $100〜1,500（¥5,690〜169,345）になる。M・MS とほかの組は変わらない。
-- 貴文の M・MS 行は作らない（財布メンズの M・MS は新規出品しない）。
-- 変更前: E:\backup\sqlserver\kifumi_mens_wallet_ry_20261124\

SET XACT_ABORT ON;
BEGIN TRAN;

INSERT INTO mst.presets_price_ranges
    (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, high_jpy_target,
     vendor_code, delete_flag, keep_low_usd_target, keep_high_usd_target)
SELECT N'貴文', N'財布メンズ', 400.00, 1500.00, 32660, 169345, v.vendor_code, 0, NULL, NULL
FROM (VALUES ('R'), ('Y')) AS v(vendor_code)
WHERE NOT EXISTS (
    SELECT 1 FROM mst.presets_price_ranges r
    WHERE r.preset_group = N'貴文' AND r.category_group = N'財布メンズ' AND r.vendor_code = v.vendor_code
);

IF @@ROWCOUNT <> 2
BEGIN
    RAISERROR(N'追加行数が2行ではありません。ロールバックします。', 16, 1);
    ROLLBACK TRAN;
    RETURN;
END

COMMIT TRAN;
