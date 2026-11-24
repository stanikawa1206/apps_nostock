-- 2026-11-24 貴文に トレカ・遊戯王カード の M・MS 新規出品を追加する（$400〜1,000、Collectibles）。
-- 円の範囲は compute_cost_range_jpy_from_usd_range('DDP', 400, 1000) = (32660, 86600)。
-- 収集範囲は次のサイクルの lookup 再生成で 貴文③（$50〜100）と合わせて $50〜1,000 になる。
-- 既存出品の価格範囲外判定はアカウント自身の範囲なので、貴文③ は $50〜100 のまま（keep_* 廃止後）。
-- 前提: トレカの出品前判定（card_listing_check：単品以外・鑑定品の NG）が全台に反映済みであること。
SET XACT_ABORT ON;
BEGIN TRAN;
INSERT INTO mst.presets_price_ranges (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, high_jpy_target, vendor_code, delete_flag)
SELECT N'貴文', g.category_group, 400.00, 1000.00, 32660, 86600, v.vendor_code, 0
FROM (VALUES (N'トレカ'), (N'遊戯王カード')) AS g(category_group)
CROSS JOIN (VALUES ('M'), ('MS')) AS v(vendor_code)
WHERE NOT EXISTS (SELECT 1 FROM mst.presets_price_ranges r
                  WHERE r.preset_group = N'貴文' AND r.category_group = g.category_group AND r.vendor_code = v.vendor_code);
IF @@ROWCOUNT <> 4
BEGIN
    RAISERROR(N'追加行数が4行ではありません。ロールバックします。', 16, 1);
    ROLLBACK TRAN;
    RETURN;
END
COMMIT TRAN;
