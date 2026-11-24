-- kifumi_tcg_mms_20261124.sql の取り消し。貴文のトレカ出品が残っている間は削除せず delete_flag=1 にする:
--   UPDATE mst.presets_price_ranges SET delete_flag = 1 WHERE preset_group = N'貴文' AND category_group IN (N'トレカ', N'遊戯王カード') AND vendor_code IN ('M', 'MS');
DELETE FROM mst.presets_price_ranges
WHERE preset_group = N'貴文' AND category_group IN (N'トレカ', N'遊戯王カード') AND vendor_code IN ('M', 'MS');
