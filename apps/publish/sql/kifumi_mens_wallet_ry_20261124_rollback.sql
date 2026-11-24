-- kifumi_mens_wallet_ry_20261124.sql の取り消し（貴文の財布メンズ R・Y の行を削除する）。
-- 貴文の財布メンズの出品が残っている間は、物理削除ではなく delete_flag=1 にして在庫管理を続けること:
--   UPDATE mst.presets_price_ranges SET delete_flag = 1
--   WHERE preset_group = N'貴文' AND category_group = N'財布メンズ' AND vendor_code IN ('R', 'Y');
-- 出品が無い（まだ1件も出品していない）場合だけ、次の DELETE で元に戻す。次のサイクルの lookup 再生成で収集範囲も戻る。
DELETE FROM mst.presets_price_ranges
WHERE preset_group = N'貴文'
  AND category_group = N'財布メンズ'
  AND vendor_code IN ('R', 'Y');
