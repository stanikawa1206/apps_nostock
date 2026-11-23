-- mms_stop_4groups_20261123.sql の取り消し（M・MS の新規出品を再開する）。
-- 次の在庫管理サイクルの lookup 再生成で、漸減で消えた lookup も戻る。
UPDATE mst.presets_price_ranges
SET delete_flag = 0
WHERE category_group IN (N'アクセサリー', N'スカーフ', N'スカーフメンズ', N'財布メンズ')
  AND vendor_code IN ('M', 'MS')
  AND preset_group IN (N'BUZZ', N'貴文', N'川島');
