-- 段階2 有効化の戻し: 谷川② の5グループの Yahoo!フリマ の価格帯の行を消す。
-- 次の lookup 再生成で Y の preset が消える（Y の未終了出品が残っていれば delete_flag=1 にして残すこと。
-- 行を消すと既存出品の在庫管理が止まる）。
SET XACT_ABORT ON;
BEGIN TRAN;
DELETE FROM mst.presets_price_ranges WHERE preset_group = N'谷川②' AND vendor_code = 'Y' AND category_group IN (N'デジカメ',N'ビデオカメラ',N'レンズ',N'ペン',N'製図用品');
COMMIT;
