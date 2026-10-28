-- Tombow・Pelikan のラクマID整理（2026-10-27）
-- 前提: rakuma_brand_import_20261027.sql 適用済み（Tombow 39214 にラクマ5560が非primaryで入っている）。
--
-- Tombow(39214): 既存のラクマID 10523 はラクマ上「トンボガクセイフク(TOMBOW)」＝トンボ学生服
--   （fril.jp/brand/10523 は制服・体操服）。トンボ鉛筆は 5560（「トンボエンピツ(トンボ鉛筆)」）。
--   10523 を外し、5560 を primary にする（preset名は「トンボペンR」等のまま、検索IDだけ変わる）。
-- Pelikan: メルカリには 13843「ペリカン」と 57582「Pelikan」の2つのIDがあり、どちらも実体はペリカン社で
--   メルカリ検索に使われている（13843=ペン各種、57582=万年筆のブランド依存カテゴリー）。
--   ラクマのペリカンは 6639 の1つだけで、ラクマのpresetを作るのは brand_category_groups に所属のある 13843 だけ。
--   57582 側のラクマ6639は使われておらず、所属が増えると同じ検索が重複するので外す。

-- Tombow
DELETE FROM mst.vendor_brand_map
WHERE brand_id = 39214 AND vendor_name = N'ラクマ' AND vendor_brand_id = 10523;

UPDATE mst.vendor_brand_map
SET is_primary = 1,
    note = N'トンボ鉛筆（ラクマ表記: トンボエンピツ）。旧ラクマID 10523 はトンボ学生服だったため差し替え'
WHERE brand_id = 39214 AND vendor_name = N'ラクマ' AND vendor_brand_id = 5560;

-- Pelikan
DELETE FROM mst.vendor_brand_map
WHERE brand_id = 57582 AND vendor_name = N'ラクマ' AND vendor_brand_id = 6639;

-- 旧列（Access表示用に残している brand_id_rkm）も合わせる。プログラムからは読まない。
UPDATE mst.presets_brand SET brand_id_rkm = 5560 WHERE brand_id = 39214 AND brand_id_rkm = 10523;
UPDATE mst.presets_brand SET brand_id_rkm = NULL WHERE brand_id = 57582 AND brand_id_rkm = 6639;
GO

-- 1つの仕入先ブランドIDを複数のブランドで使わない（同じ検索の重複を防ぐ）
CREATE UNIQUE INDEX UX_vendor_brand_map_vendor_brand
    ON mst.vendor_brand_map (vendor_name, vendor_brand_id)
    WHERE is_active = 1;
GO
