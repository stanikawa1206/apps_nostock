-- メルカリ（M・MS）のボールペン(5249)・万年筆(5262)のブランド別 eBay カテゴリーの不足分をマスターに足す（2026-11-16）。
-- 生成: python -m apps.publish.sql.yahoo_tanikawa2_master_20261116 write（件数・根拠は PEN_ADDITIONS）
-- is_active=0 で入れる = マスターとして持つだけで preset は作らない（M・MS の収集・出品は増えない。ユーザー指示 11/16）。
-- 将来メルカリで再開するときは、その行を is_active=1 にする。
-- 前提: presets_categories_is_active_20261116.sql（列の追加）と、lookup 生成が is_active=1 だけを使うコードの本番反映
--       （古いコードの在庫管理ループが動いている間に流すと、次の lookup 再生成で M・MS の preset ができてしまう）。
-- ラクマのペンは1カテゴリー（1604 ペン/マーカー → eBay 967）でブランド別にできないので変更しない。
SET XACT_ABORT ON;
BEGIN TRAN;
IF COL_LENGTH('mst.presets_categories', 'is_active') IS NULL THROW 50001, N'先に presets_categories_is_active_20261116.sql を流す', 1;
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'29828', NULL, 1912, 1, N'メルカリ', 0);  -- モンブラン
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'165643', NULL, 57582, 1, N'メルカリ', 0);  -- Pelikan
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 49148, 1, N'メルカリ', 0);  -- 三菱鉛筆
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 34953, 1, N'メルカリ', 0);  -- ゼブラ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 42196, 1, N'メルカリ', 0);  -- ハローキティ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 44990, 1, N'メルカリ', 0);  -- プラチナ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 16649, 1, N'メルカリ', 0);  -- コクヨ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 30428, 1, N'メルカリ', 0);  -- サクラクレパス
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 34791, 1, N'メルカリ', 0);  -- セーラー
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 39214, 1, N'メルカリ', 0);  -- トンボ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 34088, 1, N'メルカリ', 0);  -- ステッドラー
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 25669, 1, N'メルカリ', 0);  -- Omas
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 47727, 1, N'メルカリ', 0);  -- マーレン
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5249, N'ボールペン', NULL, N'7279', NULL, 30033, 1, N'メルカリ', 0);  -- Conklin
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5262, N'万年筆', NULL, N'14002', NULL, 13843, 1, N'メルカリ', 0);  -- ペリカン
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5262, N'万年筆', NULL, N'7281', NULL, 44990, 1, N'メルカリ', 0);  -- プラチナ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5262, N'万年筆', NULL, N'165626', NULL, 42986, 1, N'メルカリ', 0);  -- ビスコンティ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5262, N'万年筆', NULL, N'7281', NULL, 34088, 1, N'メルカリ', 0);  -- ステッドラー
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) VALUES (5262, N'万年筆', NULL, N'7281', NULL, 39214, 1, N'メルカリ', 0);  -- トンボ
UPDATE mst.presets_brand SET default_brand_en = N'Marlen' WHERE brand_id = 47727 AND default_brand_en = N'Marien';  -- マーレン の eBay Brand の誤記
COMMIT;
