-- 谷川② デジカメ・ビデオカメラ・レンズ・ペン・製図用品に Yahoo!フリマ を追加: 段階1 マスター（2026-11-16）
-- 生成: python -m apps.publish.sql.yahoo_tanikawa2_master_20261116 write（方針・ブランドIDの根拠はそのファイル）
-- この段階では presets_price_ranges に Y の行が無いので、lookup に Y の preset は作られない（在庫取得・出品は始まらない）。
-- 前提: Yahoo!フリマのグループ別判定（腕時計以外の判定を腕時計だけに・レンズの Type・ブランドなし検索）の
--       コードを本番（mouse・全VPS）に反映してから。古いコードはブランドなし（brand_id=0）を brandIds=0 で検索してしまう。
-- 件数: presets_categories 75 / category_group_categories 23 / vendor_brand_map 56 / brand_category_group_vendors 14+2 / 関数 dbo.fn_take_one_candidates_base
SET XACT_ABORT ON;
BEGIN TRAN;

-- 1) カテゴリー（Yahoo!フリマ genreCategoryIds）と eBay カテゴリー・Type。写す元のメルカリの行を行末に記す
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47734, N'ミラーレス一眼', N'Mirrorless Camera', N'31388', NULL, 0, 0, N'ＰａｙＰａｙフリマ');  -- メルカリ 4006 ミラーレス一眼
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (19801, N'コンパクトデジタルカメラ', N'Compact Digital Camera', N'31388', NULL, 0, 0, N'ＰａｙＰａｙフリマ');  -- メルカリ 4007 コンパクトデジタルカメラ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47735, N'デジタル一眼レフ', N'Digital Single-Lens Reflex', N'31388', NULL, 0, 0, N'ＰａｙＰａｙフリマ');  -- メルカリ 4009 デジタル一眼レフ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (49429, N'ビデオカメラ本体', N'Camcorders', N'11724', NULL, 0, 0, N'ＰａｙＰａｙフリマ');  -- メルカリ 4012 ビデオカメラ本体
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (49419, N'アクションカメラ', N'Helmet/Action', N'11724', NULL, 0, 0, N'ＰａｙＰａｙフリマ');  -- メルカリ 4012 ビデオカメラ本体
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (2465, N'交換レンズ', NULL, N'3323', NULL, 0, 0, N'ＰａｙＰａｙフリマ');  -- メルカリ 846 レンズ(単焦点)
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47299, N'シャープペン', N'', N'7284', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5253 シャープペン
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47312, N'マーカー', N'', N'28109', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5257 マーカー
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47288, N'色鉛筆', N'', N'28108', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5266 色鉛筆
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47539, N'筆ペン', N'', N'28109', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5269 筆ペン
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47521, N'直線・方眼定規', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5312 直線・方眼定規
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47522, N'コンパス', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5313 コンパス
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47526, N'製図用ブラシ', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5314 製図用ブラシ
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47523, N'字消し板', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5315 字消し板
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47520, N'その他製図用品', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5317 その他
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47528, N'三角スケール', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5318 三角スケール
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47529, N'三角定規', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5319 三角定規
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47530, N'分度器', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5320 分度器
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47531, N'製図テンプレート', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5321 製図テンプレート
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47532, N'雲形定規', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5322 雲形定規
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47533, N'製図用シャープペンシル', NULL, N'14017', NULL, 0, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5323 製図用シャープペンシル
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'73534', N'', 289, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=289
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'29827', N'', 522, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=522
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'73532', N'', 773, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=773
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'29829', N'', 902, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=902
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'29828', NULL, 1912, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=1912
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 8710, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=8710
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'7279', N'', 10505, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=10505
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'165643', NULL, 13843, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=13843
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'165640', N'', 15344, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=15344
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'165646', N'', 15531, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=15531
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 16649, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=16649
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 25669, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=25669
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 30033, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=30033
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 30428, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=30428
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'165647', N'', 31766, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=31766
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 34088, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=34088
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 34791, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=34791
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 34953, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=34953
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 39214, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=39214
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 42196, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=42196
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'165648', NULL, 42986, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=42986
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'165641', N'', 43647, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=43647
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 44990, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=44990
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'7279', N'', 46802, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=46802
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 47727, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=47727
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', NULL, N'7279', NULL, 49148, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=49148
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'7279', N'', 50591, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=50591
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'165642', N'', 52017, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=52017
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47303, N'ボールペン', N'', N'165645', N'', 54175, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5249 brand_id=54175
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'14004', NULL, 289, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=289
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'150124', N'', 522, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=522
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'73536', N'', 773, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=773
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'14001', N'', 902, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=902
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'14000', N'', 1912, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=1912
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'73537', N'', 10505, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=10505
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'165628', N'', 13527, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=13527
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'14002', NULL, 13843, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=13843
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'165621', N'', 15344, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=15344
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'165624', N'', 15531, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=15531
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'165629', N'', 25669, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=25669
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'60819', NULL, 30026, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=30026
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'73535', N'', 30033, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=30033
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'14003', NULL, 31766, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=31766
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'7281', NULL, 34088, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=34088
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'165625', N'', 34791, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=34791
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'7281', NULL, 39214, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=39214
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'165626', NULL, 42986, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=42986
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'165622', N'', 43647, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=43647
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'7281', NULL, 44990, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=44990
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'7281', N'', 46802, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=46802
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'165623', NULL, 47727, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=47727
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'7281', N'', 50591, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=50591
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', N'', N'29831', N'', 52017, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=52017
INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) VALUES (47315, N'万年筆', NULL, N'73538', NULL, 54175, 1, N'ＰａｙＰａｙフリマ');  -- メルカリ 5262 brand_id=54175

-- 2) グループ所属（ペンのボールペン・万年筆はブランド別 is_brand_dependent=1）。製図板 47524 は入れない
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'デジカメ', 47734, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'デジカメ', 19801, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'デジカメ', 47735, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ビデオカメラ', 49429, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ビデオカメラ', 49419, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'レンズ', 2465, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ペン', 47299, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ペン', 47312, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ペン', 47288, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ペン', 47539, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47521, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47522, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47526, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47523, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47520, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47528, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47529, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47530, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47531, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47532, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'製図用品', 47533, 0, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ペン', 47303, 1, N'ＰａｙＰａｙフリマ');
INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) VALUES (N'ペン', 47315, 1, N'ＰａｙＰａｙフリマ');

-- 3) ブランドの Yahoo!フリマ ID（検索用。eBay の Brand は presets_brand.default_brand_en）。行末は谷川②の条件内の販売中件数
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3276, N'ＰａｙＰａｙフリマ', 100537, N'オリンパス', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 189件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3277, N'ＰａｙＰａｙフリマ', 100614, N'キヤノン', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 1489件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3285, N'ＰａｙＰａｙフリマ', 101091, N'SONY', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 367件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3290, N'ＰａｙＰａｙフリマ', 101345, N'ニコン', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 791件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3291, N'ＰａｙＰａｙフリマ', 101460, N'Panasonic', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 243件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3294, N'ＰａｙＰａｙフリマ', 101801, N'ペンタックス', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 109件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3296, N'ＰａｙＰａｙフリマ', 102365, N'FUJIFILM', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 137件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (4194, N'ＰａｙＰａｙフリマ', 102073, N'ライカ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 50件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (4196, N'ＰａｙＰａｙフリマ', 104459, N'ケンコー・トキナー', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 22件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (4197, N'ＰａｙＰａｙフリマ', 116853, N'コシナ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 26件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (4198, N'ＰａｙＰａｙフリマ', 100774, N'コニカミノルタ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 18件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (4199, N'ＰａｙＰａｙフリマ', 100880, N'シグマ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 106件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (4203, N'ＰａｙＰａｙフリマ', 101276, N'トキナー', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 12件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (4204, N'ＰａｙＰａｙフリマ', 100067, N'TAMRON', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 58件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (15358, N'ＰａｙＰａｙフリマ', 135365, N'CONTAX', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 3件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (15592, N'ＰａｙＰａｙフリマ', 116440, N'ZEISS', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 39件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (24997, N'ＰａｙＰａｙフリマ', 163295, N'OM SYSTEM', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 2件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (7202, N'ＰａｙＰａｙフリマ', 116363, N'GoPro', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 121件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (15577, N'ＰａｙＰａｙフリマ', 101536, N'Victor(オーディオ)', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 2件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (21803, N'ＰａｙＰａｙフリマ', 136873, N'Insta360', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 123件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (37078, N'ＰａｙＰａｙフリマ', 116224, N'DJI', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 142件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (289, N'ＰａｙＰａｙフリマ', 102616, N'WATERMAN', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 90件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (522, N'ＰａｙＰａｙフリマ', 100707, N'CROSS', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 197件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (902, N'ＰａｙＰａｙフリマ', 102615, N'PARKER', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 226件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (1912, N'ＰａｙＰａｙフリマ', 102030, N'MONTBLANC（筆記具、時計）', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 728件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (3633, N'ＰａｙＰａｙフリマ', 107672, N'フェリシモ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 9件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (8710, N'ＰａｙＰａｙフリマ', 121008, N'uni（三菱鉛筆）', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 19件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (10505, N'ＰａｙＰａｙフリマ', 102632, N'PILOT（文具）', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 330件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (13330, N'ＰａｙＰａｙフリマ', 108197, N'Miffy', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 1件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (13843, N'ＰａｙＰａｙフリマ', 101780, N'Pelikan（筆記具）', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 136件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (15344, N'ＰａｙＰａｙフリマ', 102619, N'CARAN d''ACHE', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 102件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (15531, N'ＰａｙＰａｙフリマ', 102621, N'S.T.DUPONT', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 16件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (16649, N'ＰａｙＰａｙフリマ', 100759, N'KOKUYO', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 2件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (25669, N'ＰａｙＰａｙフリマ', 159756, N'OMAS', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 8件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (29816, N'ＰａｙＰａｙフリマ', 100776, N'コヒノール', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 1件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (30026, N'ＰａｙＰａｙフリマ', 102627, N'コンウェイ・スチュワート', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 1件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (30033, N'ＰａｙＰａｙフリマ', 102625, N'コンクリン', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 9件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (30428, N'ＰａｙＰａｙフリマ', 102633, N'サクラクレパス', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 4件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (31766, N'ＰａｙＰａｙフリマ', 102620, N'SHEAFFER', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 25件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (32497, N'ＰａｙＰａｙフリマ', 115731, N'シヤチハタ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 1件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (34088, N'ＰａｙＰａｙフリマ', 101016, N'ステッドラー', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 49件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (34791, N'ＰａｙＰａｙフリマ', 102631, N'セーラー万年筆', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 130件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (34953, N'ＰａｙＰａｙフリマ', 115771, N'ZEBRA', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 6件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (35572, N'ＰａｙＰａｙフリマ', 104358, N'ダーウェント', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 4件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (39214, N'ＰａｙＰａｙフリマ', 117698, N'トンボ鉛筆', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 7件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (42196, N'ＰａｙＰａｙフリマ', 125889, N'ハローキティ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 5件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (42986, N'ＰａｙＰａｙフリマ', 102629, N'ビスコンティ', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 1件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (43647, N'ＰａｙＰａｙフリマ', 101579, N'ファーバーカステル', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 20件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (44990, N'ＰａｙＰａｙフリマ', 118928, N'プラチナ万年筆', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 6件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (46802, N'ＰａｙＰａｙフリマ', 102638, N'ぺんてる', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 111件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (47469, N'ＰａｙＰａｙフリマ', 104089, N'ホルベイン', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 2件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (47727, N'ＰａｙＰａｙフリマ', 102624, N'マーレン', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 2件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (49148, N'ＰａｙＰａｙフリマ', 115772, N'三菱鉛筆', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 141件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (50591, N'ＰａｙＰａｙフリマ', 102618, N'Montegrappa', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 6件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (52017, N'ＰａｙＰａｙフリマ', 102109, N'LAMY', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 101件
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) VALUES (54175, N'ＰａｙＰａｙフリマ', 102280, N'ロットリング', 1, 1, N'Yahoo!フリマ 谷川② 2026-11-16', SYSDATETIME());  -- 23件

-- 4) ラクマ専用の所属（10/27 ラクマ拡充）を Yahoo!フリマ でも使う（メルカリは従来どおり使わない）
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (3294, N'デジカメ', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (4199, N'デジカメ', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (3282, N'ビデオカメラ', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (289, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (433, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (522, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (809, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (1326, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (2186, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (2945, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (15531, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (31766, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (52017, N'ペン', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (3303, N'レンズ', N'ＰａｙＰａｙフリマ', N'Yahoo!フリマ 谷川② 2026-11-16: ラクマ専用の所属を Yahoo!フリマ でも使う', GETDATE());

-- 4-2) 上で Y の ID を入れたブランドが、Y のある他のグループ（谷川④の腕時計）に所属している場合、その所属を
--      今の仕入先（メルカリ・ラクマ）だけに限る（行が無い所属は全仕入先共通なので、限らないと Y の preset ができる）
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (3291, N'腕時計', N'メルカリ', N'Yahoo!フリマ 谷川② 2026-11-16: Y を追加しないため所属を今の仕入先に限る', GETDATE());
INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) VALUES (3291, N'腕時計', N'ラクマ', N'Yahoo!フリマ 谷川② 2026-11-16: Y を追加しないため所属を今の仕入先に限る', GETDATE());

COMMIT;
GO

-- 5) 出品候補関数: presets_categories を (category_id, vendor_group) ごとに1行にして結合（候補の重複をなくす）
CREATE OR ALTER FUNCTION dbo.fn_take_one_candidates_base
(
    @preset_group NVARCHAR(100)
)
RETURNS TABLE
AS
RETURN
(
    WITH Candidate AS
    (
        SELECT
            v.vendor_item_id,
            v.vendor_name,
            v.price,
            v.shipping_region,
            v.shipping_days,
            v.preset,

            pl.mode,
            pl.default_brand_en,
            pl.category_id_ebay,
            pl.department,
            pl.type_ebay,
            pl.category_group,

            pc.is_collectibles,


            r.low_jpy_target,
            r.high_jpy_target,
            r.category_priority,

            v.item_condition_id,

            CAST(1 AS bit) AS is_ok_logic,
            vd.publish_priority
        FROM trx.vendor_item v
        INNER JOIN mst.presets_lookup pl
            ON pl.preset = v.preset
        INNER JOIN mst.vendors vd
            ON vd.vendor_name = pl.vendor_name
        -- 2026-11-13 価格帯はアカウント×カテゴリーグループ×仕入先ごと（主キー）。仕入先まで
        -- 一致させるので候補は増殖しない。delete_flag=1（新規出品停止・既存出品の在庫管理だけ続ける）
        -- の行からは候補を作らない。
        INNER JOIN mst.presets_price_ranges r
            ON r.preset_group = @preset_group
            AND r.category_group = pl.category_group
            AND r.vendor_code = vd.vendor_code
            AND r.delete_flag = 0
        -- 2026-11-16 presets_categories は (category_id, brand_id, vendor_group) ごとの行で、ペンのボールペン・万年筆は
        -- ブランド別に複数行ある。category_id だけで結合すると候補がその行数だけ重複していた（メルカリのペン72 preset で
        -- 1,260行）。使うのは is_collectibles だけ（同じ category_id・vendor_group の行で値は同じ）なので、
        -- 仕入先の vendor_group の (category_id) ごとに1行にしてから結合する（Yahoo!フリマの追加でも重複しない）。
        INNER JOIN (
            SELECT category_id, vendor_group, CAST(MAX(CAST(is_collectibles AS int)) AS bit) AS is_collectibles
            FROM mst.presets_categories
            GROUP BY category_id, vendor_group
        ) pc
            ON pc.category_id = pl.category_id
            AND pc.vendor_group = vd.vendor_group
        LEFT JOIN mst.seller s
            ON s.seller_id = v.seller_id
        LEFT JOIN mst.category_groups cg
            ON cg.category_group = pl.category_group
        LEFT JOIN (
            -- default_brand_en が presets_brand 側で重複していても候補が増殖しないよう、
            -- 1ブランド名につき1行に集約してから結合する（is_high_risk_brandはOR的にMAXで合成）。
            SELECT
                default_brand_en,
                MAX(CAST(is_high_risk_brand AS INT)) AS is_high_risk_brand
            FROM mst.presets_brand
            WHERE default_brand_en IS NOT NULL AND default_brand_en <> N''
            GROUP BY default_brand_en
        ) pb
            ON pb.default_brand_en = pl.default_brand_en

        WHERE

            -- ============================================================
            -- ① 基本条件（処理対象）
            -- ============================================================
            v.processing_at IS NULL
            AND (v.status = N'販売中' OR v.status IS NULL)
            AND ISNULL(v.出品不可flg, 0) = 0

            -- ============================================================
            -- ② 既に出品済みの商品は除外
            -- ============================================================
            AND NOT EXISTS (
                SELECT 1
                FROM trx.listings l
                WHERE l.vendor_name = v.vendor_name
                  AND l.vendor_item_id = v.vendor_item_id
                  AND l.is_deleted = 0
            )

            -- ============================================================
            -- ③ 商品価格が担当レンジ内
            -- ============================================================
            AND (
                v.price IS NULL
                OR (
                    v.price >= r.low_jpy_target
                    AND v.price <= r.high_jpy_target
                )
            )

            -- ============================================================
            -- ④ セラー品質チェック（閾値は mst.vendors.base_rating_threshold を参照）
            -- ============================================================
            AND ISNULL(s.is_ng, 0) = 0

            AND (
                v.seller_id IS NULL
                OR s.seller_id IS NULL  -- seller_idはあるがmst.sellerに情報が無い（まだ未評価）。NG扱いにせずheavyでの取得に委ねる
                OR (
                    ISNULL(s.rating_count, 0)
                    + ISNULL(DATEDIFF(DAY, s.last_checked_at, SYSDATETIME()), 0) * 0.5
                ) >= vd.base_rating_threshold
            )

            -- ============================================================
            -- ⑤ 商品情報が古すぎない
            -- ============================================================
            AND (
                v.vendor_updated_at IS NULL
                OR v.vendor_updated_at >= DATEADD(DAY, -40, SYSDATETIME())
            )

            -- ============================================================
            -- ⑥ 出品不可判定
            -- ============================================================
            AND ISNULL(v.[出品状況], N'') NOT IN (
                N'NG(GA補色)',
                N'NG(危険素材)',
                N'保留(素材確認)',   -- 2026-11-04 素材判定で判断できない商品（手動確認まで出品しない）
                N'ポリシーNG',
                N'NG(ジャンク疑い)',
                N'NG(バリエーション商品)'
            )
AND NOT (
    ISNULL(v.出品状況, N'') = N'解析失敗'
    AND ISNULL(v.出品状況詳細, N'') = N'no valid images (all <500px or error)'
)


            AND ISNULL(v.shipping_days, N'') NOT IN (
                N'8〜14日で発送',
                N'90日以内で発送'
            )

            -- ============================================================
            -- ⑦ 一時NG商品の再チェック期間
            -- ============================================================
            AND NOT (
                ISNULL(v.[出品状況], N'') IN (
                    N'低需要NG',
                    N'配送条件NG',
                    N'NG(高リスク商品制限)',
                    N'NG(古い更新)'
                )
                AND v.last_checked_at >= DATEADD(MONTH, -1, SYSDATETIME())
            )

            -- ============================================================
            -- ⑦-2 ラクマ「購入申請あり」商品の再チェック抑制（30日間）
            -- 2026-10-15追加。last_checked_atは詳細チェック時に毎回SYSDATETIME()で
            -- 更新されるため、再度NGと確定すればその時点から自動的に30日延長される。
            -- 30日経過すれば本条件がFalseとなり、追加のバッチ処理なしで自動的に
            -- 候補へ戻る。NG(購入申請あり)はpublish_ebay.py側の仕様でラクマ以外には
            -- 付与されないため、メルカリ・メルカリshopsへの影響はない。
            -- 既存の⑦ブロック（DATEADD(MONTH,-1,...)）とは意図的に分離している。
            -- 月単位だと月の日数で28〜31日とブレるため、正確に30日固定とするため。
            -- ============================================================
            AND NOT (
                ISNULL(v.[出品状況], N'') = N'NG(購入申請あり)'
                AND v.last_checked_at >= DATEADD(DAY, -30, SYSDATETIME())
            )

-- ============================================================
-- ⑧ 新規出品制限（危険商品）
-- 新品ルール／ペン・製図用品の新品除外は既存仕様のまま維持。
-- High Riskカテゴリー・ブランドの判定基準および閾値は
-- mst.category_groups.is_high_risk / mst.presets_brand.is_high_risk_brand /
-- mst.vendors.high_risk_rating_threshold を参照する。
-- ============================================================
AND NOT (
    s.seller_id IS NOT NULL
    AND (
        (
            (v.item_condition_id = 1 AND pl.category_group NOT IN (N'ペン', N'製図用品'))
            OR (
                v.item_condition_id <> 1
                AND ISNULL(pb.is_high_risk_brand, 0) = 1
            )
        )
        AND ISNULL(s.rating_count, 0) < vd.high_risk_rating_threshold

        OR

        (
            ISNULL(cg.is_high_risk, 0) = 1
            AND ISNULL(s.rating_count, 0) < vd.high_risk_rating_threshold
        )
    )
    AND ISNULL(s.allow_new_items, 0) <> 1
    AND s.last_checked_at IS NOT NULL
    AND s.last_checked_at >= DATEADD(DAY, -30, SYSDATETIME())
)    )

    -- ★ 本番fn_take_one_candidatesと異なり、ここでは最終的なpriority絞り込みを行わない。
    --   全vendor・全categoryの出品可能候補をそのまま返す（集計・分析用途向け）。
    SELECT * FROM Candidate
);
GO
