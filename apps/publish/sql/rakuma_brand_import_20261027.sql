-- ラクマブランド追加(候補Excel 308行) 自動生成: plan_brand_import.py
SET XACT_ABORT ON;
BEGIN TRAN;

-- 1) presets_brand 新規(184件)
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (15, N'4°C', N'4°C', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (24, N'アーカー', N'AHKAH', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (57, N'アガタ', N'AGATHA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (58, N'アガット', N'agete', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (72, N'アクシーズファム', N'axes femme', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (111, N'アナスイ', N'ANNA SUI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (119, N'アニヤハインドマーチ', N'ANYA HINDMARCH', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (131, N'アフタヌーンティー', N'AfternoonTea', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (176, N'アレキサンダーマックイーン', N'Alexander McQueen', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (188, N'アンジェリックプリティー', N'Angelic Pretty', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (192, N'アンダーカバー', N'UNDERCOVER', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (197, N'アンテプリマ', N'ANTEPRIMA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (299, N'エイチアンドエム', N'H&M', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (356, N'エムシーエム', N'MCM', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (425, N'カナルヨンドシー', N'canal4°C', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (435, N'カルバンクライン', N'Calvin Klein', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (446, N'キャスキッドソン', N'Cath Kidston', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (492, N'グリーンレーベルリラクシング', N'green label relaxing', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (497, N'クリスチャンルブタン', N'Christian Louboutin', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (509, N'クレージュ', N'Courreges', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (512, N'グレゴリー', N'Gregory', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (563, N'コムデギャルソン', N'COMME des GARCONS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (572, N'ザノースフェイス', N'THE NORTH FACE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (588, N'サマンサタバサ', N'Samantha Thavasa', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (590, N'サマンサタバサプチチョイス', N'Samantha Thavasa Petit Choice', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (597, N'ザラ', N'ZARA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (646, N'ジミーチュウ', N'JIMMY CHOO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (686, N'ジョルジオアルマーニ', N'GIORGIO ARMANI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (692, N'ジルスチュアート', N'JILLSTUART', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (748, N'ソニアリキエル', N'SONIA RYKIEL', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (758, N'タケオキクチ', N'TAKEO KIKUCHI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (759, N'ダコタ', N'Dakota', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (782, N'チャムス', N'CHUMS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (791, N'ツモリチサトキャリー', N'tsumori chisato CARRY', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (826, N'ドゥーズィエムクラス', N'DEUXIEME CLASSE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (831, N'トゥモローランド', N'TOMORROWLAND', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (840, N'トミーヒルフィガー', N'TOMMY HILFIGER', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (847, N'ドリスヴァンノッテン', N'DRIES VAN NOTEN', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (857, N'ナイキ', N'NIKE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (867, N'ナノユニバース', N'nano universe', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (873, N'ナンバーナイン', N'NUMBER (N)INE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (882, N'ニナリッチ', N'NINA RICCI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (907, N'バーニーズニューヨーク', N'BARNEYS NEW YORK', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (947, N'バリー', N'Bally', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (952, N'ハレ', N'HARE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (954, N'バレンシアガ', N'BALENCIAGA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (958, N'バンダイ', N'BANDAI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (969, N'ビームス', N'BEAMS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1001, N'ピンキーアンドダイアン', N'Pinky&Dianne', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1009, N'プーマ', N'PUMA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1117, N'ポーター', N'PORTER', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1120, N'ポールアンドジョー', N'PAUL & JOE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1146, N'マークジェイコブス', N'MARC JACOBS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1160, N'マックスマーラ', N'Max Mara', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1243, N'モンベル', N'mont bell', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1248, N'ユナイテッドアローズ', N'UNITED ARROWS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1250, N'ユニクロ', N'UNIQLO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1253, N'ヨウジヤマモト', N'Yohji Yamamoto', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1438, N'イルビゾンテ', N'IL BISONTE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1512, N'オロビアンコ', N'Orobianco', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1516, N'カステルバジャック', N'CASTELBAJAC', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1620, N'シュプリーム', N'Supreme', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1669, N'ダブルタップス', N'WTAPS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1688, N'ディッキーズ', N'Dickies', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1720, N'トルネードマート', N'TORNADO MART', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1736, N'ネイバーフッド', N'NEIGHBORHOOD', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1779, N'ピーピーエフエム', N'PPFM', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1892, N'無印良品', N'MUJI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1936, N'ランバンオンブルー', N'LANVIN en Bleu', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1977, N'ロロピアーナ', N'Loro Piana', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2272, N'ヒステリックミニ', N'HYSTERIC MINI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2279, N'ピンクハウス', N'PINK HOUSE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2283, N'ファミリア', N'familiar', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2535, N'マリメッコ', N'marimekko', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2536, N'マルニ', N'MARNI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2589, N'ウェッジウッド', N'WEDGWOOD', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2858, N'ロンシャン', N'LONGCHAMP', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (2984, N'ピエールカルダン', N'pierre cardin', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3014, N'ミキモト', N'MIKIMOTO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3045, N'レスポートサック', N'LeSportsac', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3292, N'日立', N'HITACHI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3550, N'マンシングウェア', N'Munsingwear', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3608, N'ジェラートピケ', N'gelato pique', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3616, N'マリークヮント', N'MARY QUANT', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3631, N'ロンハーマン', N'Ron Herman', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3633, N'フェリシモ', N'FELISSIMO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3654, N'ミナペルホネン', N'mina perhonen', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3784, N'ハンティングワールド', N'HUNTING WORLD', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3823, N'トゥデイフル', N'TODAYFUL', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3833, N'ルネ', N'René', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3835, N'フェリージ', N'Felisi', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3865, N'キタムラ', N'Kitamura', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3886, N'スタージュエリー', N'STAR JEWELRY', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3887, N'ヴァンドームアオヤマ', N'Vendome Aoyama', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3900, N'エストネーション', N'ESTNATION', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3928, N'フェイラー', N'FEILER', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (3965, N'サンリオ', N'Sanrio', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4024, N'エテ', N'ete', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4040, N'レリアン', N'leilian', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4059, N'ユリウス', N'JULIUS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4109, N'スターバックス', N'Starbucks', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4194, N'ライカ', N'LEICA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4196, N'ケンコートキナー', N'Kenko Tokina', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4197, N'コシナ', N'COSINA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4198, N'コニカミノルタ', N'KONICA MINOLTA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4253, N'メルセデスベンツ', N'Mercedes-Benz', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4408, N'ポロラルフローレン', N'POLO RALPH LAUREN', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4639, N'ジルバイジルスチュアート', N'JILL by JILLSTUART', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4728, N'ジョージジェンセン', N'Georg Jensen', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4749, N'フィリップオーディベール', N'Philippe Audibert', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4753, N'プラヴィダ', N'Pura Vida', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4921, N'ジョンストンズ', N'Johnstons of Elgin', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (4986, N'ワンエルディーケーセレクト', N'1LDK SELECT', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (5062, N'アルテア', N'ALTEA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (5195, N'ベルルッティ', N'BERLUTI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (5245, N'ブリーフィング', N'BRIEFING', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (5317, N'クリスチャンラクロワ', N'Christian Lacroix', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (5384, N'クロコダイル', N'Crocodile', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (5405, N'ダミアーニ', N'Damiani', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (5539, N'ファリエロサルティ', N'Faliero Sarti', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (6578, N'スヌーピー', N'SNOOPY', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (6648, N'サンシー', N'SUNSEA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (6775, N'トゥミ', N'TUMI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (6981, N'エンダースキーマ', N'Hender Scheme', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (6990, N'コモリ', N'COMOLI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7021, N'メゾンドフルール', N'Maison de FLEUR', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7032, N'ロキエ', N'Lochie', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7037, N'アタオ', N'ATAO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7111, N'ポンテヴェキオ', N'Ponte Vecchio', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7133, N'ヴァレクストラ', N'Valextra', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7202, N'ゴープロ', N'GoPro', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7238, N'シモネッタ', N'Simonetta', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7265, N'アルマーニコレツィオーニ', N'ARMANI COLLEZIONI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7317, N'ココマイスター', N'COCOMEISTER', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7331, N'ディオールオム', N'DIOR HOMME', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7454, N'プリーツプリーズ', N'PLEATS PLEASE ISSEY MIYAKE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7518, N'タサキ', N'TASAKI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (7693, N'ヒューマンメイド', N'HUMAN MADE', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (8322, N'マニプリ', N'manipuri', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (9933, N'土屋鞄製造所', N'Tsuchiya Kaban', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (9940, N'ガンゾ', N'GANZO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (10073, N'ジュエッテ', N'Jouete', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (10077, N'カヒコ', N'KAHIKO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (10181, N'フォーティファイブアール', N'45R', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (10275, N'ムーミン', N'MOOMIN', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (10492, N'アーノルドパーマー', N'Arnold Palmer', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (11001, N'アソースメレ', N'ASAUCE MELER', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (12037, N'ファブリック', N'FABRIK', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (12418, N'ハシバミ', N'Hashibami', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (12706, N'ジョシュアエリス', N'Joshua Ellis', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (12923, N'ラルコバレーノ', N'L''arcobaleno', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (13163, N'メゾンマルジェラ', N'Maison Margiela', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (13330, N'ミッフィー', N'miffy', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (14154, N'ルート', N'ROOT CO.', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (14780, N'トムウッド', N'TOM WOOD', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (15358, N'コンタックス', N'CONTAX', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (15369, N'ダブルアールエル', N'RRL', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (15574, N'ヴァンクリーフアンドアーペル', N'Van Cleef & Arpels', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (15592, N'カールツァイス', N'ZEISS', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (15792, N'ジムトンプソン', N'Jim Thompson', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (16759, N'エムピウ', N'm+', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (17576, N'サンエックス', N'San-X', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (21570, N'今治タオル', N'Imabari Towel', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (21803, N'インスタスリーシックスティ', N'insta360', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (21913, N'印傳屋', N'INDEN-YA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (24997, N'OMシステム', N'OM SYSTEM', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (26526, N'カミオジャパン', N'Kamio Japan', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (28309, N'クリームドット', N'Cream dot', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (29149, N'ケンエレファント', N'Kenelephant', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (32497, N'シヤチハタ', N'Shachihata', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (33021, N'ショウワノート', N'Showa Note', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (35039, N'セリア', N'Seria', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (37078, N'ディージェイアイ', N'DJI', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (41455, N'ハセプロ', N'HASEPRO', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (42196, N'ハローキティ', N'Hello Kitty', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (42385, N'パンドラ', N'PANDORA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (47083, N'ポケモン', N'Pokemon', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (49148, N'三菱鉛筆', N'MITSUBISHI PENCIL', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (57681, N'ボナヴェントゥーラ', N'BONAVENTURA', NULL, 1, NULL, 0);  -- メルカリID新規
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1000000, N'栃木レザー', N'Tochigi Leather', NULL, 1, NULL, 0);  -- 独自ID
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1000001, N'エイボン', N'AVON', NULL, 1, NULL, 0);  -- 独自ID
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1000002, N'ちいかわ', N'Chiikawa', NULL, 1, NULL, 0);  -- 独自ID
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1000003, N'アウロラ', N'AURORA', NULL, 1, NULL, 0);  -- 独自ID
INSERT INTO mst.presets_brand (brand_id, brand_name_ja, default_brand_en, price_class, is_active, brand_id_rkm, is_high_risk_brand) VALUES (1000004, N'欅坂46', N'Keyakizaka46', NULL, 1, NULL, 0);  -- 独自ID

-- 2) vendor_brand_map ラクマ追加(197件) / メルカリ追加(179件)
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1117, N'ラクマ', 651, N'PORTER', 1, N'ラクマ候補Excel20261027: メルカリ1117=吉田カバン(subname PORTER、カナ ポーター／ヨシダカバン)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1438, N'ラクマ', 1097, N'IL BISONTE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5195, N'ラクマ', 2105, N'BERLUTI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1243, N'ラクマ', 1161, N'mont bell', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (572, N'ラクマ', 798, N'THE NORTH FACE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (954, N'ラクマ', 86, N'BALENCIAGA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (13163, N'ラクマ', 1220, N'Maison Margiela(旧Maison Martin Margiela)', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (758, N'ラクマ', 792, N'TAKEO KIKUCHI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4408, N'ラクマ', 647, N'POLO RALPH LAUREN', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (947, N'ラクマ', 87, N'Bally', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (435, N'ラクマ', 133, N'Calvin Klein', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (495, N'ラクマ', 160, N'Christian Dior', 0, N'ラクマ候補Excel20261027: メルカリ495=Christian Dior。既存dior(ラクマ1041=Dior)と同一ブランドのラクマ別ID');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (176, N'ラクマ', 1289, N'Alexander McQueen', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (969, N'ラクマ', 97, N'BEAMS', 1, N'ラクマ候補Excel20261027: 名称一致2件→カナで1件に特定(カナ一致)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (840, N'ラクマ', 810, N'TOMMY HILFIGER', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (9940, N'ラクマ', 5941, N'GANZO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (601, N'ラクマ', 3806, N'Salvatore Ferragamo', 0, N'ラクマ候補Excel20261027: メルカリ601=Salvatore Ferragamo。既存(ラクマ279=FERRAGAMO)と同一ブランドのラクマ別ID');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7317, N'ラクマ', 7779, N'COCOMEISTER', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (597, N'ラクマ', 869, N'ZARA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1512, N'ラクマ', 1426, N'Orobianco', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5245, N'ラクマ', 2175, N'BRIEFING', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (563, N'ラクマ', 188, N'COMME des GARCONS', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7133, N'ラクマ', 4626, N'Valextra', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3784, N'ラクマ', 367, N'HUNTING WORLD', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1516, N'ラクマ', 143, N'CASTELBAJAC', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5539, N'ラクマ', 2570, N'Faliero Sarti', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6981, N'ラクマ', 4449, N'Hender Scheme', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (782, N'ラクマ', 164, N'CHUMS', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1117, N'ラクマ', 6978, N'吉田カバン', 0, N'ラクマ候補Excel20261027: メルカリ1117=吉田カバン。PORTERと同一メルカリID');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (497, N'ラクマ', 1348, N'Christian Louboutin', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1253, N'ラクマ', 867, N'Yohji Yamamoto', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1000000, N'ラクマ', 7014, N'栃木レザー', 1, N'ラクマ候補Excel20261027: メルカリに『栃木レザー』のブランド登録無し');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5384, N'ラクマ', 2354, N'Crocodile', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1248, N'ラクマ', 830, N'UNITED ARROWS', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1669, N'ラクマ', 4283, N'W)taps', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (9933, N'ラクマ', 5519, N'土屋鞄製造所', 1, N'ラクマ候補Excel20261027: メルカリ9933=土屋鞄(土屋鞄製造所の略称)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3835, N'ラクマ', 1283, N'Felisi', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1620, N'ラクマ', 782, N'Supreme', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (686, N'ラクマ', 315, N'GIORGIO ARMANI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10492, N'ラクマ', 62, N'Arnold Palmer', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (14154, N'ラクマ', 15127, N'ROOT CO.', 1, N'ラクマ候補Excel20261027: 名称一致・カナ類似');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1250, N'ラクマ', 829, N'UNIQLO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (356, N'ラクマ', 526, N'MCM', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6648, N'ラクマ', 4006, N'SUNSEA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4986, N'ラクマ', 1842, N'1LDK SELECT', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (952, N'ラクマ', 342, N'HARE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7331, N'ラクマ', 5391, N'DIOR HOMME', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (847, N'ラクマ', 2461, N'DRIES VAN NOTEN', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6775, N'ラクマ', 4172, N'TUMI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (192, N'ラクマ', 827, N'UNDERCOVER', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (857, N'ラクマ', 592, N'NIKE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (873, N'ラクマ', 602, N'NUMBER (N)INE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (539, N'ラクマ', 18655, N'COACH OUTLET', 0, N'ラクマ候補Excel20261027: メルカリにCOACH OUTLETの個別ブランド無し。COACH(539、ラクマ177)のアウトレットライン');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3550, N'ラクマ', 3379, N'Munsingwear', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4253, N'ラクマ', 16930, N'Mercedes-Benz', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (867, N'ラクマ', 576, N'nano・universe', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6990, N'ラクマ', 4460, N'COMOLI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4921, N'ラクマ', 1708, N'Johnstons', 1, N'ラクマ候補Excel20261027: メルカリ4921=Johnstons of Elgin(ジョンストンズの正式名称)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (831, N'ラクマ', 811, N'TOMORROWLAND', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (759, N'ラクマ', 1028, N'Dakota', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (16759, N'ラクマ', 5860, N'm+', 1, N'ラクマ候補Excel20261027: 名称一致3件→カナで1件に特定(カナ一致)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7265, N'ラクマ', 5359, N'ARMANI COLLEZIONI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ類似');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1720, N'ラクマ', 815, N'TORNADO MART', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5062, N'ラクマ', 1940, N'ALTEA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (907, N'ラクマ', 874, N'BARNEYS NEW YORK', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (299, N'ラクマ', 339, N'H&M', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3045, N'ラクマ', 473, N'LeSportsac', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1779, N'ラクマ', 654, N'PPFM', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (512, N'ラクマ', 331, N'Gregory', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4059, N'ラクマ', 2994, N'JULIUS', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1977, N'ラクマ', 3170, N'Loro Piana', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4109, N'ラクマ', 10596, N'Starbucks', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3014, N'ラクマ', 1791, N'MIKIMOTO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7518, N'ラクマ', 5504, N'TASAKI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (42385, N'ラクマ', 12071, N'PANDORA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (28309, N'ラクマ', 7842, N'Cream dot', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4753, N'ラクマ', 1451, N'Pura Vida', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (58, N'ラクマ', 21, N'agete', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (882, N'ラクマ', 594, N'NINA RICCI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15574, N'ラクマ', 4226, N'Van Cleef & Arpels', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4024, N'ラクマ', 1259, N'ete', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4728, N'ラクマ', 1417, N'Georg Jensen', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3631, N'ラクマ', 1224, N'Ron Herman', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1000001, N'ラクマ', 942, N'AVON', 1, N'ラクマ候補Excel20261027: ラクマAVON(エイボン)。メルカリにAVON/エイボン無し(候補のAvon Houseは別ブランド)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3887, N'ラクマ', 838, N'Vendome Aoyama', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10073, N'ラクマ', 12127, N'Jouete', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3965, N'ラクマ', 4821, N'サンリオ', 1, N'ラクマ候補Excel20261027: メルカリ3965=Sanrio(カナ サンリオ)。表記違いのみ');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3886, N'ラクマ', 773, N'STAR JEWELRY', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7032, N'ラクマ', 4510, N'Lochie', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15, N'ラクマ', 5, N'4°C', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (492, N'ラクマ', 1082, N'UNITED ARROWS green label relaxing', 1, N'ラクマ候補Excel20261027: メルカリ492=green label relaxing(ユナイテッドアローズのレーベル。同一ブランド)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3900, N'ラクマ', 1578, N'ESTNATION', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7111, N'ラクマ', 4600, N'Ponte Vecchio', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (826, N'ラクマ', 1225, N'DEUXIEME CLASSE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2589, N'ラクマ', 4700, N'WEDGWOOD', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (24, N'ラクマ', 913, N'AHKAH', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2283, N'ラクマ', 272, N'familiar', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (958, N'ラクマ', 948, N'BANDAI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2984, N'ラクマ', 632, N'pierre cardin', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (111, N'ラクマ', 51, N'ANNA SUI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (509, N'ラクマ', 193, N'Courreges', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (748, N'ラクマ', 763, N'SONIA RYKIEL', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4749, N'ラクマ', 1447, N'Philippe Audibert', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (197, N'ラクマ', 924, N'ANTEPRIMA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (425, N'ラクマ', 1605, N'canal4°C', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3654, N'ラクマ', 1665, N'mina perhonen', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (57, N'ラクマ', 22, N'AGATHA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5405, N'ラクマ', 2380, N'Damiani', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7238, N'ラクマ', 4984, N'Simonetta', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3833, N'ラクマ', 1721, N'Rene', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (42196, N'ラクマ', 5280, N'ハローキティ', 1, N'ラクマ候補Excel20261027: メルカリ42196=HELLO KITTY(カナ ハローキティ)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5317, N'ラクマ', 2266, N'Christian Lacroix', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (14780, N'ラクマ', 9554, N'TOM WOOD', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10077, N'ラクマ', 10054, N'KAHIKO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3928, N'ラクマ', 1776, N'FEILER', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4040, N'ラクマ', 1134, N'leilian', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7037, N'ラクマ', 4517, N'ATAO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (188, N'ラクマ', 49, N'Angelic Pretty', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6578, N'ラクマ', 3919, N'SNOOPY', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (13330, N'ラクマ', 10424, N'miffy', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1000002, N'ラクマ', 10415, N'ちいかわ', 1, N'ラクマ候補Excel20261027: メルカリに『ちいかわ』のブランド登録無し(名称・カナとも該当なし)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (21913, N'ラクマ', 5612, N'印傳屋', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (590, N'ラクマ', 1804, N'Samantha Thavasa Petit Choice', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (646, N'ラクマ', 1112, N'JIMMY CHOO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3865, N'ラクマ', 438, N'Kitamura', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3616, N'ラクマ', 518, N'MARY QUANT', 1, N'ラクマ候補Excel20261027: 名称一致・カナ類似');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1146, N'ラクマ', 515, N'MARC JACOBS', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (588, N'ラクマ', 726, N'Samantha Thavasa', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (47083, N'ラクマ', 4819, N'ポケモン', 1, N'ラクマ候補Excel20261027: メルカリ47083=Pokemon(カナ ポケモン／ポケットモンスター)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1120, N'ラクマ', 621, N'PAUL & JOE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10275, N'ラクマ', 7889, N'MOOMIN', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (57681, N'ラクマ', 7574, N'BONAVENTURA', 1, N'ラクマ候補Excel20261027: メルカリ57681=BONAVENTURA(名称完全一致。カナはヴェ/ベの表記揺れのみ)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (131, N'ラクマ', 910, N'AfternoonTea', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (119, N'ラクマ', 1267, N'ANYA HINDMARCH', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (692, N'ラクマ', 404, N'JILLSTUART', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1001, N'ラクマ', 638, N'Pinky&Dianne', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2858, N'ラクマ', 485, N'LONGCHAMP', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3965, N'ラクマ', 5281, N'マイメロディ', 0, N'ラクマ候補Excel20261027: マイメロディはサンリオのキャラクター。メルカリに個別ブランド無し。Sanrio(3965)へ統合');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7021, N'ラクマ', 4498, N'Maison de FLEUR', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4109, N'ラクマ', 4468, N'Starbucks Coffee', 0, N'ラクマ候補Excel20261027: メルカリ4109=Starbucks');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (791, N'ラクマ', 11347, N'tsumori chisato CARRY', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1936, N'ラクマ', 1652, N'LANVIN en Bleu', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2536, N'ラクマ', 1319, N'MARNI', 1, N'ラクマ候補Excel20261027: 名称一致2件→カナで1件に特定(カナ一致)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (17576, N'ラクマ', 10420, N'リラックマ', 1, N'ラクマ候補Excel20261027: リラックマはサンエックスのキャラクター。メルカリに個別ブランド無し。san-x(17576)で登録');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12923, N'ラクマ', 11744, N'L''arcobaleno', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (446, N'ラクマ', 144, N'Cath Kidston', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12037, N'ラクマ', 11939, N'FABRIK', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12418, N'ラクマ', 8008, N'Hashibami', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4639, N'ラクマ', 1279, N'JILL by JILLSTUART', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3823, N'ラクマ', 1730, N'TODAYFUL', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2279, N'ラクマ', 637, N'PINK HOUSE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (21570, N'ラクマ', 5218, N'今治タオル', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (11001, N'ラクマ', 10398, N'ASAUCE MELER', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2535, N'ラクマ', 1147, N'marimekko', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (8322, N'ラクマ', 8067, N'manipuri', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2272, N'ラクマ', 371, N'HYSTERIC MINI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (72, N'ラクマ', 75, N'axes femme', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7454, N'ラクマ', 5623, N'PLEATS PLEASE ISSEY MIYAKE', 1, N'ラクマ候補Excel20261027: メルカリ7454=PLEATS PLEASE(イッセイミヤケのライン。同一ブランド)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1160, N'ラクマ', 522, N'Max Mara', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (29149, N'ラクマ', 10428, N'Kenelephant', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12706, N'ラクマ', 11789, N'Joshua Ellis', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3608, N'ラクマ', 308, N'gelato pique', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15792, N'ラクマ', 5532, N'Jim Thompson', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10181, N'ラクマ', 5348, N'45R', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (41455, N'ラクマ', 12307, N'HASEPRO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (35039, N'ラクマ', 7903, N'Seria', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (49148, N'ラクマ', 5594, N'三菱鉛筆', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (39214, N'ラクマ', 5560, N'トンボ鉛筆', 0, N'ラクマ候補Excel20261027: メルカリ39214=トンボ鉛筆。既存Tombowのラクマ10523はカナ『トンボガクセイフク』(トンボ学生服)で、トンボ鉛筆本来のラクマIDは5560');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3633, N'ラクマ', 276, N'FELISSIMO', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (49148, N'ラクマ', 4667, N'三菱', 0, N'ラクマ候補Excel20261027: ラクマ4667 三菱(ミツビシ)の出現は筆記具カテゴリーが中心(848:17件/1604:19件)。三菱鉛筆(メルカリ49148)の略称として選ばれている。メルカリ4245 三菱は三菱グループ全般で筆記具ブランドではない');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (26526, N'ラクマ', 12636, N'カミオジャパン', 1, N'ラクマ候補Excel20261027: メルカリ26526=KAMIO JAPAN(カナ カミオジャパン)。26525 Kamio Japan(カナ カミオ)より一致度が高い');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (33021, N'ラクマ', 5436, N'ショウワノート', 1, N'ラクマ候補Excel20261027: メルカリ33021=SHOWA NOTE(カナ ショウワノート)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1892, N'ラクマ', 569, N'MUJI (無印良品)', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2186, N'ラクマ', 5341, N'トイ・ストーリー', 0, N'ラクマ候補Excel20261027: トイ・ストーリーはディズニー/ピクサー作品。メルカリに個別ブランド無し。既存ディズニー(2186)へ統合');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2186, N'ラクマ', 5324, N'くまのプーさん', 0, N'ラクマ候補Excel20261027: くまのプーさんはディズニーのキャラクター。メルカリに個別ブランド無し。既存ディズニー(2186)へ統合');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1009, N'ラクマ', 665, N'PUMA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (47469, N'ラクマ', 12549, N'ホルベイン工業', 0, N'ラクマ候補Excel20261027: 既存ホルベイン(メルカリ47469 HOLBEIN、ラクマ17731=HOLBEIN)の製造元名義のラクマ別ID');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1000003, N'ラクマ', 5688, N'AURORA', 1, N'ラクマ候補Excel20261027: ラクマAURORA(カナ アウロラ=イタリアの筆記具)。メルカリ15699 AURORAはカナ『オーロラワールド』(ぬいぐるみ)で別ブランド。筆記具のAURORAはメルカリに無し');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (32497, N'ラクマ', 5545, N'Shachihata', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7693, N'ラクマ', 7873, N'HUMAN MADE', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1688, N'ラクマ', 219, N'Dickies', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1736, N'ラクマ', 584, N'NEIGHBORHOOD', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1000004, N'ラクマ', 5175, N'欅坂46(けやき坂46)', 1, N'ラクマ候補Excel20261027: メルカリに『欅坂46』のブランド登録無し');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15369, N'ラクマ', 3764, N'RRL', 1, N'ラクマ候補Excel20261027: メルカリ15369=DOUBLE RL(カナ ダブルアールエル)。RRLの正式表記');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4194, N'ラクマ', 4896, N'LEICA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4198, N'ラクマ', 5540, N'KONICA MINOLTA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3296, N'ラクマ', 4966, N'富士フイルム', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15358, N'ラクマ', 15192, N'CONTAX', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4196, N'ラクマ', 15158, N'Kenko Tokina', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15592, N'ラクマ', 15184, N'ZEISS', 1, N'ラクマ候補Excel20261027: 名称一致・カナ類似');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4197, N'ラクマ', 15277, N'COSINA', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (24997, N'ラクマ', 18411, N'OM Digital Solutions', 1, N'ラクマ候補Excel20261027: メルカリ24997=OM SYSTEM(OM Digital Solutionsのカメラブランド)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (37078, N'ラクマ', 17037, N'DJI', 1, N'ラクマ候補Excel20261027: 名称一致・カナ類似');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7202, N'ラクマ', 4882, N'GoPro', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (21803, N'ラクマ', 15130, N'insta360', 1, N'ラクマ候補Excel20261027: メルカリ21803=insta360(名称完全一致。カナは360の読み違いのみ)');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3292, N'ラクマ', 4668, N'日立', 1, N'ラクマ候補Excel20261027: 名称一致・カナ一致');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3272, N'ラクマ', 4673, N'Apple', 0, N'ラクマ候補Excel20261027: メルカリ3272=Apple。既存Appleはラクマ5294=Apple Watchのみ紐付け。ラクマ4673=Appleを追加');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1117, N'メルカリ', 1117, N'吉田カバン', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1438, N'メルカリ', 1438, N'ILBISONTE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5195, N'メルカリ', 5195, N'Berluti', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1243, N'メルカリ', 1243, N'montbell', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (572, N'メルカリ', 572, N'THE NORTH FACE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (954, N'メルカリ', 954, N'BALENCIAGA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (13163, N'メルカリ', 13163, N'Maison Margiela', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (758, N'メルカリ', 758, N'TAKEO KIKUCHI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4408, N'メルカリ', 4408, N'POLO RALPH LAUREN', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (947, N'メルカリ', 947, N'Bally', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (435, N'メルカリ', 435, N'Calvin Klein', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (176, N'メルカリ', 176, N'ALEXANDER McQUEEN', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (969, N'メルカリ', 969, N'BEAMS', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (840, N'メルカリ', 840, N'TOMMY HILFIGER', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (9940, N'メルカリ', 9940, N'ganzo', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7317, N'メルカリ', 7317, N'COCOMEISTER', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (597, N'メルカリ', 597, N'ZARA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1512, N'メルカリ', 1512, N'Orobianco', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5245, N'メルカリ', 5245, N'BRIEFING', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (563, N'メルカリ', 563, N'COMME des GARCONS', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7133, N'メルカリ', 7133, N'Valextra', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3784, N'メルカリ', 3784, N'HUNTING WORLD', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1516, N'メルカリ', 1516, N'CASTELBAJAC', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5539, N'メルカリ', 5539, N'Faliero Sarti', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6981, N'メルカリ', 6981, N'Hender Scheme', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (782, N'メルカリ', 782, N'Chums', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (497, N'メルカリ', 497, N'Christian Louboutin', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1253, N'メルカリ', 1253, N'Yohji Yamamoto', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5384, N'メルカリ', 5384, N'Crocodile', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1248, N'メルカリ', 1248, N'UNITED ARROWS', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1669, N'メルカリ', 1669, N'WTAPS', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (9933, N'メルカリ', 9933, N'土屋鞄', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3835, N'メルカリ', 3835, N'Felisi', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1620, N'メルカリ', 1620, N'SUPREME', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (686, N'メルカリ', 686, N'Giorgio Armani', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10492, N'メルカリ', 10492, N'Arnold Palmer', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (14154, N'メルカリ', 14154, N'ROOT CO', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1250, N'メルカリ', 1250, N'UNIQLO', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (356, N'メルカリ', 356, N'MCM', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6648, N'メルカリ', 6648, N'SUNSEA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4986, N'メルカリ', 4986, N'1LDK SELECT', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (952, N'メルカリ', 952, N'HARE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7331, N'メルカリ', 7331, N'Dior Homme', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (847, N'メルカリ', 847, N'DRIES VAN NOTEN', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6775, N'メルカリ', 6775, N'TUMI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (192, N'メルカリ', 192, N'UNDERCOVER', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (857, N'メルカリ', 857, N'NIKE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (873, N'メルカリ', 873, N'NUMBER (N)INE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3550, N'メルカリ', 3550, N'Munsingwear', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4253, N'メルカリ', 4253, N'Mercedes-Benz', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (867, N'メルカリ', 867, N'nano universe', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6990, N'メルカリ', 6990, N'COMOLI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4921, N'メルカリ', 4921, N'Johnstons of Elgin', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (831, N'メルカリ', 831, N'TOMORROWLAND', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (759, N'メルカリ', 759, N'Dakota', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (16759, N'メルカリ', 16759, N'm+', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7265, N'メルカリ', 7265, N'ARMANI COLLEZIONI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1720, N'メルカリ', 1720, N'TORNADO MART', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5062, N'メルカリ', 5062, N'ALTEA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (907, N'メルカリ', 907, N'BARNEYS NEW YORK', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (299, N'メルカリ', 299, N'H&M', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3045, N'メルカリ', 3045, N'LeSportsac', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1779, N'メルカリ', 1779, N'PPFM', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (512, N'メルカリ', 512, N'GREGORY', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4059, N'メルカリ', 4059, N'JULIUS', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1977, N'メルカリ', 1977, N'Loro Piana', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4109, N'メルカリ', 4109, N'Starbucks', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3014, N'メルカリ', 3014, N'MIKIMOTO', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7518, N'メルカリ', 7518, N'TASAKI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (42385, N'メルカリ', 42385, N'PANDORA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (28309, N'メルカリ', 28309, N'Cream dot.', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4753, N'メルカリ', 4753, N'Pura Vida', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (58, N'メルカリ', 58, N'agete', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (882, N'メルカリ', 882, N'NINA RICCI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15574, N'メルカリ', 15574, N'Van Cleef & Arpels', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4024, N'メルカリ', 4024, N'ete', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4728, N'メルカリ', 4728, N'Georg Jensen', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3631, N'メルカリ', 3631, N'Ron Herman', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3887, N'メルカリ', 3887, N'Vendome Aoyama', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10073, N'メルカリ', 10073, N'Jouete', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3965, N'メルカリ', 3965, N'Sanrio', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3886, N'メルカリ', 3886, N'STAR JEWELRY', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7032, N'メルカリ', 7032, N'Lochie', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15, N'メルカリ', 15, N'4℃', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (492, N'メルカリ', 492, N'green label relaxing', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3900, N'メルカリ', 3900, N'ESTNATION', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7111, N'メルカリ', 7111, N'PonteVecchio', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (826, N'メルカリ', 826, N'Deuxieme Classe', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2589, N'メルカリ', 2589, N'WEDGWOOD', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (24, N'メルカリ', 24, N'AHKAH', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2283, N'メルカリ', 2283, N'familiar', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (958, N'メルカリ', 958, N'BANDAI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2984, N'メルカリ', 2984, N'pierre cardin', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (111, N'メルカリ', 111, N'Anna Sui', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (509, N'メルカリ', 509, N'Courreges', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (748, N'メルカリ', 748, N'SONIA RYKIEL', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4749, N'メルカリ', 4749, N'Philippe Audibert', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (197, N'メルカリ', 197, N'ANTEPRIMA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (425, N'メルカリ', 425, N'Canal 4℃', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3654, N'メルカリ', 3654, N'mina perhonen', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (57, N'メルカリ', 57, N'AGATHA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5405, N'メルカリ', 5405, N'Damiani', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7238, N'メルカリ', 7238, N'Simonetta', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3833, N'メルカリ', 3833, N'Rene', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (42196, N'メルカリ', 42196, N'HELLO KITTY', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (5317, N'メルカリ', 5317, N'Christian Lacroix', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (14780, N'メルカリ', 14780, N'TOM WOOD', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10077, N'メルカリ', 10077, N'Kahiko', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3928, N'メルカリ', 3928, N'FEILER', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4040, N'メルカリ', 4040, N'leilian', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7037, N'メルカリ', 7037, N'ATAO', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (188, N'メルカリ', 188, N'Angelic Pretty', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (6578, N'メルカリ', 6578, N'SNOOPY', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (13330, N'メルカリ', 13330, N'Miffy', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (21913, N'メルカリ', 21913, N'印傳屋', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (590, N'メルカリ', 590, N'Samantha Thavasa Petit Choice', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (646, N'メルカリ', 646, N'Jimmy Choo', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3865, N'メルカリ', 3865, N'Kitamura', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3616, N'メルカリ', 3616, N'MARY QUANT', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1146, N'メルカリ', 1146, N'MARC JACOBS', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (588, N'メルカリ', 588, N'Samantha Thavasa', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (47083, N'メルカリ', 47083, N'Pokemon', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1120, N'メルカリ', 1120, N'PAUL＆JOE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10275, N'メルカリ', 10275, N'MOOMIN', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (57681, N'メルカリ', 57681, N'BONAVENTURA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (131, N'メルカリ', 131, N'AfternoonTea', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (119, N'メルカリ', 119, N'Anya Hindmarch', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (692, N'メルカリ', 692, N'JILL STUART', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1001, N'メルカリ', 1001, N'Pinky&Dianne', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2858, N'メルカリ', 2858, N'Longchamp', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7021, N'メルカリ', 7021, N'Maison de FLEUR', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (791, N'メルカリ', 791, N'tsumori chisato CARRY', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1936, N'メルカリ', 1936, N'LANVIN en Bleu', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2536, N'メルカリ', 2536, N'marni', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (17576, N'メルカリ', 17576, N'san-x', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12923, N'メルカリ', 12923, N'L''arcobaleno', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (446, N'メルカリ', 446, N'CathKidston', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12037, N'メルカリ', 12037, N'FABRIK', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12418, N'メルカリ', 12418, N'Hashibami', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4639, N'メルカリ', 4639, N'JILL by JILLSTUART', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3823, N'メルカリ', 3823, N'TODAYFUL', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2279, N'メルカリ', 2279, N'PINK HOUSE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (21570, N'メルカリ', 21570, N'今治謹製 ／ 今治タオル', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (11001, N'メルカリ', 11001, N'ASAUCE MELER', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2535, N'メルカリ', 2535, N'marimekko', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (8322, N'メルカリ', 8322, N'manipuri', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (2272, N'メルカリ', 2272, N'HYSTERIC MINI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (72, N'メルカリ', 72, N'axes femme', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7454, N'メルカリ', 7454, N'PLEATS PLEASE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1160, N'メルカリ', 1160, N'Max Mara', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (29149, N'メルカリ', 29149, N'Kenelephant', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (12706, N'メルカリ', 12706, N'Joshua Ellis', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3608, N'メルカリ', 3608, N'gelato pique', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15792, N'メルカリ', 15792, N'JimThompson', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (10181, N'メルカリ', 10181, N'45R', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (41455, N'メルカリ', 41455, N'HASEPRO', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (35039, N'メルカリ', 35039, N'Seria', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (49148, N'メルカリ', 49148, N'三菱鉛筆 ／ uni', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3633, N'メルカリ', 3633, N'FELISSIMO', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (26526, N'メルカリ', 26526, N'KAMIO JAPAN', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (33021, N'メルカリ', 33021, N'SHOWA NOTE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1892, N'メルカリ', 1892, N'無印良品', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1009, N'メルカリ', 1009, N'PUMA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (32497, N'メルカリ', 32497, N'Shachihata', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7693, N'メルカリ', 7693, N'HUMAN MADE', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1688, N'メルカリ', 1688, N'Dickies', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (1736, N'メルカリ', 1736, N'NEIGHBORHOOD', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15369, N'メルカリ', 15369, N'DOUBLE RL', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4194, N'メルカリ', 4194, N'Leica', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4198, N'メルカリ', 4198, N'KONICA MINOLTA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15358, N'メルカリ', 15358, N'CONTAX', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4196, N'メルカリ', 4196, N'KenkoTokina', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (15592, N'メルカリ', 15592, N'ZEISS', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (4197, N'メルカリ', 4197, N'COSINA', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (24997, N'メルカリ', 24997, N'OM SYSTEM', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (37078, N'メルカリ', 37078, N'DJI', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (7202, N'メルカリ', 7202, N'GoPro', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (21803, N'メルカリ', 21803, N'insta360', 1, N'メルカリID=brand_id');
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note) VALUES (3292, N'メルカリ', 3292, N'日立', 1, N'メルカリID=brand_id');

-- 3) brand_category_groups 追加(275件)
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1121, N'財布メンズ');  -- Paul Smith
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (773, N'財布メンズ');  -- Dunhill
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1069, N'財布メンズ');  -- BVLGARI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (799, N'財布メンズ');  -- DIESEL
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7556, N'財布メンズ');  -- Vivienne Westwood
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (376, N'財布メンズ');  -- EMPORIO ARMANI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1719, N'財布メンズ');  -- DOLCE&GABBANA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1478, N'財布メンズ');  -- ETRO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1912, N'財布メンズ');  -- MONTBLANC
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1284, N'財布メンズ');  -- LANVIN
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (809, N'アクセサリー');  -- Tiffany & Co.
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (658, N'アクセサリー');  -- CHANEL
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2945, N'アクセサリー');  -- SWAROVSKI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (433, N'アクセサリー');  -- Cartier
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1069, N'アクセサリー');  -- BVLGARI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2186, N'アクセサリー');  -- Disney
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (219, N'アクセサリー');  -- Yves Saint Laurent
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1205, N'アクセサリー');  -- MIU MIU
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2186, N'財布');  -- Disney
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7556, N'財布');  -- Vivienne Westwood
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1121, N'財布');  -- Paul Smith
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1069, N'財布');  -- BVLGARI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (116, N'財布');  -- agnes b.
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (219, N'財布');  -- Yves Saint Laurent
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (773, N'財布');  -- Dunhill
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (809, N'財布');  -- Tiffany & Co.
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (809, N'スカーフ');  -- Tiffany & Co.
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2186, N'スカーフ');  -- Disney
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1069, N'スカーフ');  -- BVLGARI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (219, N'スカーフ');  -- Yves Saint Laurent
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2945, N'スカーフ');  -- SWAROVSKI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2186, N'ペン');  -- Disney
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (522, N'ペン');  -- CROSS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (289, N'ペン');  -- Waterman
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (809, N'ペン');  -- Tiffany & Co.
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (52017, N'ペン');  -- LAMY
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (31766, N'ペン');  -- SHEAFFER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (433, N'ペン');  -- Cartier
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1326, N'ペン');  -- LOUIS VUITTON
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2945, N'ペン');  -- SWAROVSKI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (15531, N'ペン');  -- S.T. Dupont
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3303, N'レンズ');  -- RICOH
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3294, N'デジカメ');  -- PENTAX
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4199, N'デジカメ');  -- SIGMA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3282, N'ビデオカメラ');  -- SHARP
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1117, N'財布メンズ');  -- PORTER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1438, N'財布メンズ');  -- IL BISONTE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5195, N'財布メンズ');  -- BERLUTI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1243, N'財布メンズ');  -- mont bell
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (572, N'財布メンズ');  -- THE NORTH FACE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (954, N'財布メンズ');  -- BALENCIAGA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (13163, N'財布メンズ');  -- Maison Margiela(旧Maison Martin Margiela)
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (758, N'財布メンズ');  -- TAKEO KIKUCHI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4408, N'財布メンズ');  -- POLO RALPH LAUREN
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (947, N'財布メンズ');  -- Bally
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (435, N'財布メンズ');  -- Calvin Klein
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (176, N'財布メンズ');  -- Alexander McQueen
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (969, N'財布メンズ');  -- BEAMS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (840, N'財布メンズ');  -- TOMMY HILFIGER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (9940, N'財布メンズ');  -- GANZO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7317, N'財布メンズ');  -- COCOMEISTER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (597, N'財布メンズ');  -- ZARA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1512, N'財布メンズ');  -- Orobianco
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5245, N'財布メンズ');  -- BRIEFING
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (563, N'財布メンズ');  -- COMME des GARCONS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7133, N'財布メンズ');  -- Valextra
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3784, N'財布メンズ');  -- HUNTING WORLD
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1516, N'財布メンズ');  -- CASTELBAJAC
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5539, N'財布メンズ');  -- Faliero Sarti
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (6981, N'財布メンズ');  -- Hender Scheme
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (782, N'財布メンズ');  -- CHUMS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (497, N'財布メンズ');  -- Christian Louboutin
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1253, N'財布メンズ');  -- Yohji Yamamoto
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1000000, N'財布メンズ');  -- 栃木レザー
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5384, N'財布メンズ');  -- Crocodile
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1248, N'財布メンズ');  -- UNITED ARROWS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1669, N'財布メンズ');  -- W)taps
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (9933, N'財布メンズ');  -- 土屋鞄製造所
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3835, N'財布メンズ');  -- Felisi
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1620, N'財布メンズ');  -- Supreme
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (686, N'財布メンズ');  -- GIORGIO ARMANI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (10492, N'財布メンズ');  -- Arnold Palmer
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (14154, N'財布メンズ');  -- ROOT CO.
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1250, N'財布メンズ');  -- UNIQLO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (356, N'財布メンズ');  -- MCM
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (6648, N'財布メンズ');  -- SUNSEA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4986, N'財布メンズ');  -- 1LDK SELECT
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (952, N'財布メンズ');  -- HARE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7331, N'財布メンズ');  -- DIOR HOMME
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (847, N'財布メンズ');  -- DRIES VAN NOTEN
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (6775, N'財布メンズ');  -- TUMI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (192, N'財布メンズ');  -- UNDERCOVER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (857, N'財布メンズ');  -- NIKE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (873, N'財布メンズ');  -- NUMBER (N)INE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3550, N'財布メンズ');  -- Munsingwear
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4253, N'財布メンズ');  -- Mercedes-Benz
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (867, N'財布メンズ');  -- nano・universe
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (6990, N'財布メンズ');  -- COMOLI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4921, N'財布メンズ');  -- Johnstons
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (831, N'財布メンズ');  -- TOMORROWLAND
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (759, N'財布メンズ');  -- Dakota
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (16759, N'財布メンズ');  -- m+
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7265, N'財布メンズ');  -- ARMANI COLLEZIONI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1720, N'財布メンズ');  -- TORNADO MART
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5062, N'財布メンズ');  -- ALTEA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (907, N'財布メンズ');  -- BARNEYS NEW YORK
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (299, N'財布メンズ');  -- H&M
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3045, N'財布メンズ');  -- LeSportsac
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1779, N'財布メンズ');  -- PPFM
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (512, N'財布メンズ');  -- Gregory
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4059, N'財布メンズ');  -- JULIUS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1977, N'財布メンズ');  -- Loro Piana
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4109, N'財布メンズ');  -- Starbucks
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3014, N'アクセサリー');  -- MIKIMOTO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7518, N'アクセサリー');  -- TASAKI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (42385, N'アクセサリー');  -- PANDORA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (28309, N'アクセサリー');  -- Cream dot
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4753, N'アクセサリー');  -- Pura Vida
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (58, N'アクセサリー');  -- agete
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (882, N'アクセサリー');  -- NINA RICCI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (15574, N'アクセサリー');  -- Van Cleef & Arpels
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4024, N'アクセサリー');  -- ete
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4728, N'アクセサリー');  -- Georg Jensen
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3631, N'アクセサリー');  -- Ron Herman
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1000001, N'アクセサリー');  -- AVON
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3887, N'アクセサリー');  -- Vendome Aoyama
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (10073, N'アクセサリー');  -- Jouete
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3965, N'アクセサリー');  -- サンリオ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3886, N'アクセサリー');  -- STAR JEWELRY
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7032, N'アクセサリー');  -- Lochie
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (15, N'アクセサリー');  -- 4°C
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (492, N'アクセサリー');  -- UNITED ARROWS green label relaxing
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3900, N'アクセサリー');  -- ESTNATION
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7111, N'アクセサリー');  -- Ponte Vecchio
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (826, N'アクセサリー');  -- DEUXIEME CLASSE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2589, N'アクセサリー');  -- WEDGWOOD
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (24, N'アクセサリー');  -- AHKAH
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2283, N'アクセサリー');  -- familiar
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (958, N'アクセサリー');  -- BANDAI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2984, N'アクセサリー');  -- pierre cardin
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (111, N'アクセサリー');  -- ANNA SUI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (509, N'アクセサリー');  -- Courreges
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (748, N'アクセサリー');  -- SONIA RYKIEL
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4749, N'アクセサリー');  -- Philippe Audibert
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (197, N'アクセサリー');  -- ANTEPRIMA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (425, N'アクセサリー');  -- canal4°C
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3654, N'アクセサリー');  -- mina perhonen
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (57, N'アクセサリー');  -- AGATHA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5405, N'アクセサリー');  -- Damiani
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7238, N'アクセサリー');  -- Simonetta
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (954, N'アクセサリー');  -- BALENCIAGA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3833, N'アクセサリー');  -- Rene
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (42196, N'アクセサリー');  -- ハローキティ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5317, N'アクセサリー');  -- Christian Lacroix
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (14780, N'アクセサリー');  -- TOM WOOD
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (10077, N'アクセサリー');  -- KAHIKO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3928, N'アクセサリー');  -- FEILER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4040, N'アクセサリー');  -- leilian
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7037, N'アクセサリー');  -- ATAO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (188, N'アクセサリー');  -- Angelic Pretty
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3965, N'財布');  -- サンリオ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (6578, N'財布');  -- SNOOPY
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2283, N'財布');  -- familiar
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1438, N'財布');  -- IL BISONTE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (954, N'財布');  -- BALENCIAGA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3045, N'財布');  -- LeSportsac
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (13330, N'財布');  -- miffy
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4109, N'財布');  -- Starbucks
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (42196, N'財布');  -- ハローキティ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1000002, N'財布');  -- ちいかわ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (21913, N'財布');  -- 印傳屋
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (111, N'財布');  -- ANNA SUI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (590, N'財布');  -- Samantha Thavasa Petit Choice
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7037, N'財布');  -- ATAO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (646, N'財布');  -- JIMMY CHOO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3865, N'財布');  -- Kitamura
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3616, N'財布');  -- MARY QUANT
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1146, N'財布');  -- MARC JACOBS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (588, N'財布');  -- Samantha Thavasa
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (47083, N'財布');  -- ポケモン
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3928, N'財布');  -- FEILER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1120, N'財布');  -- PAUL & JOE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (10275, N'財布');  -- MOOMIN
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (197, N'財布');  -- ANTEPRIMA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (57681, N'財布');  -- BONAVENTURA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (131, N'財布');  -- AfternoonTea
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (119, N'財布');  -- ANYA HINDMARCH
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (692, N'財布');  -- JILLSTUART
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1001, N'財布');  -- Pinky&Dianne
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (759, N'財布');  -- Dakota
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2858, N'財布');  -- LONGCHAMP
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7021, N'財布');  -- Maison de FLEUR
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (791, N'財布');  -- tsumori chisato CARRY
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1936, N'財布');  -- LANVIN en Bleu
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2536, N'財布');  -- MARNI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (17576, N'財布');  -- リラックマ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (12923, N'財布');  -- L'arcobaleno
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1243, N'財布');  -- mont bell
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (446, N'財布');  -- Cath Kidston
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (12037, N'財布');  -- FABRIK
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (12418, N'財布');  -- Hashibami
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3965, N'スカーフ');  -- サンリオ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4921, N'スカーフ');  -- Johnstons
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (5539, N'スカーフ');  -- Faliero Sarti
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (958, N'スカーフ');  -- BANDAI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4639, N'スカーフ');  -- JILL by JILLSTUART
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3823, N'スカーフ');  -- TODAYFUL
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2279, N'スカーフ');  -- PINK HOUSE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4109, N'スカーフ');  -- Starbucks
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (21570, N'スカーフ');  -- 今治タオル
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (11001, N'スカーフ');  -- ASAUCE MELER
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1000002, N'スカーフ');  -- ちいかわ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1248, N'スカーフ');  -- UNITED ARROWS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2535, N'スカーフ');  -- marimekko
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (8322, N'スカーフ');  -- manipuri
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (13330, N'スカーフ');  -- miffy
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2272, N'スカーフ');  -- HYSTERIC MINI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (72, N'スカーフ');  -- axes femme
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7454, N'スカーフ');  -- PLEATS PLEASE ISSEY MIYAKE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (42196, N'スカーフ');  -- ハローキティ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1160, N'スカーフ');  -- Max Mara
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (29149, N'スカーフ');  -- Kenelephant
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (10275, N'スカーフ');  -- MOOMIN
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (12706, N'スカーフ');  -- Joshua Ellis
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3608, N'スカーフ');  -- gelato pique
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (692, N'スカーフ');  -- JILLSTUART
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (15792, N'スカーフ');  -- Jim Thompson
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (6578, N'スカーフ');  -- SNOOPY
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (2283, N'スカーフ');  -- familiar
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (10181, N'スカーフ');  -- 45R
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7265, N'スカーフ');  -- ARMANI COLLEZIONI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (41455, N'スカーフ');  -- HASEPRO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (35039, N'スカーフ');  -- Seria
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1243, N'スカーフ');  -- mont bell
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3631, N'スカーフ');  -- Ron Herman
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (49148, N'ペン');  -- 三菱鉛筆
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3633, N'ペン');  -- FELISSIMO
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3965, N'ペン');  -- サンリオ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (42196, N'ペン');  -- ハローキティ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (26526, N'ペン');  -- カミオジャパン
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1000002, N'ペン');  -- ちいかわ
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (6578, N'ペン');  -- SNOOPY
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (33021, N'ペン');  -- ショウワノート
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1892, N'ペン');  -- MUJI (無印良品)
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (47083, N'ペン');  -- ポケモン
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (13330, N'ペン');  -- miffy
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1009, N'ペン');  -- PUMA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1000003, N'ペン');  -- AURORA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (32497, N'ペン');  -- Shachihata
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1620, N'スカーフメンズ');  -- Supreme
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7693, N'スカーフメンズ');  -- HUMAN MADE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1688, N'スカーフメンズ');  -- Dickies
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1736, N'スカーフメンズ');  -- NEIGHBORHOOD
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (572, N'スカーフメンズ');  -- THE NORTH FACE
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1000004, N'スカーフメンズ');  -- 欅坂46(けやき坂46)
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (15369, N'スカーフメンズ');  -- RRL
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4253, N'スカーフメンズ');  -- Mercedes-Benz
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4408, N'スカーフメンズ');  -- POLO RALPH LAUREN
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (1669, N'スカーフメンズ');  -- W)taps
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4986, N'スカーフメンズ');  -- 1LDK SELECT
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4194, N'レンズ');  -- LEICA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4198, N'レンズ');  -- KONICA MINOLTA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (15358, N'レンズ');  -- CONTAX
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4196, N'レンズ');  -- Kenko Tokina
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (15592, N'レンズ');  -- ZEISS
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4197, N'レンズ');  -- COSINA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4194, N'デジカメ');  -- LEICA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (24997, N'デジカメ');  -- OM Digital Solutions
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4198, N'デジカメ');  -- KONICA MINOLTA
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (4196, N'デジカメ');  -- Kenko Tokina
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (37078, N'ビデオカメラ');  -- DJI
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (7202, N'ビデオカメラ');  -- GoPro
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (21803, N'ビデオカメラ');  -- insta360
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (3292, N'ビデオカメラ');  -- 日立
INSERT INTO mst.brand_category_groups (brand_id, category_group) VALUES (495, N'腕時計');  -- Christian Dior

COMMIT;
