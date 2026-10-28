-- ============================================================================
-- mst.vendor_brand_map（仕入先別ブランドID）新設と既存データの移行
-- 作成日: 2026-10-27（dev: nostock_dev で適用・検証）
--
-- ブランド管理の構造:
--   mst.presets_brand        共通ブランドマスター（1ブランド1行）。brand_id は原則メルカリのブランドID。
--                            メルカリに存在しないブランドのみ 1,000,000 から独自採番する。
--                            ブランド名・英語名・価格区分・ハイリスク設定はここで一元管理する。
--   mst.vendor_brand_map     仕入先ごとの検索用ブランドID。1ブランドに同じ仕入先のIDを複数持てる
--                            （例: Apple → ラクマ 5294 Apple Watch / 4673 Apple）。
--   mst.brand_category_groups 仕入先共通のカテゴリーグループ所属（変更なし）。
--
-- vendor_name 列には mst.vendors.vendor_group（ブランドIDの採番体系）を入れる。
-- メルカリshops はメルカリのブランドIDを共有するため 'メルカリ' の行で両方に効く。
-- 将来 楽天・ヤフオク等を追加する場合は、mst.vendors に仕入先を追加し、その vendor_group で行を追加する。
--
-- is_primary = 1 の行は preset 名を「ブランド名+カテゴリー名+men+仕入先コード」のまま使う
-- （既存 preset 名を変えないため、移行した既存IDはすべて primary）。
-- is_primary = 0 の行（同一仕入先の2つ目以降のID）は preset 名の末尾に「#仕入先ブランドID」を付ける。
--
-- presets_brand.brand_id_rkm は移行後は参照しない（refresh_presets_lookup は vendor_brand_map を読む）。
-- Access のリンクテーブル(mst_presets_brand)が列を持っているため、列自体は残している。
-- ============================================================================

SET XACT_ABORT ON;
BEGIN TRAN;

CREATE TABLE mst.vendor_brand_map (
    brand_id          int            NOT NULL,
    vendor_name       varchar(50)    COLLATE Japanese_CI_AS NOT NULL,
    vendor_brand_id   bigint         NOT NULL,
    vendor_brand_name nvarchar(255)  NULL,
    is_primary        bit            NOT NULL CONSTRAINT DF_vendor_brand_map_is_primary DEFAULT (1),
    is_active         bit            NOT NULL CONSTRAINT DF_vendor_brand_map_is_active DEFAULT (1),
    note              nvarchar(400)  NULL,
    created_at        datetime2(0)   NOT NULL CONSTRAINT DF_vendor_brand_map_created_at DEFAULT (SYSDATETIME()),
    CONSTRAINT PK_vendor_brand_map PRIMARY KEY (brand_id, vendor_name, vendor_brand_id),
    CONSTRAINT FK_vendor_brand_map_presets_brand FOREIGN KEY (brand_id) REFERENCES mst.presets_brand (brand_id)
);

-- 同一ブランド・同一仕入先で preset 名が衝突しないよう、primary は1件だけ
CREATE UNIQUE INDEX UX_vendor_brand_map_primary
    ON mst.vendor_brand_map (brand_id, vendor_name)
    WHERE is_primary = 1;

-- 仕入先ブランドIDからの逆引き用
CREATE INDEX IX_vendor_brand_map_vendor
    ON mst.vendor_brand_map (vendor_name, vendor_brand_id);

-- 既存データの移行（presets_brand の内容をそのまま写す。is_active=0 のブランドも写す）
-- メルカリ: brand_id がメルカリIDそのもの
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note)
SELECT brand_id, 'メルカリ', brand_id, brand_name_ja, 1, N'移行: presets_brand.brand_id'
FROM mst.presets_brand
WHERE brand_id < 1000000;

-- ラクマ: presets_brand.brand_id_rkm
INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, note)
SELECT brand_id, 'ラクマ', brand_id_rkm, brand_name_ja, 1, N'移行: presets_brand.brand_id_rkm'
FROM mst.presets_brand
WHERE brand_id_rkm IS NOT NULL;

COMMIT;
