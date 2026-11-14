-- ============================================================================
-- drop_old_price_settings_20261114.sql の戻し（値は 2026-11-14 適用直前のもの）
-- 先に daily_check.py / publish_ebay.py / update_jpy_price_range.py を戻すこと
-- （戻した後のコードは category_groups の価格列と ebay_account_excluded_vendors を読む）。
-- mst.v_presets・dbo.vw_vendor_item_unlisted_active は削除前も壊れていた（定義だけ戻す）。
-- ============================================================================
SET XACT_ABORT ON;
BEGIN TRAN;

CREATE TABLE mst.ebay_account_excluded_vendors (
    account      nvarchar(100) COLLATE Japanese_CI_AS NOT NULL,
    vendor_code  varchar(10)   COLLATE Japanese_CI_AS NOT NULL,
    note         nvarchar(400) NULL,
    created_at   datetime2(0)  NOT NULL CONSTRAINT DF_ebay_account_excluded_vendors_created_at DEFAULT (SYSDATETIME()),
    CONSTRAINT PK_ebay_account_excluded_vendors PRIMARY KEY (account, vendor_code),
    CONSTRAINT FK_ebay_account_excluded_vendors_account FOREIGN KEY (account) REFERENCES mst.ebay_accounts (account),
    CONSTRAINT FK_ebay_account_excluded_vendors_vendor FOREIGN KEY (vendor_code) REFERENCES mst.vendors (vendor_code)
);
INSERT INTO mst.ebay_account_excluded_vendors (account, vendor_code, note, created_at) VALUES
    (N'BUZZ②', N'R', NULL, '2026-10-28T11:32:53'),
    (N'BUZZ②', N'Y', NULL, '2026-10-28T11:32:53'),
    (N'貴文③', N'R', NULL, '2026-10-28T15:39:21'),
    (N'貴文③', N'Y', NULL, '2026-10-28T15:39:21'),
    (N'谷川④', N'M', N'谷川④はM・MSの新規出品を停止（既存出品は販売終了まで在庫管理）', '2026-11-12T20:27:50'),
    (N'谷川④', N'MS', N'谷川④はM・MSの新規出品を停止（既存出品は販売終了まで在庫管理）', '2026-11-12T20:27:50');

ALTER TABLE mst.category_groups ADD
    low_usd_target  decimal(10,2) NULL,
    high_usd_target decimal(10,2) NULL,
    low_jpy_target  int NULL,
    high_jpy_target int NULL;
COMMIT;
GO

SET XACT_ABORT ON;
BEGIN TRAN;
UPDATE mst.category_groups SET low_usd_target = 300.00, high_usd_target = 1500.00, low_jpy_target = 23670, high_jpy_target = 131550 WHERE category_group = N'アクセサリー';
UPDATE mst.category_groups SET low_usd_target = 100.00, high_usd_target = 1500.00, low_jpy_target = 5690, high_jpy_target = 131550 WHERE category_group = N'スカーフ';
UPDATE mst.category_groups SET low_usd_target = 100.00, high_usd_target = 1500.00, low_jpy_target = 5690, high_jpy_target = 131550 WHERE category_group = N'スカーフメンズ';
UPDATE mst.category_groups SET low_usd_target = 200.00, high_usd_target = 800.00, low_jpy_target = 14680, high_jpy_target = 68620 WHERE category_group = N'デジカメ';
UPDATE mst.category_groups SET low_usd_target = 50.00, high_usd_target = 100.00, low_jpy_target = 1195, high_jpy_target = 5690 WHERE category_group = N'トレカ';
UPDATE mst.category_groups SET low_usd_target = 200.00, high_usd_target = 800.00, low_jpy_target = 14680, high_jpy_target = 68620 WHERE category_group = N'ビデオカメラ';
UPDATE mst.category_groups SET low_usd_target = 50.00, high_usd_target = 1500.00, low_jpy_target = 1195, high_jpy_target = 131550 WHERE category_group = N'ペン';
UPDATE mst.category_groups SET low_usd_target = 200.00, high_usd_target = 800.00, low_jpy_target = 14680, high_jpy_target = 68620 WHERE category_group = N'レンズ';
UPDATE mst.category_groups SET low_usd_target = 100.00, high_usd_target = 1500.00, low_jpy_target = 5690, high_jpy_target = 169345 WHERE category_group = N'財布';
UPDATE mst.category_groups SET low_usd_target = 100.00, high_usd_target = 1500.00, low_jpy_target = 5690, high_jpy_target = 169345 WHERE category_group = N'財布メンズ';
UPDATE mst.category_groups SET low_usd_target = 50.00, high_usd_target = 1500.00, low_jpy_target = 1195, high_jpy_target = 131550 WHERE category_group = N'製図用品';
UPDATE mst.category_groups SET low_usd_target = 50.00, high_usd_target = 100.00, low_jpy_target = 1195, high_jpy_target = 5690 WHERE category_group = N'遊戯王カード';
UPDATE mst.category_groups SET low_usd_target = 100.00, high_usd_target = 300.00, low_jpy_target = 5690, high_jpy_target = 23670 WHERE category_group = N'腕時計';
ALTER TABLE mst.category_groups ALTER COLUMN low_usd_target  decimal(10,2) NOT NULL;
ALTER TABLE mst.category_groups ALTER COLUMN high_usd_target decimal(10,2) NOT NULL;
ALTER TABLE mst.presets_price_ranges DROP COLUMN keep_low_usd_target, keep_high_usd_target;
COMMIT;
GO

-- ----------------------------------------------------------------------------
-- 本番TVF dbo.fn_take_one_candidates と同じ絞り込み（publish_priority/category_priority が
-- 全体最小の組合せ）を、アカウントの除外仕入先を除いた候補の中で行う。
-- 除外を絞り込みの後に掛けると、除外仕入先(例: ラクマ=publish_priority 1)が最小優先度を
-- 占めた時に候補0件(=EMPTY)と誤判定するため、MIN の計算前に除外する。
-- dbo.fn_take_one_candidates(@preset_group) は Access・旧コード向けにそのまま残す。
-- ----------------------------------------------------------------------------
CREATE OR ALTER FUNCTION dbo.fn_take_one_candidates_for_account
(
    @preset_group NVARCHAR(100),
    @account      NVARCHAR(100)
)
RETURNS TABLE
AS
RETURN
(
    WITH Allowed AS
    (
        SELECT t.*
        FROM dbo.fn_take_one_candidates_base(@preset_group) t
        WHERE NOT EXISTS (
            SELECT 1
            FROM mst.ebay_account_excluded_vendors x
            INNER JOIN mst.vendors vd
                ON vd.vendor_code = x.vendor_code
            WHERE x.account = @account
              AND vd.vendor_name = t.vendor_name
        )
    )
    SELECT *
    FROM Allowed a
    WHERE a.publish_priority * 1000 + a.category_priority =
    (
        SELECT MIN(publish_priority * 1000 + category_priority)
        FROM Allowed
    )
);
GO

CREATE OR ALTER VIEW mst.v_presets
AS
SELECT pb.brand_name_ja + pc.category_name_ja + CASE WHEN pc.department = 'Men' THEN 'men' ELSE '' END + v.vendor_code AS preset,
       v.vendor_name, pb.brand_id, cgc.category_id, cg.mode, cg.low_usd_target, cg.high_usd_target, pc.category_id_ebay, pc.department, pb.default_brand_en, pc.type_ebay,
       cg.low_jpy_target, cg.high_jpy_target, cg.category_group, pb.is_active, pb.is_listing_target
FROM mst.presets_brand pb
INNER JOIN mst.brand_category_groups bcg ON pb.brand_id = bcg.brand_id
INNER JOIN mst.category_group_categories cgc ON bcg.category_group = cgc.category_group
INNER JOIN mst.presets_categories pc ON cgc.category_id = pc.category_id
INNER JOIN mst.category_groups cg ON cgc.category_group = cg.category_group
CROSS JOIN mst.vendors v
WHERE pb.is_active = 1 AND cgc.is_brand_dependent = 0 AND cg.is_scrape_stopped = 0
UNION ALL
SELECT pb.brand_name_ja + pc.category_name_ja + CASE WHEN pc.department = 'Men' THEN 'men' ELSE '' END + v.vendor_code AS preset,
       v.vendor_name, pb.brand_id, pc.category_id, cg.mode, cg.low_usd_target, cg.high_usd_target, pc.category_id_ebay, pc.department, pb.default_brand_en, pc.type_ebay,
       cg.low_jpy_target, cg.high_jpy_target, cg.category_group, pb.is_active, pb.is_listing_target
FROM mst.presets_categories pc
INNER JOIN mst.presets_brand pb ON pb.brand_id = pc.brand_id
INNER JOIN mst.category_group_categories cgc ON pc.category_id = cgc.category_id
INNER JOIN mst.category_groups cg ON cgc.category_group = cg.category_group
CROSS JOIN mst.vendors v
WHERE pb.is_active = 1 AND cgc.is_brand_dependent = 1 AND cg.is_scrape_stopped = 0;
GO

CREATE OR ALTER VIEW dbo.vw_vendor_item_unlisted_active AS

SELECT

    vendor_item.*,

    preset_info.mode,

    preset_info.low_usd_target,

    preset_info.high_usd_target

FROM trx.vendor_item AS vendor_item

LEFT JOIN mst.v_presets AS preset_info

    ON preset_info.preset = vendor_item.preset

WHERE

    vendor_item.status = N'販売中'

    AND NOT EXISTS (

        SELECT 1

        FROM trx.listings

        WHERE trx.listings.vendor_name = vendor_item.vendor_name

          AND trx.listings.vendor_item_id = vendor_item.vendor_item_id

    );
GO
