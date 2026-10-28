-- ============================================================================
-- mst.ebay_account_excluded_vendors（eBayアカウント別の除外仕入先）新設
-- dbo.fn_take_one_candidates_for_account（アカウント別の出品候補TVF）新設
-- 作成日: 2026-10-27（dev: nostock_dev で適用・検証）
--
-- mst.presets_lookup は全アカウント共通のまま。出品候補を選ぶ時点で、
-- 対象アカウントの除外仕入先(vendor_code)の商品を候補から外す。
-- 除外行が無いアカウントは従来の dbo.fn_take_one_candidates と同じ結果になる。
-- mst.vendors.publish_priority は変更しない。
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

INSERT INTO mst.ebay_account_excluded_vendors (account, vendor_code, note)
VALUES (N'BUZZ②', 'R', N'BUZZ②ではラクマのプリセットを使用しない');

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
