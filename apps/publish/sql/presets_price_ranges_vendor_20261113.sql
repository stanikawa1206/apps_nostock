-- ============================================================================
-- mst.presets_price_ranges を アカウント(preset_group)×カテゴリーグループ×仕入先(vendor_code) に展開する
-- 適用日: 2026-11-13
--
-- 実行は apps/publish/sql/apply_presets_price_ranges_vendor_20261113.py から（1トランザクション。
-- この後に fn_take_one_candidates_base.sql を同じトランザクションで適用し、検査してから commit する）。
-- 戻し方: presets_price_ranges_vendor_20261113_rollback.sql
--
-- 展開のルール（変更前の出品条件をそのまま再現する）:
--   変更前の1行 (preset_group, category_group) を、そのカテゴリーグループに検索カテゴリー
--   （mst.category_group_categories.vendor_group）がある仕入先ごとの行にする。USD・JPY・category_priority は引き継ぐ。
--   - 新規出品除外（mst.ebay_account_excluded_vendors）に無い仕入先 → delete_flag = 0（新規出品する）
--   - 除外中の仕入先で、そのアカウントの未終了出品がそのカテゴリーグループ×仕入先に残っている
--     → delete_flag = 1（新規出品しない。既存出品の在庫管理・lookup の漸減のために残す）
--   - 除外中で既存出品も無い仕入先 → 行を作らない
--
-- 既存出品を価格範囲外で終了する判定の範囲として、mst.presets_lookup に keep_low/high_usd_target を追加する
-- （既存行は現在の low/high_usd_target＝従来の範囲 で埋める。以後は refresh_presets_lookup が作る）。
-- ============================================================================

SET XACT_ABORT ON;

SELECT preset_group, category_group, low_usd_target, high_usd_target,
       low_jpy_target, high_jpy_target, category_priority
INTO #ppr_old
FROM mst.presets_price_ranges;

SELECT
    o.preset_group,
    o.category_group,
    vd.vendor_code,
    o.low_usd_target,
    o.high_usd_target,
    o.low_jpy_target,
    o.high_jpy_target,
    o.category_priority,
    CAST(CASE WHEN x.vendor_code IS NULL THEN 0 ELSE 1 END AS bit) AS delete_flag
INTO #ppr_new
FROM #ppr_old AS o
INNER JOIN mst.vendors AS vd
    ON EXISTS (
        SELECT 1 FROM mst.category_group_categories AS c
        WHERE c.category_group = o.category_group AND c.vendor_group = vd.vendor_group
    )
LEFT JOIN mst.ebay_accounts AS a
    ON a.preset_group = o.preset_group
LEFT JOIN mst.ebay_account_excluded_vendors AS x
    ON x.account = a.account AND x.vendor_code = vd.vendor_code
WHERE x.vendor_code IS NULL
   OR EXISTS (
        SELECT 1
        FROM trx.listings AS l
        INNER JOIN trx.vendor_item AS v
            ON v.vendor_name = l.vendor_name AND v.vendor_item_id = l.vendor_item_id
        INNER JOIN mst.presets_lookup AS p
            ON p.preset = v.preset
        WHERE l.is_deleted = 0
          AND l.account = a.account
          AND l.vendor_name = vd.vendor_name
          AND p.category_group = o.category_group
   );

ALTER TABLE mst.presets_price_ranges DROP CONSTRAINT PK_presets_price_ranges_new;
DELETE FROM mst.presets_price_ranges;
ALTER TABLE mst.presets_price_ranges ADD vendor_code varchar(10) NOT NULL;
ALTER TABLE mst.presets_price_ranges ADD delete_flag bit NOT NULL
    CONSTRAINT DF_mst_presets_price_ranges_delete_flag DEFAULT (0);
GO

INSERT INTO mst.presets_price_ranges (
    preset_group, category_group, vendor_code, low_usd_target, high_usd_target,
    low_jpy_target, high_jpy_target, category_priority, delete_flag
)
SELECT preset_group, category_group, vendor_code, low_usd_target, high_usd_target,
       low_jpy_target, high_jpy_target, category_priority, delete_flag
FROM #ppr_new;

ALTER TABLE mst.presets_price_ranges ADD CONSTRAINT PK_presets_price_ranges
    PRIMARY KEY CLUSTERED (preset_group, category_group, vendor_code);
ALTER TABLE mst.presets_price_ranges ADD CONSTRAINT FK_presets_price_ranges_vendor_code
    FOREIGN KEY (vendor_code) REFERENCES mst.vendors (vendor_code);

ALTER TABLE mst.presets_lookup ADD keep_low_usd_target decimal(10, 2) NULL, keep_high_usd_target decimal(10, 2) NULL;
GO

UPDATE mst.presets_lookup
SET keep_low_usd_target = low_usd_target,
    keep_high_usd_target = high_usd_target;

ALTER TABLE mst.presets_lookup ALTER COLUMN keep_low_usd_target decimal(10, 2) NOT NULL;
ALTER TABLE mst.presets_lookup ALTER COLUMN keep_high_usd_target decimal(10, 2) NOT NULL;
