-- ============================================================================
-- 旧価格設定の廃止（2026-11-14）
--   mst.presets_price_ranges（アカウント×カテゴリーグループ×仕入先＋delete_flag）への一元化の続き。
--   1. presets_price_ranges に keep_low/high_usd_target（既存出品の終了判定に使う範囲）を追加し、
--      mst.category_groups の low/high_usd_target をそのまま写す（lookup の keep_* は変わらない）。
--   2. dbo.fn_take_one_candidates_for_account から mst.ebay_account_excluded_vendors の除外をやめる
--      （新規出品しない仕入先は delete_flag=1 の行か行なしで、base 関数が既に除いている）。
--   3. mst.ebay_account_excluded_vendors を削除。
--   4. mst.category_groups の low/high_usd_target・low/high_jpy_target を削除（mode 等は残す）。
-- 適用は apply_drop_old_price_settings_20261114.py（1トランザクション・検査付き）。戻しは *_rollback.sql。
-- ※ mst.v_presets は 2026-09-03 から is_listing_target が無く壊れている（今回の対象外・未修正）。
-- ============================================================================

ALTER TABLE mst.presets_price_ranges ADD
    keep_low_usd_target  decimal(10,2) NULL,
    keep_high_usd_target decimal(10,2) NULL;
GO

UPDATE r
   SET r.keep_low_usd_target  = cg.low_usd_target,
       r.keep_high_usd_target = cg.high_usd_target
  FROM mst.presets_price_ranges r
 INNER JOIN mst.category_groups cg ON cg.category_group = r.category_group;
GO

-- ----------------------------------------------------------------------------
-- 本番TVF dbo.fn_take_one_candidates と同じ絞り込み（publish_priority/category_priority が
-- 全体最小の組合せ）をアカウントごとに行う。新規出品しない仕入先は mst.presets_price_ranges の
-- delete_flag=1（または行なし）で base 関数が除くので、ここでは除外しない（2026-11-14
-- アカウント別の除外仕入先テーブルを廃止）。@account は呼び出し元との互換のため残す。
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

DROP TABLE mst.ebay_account_excluded_vendors;
GO

ALTER TABLE mst.category_groups DROP COLUMN low_usd_target, high_usd_target, low_jpy_target, high_jpy_target;
GO
