-- ============================================================================
-- ロールバック用: ③共通TVF化（2026-10-14適用）を元に戻す
--
-- 実行すると、dbo.fn_take_one_candidates と dbo.vw_take_one_summary を
-- 共通TVF化・全候補集計化の前の状態（fn_take_one_candidates単体で完結する
-- 優先順位フィルタ込みのロジック）へ即時復元する。
--
-- 手順:
--   1. fn_take_one_candidates を変更前の定義（バックアップファイルの内容）へ戻す
--   2. vw_take_one_summary を「fn_take_one_candidates（フィルタ後）」を参照する形へ戻す
--   3. （任意）不要になった fn_take_one_candidates_base を削除
--      ※ 1・2を先に実行してから行うこと（先に消すとビュー・TVFがエラーになる）
-- ============================================================================

-- ---- 1. 本番TVFを変更前の定義へ復元 ----
-- fn_take_one_candidates_backup_20261014_before_shared_tvf.sql の内容をそのまま実行してください。
-- （CREATE OR ALTER FUNCTION [dbo].[fn_take_one_candidates] ... ）

-- ---- 2. 集計ビューを変更前の定義へ復元 ----
CREATE OR ALTER VIEW dbo.vw_take_one_summary
AS
SELECT
    p.preset_group,
    c.category_group,
    COUNT(*) AS item_count,
    c.vendor_name
FROM
    (
        SELECT DISTINCT
            preset_group
        FROM mst.presets_price_ranges
    ) AS p
CROSS APPLY
    dbo.fn_take_one_candidates(p.preset_group) AS c
GROUP BY
    p.preset_group,
    c.category_group,
    c.vendor_name;
GO

-- ---- 3. （任意）base TVFの削除。1・2を実行し、両方が正常動作することを確認してから実行する ----
-- DROP FUNCTION IF EXISTS dbo.fn_take_one_candidates_base;
--
-- 補足: fn_take_one_candidates_base_nolock は2026-10-14に一時的に作成されたが、
-- 集計側セッションを READ UNCOMMITTED にする方式へ切り替えたことで不要になり、
-- 既に削除済み（vw_take_one_summary.sql を参照）。
