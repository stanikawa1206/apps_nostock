-- ============================================================================
-- dbo.fn_take_one_candidates（本番・ラッパー版）
-- 適用日: 2026-10-14
--
-- ③共通TVF化後の本番TVF。候補抽出ロジックは dbo.fn_take_one_candidates_base に
-- 全面移譲し、ここでは base が返した候補の中から、既存仕様どおり
-- publish_priority が全体最小の候補だけを絞り込む（2026-11-17 category_priority を削除。
-- drop_category_priority_20261117.sql。それまでは (publish_priority, category_priority) の組合せ）。
--
-- 関数名・引数(@preset_group)・戻り値（列構成）・絞り込みロジックは
-- 変更前(fn_take_one_candidates_backup_20261014_before_shared_tvf.sql)と完全に同一。
-- publish_ebay.py 側のコードは無変更。
--
-- 変更前に戻す場合は fn_take_one_candidates_backup_20261014_before_shared_tvf.sql を
-- そのまま ALTER FUNCTION として実行すればよい（rollback_shared_tvf.sql 参照）。
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.fn_take_one_candidates
(
    @preset_group NVARCHAR(100)
)
RETURNS TABLE
AS
RETURN
(
    SELECT *
    FROM dbo.fn_take_one_candidates_base(@preset_group) t
    WHERE t.publish_priority =
    (
        SELECT MIN(publish_priority)
        FROM dbo.fn_take_one_candidates_base(@preset_group)
    )
);
