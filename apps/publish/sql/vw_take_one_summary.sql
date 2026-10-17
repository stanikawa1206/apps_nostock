-- ============================================================================
-- dbo.vw_take_one_summary
-- 最終更新: 2026-10-14
--
-- 列: preset_group, category_group, item_count, vendor_name（変更前と同じ4列・同じ順序）
--
-- 集計元は本番と共有の dbo.fn_take_one_candidates_base（ロックヒント無し、
-- dbo.fn_take_one_candidates と同一の候補抽出ロジック）。出品条件は
-- fn_take_one_candidates_base の1箇所に一元化されており、本ビューと本番TVFの
-- どちらも同じ定義を参照するため、条件変更時の二重管理・ドリフトは発生しない。
--
-- 経緯:
--   1) 当初は fn_take_one_candidates_base（ロックヒント無し）を直接参照していたが、
--      本番VPS側の在庫更新トランザクションとページロックが衝突し、デッドロック(40001)が
--      頻発することを確認（system_health拡張イベントのデッドロックグラフで確認済み。
--      本クエリのSELECTは常にvictim側となり、書き込み側は正常に完了していた＝
--      本番データ更新への実害は無し）。
--   2) 対策として一時的に NOLOCK 付きの専用TVF fn_take_one_candidates_base_nolock を
--      新設して回避したが、出品条件が二重管理になる問題があった。
--   3) 本クエリを実行するセッション側で SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
--      を指定すれば、NOLOCKヒントと同様にブロッキング・デッドロックを回避できることを検証
--      (Accessのパススルークエリ相当のバッチで3回連続成功、約17秒で安定)。
--      これにより fn_take_one_candidates_base_nolock は不要と判断し削除、本ビューは
--      fn_take_one_candidates_base を直接参照する構成に戻した（出品条件は一元管理）。
--
-- 【Accessからの参照方法】
--   通常のリンクテーブルとして本ビューを開くと、既定の分離レベル(READ COMMITTED)で
--   接続されるため、デッドロックのリスクが残る。安全に参照するには、Accessで
--   パススルークエリを作成し、以下のSQLをそのまま1バッチとして実行すること。
--
--     SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
--     SELECT preset_group, category_group, item_count, vendor_name
--     FROM dbo.vw_take_one_summary;
--
--   出品計画のための概算値であり、同時書き込みによる多少の誤差は許容する方針
--   （2026-10-14 ユーザー承認）。
-- ============================================================================

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
    dbo.fn_take_one_candidates_base(p.preset_group) AS c
GROUP BY
    p.preset_group,
    c.category_group,
    c.vendor_name;
