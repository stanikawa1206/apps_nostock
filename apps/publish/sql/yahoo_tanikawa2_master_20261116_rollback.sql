-- 段階1 マスターの戻し（2026-11-16）。先に enable の戻し（presets_price_ranges の Y 行の削除）を流すこと。
-- コードは戻さなくてよい（新しいコードは Y の新グループが無くても谷川④の腕時計を従来どおり処理する）。
SET XACT_ABORT ON;
BEGIN TRAN;
IF EXISTS (SELECT 1 FROM mst.presets_price_ranges WHERE vendor_code = 'Y' AND category_group IN (N'デジカメ',N'ビデオカメラ',N'レンズ',N'ペン',N'製図用品')) THROW 50001, N'先に yahoo_tanikawa2_enable_20261116_rollback.sql を流す', 1;
DELETE FROM mst.brand_category_group_vendors WHERE vendor_name = N'ＰａｙＰａｙフリマ' AND category_group IN (N'デジカメ',N'ビデオカメラ',N'レンズ',N'ペン',N'製図用品');
DELETE FROM mst.brand_category_group_vendors WHERE note = N'Yahoo!フリマ 谷川② 2026-11-16: Y を追加しないため所属を今の仕入先に限る';
DELETE FROM mst.vendor_brand_map WHERE vendor_name = N'ＰａｙＰａｙフリマ' AND note = N'Yahoo!フリマ 谷川② 2026-11-16';
DELETE FROM mst.category_group_categories WHERE vendor_group = N'ＰａｙＰａｙフリマ' AND category_id IN (47734,19801,47735,49429,49419,2465,47299,47312,47288,47539,47521,47522,47526,47523,47520,47528,47529,47530,47531,47532,47533,47303,47315);
DELETE FROM mst.presets_categories WHERE vendor_group = N'ＰａｙＰａｙフリマ' AND category_id IN (19801,2465,47288,47299,47303,47312,47315,47520,47521,47522,47523,47526,47528,47529,47530,47531,47532,47533,47539,47734,47735,49419,49429);
COMMIT;
GO

-- 関数を修正前の定義に戻す
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
        INNER JOIN mst.presets_categories pc
            ON pc.category_id = pl.category_id
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
