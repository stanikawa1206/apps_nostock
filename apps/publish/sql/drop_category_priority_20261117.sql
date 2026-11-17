-- ============================================================================
-- mst.presets_price_ranges.category_priority の削除（2026-11-17）
--
-- 出品候補の優先度を「仕入先 × カテゴリー」(publish_priority * 1000 + category_priority) から
-- 仕入先の mst.vendors.publish_priority（R=1 → Y=2 → MS=3 → M=4）だけにする。同じ仕入先の中は
-- カテゴリーを区別せず、出品処理（publish_ebay.TAKE_ONE_SQL）の「仕入先での出品日時が新しい順」のまま。
-- 値が 1 以外だったのは 谷川② のデジカメ・ビデオカメラ・レンズ（=2、12行）だけで、ペン系の候補が残る間は
-- カメラ系が選ばれなかった（10/8 以降 M のカメラ系の出品 0件、11/17 は Y のカメラ系 0件）。
--
-- 参照していたもの（すべてこのファイルと同じ変更で解消）:
--   dbo.fn_take_one_candidates_base        … 列 r.category_priority を返していた → 返さない
--   dbo.fn_take_one_candidates             … 絞り込みに使っていた → publish_priority だけ
--   dbo.fn_take_one_candidates_for_account … 同上
--   制約 DF_mst_presets_price_ranges_category_priority（DEFAULT）… 削除。列の自動統計は列と一緒に消える
--   apps/publish/publish_ebay.py TAKE_ONE_SQL … 先に反映すること（下の「順番」）
--   Access ebay在庫管理.accdb のリンクテーブル mst_presets_price_ranges … 削除後にリンクの更新が必要
--   （クエリ Q_PresetGroup はこの列を使っていない。フォーム・レポート・VBA・マクロは無い）
--
-- 順番（publish_ebay が動いていない時間に行う）:
--   1. publish_ebay.py（TAKE_ONE_SQL が category_priority を読まない版）を全台に反映
--      （新しいコードは変更前・変更後のどちらの関数でも動く。古いコードは変更後の関数では動かない）
--   2. このファイルを実行（関数3つ → 制約 → 列。1つのトランザクション）
--   3. Access のリンクテーブル mst_presets_price_ranges を更新
-- 戻すとき: drop_category_priority_20261117_rollback.sql
-- ============================================================================
SET XACT_ABORT ON;
BEGIN TRANSACTION;
GO
IF COL_LENGTH('mst.presets_price_ranges', 'category_priority') IS NULL
    THROW 50000, N'mst.presets_price_ranges.category_priority は既にありません（適用済み）', 1;
GO

-- 1) 出品候補の base 関数: category_priority を返さない
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
        -- 2026-11-16 presets_categories は (category_id, brand_id, vendor_group) ごとの行で、ペンのボールペン・万年筆は
        -- ブランド別に複数行ある。category_id だけで結合すると候補がその行数だけ重複していた（メルカリのペン72 preset で
        -- 1,260行）。使うのは is_collectibles だけ（同じ category_id・vendor_group の行で値は同じ）なので、
        -- 仕入先の vendor_group の (category_id) ごとに1行にしてから結合する（Yahoo!フリマの追加でも重複しない）。
        INNER JOIN (
            SELECT category_id, vendor_group, CAST(MAX(CAST(is_collectibles AS int)) AS bit) AS is_collectibles
            FROM mst.presets_categories
            GROUP BY category_id, vendor_group
        ) pc
            ON pc.category_id = pl.category_id
            AND pc.vendor_group = vd.vendor_group
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

    -- ★ 本番fn_take_one_candidatesと異なり、ここでは最終的なpriority絞り込み（publish_priority）を行わない。
    --   全vendor・全categoryの出品可能候補をそのまま返す（集計・分析用途向け）。
    SELECT * FROM Candidate
);
GO

-- 2) 本番TVF（Access・旧コード向け）: publish_priority だけで絞り込む
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
GO

-- 3) アカウント別TVF: 同上
-- ----------------------------------------------------------------------------
-- 本番TVF dbo.fn_take_one_candidates と同じ絞り込み（仕入先の publish_priority が全体最小）を
-- アカウントごとに行う。新規出品しない仕入先は mst.presets_price_ranges の delete_flag=1（または行なし）で
-- base 関数が除くので、ここでは除外しない（2026-11-14 アカウント別の除外仕入先テーブルを廃止）。
-- @account は呼び出し元との互換のため残す。2026-11-17 category_priority を削除。
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
    WHERE a.publish_priority =
    (
        SELECT MIN(publish_priority)
        FROM Allowed
    )
);
GO

-- 4) 制約と列
ALTER TABLE mst.presets_price_ranges DROP CONSTRAINT DF_mst_presets_price_ranges_category_priority;
ALTER TABLE mst.presets_price_ranges DROP COLUMN category_priority;
GO

COMMIT TRANSACTION;
GO
