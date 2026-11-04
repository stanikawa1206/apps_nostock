-- 2026-11-04 危険素材・規制素材判定（apps/publish/material_check.py）
-- 1) 判定結果の保存先。規制素材の語がある商品だけ保存する（語が無い商品は保存しない）。
--    result: OK / NG / HOLD、stage: 1=キーワード 2=AI 9=手動
--    手動で確認して出品可にする時は result='OK', stage=9 にし、trx.vendor_item の 出品状況 を NULL に戻す。
IF OBJECT_ID('trx.material_check', 'U') IS NULL
CREATE TABLE trx.material_check (
    vendor_name       NVARCHAR(100)  NOT NULL,
    vendor_item_id    NVARCHAR(200)  NOT NULL,
    text_hash         CHAR(64)       NOT NULL,
    result            NVARCHAR(10)   NOT NULL,
    stage             TINYINT        NOT NULL,
    materials         NVARCHAR(200)  NULL,
    reason            NVARCHAR(1000) NULL,
    hits              NVARCHAR(4000) NULL,
    ai_model          NVARCHAR(50)   NULL,
    ai_result         NVARCHAR(MAX)  NULL,
    ai_input_tokens   INT            NULL,
    ai_output_tokens  INT            NULL,
    ai_sec            DECIMAL(6, 1)  NULL,
    checked_at        DATETIME2      NOT NULL CONSTRAINT DF_material_check_checked_at DEFAULT SYSDATETIME(),
    CONSTRAINT PK_material_check PRIMARY KEY (vendor_name, vendor_item_id)
);
GO

-- 2) 候補抽出 ⑥ に「保留(素材確認)」を追加（AIでも判断できない・AIエラー・記載の矛盾の商品を出品しない）。
--    fn_take_one_candidates_base.sql を更新して CREATE OR ALTER で適用する（このファイルは 1) のみ）。
