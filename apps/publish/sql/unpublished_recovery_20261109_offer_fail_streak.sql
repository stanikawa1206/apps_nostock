-- 2026-11-09 UNPUBLISHED再公開待ち: 今の offer で再公開(publishOffer)が 25001(http500) で続けて失敗した回数
--   offer_fail_streak : 25001 以外のエラーで 0 に戻し、offer を作り直したら新しい offer で数え直す。
--                       ORPHAN_RECREATE_AFTER_25001 回に達したら、日数を待たずに offer を作り直す。
IF COL_LENGTH('trx.unpublished_recovery', 'offer_fail_streak') IS NULL
    ALTER TABLE trx.unpublished_recovery ADD offer_fail_streak INT NOT NULL
        CONSTRAINT DF_unpublished_recovery_offer_fail_streak DEFAULT 0;
