-- 2026-11-09 UNPUBLISHED再公開待ち: 同じ offer で再公開が続けて失敗した時の offer 作り直し用の列
--   offer_payload       : 作り直しに使う offer の内容（getOffer の結果から offerId・status・listing を除いた JSON）。
--                         元の offer を削除する前に保存する。新しい offer を作った後は NULL に戻す。
--   offer_recreated_utc : offer を作り直した時刻(UTC)。NULL なら since_utc から、値があればこの時刻から
--                         ORPHAN_RECREATE_AFTER_DAYS 日 再公開できなければ、もう一度作り直す。
IF COL_LENGTH('trx.unpublished_recovery', 'offer_payload') IS NULL
    ALTER TABLE trx.unpublished_recovery ADD offer_payload NVARCHAR(MAX) NULL;
IF COL_LENGTH('trx.unpublished_recovery', 'offer_recreated_utc') IS NULL
    ALTER TABLE trx.unpublished_recovery ADD offer_recreated_utc DATETIME2 NULL;
