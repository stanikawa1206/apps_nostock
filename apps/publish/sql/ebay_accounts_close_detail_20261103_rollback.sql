-- ebay_accounts_close_detail_20261103.sql の取り消し。
-- 先にコード（git revert）を戻してから実行すること（新しいコードは close_detail を参照する）。
IF COL_LENGTH('mst.ebay_accounts', 'close_detail') IS NOT NULL
    ALTER TABLE mst.ebay_accounts DROP COLUMN close_detail;
