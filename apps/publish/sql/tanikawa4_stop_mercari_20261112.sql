-- ============================================================================
-- 谷川④: メルカリ(M)・メルカリshops(MS)からの新規出品を止める（2026-11-12）
-- 既存のeBay出品・在庫管理・価格改定はそのまま（presets_lookup は変えない）。
-- 腕時計グループの M/MS の lookup は、既存出品が0件になったものから
-- daily_check.prune_unlisted_lookups がサイクルごとに消す（除外行があることが条件）。
-- 戻し方: tanikawa4_stop_mercari_20261112_rollback.sql
-- ============================================================================
INSERT INTO mst.ebay_account_excluded_vendors (account, vendor_code, note)
SELECT N'谷川④', c.vendor_code, N'谷川④はM・MSの新規出品を停止（既存出品は販売終了まで在庫管理）'
FROM (VALUES ('M'), ('MS')) AS c(vendor_code)
WHERE NOT EXISTS (
    SELECT 1 FROM mst.ebay_account_excluded_vendors x
    WHERE x.account = N'谷川④' AND x.vendor_code = c.vendor_code
);

SELECT account, vendor_code, note, created_at FROM mst.ebay_account_excluded_vendors ORDER BY account, vendor_code;
