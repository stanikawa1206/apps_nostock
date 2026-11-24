-- 2026-11-24 keep_* 廃止の移行段階2: 既存出品の価格範囲外判定はアカウント自身の low/high_usd_target で行う
-- （apps/inventory/listing_price_range.py・check_remaining_ebay.py）。keep_* はどのコードも参照しないので列を削除する。
-- 前提: 全台の pull と scrape_worker の入れ替え、inventory_loop の再起動（旧 daily_check が keep_* を読み書きしなくなる）が済んでいること。
-- 削除前の値: E:\backup\sqlserver\keep_removal_20261124\presets_price_ranges_before.csv（keep_* の値を含む全行）。
-- Access（ebay在庫管理.accdb）の mst_presets_price_ranges・mst_presets_lookup のリンクを削除後に更新すること。
SET XACT_ABORT ON;
BEGIN TRAN;
ALTER TABLE mst.presets_price_ranges DROP COLUMN keep_low_usd_target, keep_high_usd_target;
ALTER TABLE mst.presets_lookup DROP COLUMN keep_low_usd_target, keep_high_usd_target;
COMMIT TRAN;
