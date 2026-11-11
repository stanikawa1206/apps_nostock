-- 戻し用（yahoo_fleamarket_watch_master_20261111.sql の登録を取り消す）
-- 先に出品中の Yahoo!フリマ商品が無いことを確認する（あれば trx.listings に残り、在庫確認は続く）
-- mst.vendors.is_scrape_stopped 列（2026-11-11 追加・既定0）は残す
SET XACT_ABORT ON;
BEGIN TRAN;
DELETE FROM mst.ebay_account_excluded_vendors WHERE vendor_code='Y';
DELETE FROM mst.brand_category_group_vendors WHERE vendor_name=N'ＰａｙＰａｙフリマ' AND category_group=N'腕時計';
DELETE FROM mst.vendor_brand_map WHERE vendor_name=N'ＰａｙＰａｙフリマ';
DELETE FROM mst.category_group_categories WHERE vendor_group=N'ＰａｙＰａｙフリマ';
DELETE FROM mst.presets_categories WHERE vendor_group=N'ＰａｙＰａｙフリマ';
DELETE FROM mst.presets_lookup WHERE vendor_name=N'ＰａｙＰａｙフリマ';
DELETE FROM mst.vendors WHERE vendor_name=N'ＰａｙＰａｙフリマ';
-- COMMIT;
