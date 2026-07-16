-- is_closed_today 廃止に伴うカラム削除
-- 状態管理は mst.ebay_accounts.close_reason (NULL/DONE/EMPTY/LIMIT) のみで行う。
-- 事前にコード側(publish_ebay.py / publish_manager.py / get_active_listings.py)の
-- close_reason ベースへの置き換えが反映済みであることを確認してから実行すること。

ALTER TABLE mst.ebay_accounts
DROP COLUMN is_closed_today;
