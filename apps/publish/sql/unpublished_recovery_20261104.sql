-- 2026-11-04 UNPUBLISHED出品の再公開待ち（保留）
-- publishOffer がエラーでも出品されていた（offer が UNPUBLISHED）出品を終了して作り直す時に、
-- 再公開できなかった商品を残し、publish_ebay が後から再公開する（publish_ebay.retry_pending_orphan_republishes）。
--   status: pending（再処理待ち）/ running（処理中）/ done（出品済み）/ gave_up（再試行の上限）/ stopped（要確認）
--   gave_up / stopped を再処理するには status='pending', attempts=0, next_at=SYSDATETIME() にする。
IF OBJECT_ID('trx.unpublished_recovery', 'U') IS NULL
CREATE TABLE trx.unpublished_recovery (
    account            NVARCHAR(50)   NOT NULL,
    vendor_item_id     NVARCHAR(200)  NOT NULL,
    vendor_name        NVARCHAR(100)  NULL,
    offer_id           NVARCHAR(50)   NOT NULL,
    orphan_listing_id  NVARCHAR(50)   NOT NULL,
    since_utc          DATETIME2      NOT NULL,   -- 元の出品を終了した時刻(UTC)。これ以降に始まった同じSKUの出品を二重出品として探す
    is_collectibles    BIT            NULL,
    status             NVARCHAR(20)   NOT NULL,
    attempts           INT            NOT NULL CONSTRAINT DF_unpublished_recovery_attempts DEFAULT 0,
    next_at            DATETIME2      NOT NULL,
    last_error         NVARCHAR(1000) NULL,
    new_listing_id     NVARCHAR(50)   NULL,
    created_at         DATETIME2      NOT NULL CONSTRAINT DF_unpublished_recovery_created_at DEFAULT SYSDATETIME(),
    updated_at         DATETIME2      NOT NULL CONSTRAINT DF_unpublished_recovery_updated_at DEFAULT SYSDATETIME(),
    CONSTRAINT PK_unpublished_recovery PRIMARY KEY (account, vendor_item_id)
);
