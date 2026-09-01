-- ==============================================================================
-- trx.ebay_transactions
--
-- eBay Finances API（/transaction, /payout）から取得した資金移動データを、
-- セラーハブの "Transaction report (CSV)" と同じ「1フラットテーブル」構成で保持する。
-- 親子分割（Payout / Transaction を別テーブルに正規化）は行わない。
--
-- record_type = 'TRANSACTION' : 個々の売上・返金・手数料等（/sell/finances/v1/transaction）
-- record_type = 'PAYOUT'      : Payout自体の行（/sell/finances/v1/payout）
--   → CSVレポートの "Type = Payout" 行に相当。net_amount にはPayout総額が入る。
--
-- UPSERTキー: (account, record_type, record_id)
--   record_id は TRANSACTION なら transactionId、PAYOUT なら payoutId。
--   eBay側のIDは名前空間が別（transactionId と payoutId は衝突しない）だが、
--   念のため record_type も複合キーに含めて明示的に区別する。
-- ==============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'trx')
BEGIN
    EXEC('CREATE SCHEMA trx');
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'trx' AND t.name = 'ebay_transactions'
)
BEGIN
    CREATE TABLE trx.ebay_transactions (
        account              NVARCHAR(50)   NOT NULL,
        record_type          VARCHAR(20)    NOT NULL,   -- 'TRANSACTION' | 'PAYOUT'
        record_id            VARCHAR(50)    NOT NULL,   -- transactionId または payoutId

        transaction_date     DATETIME2(3)   NULL,       -- transactionDate / payoutDate
        transaction_type     VARCHAR(50)    NULL,       -- SALE / REFUND / NON_SALE_CHARGE / CREDIT /
                                                          -- SHIPPING_LABEL / DISPUTE / TRANSFER / PAYOUT ...
        transaction_status   VARCHAR(50)    NULL,       -- transactionStatus / payoutStatus

        order_id             VARCHAR(50)    NULL,       -- orderId（SALE/REFUND等のみ）
        payout_id            VARCHAR(50)    NULL,       -- 紐づくPayout ID（record_type='TRANSACTION'のみ値あり）

        buyer_username        NVARCHAR(100)  NULL,

        ebay_item_id          VARCHAR(50)    NULL,       -- legacyItemId（Fulfillment APIで補完）
        custom_label          NVARCHAR(200)  NULL,       -- SKU（Fulfillment APIで補完）
        item_title            NVARCHAR(500)  NULL,       -- 商品タイトル（Fulfillment APIで補完）
        quantity              INT            NULL,

        gross_amount           DECIMAL(18,2)  NULL,       -- totalFeeBasisAmount.value
        final_value_fee         DECIMAL(18,2)  NULL,       -- FINAL_VALUE_FEE
        fixed_fee                DECIMAL(18,2)  NULL,       -- FINAL_VALUE_FEE_FIXED_PER_ORDER
        international_fee        DECIMAL(18,2)  NULL,       -- INTERNATIONAL_FEE
        ad_fee                   DECIMAL(18,2)  NULL,       -- AD_FEE（Promoted Listings）
        other_fee_amount         DECIMAL(18,2)  NULL,       -- 上記以外のmarketplaceFees合算
        total_fee_amount         DECIMAL(18,2)  NULL,       -- totalFeeAmount.value（APIの合計値をそのまま信頼）

        net_amount               DECIMAL(18,2)  NOT NULL,   -- amount.value（純額 / Payout総額）
        currency                 VARCHAR(10)    NULL,

        payout_status_detail       VARCHAR(50)    NULL,       -- record_type='PAYOUT': payoutStatus
        bank_reference             NVARCHAR(100)  NULL,       -- record_type='PAYOUT': bankReference

        memo                      NVARCHAR(1000) NULL,
        raw_json                  NVARCHAR(MAX)  NULL,       -- API生レスポンス（監査・デバッグ用）

        created_at                 DATETIME2(3)   NOT NULL CONSTRAINT DF_ebay_transactions_created_at DEFAULT SYSUTCDATETIME(),
        updated_at                 DATETIME2(3)   NOT NULL CONSTRAINT DF_ebay_transactions_updated_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_ebay_transactions PRIMARY KEY CLUSTERED (account, record_type, record_id)
    );

    CREATE INDEX IX_ebay_transactions_order_id
        ON trx.ebay_transactions (order_id)
        WHERE order_id IS NOT NULL;

    CREATE INDEX IX_ebay_transactions_payout_id
        ON trx.ebay_transactions (payout_id)
        WHERE payout_id IS NOT NULL;

    CREATE INDEX IX_ebay_transactions_account_date
        ON trx.ebay_transactions (account, transaction_date);
END
GO
