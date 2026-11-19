-- ============================================================================
-- 在庫管理の実績集計・異常明細の表（2026-11-19）
--
-- trx.inventory_ops_stats … 10分ごと × 種類 × 仕入先 × 台 × アカウント × 結果 × 原因 の件数（全商品の明細は持たない）
--   kind: 在庫チェック / 価格更新 / 出品終了 / 検索ジョブ
--   items    … 処理した商品（ジョブ）の数。1回の処理で1件（同じ商品を同じ10分で2回処理すれば2）
--   attempts … 仕入先・eBay への試行回数（retry を含む）
--   retried  … retry が1回以上あった商品の数（結果が成功なら「retryで回復」）
--   bucket_at は DB サーバーの時刻（SYSDATETIME）を10分単位に切り捨てたもの（台の時計に依存しない）
--   書き込みは apps/inventory/ops_stats.py がプロセスごとに数分おきにまとめて INSERT する
--   （同じキーの行が複数あってよい。読む側で SUM する）。
-- trx.inventory_ops_issue … 異常の明細（1商品1行。未解決の間は同じ行を更新）
--   issue_kind: 出品終了失敗 / 仕入不可出品中 / 判定不可
--
-- 保存期間: どちらも60日（apps/inventory/db_archive_daily.py が削除。issue は解決済みの行だけ）。
-- 戻し: inventory_ops_tables_20261119_rollback.sql
-- ============================================================================

IF OBJECT_ID('trx.inventory_ops_stats') IS NOT NULL
    THROW 50000, N'trx.inventory_ops_stats は既にあります（適用済み）', 1;

CREATE TABLE trx.inventory_ops_stats (
    stat_id     bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_inventory_ops_stats PRIMARY KEY,
    bucket_at   datetime2(0)  NOT NULL,
    host        nvarchar(50)  NOT NULL,
    kind        nvarchar(20)  NOT NULL,
    vendor_name nvarchar(50)  NULL,
    account     nvarchar(50)  NULL,
    result      nvarchar(20)  NOT NULL,
    cause       nvarchar(100) NULL,
    items       int NOT NULL,
    attempts    int NOT NULL,
    retried     int NOT NULL,
    created_at  datetime2(0)  NOT NULL CONSTRAINT DF_inventory_ops_stats_created_at DEFAULT (SYSDATETIME())
);
CREATE INDEX IX_inventory_ops_stats_bucket ON trx.inventory_ops_stats (bucket_at, kind) INCLUDE (vendor_name, account, result, cause, items, attempts, retried, host);

CREATE TABLE trx.inventory_ops_issue (
    issue_id       bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_inventory_ops_issue PRIMARY KEY,
    issue_kind     nvarchar(20)  NOT NULL,
    vendor_name    nvarchar(50)  NOT NULL,
    vendor_item_id nvarchar(64)  NOT NULL,
    account        nvarchar(50)  NULL,
    listing_id     nvarchar(30)  NULL,
    cause          nvarchar(400) NULL,
    first_at       datetime2(0)  NOT NULL,
    last_at        datetime2(0)  NOT NULL,
    occurrences    int NOT NULL,
    notified_at    datetime2(0)  NULL,
    resolved_at    datetime2(0)  NULL
);
-- 未解決の行は 種類 × 商品 で1行だけ
CREATE UNIQUE INDEX UX_inventory_ops_issue_open ON trx.inventory_ops_issue (issue_kind, vendor_name, vendor_item_id) WHERE resolved_at IS NULL;
CREATE INDEX IX_inventory_ops_issue_last ON trx.inventory_ops_issue (resolved_at, last_at);
