"""
日次の DB 整理（タスク nostock_db_archive_daily、毎日 23:00 開始）。

  1) trx.scrape_job : 作成（created_at）から3日より古い done / error のジョブを削除する（アーカイブしない）。
                       pending・pending2・retry_wait・running は日付に関係なく残す。
  2) trx.vendor_item: 古い商品を別DB nostock_archive の trx.vendor_item に移す。
                       1バッチ（既定1,000件）ごとに「アーカイブへ保存 → 本番から削除」を同じトランザクションで行い、
                       件数が合わなければそのバッチを取り消す。

  2:30（--deadline）を過ぎたら次のバッチに入らずに終わり、残りは翌日の実行で続ける。
  失敗したバッチはロールバックするだけなので、そのまま再実行してよい。
  二重起動は sp_getapplock で防ぐ（後から起動した方はすぐ終わる）。

vendor_item のアーカイブ条件（2026-11-16 調査の表B）:
  - COALESCE(vendor_created_at, created_at) が90日以上前（DATEDIFF(DAY, …, 今) >= 90）
  - last_checked_at が30日以上前（NULL は対象外）
  - eBay に出品中でない（trx.listings に is_deleted=0 の行が無い）
  - eBay の出品終了（COALESCE(deleted_at, error_at, start_time)）から90日以上たっている
  - 出品の確保中・在庫確認のロック中でない（processing_at・remaining_check_lock が NULL）
  - trx.ebay_orders に注文が無い
  - 出品不可flg=0 で、永久NG（GA補色・危険素材・素材確認保留・ポリシーNG・ジャンク疑い・バリエーション）でない
  アーカイブに同じキーがあれば、全列と archived_at を上書きする。fetch_active はアーカイブを見ずに本番へ登録する。

使い方:
  python -m apps.inventory.db_archive_daily                    # 日次（scrape_job → vendor_item）
  python -m apps.inventory.db_archive_daily --max-rows 10000 --skip-scrape-job   # 件数を絞って試す
  python -m apps.inventory.db_archive_daily --setup            # アーカイブDBとテーブルを作る（初回だけ）
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

LOG_DIR = PROJECT_ROOT / "logs" / "db_archive"
APP_NAME = "nostock_db_archive"
APPLOCK = "nostock_db_archive_daily"

VENDOR_ITEM_BATCH = 1000
SCRAPE_JOB_BATCH = 2000
SCRAPE_JOB_KEEP_DAYS = 3
LOCK_TIMEOUT_MS = 10000
MAX_CONSECUTIVE_ERRORS = 5
PAUSE_SEC = 0.2

PERMANENT_NG = ("NG(GA補色)", "NG(危険素材)", "保留(素材確認)", "ポリシーNG", "NG(ジャンク疑い)", "NG(バリエーション商品)")

# 本番に残す商品の条件（候補の抽出と、ロックした後の確認の両方で使う）
ARCHIVE_WHERE = """
    DATEDIFF(DAY, COALESCE(v.vendor_created_at, v.created_at), SYSDATETIME()) >= 90
    AND DATEDIFF(DAY, v.last_checked_at, SYSDATETIME()) >= 30
    AND v.processing_at IS NULL
    AND v.remaining_check_lock IS NULL
    AND ISNULL(v.出品不可flg, 0) = 0
    AND ISNULL(v.出品状況, N'') NOT IN ({ng})
    AND NOT EXISTS (
        SELECT 1 FROM trx.listings l
         WHERE l.vendor_name = v.vendor_name AND l.vendor_item_id = v.vendor_item_id
           AND (ISNULL(l.is_deleted, 0) = 0
                OR COALESCE(l.deleted_at, l.error_at, l.start_time) IS NULL
                OR DATEDIFF(DAY, COALESCE(l.deleted_at, l.error_at, l.start_time), SYSDATETIME()) < 90)
    )
    AND NOT EXISTS (SELECT 1 FROM trx.ebay_orders o WHERE o.vendor_item_id = v.vendor_item_id)
""".format(ng=", ".join(f"N'{s}'" for s in PERMANENT_NG))


_log_file = None


def log(msg: str) -> None:
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    if _log_file:
        with open(_log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def connect() -> pyodbc.Connection:
    conn = pyodbc.connect(
        f"DRIVER={os.getenv('DB_DRIVER')};SERVER={os.getenv('DB_SERVER')};DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};PWD={os.getenv('DB_PASS')};Encrypt=no;TrustServerCertificate=yes;APP={APP_NAME};",
        autocommit=True,
    )
    cur = conn.cursor()
    cur.execute(f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS}; SET DEADLOCK_PRIORITY LOW;")
    return conn


def space_mb(cur, db: str) -> tuple[int, int]:
    """データファイルの (大きさ, 使用中) MB。"""
    # FILEPROPERTY は今のDBのファイルしか見ないので、そのDBに切り替えて測って戻す
    cur.execute(f"USE [{db}]")
    try:
        cur.execute("SELECT SUM(CAST(size AS BIGINT)) * 8 / 1024, SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS BIGINT)) * 8 / 1024 "
                    "FROM sys.database_files WHERE type = 0")
        row = cur.fetchone()
    finally:
        cur.execute(f"USE [{os.getenv('DB_NAME')}]")
    return int(row[0] or 0), int(row[1] or 0)


def column_list(cur) -> list[str]:
    cur.execute("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('trx.vendor_item') ORDER BY column_id")
    return [f"[{r[0]}]" for r in cur.fetchall()]


def setup(conn, archive_db: str) -> None:
    cur = conn.cursor()
    cur.execute("SELECT DB_ID(?)", archive_db)
    if cur.fetchone()[0] is None:
        cur.execute(f"CREATE DATABASE [{archive_db}]")
        cur.execute(f"ALTER DATABASE [{archive_db}] SET RECOVERY SIMPLE")
        log(f"[SETUP] CREATE DATABASE {archive_db}")
    cur.execute(f"SELECT name FROM [{archive_db}].sys.database_files")
    for (fname,) in cur.fetchall():
        cur.execute(f"ALTER DATABASE [{archive_db}] MODIFY FILE (NAME = N'{fname}', FILEGROWTH = 256MB)")
    cur.execute(f"SELECT OBJECT_ID(N'[{archive_db}].trx.vendor_item')")
    if cur.fetchone()[0] is None:
        cur.execute(f"EXEC [{archive_db}].sys.sp_executesql N'IF SCHEMA_ID(''trx'') IS NULL EXEC(''CREATE SCHEMA trx'')'")
        cur.execute(f"SELECT TOP (0) * INTO [{archive_db}].trx.vendor_item FROM trx.vendor_item")
        cur.execute(f"ALTER TABLE [{archive_db}].trx.vendor_item ADD archived_at DATETIME2(3) NOT NULL")
        cur.execute(f"EXEC [{archive_db}].sys.sp_executesql N'ALTER TABLE trx.vendor_item ADD CONSTRAINT PK_vendor_item_archive "
                    f"PRIMARY KEY CLUSTERED (vendor_name, vendor_item_id)'")
        log(f"[SETUP] CREATE TABLE {archive_db}.trx.vendor_item（本番と同じ列 + archived_at、主キー vendor_name, vendor_item_id）")
    log("[SETUP] 完了")


def purge_scrape_job(conn, deadline: datetime) -> dict:
    cur = conn.cursor()
    t0 = time.time()
    cur.execute(f"SELECT DATEADD(DAY, -{SCRAPE_JOB_KEEP_DAYS}, SYSDATETIME())")
    cutoff = cur.fetchone()[0]
    cur.execute("SELECT MIN(job_id), MAX(job_id), COUNT_BIG(*) FROM trx.scrape_job "
                "WHERE created_at < ? AND status IN ('done', 'error')", cutoff)
    lo, hi, target = cur.fetchone()
    log(f"[SCRAPE_JOB] 開始 cutoff={cutoff:%Y-%m-%d %H:%M:%S} 対象={target:,} job_id {lo}〜{hi}")
    deleted = errors = streak = 0
    stopped = ""
    if lo is not None:
        start = lo
        while start <= hi:
            if datetime.now() >= deadline:
                stopped = "期限"
                break
            end = start + SCRAPE_JOB_BATCH - 1
            try:
                cur.execute("""DELETE FROM trx.scrape_job WITH (ROWLOCK)
                                WHERE job_id BETWEEN ? AND ? AND created_at < ? AND status IN ('done', 'error')""",
                            start, end, cutoff)
                deleted += max(cur.rowcount, 0)
                streak = 0
                start = end + 1
            except pyodbc.Error as e:
                errors += 1
                streak += 1
                log(f"[SCRAPE_JOB][ERROR] job_id {start}〜{end}: {e}")
                if streak >= MAX_CONSECUTIVE_ERRORS:
                    stopped = "エラー連続"
                    break
                time.sleep(5)
                continue
            time.sleep(PAUSE_SEC)
    sec = time.time() - t0
    log(f"[SCRAPE_JOB] 終了 削除={deleted:,}/{target:,} エラー={errors} {sec:.0f}秒" + (f" 中断({stopped})" if stopped else ""))
    return {"deleted": deleted, "target": target, "errors": errors, "sec": sec, "stopped": stopped}


def archive_vendor_item(conn, archive_db: str, deadline: datetime, max_rows: int | None, batch: int) -> dict:
    cur = conn.cursor()
    t0 = time.time()
    cols = column_list(cur)
    cur.execute(f"SELECT name FROM [{archive_db}].sys.columns WHERE object_id = OBJECT_ID(N'[{archive_db}].trx.vendor_item')")
    arch_cols = {f"[{r[0]}]" for r in cur.fetchall()}
    missing = [c for c in cols if c not in arch_cols]
    if missing:
        raise RuntimeError(f"アーカイブのテーブルに無い列があります: {missing}（本番の列が増えたらアーカイブにも足すこと）")
    col_sql = ", ".join(cols)
    del_cols = ", ".join(f"deleted.{c}" for c in cols)

    cur.execute("IF OBJECT_ID('tempdb..#cand') IS NOT NULL DROP TABLE #cand")
    cur.execute("CREATE TABLE #cand (seq INT IDENTITY(1,1) PRIMARY KEY, vendor_name NVARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL, "
                "vendor_item_id NVARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL)")
    top = f"TOP ({int(max_rows)})" if max_rows else ""
    cur.execute(f"INSERT INTO #cand (vendor_name, vendor_item_id) SELECT {top} v.vendor_name, v.vendor_item_id "
                f"FROM trx.vendor_item v WHERE {ARCHIVE_WHERE} ORDER BY v.vendor_name, v.vendor_item_id")
    cur.execute("SELECT COUNT(*) FROM #cand")
    target = cur.fetchone()[0]
    log(f"[VENDOR_ITEM] 開始 対象={target:,}（抽出 {time.time() - t0:.0f}秒）batch={batch}" + (f" 上限={max_rows:,}" if max_rows else ""))

    cur.execute("IF OBJECT_ID('tempdb..#k') IS NOT NULL DROP TABLE #k")
    cur.execute("CREATE TABLE #k (vendor_name NVARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL, "
                "vendor_item_id NVARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL, PRIMARY KEY (vendor_name, vendor_item_id))")

    archived = skipped = errors = streak = batches = 0
    stopped = ""
    last = 0
    while last < target:
        if datetime.now() >= deadline:
            stopped = "期限"
            break
        lo, hi = last + 1, last + batch
        try:
            cur.execute("TRUNCATE TABLE #k")
            cur.execute("BEGIN TRANSACTION")
            # 対象の行をロックしてから条件を確かめ直す（他の処理が使っている行は READPAST で飛ばす）
            # FORCESEEK: 主キーで1行ずつ引く（表を走査すると、他の処理がロック中の行で待ってしまう）
            # （パラメーター付きの SELECT INTO で作る一時テーブルはその文の中だけで消えるので、#k は先に作っておく）
            cur.execute(f"""
                INSERT INTO #k (vendor_name, vendor_item_id)
                SELECT v.vendor_name, v.vendor_item_id
                  FROM #cand c
                  JOIN trx.vendor_item v WITH (UPDLOCK, ROWLOCK, READPAST, FORCESEEK)
                    ON v.vendor_name = c.vendor_name AND v.vendor_item_id = c.vendor_item_id
                 WHERE c.seq BETWEEN ? AND ? AND {ARCHIVE_WHERE}""", lo, hi)
            cur.execute("SELECT COUNT(*) FROM #k")
            n = cur.fetchone()[0]
            if n:
                cur.execute(f"""DELETE a FROM [{archive_db}].trx.vendor_item a
                                  JOIN #k k ON k.vendor_name = a.vendor_name AND k.vendor_item_id = a.vendor_item_id""")
                cur.execute(f"""
                    DELETE v
                    OUTPUT {del_cols}, SYSDATETIME() INTO [{archive_db}].trx.vendor_item ({col_sql}, archived_at)
                      FROM #k k
                      JOIN trx.vendor_item v WITH (ROWLOCK, FORCESEEK)
                        ON v.vendor_name = k.vendor_name AND v.vendor_item_id = k.vendor_item_id""")
                deleted = cur.rowcount
                cur.execute(f"""SELECT COUNT(*) FROM [{archive_db}].trx.vendor_item a
                                  JOIN #k k ON k.vendor_name = a.vendor_name AND k.vendor_item_id = a.vendor_item_id""")
                saved = cur.fetchone()[0]
                if deleted != n or saved != n:
                    raise RuntimeError(f"件数不一致 対象={n} 本番削除={deleted} アーカイブ={saved}")
            cur.execute("COMMIT TRANSACTION")
            archived += n
            skipped += (min(hi, target) - lo + 1) - n
            batches += 1
            streak = 0
            last = hi
        except Exception as e:
            try:
                cur.execute("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
            except pyodbc.Error:
                pass
            errors += 1
            streak += 1
            log(f"[VENDOR_ITEM][ERROR] seq {lo}〜{hi} を取り消し: {e}")
            if isinstance(e, RuntimeError) or streak >= MAX_CONSECUTIVE_ERRORS:
                stopped = "件数不一致" if isinstance(e, RuntimeError) else "エラー連続"
                break
            last = hi  # ロック待ちなどで失敗した範囲は今日は飛ばす（翌日の抽出で拾い直す）
            time.sleep(5)
            continue
        if batches % 10 == 0:
            log(f"[VENDOR_ITEM] 進捗 {archived:,}/{target:,} 件 {time.time() - t0:.0f}秒")
        time.sleep(PAUSE_SEC)
    cur.execute("DROP TABLE #cand; DROP TABLE #k")
    sec = time.time() - t0
    log(f"[VENDOR_ITEM] 終了 アーカイブ・削除={archived:,}/{target:,} 飛ばした={skipped:,} エラー={errors} "
        f"残り={max(target - last, 0):,} {sec:.0f}秒" + (f" 中断({stopped})" if stopped else ""))
    return {"archived": archived, "target": target, "skipped": skipped, "errors": errors, "sec": sec,
            "stopped": stopped, "remaining": max(target - last, 0)}


def next_deadline(hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    now = datetime.now()
    d = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return d if d > now else d + timedelta(days=1)


OPS_KEEP_DAYS = 60


def purge_ops_tables(conn) -> dict:
    """在庫管理の実績集計（trx.inventory_ops_stats）と解決済みの異常明細（trx.inventory_ops_issue）の60日より古い行を消す。"""
    cur = conn.cursor()
    cur.execute(f"DELETE FROM trx.inventory_ops_stats WHERE bucket_at < DATEADD(DAY, -{OPS_KEEP_DAYS}, SYSDATETIME())")
    stats = max(cur.rowcount, 0)
    cur.execute(f"DELETE FROM trx.inventory_ops_issue WHERE resolved_at < DATEADD(DAY, -{OPS_KEEP_DAYS}, SYSDATETIME())")
    issues = max(cur.rowcount, 0)
    log(f"[OPS] 実績集計 {stats:,} 行・解決済みの異常明細 {issues:,} 行を削除（{OPS_KEEP_DAYS}日より前）")
    return {"stats": stats, "issues": issues}


def main() -> int:
    global _log_file
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive-db", default="nostock_archive")
    ap.add_argument("--deadline", default="02:30", help="この時刻を過ぎたら次のバッチに入らない（HH:MM）")
    ap.add_argument("--max-rows", type=int, default=None, help="vendor_item の対象件数の上限（試験用）")
    ap.add_argument("--batch", type=int, default=VENDOR_ITEM_BATCH)
    ap.add_argument("--skip-scrape-job", action="store_true")
    ap.add_argument("--skip-vendor-item", action="store_true")
    ap.add_argument("--setup", action="store_true")
    a = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _log_file = LOG_DIR / f"{datetime.now():%Y%m%d}.log"
    conn = connect()
    cur = conn.cursor()
    if a.setup:
        setup(conn, a.archive_db)
        return 0

    cur.execute("DECLARE @r INT; EXEC @r = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', "
                "@LockOwner = 'Session', @LockTimeout = 0; SELECT @r", APPLOCK)
    if cur.fetchone()[0] < 0:
        log("[START] 別の実行が動いているので終わります")
        return 2

    deadline = next_deadline(a.deadline)
    db = os.getenv("DB_NAME")
    before = space_mb(cur, db)
    arch_before = space_mb(cur, a.archive_db)
    log(f"[START] pid={os.getpid()} 期限={deadline:%m/%d %H:%M} {db} ファイル{before[0]:,}MB 使用{before[1]:,}MB "
        f"空き{before[0] - before[1]:,}MB / {a.archive_db} 使用{arch_before[1]:,}MB")
    t0 = time.time()
    rc = 0
    sj = vi = None
    try:
        if not a.skip_scrape_job:
            sj = purge_scrape_job(conn, deadline)
        purge_ops_tables(conn)
        if not a.skip_vendor_item:
            vi = archive_vendor_item(conn, a.archive_db, deadline, a.max_rows, a.batch)
            if vi["errors"]:
                rc = 1
        if sj and sj["errors"]:
            rc = 1
    except Exception as e:
        log(f"[FATAL] {type(e).__name__}: {e}")
        rc = 1
    after = space_mb(cur, db)
    arch_after = space_mb(cur, a.archive_db)
    log(f"[END] rc={rc} {time.time() - t0:.0f}秒 {db} 使用{before[1]:,}→{after[1]:,}MB 空き{before[0] - before[1]:,}→"
        f"{after[0] - after[1]:,}MB / {a.archive_db} 使用{arch_before[1]:,}→{arch_after[1]:,}MB"
        + (f" / scrape_job 削除{sj['deleted']:,}" if sj else "")
        + (f" / vendor_item アーカイブ{vi['archived']:,} 残り{vi['remaining']:,}" if vi else ""))
    return rc


if __name__ == "__main__":
    sys.exit(main())
