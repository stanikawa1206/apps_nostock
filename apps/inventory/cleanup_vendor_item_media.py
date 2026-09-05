# apps/inventory/cleanup_vendor_item_media.py
# -*- coding: utf-8 -*-
"""
trx.vendor_item の不要な description / description_en / image_url2〜20 を
日次でNULL化するクリーンアップ処理。

在庫管理(daily_check.py) / fetch_sold_ebay.py / check_remaining_ebay.py 等の
既存処理には一切組み込まず、独立した日次バッチとしてWindowsタスクスケジューラ
「VendorItemMediaCleanup」（毎日04:00）から起動する。

対象:
    status IN ('売り切れ', '削除', 'オークション')
    AND (description, description_en, image_url2〜20 のいずれかが非NULL)

NULL化する列:
    description, description_en, image_url2 〜 image_url20

NULL化しない列:
    image_url1（message_viewer.py の過去メッセージのサムネイル表示で使用）
    上記以外の全列

レコード自体はDELETEしない。

小バッチ(既定2000件)ごとにUPDATEしてcommitし、バッチ間に短いsleepを挟むことで、
scrape_worker等の常時稼働している他処理への影響を抑える。
WHERE句が「まだNULL化されていない行」を絞り込む形になっているため、
初回・2回目以降とも同一ロジックのまま繰り返し実行できる。

事前のSELECT COUNT(*)による対象件数取得は行わない（日次実行時の余計な負荷を避けるため）。
UPDATE TOP(2000)を繰り返し、各バッチのcursor.rowcountを累計して実際の更新件数を記録し、
rowcount=0になった時点で終了する。
"""

import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyodbc

sys.path.append(str(Path(__file__).resolve().parents[2]))

from apps.common.utils import get_sql_server_connection

# =========================
# 設定
# =========================
BATCH_SIZE = 2000
SLEEP_BETWEEN_BATCHES_SEC = 0.3

# デッドロック(SQL Serverエラー1205)発生時のみリトライする。
# scrape_worker.py の upsert_vendor_items() と同じ方針。
MAX_DEADLOCK_RETRY = 3
DEADLOCK_RETRY_SLEEP_BASE_SEC = 0.5

JST = timezone(timedelta(hours=9))


def now_jst() -> datetime:
    return datetime.now(JST).replace(tzinfo=None)


def log(msg: str) -> None:
    ts = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =========================
# SQL
# =========================
_IMAGE_URL_NULL_COLS = [f"image_url{i}" for i in range(2, 21)]

_TARGET_WHERE = """
    status IN (N'売り切れ', N'削除', N'オークション')
    AND (
        description IS NOT NULL
        OR description_en IS NOT NULL
        OR image_url2 IS NOT NULL OR image_url3 IS NOT NULL OR image_url4 IS NOT NULL
        OR image_url5 IS NOT NULL OR image_url6 IS NOT NULL OR image_url7 IS NOT NULL
        OR image_url8 IS NOT NULL OR image_url9 IS NOT NULL OR image_url10 IS NOT NULL
        OR image_url11 IS NOT NULL OR image_url12 IS NOT NULL OR image_url13 IS NOT NULL
        OR image_url14 IS NOT NULL OR image_url15 IS NOT NULL OR image_url16 IS NOT NULL
        OR image_url17 IS NOT NULL OR image_url18 IS NOT NULL OR image_url19 IS NOT NULL
        OR image_url20 IS NOT NULL
    )
"""

_SET_CLAUSE = ",\n        ".join(
    ["description = NULL", "description_en = NULL"]
    + [f"{c} = NULL" for c in _IMAGE_URL_NULL_COLS]
)

UPDATE_SQL = f"""
    UPDATE TOP ({BATCH_SIZE}) trx.vendor_item WITH (ROWLOCK, READPAST)
    SET
        {_SET_CLAUSE}
    WHERE {_TARGET_WHERE}
"""


# =========================
# デッドロックリトライ共通処理
# =========================
def _with_deadlock_retry(conn, work_fn):
    """
    work_fn(cur) を実行してcommitする。work_fn内でカーソルからの読み取り
    （fetchone/rowcount等）まで完結させること（commit前に読み取りを終える必要があるため）。

    SQL Serverエラー1205（デッドロックの犠牲者になった）の場合のみ、
    rollback→少し待機→最大MAX_DEADLOCK_RETRY回までリトライする。
    それ以外のエラーはリトライせず即座に送出する（無条件の握り潰しはしない）。

    戻り値: (work_fnの戻り値, deadlock_retry_count)
    """
    deadlock_retries = 0

    for attempt in range(1, MAX_DEADLOCK_RETRY + 1):
        cur = conn.cursor()
        try:
            result = work_fn(cur)
            conn.commit()
            return result, deadlock_retries

        except pyodbc.Error as e:
            if "1205" not in str(e):
                conn.rollback()
                raise

            deadlock_retries += 1
            conn.rollback()

            if attempt >= MAX_DEADLOCK_RETRY:
                raise RuntimeError(
                    f"デッドロックにより{MAX_DEADLOCK_RETRY}回リトライしても失敗しました: {e}"
                ) from e

            log(f"[DEADLOCK RETRY] attempt={attempt}/{MAX_DEADLOCK_RETRY}: {e}")
            time.sleep(DEADLOCK_RETRY_SLEEP_BASE_SEC * attempt)

        finally:
            try:
                cur.close()
            except Exception:
                pass

    # ここには到達しない想定（ループ内でreturnまたはraiseするため）
    raise RuntimeError("_with_deadlock_retry: unexpected fallthrough")


# =========================
# 本体処理
# =========================
def run_cleanup(conn) -> tuple[int, int, int]:
    """
    バッチ単位でUPDATEを繰り返す。
    戻り値: (NULL化した合計件数, バッチ数, デッドロックリトライ合計回数)
    """
    total_updated = 0
    batch_no = 0
    total_deadlock_retries = 0

    def work(cur):
        cur.execute(UPDATE_SQL)
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    while True:
        batch_start = time.time()
        affected, deadlock_retries = _with_deadlock_retry(conn, work)
        batch_elapsed = time.time() - batch_start
        total_deadlock_retries += deadlock_retries

        if affected <= 0:
            log(f"[BATCH DONE] これ以上対象なし (最終確認バッチ elapsed={batch_elapsed:.2f}s)")
            break

        batch_no += 1
        total_updated += affected
        retry_note = f" deadlock_retries={deadlock_retries}" if deadlock_retries else ""
        log(
            f"[BATCH {batch_no}] updated={affected:,} "
            f"elapsed={batch_elapsed:.2f}s total_so_far={total_updated:,}{retry_note}"
        )

        time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

    return total_updated, batch_no, total_deadlock_retries


def main() -> None:
    overall_start = time.time()
    log("=== cleanup_vendor_item_media START ===")
    log(f"BATCH_SIZE={BATCH_SIZE} SLEEP_BETWEEN_BATCHES_SEC={SLEEP_BETWEEN_BATCHES_SEC}")

    conn = get_sql_server_connection()
    conn.autocommit = False

    try:
        total_updated, batch_count, total_deadlock_retries = run_cleanup(conn)

        overall_elapsed = time.time() - overall_start
        log(f"NULL化した件数合計: {total_updated:,}")
        log(f"実行バッチ数: {batch_count}")
        log(f"デッドロックリトライ合計回数: {total_deadlock_retries}")
        log(f"全体所要時間: {overall_elapsed:.2f}秒 ({overall_elapsed / 60:.1f}分)")
        log("=== cleanup_vendor_item_media END (SUCCESS) ===")

    except Exception:
        import traceback

        log("=== cleanup_vendor_item_media END (ERROR) ===")
        traceback.print_exc()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
