# fetch_active_ebay.py
# 役割: fetch_active_ebay の task を trx.scrape_job に積むだけ

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

import json
import time
from datetime import datetime
import pyodbc
from apps.common.utils import get_sql_server_connection
from apps.adapters.mercari_search import fetch_active_presets

# 直前の事前soldジョブ（VPSワーカーの MERGE trx.vendor_item）がまだ running のうちに
# 下記の全件UPDATEが走ると、vendor_item のページロックでデッドロック(1205)の犠牲になり
# fetch_active_ebay.py 全体が異常終了していた（2026-10-18 21:55 / 10-19 15:01 他）。
# 1205の場合だけ、ロールバック→待機→同じUPDATEを再実行する。
# status を NULL にするだけのUPDATEなので、再実行しても結果は変わらない。
DEADLOCK_RETRY_MAX = 3  # 初回実行を含む
DEADLOCK_RETRY_WAIT_SEC = 5


def _is_deadlock(e: pyodbc.Error) -> bool:
    return bool(e.args) and e.args[0] == "40001" and "(1205)" in str(e)


def reset_vendor_item_status_for_active_skus(conn):
    """listings に存在する SKU の vendor_item.status を NULL クリア（vendor_nameも一致させる）"""
    for attempt in range(1, DEADLOCK_RETRY_MAX + 1):
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE vi
                       SET vi.[status] = NULL
                    FROM [trx].[vendor_item] AS vi
                    INNER JOIN [trx].[listings] AS l
                        ON vi.[vendor_name] = l.[vendor_name]
                       AND vi.[vendor_item_id] = l.[vendor_item_id]
                    WHERE l.[is_deleted] = 0
                """)
                conn.commit()
            break
        except pyodbc.Error as e:
            if not _is_deadlock(e) or attempt == DEADLOCK_RETRY_MAX:
                raise
            conn.rollback()
            print(
                f"[WARN] デッドロック(1205)検知 → {DEADLOCK_RETRY_WAIT_SEC}秒後にリトライ "
                f"({attempt}/{DEADLOCK_RETRY_MAX - 1})",
                flush=True,
            )
            time.sleep(DEADLOCK_RETRY_WAIT_SEC)
    print("[INIT] status cleared on vendor_item joined with listings", flush=True)

def main():
    conn = None
    try:
        # === DB接続 ===
        conn = get_sql_server_connection()
        run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # === 初期化（fetch_active 全体で1回だけ） ===
        print("[INIT] clearing status...", flush=True)
        reset_vendor_item_status_for_active_skus(conn)

        # === 有効プリセット取得 ===
        presets = fetch_active_presets(conn)
        if not presets:
            print("[INFO] 有効なプリセットなし", flush=True)
            return

        # === job 登録 ===
        with conn.cursor() as cur:
            for p in presets:
                payload = {
                    "preset": p["preset"],
                    "vendor_name": p["vendor_name"],
                    "mode": p["mode"],
                    "low_usd_target": p["low_usd_target"],
                    "high_usd_target": p["high_usd_target"],
                    "brand_id": p["brand_id"],
                    "category_id": p["category_id"],
                }

                cur.execute("""
                    INSERT INTO trx.scrape_job
                      (job_kind, job_payload, status, created_at)
                    VALUES
                      (?, ?, 'pending', ?)
                """, (
                    "fetch_active_ebay",
                    json.dumps(payload, ensure_ascii=False),
                    run_ts
                ))

            conn.commit()

        print(f"[DONE] {len(presets)} jobs inserted", flush=True)

    except Exception:
        import traceback
        traceback.print_exc()
        raise

    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
