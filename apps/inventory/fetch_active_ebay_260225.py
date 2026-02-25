# fetch_active_ebay_new.py
# 役割: fetch_active_ebay の task を trx.scrape_job に積むだけ

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

import json
from datetime import datetime
from apps.common.utils import get_sql_server_connection
from apps.adapters.mercari_search import fetch_active_presets

def reset_vendor_item_status_for_active_skus(conn):
    """listings に存在する SKU の vendor_item.status を NULL クリア（vendor_nameも一致させる）"""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE vi
               SET vi.[status] = NULL
            FROM [trx].[vendor_item] AS vi
            INNER JOIN [trx].[listings] AS l
                ON vi.[vendor_name] = l.[vendor_name]
               AND vi.[vendor_item_id] = l.[vendor_item_id]
        """)
        conn.commit()
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
