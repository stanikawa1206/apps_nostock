# fetch_active_ebay_new.py
# 役割: fetch_active_ebay の task を trx.scrape_job に積むだけ

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

import json
from datetime import datetime
from apps.common.utils import get_sql_server_connection
from apps.adapters.mercari_search import fetch_active_presets


def main():
    conn = get_sql_server_connection()
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    presets = fetch_active_presets(conn)
    if not presets:
        print("[INFO] 有効なプリセットなし")
        return

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

    print(f"[DONE] {len(presets)} jobs inserted")


if __name__ == "__main__":
    main()
