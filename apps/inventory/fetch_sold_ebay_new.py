# fetch_sold_mercari_producer.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import sys

# ===== path bootstrap =====
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection
from apps.adapters.mercari_search import fetch_active_presets


JOB_KIND = "fetch_sold_mercari"


def insert_job(conn, job_kind: str, payload: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trx.scrape_job
                (job_kind, job_payload, status, created_at)
            VALUES
                (?, ?, 'pending', GETDATE())
            """,
            job_kind,
            json.dumps(payload, ensure_ascii=False),
        )
    conn.commit()


def main():
    conn = get_sql_server_connection()

    presets = fetch_active_presets(conn)
    print(f"[PRODUCER] presets={len(presets)}")

    count = 0
    for p in presets:
        payload = {
            "preset": p["preset"],
            "vendor_name": p["vendor_name"],          # メルカリ / メルカリshops
            "brand_id": p["brand_id"],
            "category_id": p["category_id"],
            "mode": p.get("mode", "DDP"),
            "low_usd_target": p["low_usd_target"],
            "high_usd_target": p["high_usd_target"],
            "status": "sold_out|trading",
            "max_pages": 3,                            # producer 側で固定してもOK
        }

        insert_job(conn, JOB_KIND, payload)
        count += 1

    conn.close()
    print(f"✅ job投入完了: {count} 件")


if __name__ == "__main__":
    main()
