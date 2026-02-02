# apps/producer/producer_publish_ebay.py

from datetime import datetime, timezone, timedelta
import json

from apps.common.utils import get_sql_server_connection

JST = timezone(timedelta(hours=9))
def now_jst():
    return datetime.now(JST).replace(tzinfo=None)

def enqueue_publish_ebay(
    preset_group: str | None = None,
    max_listings: int | None = None,
):
    payload = {}
    if preset_group:
        payload["preset_group"] = preset_group
    if max_listings:
        payload["max_listings"] = max_listings

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trx.scrape_job (
                    job_kind,
                    job_payload,
                    status,
                    created_at
                )
                VALUES (?, ?, 'pending', ?)
            """, (
                "publish_ebay",
                json.dumps(payload, ensure_ascii=False),
                now_jst(),
            ))
        conn.commit()
        print(f"✅ enqueue publish_ebay payload={payload}")
    finally:
        conn.close()

if __name__ == "__main__":
    # 例：全preset_group対象で実行
    enqueue_publish_ebay()
