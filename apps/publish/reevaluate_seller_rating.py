# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import sys
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# sys.path bootstrap: file-direct run safe
# =========================
# このファイル: D:\apps_nostock\apps\publish\publish_ebay.py
# プロジェクトルート: D:\apps_nostock  ← parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from apps.common.utils import  get_sql_server_connection, build_driver


VENDOR_PERSONAL = "メルカリ"
VENDOR_SHOPS = "メルカリshops"

THRESHOLD_PERSONAL = 50
THRESHOLD_SHOPS = 20

BATCH_COMMIT = 50


# ====== レイヤ別チェック間隔（日） ======
def calc_check_interval_days(vendor_name: str, rating_count: int) -> int:
    # shops
    if vendor_name == VENDOR_SHOPS:
        if rating_count >= THRESHOLD_SHOPS:
            return 999999
        if rating_count >= 18:
            return 1
        if rating_count >= 12:
            return 7
        if rating_count >= 5:
            return 14
        return 30

    # personal
    if rating_count >= THRESHOLD_PERSONAL:
        return 999999
    if rating_count >= 45:
        return 1
    if rating_count >= 30:
        return 7
    if rating_count >= 10:
        return 14
    return 30


def needs_check(vendor_name: str, rating_count: int, last_checked_at: Optional[datetime]) -> bool:
    # 初期scrape：last_checked_at が NULL は無条件で対象
    if last_checked_at is None:
        return True

    interval_days = calc_check_interval_days(vendor_name, rating_count)
    if interval_days >= 999999:
        return False

    return last_checked_at <= (datetime.now() - timedelta(days=interval_days))


@dataclass(frozen=True)
class SellerKey:
    vendor_name: str
    seller_id: str


@dataclass
class SellerRow:
    vendor_name: str
    seller_id: str
    rating_count: Optional[int]
    last_checked_at: Optional[datetime]


# ====== SQL ======
SQL_SELECT_MISSING_SELLERS = """
SELECT DISTINCT
    v.vendor_name,
    v.seller_id
FROM trx.vendor_item AS v
LEFT JOIN mst.seller AS s
  ON s.vendor_name = v.vendor_name
 AND s.seller_id   = v.seller_id
WHERE
    v.vendor_name IN (?, ?)
    AND v.seller_id IS NOT NULL
    AND LTRIM(RTRIM(v.seller_id)) <> ''
    AND s.seller_id IS NULL
ORDER BY v.vendor_name, v.seller_id;
"""

# 未登録 seller を mst.seller に追加（rating_count/last_checked_at は NULL）
SQL_INSERT_MISSING_SELLER = """
INSERT INTO mst.seller (vendor_name, seller_id, seller_name, rating_count, last_checked_at, is_ng)
VALUES (?, ?, '', NULL, NULL, 0);
"""

SQL_SELECT_SELLERS_UNDER_THRESHOLD = """
SELECT
    s.vendor_name,
    s.seller_id,
    s.rating_count,
    s.last_checked_at
FROM mst.seller AS s
WHERE
    s.vendor_name IN (?, ?)
    AND ISNULL(s.is_ng,0) = 0
    AND (
        (s.vendor_name = ? AND (s.rating_count IS NULL OR s.rating_count < ?))
        OR
        (s.vendor_name = ? AND (s.rating_count IS NULL OR s.rating_count < ?))
    )
ORDER BY
    s.vendor_name,
    ISNULL(s.rating_count, 0) DESC,
    s.last_checked_at ASC;
"""

SQL_UPSERT_SELLER_RATING = """
UPDATE mst.seller
SET
    seller_name     = ?,
    rating_count    = ?,
    last_checked_at = SYSDATETIME()
WHERE vendor_name = ? AND seller_id = ?;
"""


# ====== DB操作 ======
def load_missing_sellers(conn) -> List[SellerKey]:
    with conn.cursor() as cur:
        cur.execute(SQL_SELECT_MISSING_SELLERS, (VENDOR_PERSONAL, VENDOR_SHOPS))
        rows = cur.fetchall()

    out: List[SellerKey] = []
    for vendor_name, seller_id in rows:
        sid = (seller_id or "").strip()
        if sid:
            out.append(SellerKey(vendor_name=vendor_name, seller_id=sid))
    return out


def insert_missing_sellers(conn, missing: List[SellerKey]) -> int:
    if not missing:
        return 0
    with conn.cursor() as cur:
        for m in missing:
            cur.execute(SQL_INSERT_MISSING_SELLER, (m.vendor_name, m.seller_id))
    return len(missing)


def load_mst_candidates(conn) -> List[SellerRow]:
    with conn.cursor() as cur:
        cur.execute(
            SQL_SELECT_SELLERS_UNDER_THRESHOLD,
            (VENDOR_PERSONAL, VENDOR_SHOPS, VENDOR_PERSONAL, THRESHOLD_PERSONAL, VENDOR_SHOPS, THRESHOLD_SHOPS),
        )
        rows = cur.fetchall()

    out: List[SellerRow] = []
    for r in rows:
        out.append(
            SellerRow(
                vendor_name=r[0],
                seller_id=(r[1] or "").strip(),
                rating_count=None if r[2] is None else int(r[2]),
                last_checked_at=r[3],
            )
        )
    return out


def update_seller_rating(conn, vendor_name: str, seller_id: str, seller_name: str, rating_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(SQL_UPSERT_SELLER_RATING, (seller_name, int(rating_count), vendor_name, seller_id))


# ====== Scrape ======
def build_seller_url(vendor_name: str, seller_id: str) -> str:
    if vendor_name == VENDOR_SHOPS:
        # ※ shops の seller_id が shop_id 形式で運用されている想定
        return f"https://mercari-shops.com/shops/{seller_id}"
    return f"https://jp.mercari.com/user/profile/{seller_id}"


def extract_rating_and_name(driver, vendor_name: str) -> Tuple[str, int]:
    body = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    txt = (body.text or "").replace(",", "")

    if vendor_name == VENDOR_SHOPS:
        m = re.search(r"(\d+)\s*(件|レビュー|評価)", txt)
        if not m:
            raise RuntimeError("shops rating not found")
        rating = int(m.group(1))
    else:
        m = re.search(r"(\d+)\s*件の評価", txt)
        if not m:
            raise RuntimeError("personal rating not found")
        rating = int(m.group(1))

    # 名前（取れた範囲でOK。取れなければ title）
    try:
        h1 = driver.find_element(By.CSS_SELECTOR, "h1")
        name = (h1.text or "").strip()
    except Exception:
        name = (driver.title or "").strip()

    return name, rating


def polite_sleep(i: int) -> None:
    time.sleep(1.2)
    if i % 20 == 0 and i > 0:
        time.sleep(3.0)


def main():
    conn = get_sql_server_connection()

    # 1) trx.vendor_item にいるのに mst.seller にいない seller を mst.seller に追加（初期scrape対象化）
    missing = load_missing_sellers(conn)
    inserted = insert_missing_sellers(conn, missing)
    if inserted:
        conn.commit()
    print(f"[INFO] missing sellers inserted into mst.seller: {inserted}")

    # 2) mst.seller から「閾値未達 or rating_count NULL」の seller を取り、レイヤ間隔で対象を決める
    rows = load_mst_candidates(conn)

    candidates: List[SellerKey] = []
    seen = set()
    for r in rows:
        if not r.seller_id:
            continue
        if r.rating_count is None:
            # 初期scrape：last_checked_at NULL のは needs_check が True
            rating = 0
        else:
            rating = r.rating_count
        if needs_check(r.vendor_name, rating, r.last_checked_at):
            key = SellerKey(r.vendor_name, r.seller_id)
            if key not in seen:
                candidates.append(key)
                seen.add(key)

    print(f"[INFO] candidates to scrape: {len(candidates)}")

    driver = build_driver()
    try:
        writes = 0

        for i, key in enumerate(candidates, start=1):
            url = build_seller_url(key.vendor_name, key.seller_id)
            try:
                driver.get(url)
                seller_name, rating = extract_rating_and_name(driver, key.vendor_name)

                update_seller_rating(conn, key.vendor_name, key.seller_id, seller_name, rating)
                writes += 1

                if writes >= BATCH_COMMIT:
                    conn.commit()
                    writes = 0

                print(f"[OK] {i}/{len(candidates)} {key.vendor_name} seller_id={key.seller_id} rating={rating}")

            except Exception as e:
                # 方針：失敗はDBに無理に書かない（ログのみ）
                print(f"[NG] {i}/{len(candidates)} {key.vendor_name} seller_id={key.seller_id} url={url} err={e}")

            polite_sleep(i)

        if writes:
            conn.commit()

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()
