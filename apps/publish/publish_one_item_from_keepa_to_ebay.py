# -*- coding: utf-8 -*-
"""
publish_one_gameA_to_ebay_Tanikawa4.py

目的:
  - 引数なしで実行し、preset = "ゲームA" の候補から "未出品" を1件だけ選び、
    eBay(アカウント: 谷川④) へ出品する最小フロー。
  - Amazon 由来の出品前提のため、セラー信頼度チェックは行わない。
  - 出品成功時、trx.listings へ listing_id を記録し、trx.vendor_item の出品状況を更新。
  - 価格は DDP 前提（バイヤー送料/関税込み想定）で算出（utils 設定値を利用）。

前提:
  - DB 接続・翻訳・メール送信などは utils.py の関数を使用。
  - eBay への出品は publish_ebay_adapter.post_one_item() を利用。
  - eBay アカウントのポリシーは mst.ebay_accounts テーブルに存在すること。

注意:
  - 必要に応じてテーブル/列名は環境に合わせて調整してください（在庫判定列など）。
"""

import os, sys, json, re
from typing import Optional, Dict, Any, List, Tuple
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

# ===== パス設定（common 配下を import 可能に） =====
_THIS_FILE = os.path.abspath(__file__)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_THIS_FILE))   # 例: D:\apps_nostock
_COMMON_DIR = os.path.join(_PROJECT_ROOT, "common")
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

# ===== 依存 =====
import pyodbc
from apps.common.utils import get_sql_server_connection, translate_to_english, send_mail
from apps.adapters.ebay_api import post_one_item, ApiHandledError, ListingLimitError
# ===== 固定設定 =====
EBAY_ACCOUNT = "谷川④"
PRESET_NAME = "ゲームA"              # 候補抽出はこのプリセットのみ
CATEGORY_ID = "139973"               # 例: Video Games > Games（環境に合わせて変更）
DEFAULT_BRAND_EN = "Unbranded"       # Brand 未特定時のデフォルト（必要に応じて修正）
DEPARTMENT = ""                     # Item Specifics 任意
MAX_PICS = 12

# === 出品説明のテンプレ ===
DESCRIPTION_TMPL = (
    "{title}\n\n"
    "This is shipped from Japan with tracking.\n"
    "Taxes and duties are included (DDP). Please contact us if you have any questions."
)

# ===== 価格算出(DPP/関税込/国際送料込み) =====
# utils から設定を利用: USD_JPY_RATE, PROFIT_RATE, EBAY_FEE_RATE, INTERNATIONAL_SHIPPING_JPY, DUTY_RATE,
# さらに LOW_USD_TARGET / HIGH_USD_TARGET が存在する前提（なければ適宜定義）

def _q2(x: Decimal) -> Decimal:
    """小数2桁へ丸め（銀行丸めでなく通常四捨五入）。"""
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def compute_start_price_usd_ddp(cost_jpy: int) -> str:
    """
    ゲーム向け: レンジ判定なし。常にUSDを返す（小数2桁、DDP前提）。
    """
    
    from apps.common.utils import (
        USD_JPY_RATE, PROFIT_RATE, EBAY_FEE_RATE, DUTY_RATE, INTERNATIONAL_SHIPPING_JPY
    )

    rate      = Decimal(str(USD_JPY_RATE))
    p         = Decimal(str(PROFIT_RATE))
    f         = Decimal(str(EBAY_FEE_RATE))
    intl_ship = Decimal(str(INTERNATIONAL_SHIPPING_JPY))
    duty      = Decimal(str(DUTY_RATE))

    denom = Decimal(1) - p - f - duty
    if denom <= 0:
        raise ValueError("PROFIT_RATE + EBAY_FEE_RATE + DUTY_RATE が 1.0 以上です。")

    jpy_total = (Decimal(cost_jpy) + intl_ship) / denom
    usd = (jpy_total / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{usd:.2f}"

# ===== DB 更新補助 =====

def record_ebay_listing(conn: pyodbc.Connection, listing_id: str, account_name: str,
                        vendor_item_id: str, vendor_name: str) -> None:
    """eBay listing_id を trx.listings に MERGE 記録。"""
    if not listing_id:
        return
    sql = """
MERGE INTO [trx].[listings] AS tgt
USING (SELECT ? AS listing_id, ? AS account, ? AS vendor_item_id, ? AS vendor_name) AS src
ON (tgt.listing_id = src.listing_id OR (tgt.vendor_item_id = src.vendor_item_id AND src.vendor_item_id <> ''))
WHEN MATCHED THEN
    UPDATE SET
        tgt.account        = src.account,
        tgt.vendor_item_id = src.vendor_item_id,
        tgt.vendor_name    = src.vendor_name,
        tgt.start_time     = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT ([listing_id], [start_time], [account], [vendor_item_id], [vendor_name])
    VALUES (src.listing_id, SYSDATETIME(), src.account, src.vendor_item_id, src.vendor_name);
"""
    with conn.cursor() as cur:
        cur.execute(sql, (listing_id, account_name, vendor_item_id, vendor_name))
    conn.commit()

UPSERT_LISTING_STATUS_SQL = """
MERGE INTO [trx].[vendor_item] AS tgt
USING (SELECT ? AS vendor_name, ? AS vendor_item_id) AS src
ON (tgt.vendor_name = src.vendor_name AND tgt.vendor_item_id = src.vendor_item_id)
WHEN MATCHED THEN
    UPDATE SET
        [出品日]        = CAST(SYSDATETIME() AS date),
        vendor_page     = ?,
        [出品状況]      = ?,
        [出品状況詳細]  = ?,
        last_checked_at = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (vendor_name, vendor_item_id, [出品日], vendor_page, [出品状況], [出品状況詳細],
            created_at, last_checked_at, status)
    VALUES (src.vendor_name, src.vendor_item_id, CAST(SYSDATETIME() AS date), ?, ?, ?,
            SYSDATETIME(), SYSDATETIME(), N'販売中');
"""

def mark_listing_status(conn: pyodbc.Connection, vendor_name: str, vendor_item_id: str,
                        vendor_page: Optional[int], head: str, detail: str = "") -> None:
    """vendor_item の出品状況（見出し/詳細）を更新。"""
    def _clip(s: str, n: int) -> str:
        s = (s or "").strip()
        return s if len(s) <= n else s[: max(0, n - 1)] + "…"

    with conn.cursor() as cur:
        cur.execute(
            UPSERT_LISTING_STATUS_SQL,
            (vendor_name, vendor_item_id, vendor_page, _clip(head, 100), _clip(detail, 255),
             vendor_page, _clip(head, 100), _clip(detail, 255))
        )
    conn.commit()

# ===== 候補抽出（preset=ゲームA から未出品の1件） =====

def pick_one_candidate(conn: pyodbc.Connection) -> Optional[Dict[str, Any]]:
    """trx.vendor_item から preset="ゲームA" で未出品のものを1件返す（amazon_category_path 付き）。"""
    sql = """
SELECT TOP 1
    vi.vendor_name,
    vi.vendor_item_id,
    vi.title_jp,
    vi.title_en,
    vi.price,
    vi.image_url1, vi.image_url2, vi.image_url3, vi.image_url4, vi.image_url5,
    vi.image_url6, vi.image_url7, vi.image_url8, vi.image_url9, vi.image_url10,
    a.category_path AS amazon_category_path
FROM trx.vendor_item AS vi WITH (NOLOCK)
LEFT JOIN trx.vendor_item_amazon AS a WITH (NOLOCK)
    ON a.vendor_item_id = vi.vendor_item_id
WHERE vi.preset = N'ゲームA'
  AND (vi.[出品不可flg] IS NULL OR vi.[出品不可flg] = 0)
  AND (vi.[出品状況] = N'在庫あり')
  AND NOT EXISTS (
        SELECT 1 FROM trx.listings AS l WITH (NOLOCK)
        WHERE l.vendor_name = vi.vendor_name AND l.vendor_item_id = vi.vendor_item_id
  )
ORDER BY vi.created_at DESC
"""
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cur.description]
        data = {cols[i]: row[i] for i in range(len(cols))}

    pics: List[str] = []
    for k in [f"image_url{i}" for i in range(1, 10 + 1)]:
        u = (data.get(k) or "").strip()
        if u:
            pics.append(u)
    data["pictures"] = pics[:MAX_PICS]
    return data


# ===== タイトル英訳＋整形 =====

def prepare_title_en(conn: pyodbc.Connection, vendor_name: str, vendor_item_id: str,
                     title_jp: str, title_en_existing: Optional[str]) -> Optional[str]:
    """既存英題があれば採用、なければ翻訳→整形。空なら None。"""
    if title_en_existing and title_en_existing.strip():
        s = title_en_existing.strip()
    else:
        s = (translate_to_english(title_jp) or "").strip()
    if not s:
        return None
    # 80字以内にスマートトリム
    if len(s) > 80:
        cut = s[:77]
        if " " in cut and not cut.endswith(" "):
            cut = cut[: cut.rfind(" ")]
        s = cut.rstrip() + "..."
    return s

# ===== eBay ペイロード生成 =====
def derive_platform_from_category_path(category_path: Optional[str]) -> Optional[str]:
    """
    例: 'ゲーム > 機種別 > Nintendo Switch > ゲームソフト' → 'Nintendo Switch'
    3階層目が取得できなければ None を返す。
    """
    if not category_path:
        return None
    parts = [p.strip() for p in str(category_path).split(">")]
 
    if len(parts) >= 3 and parts[2]:
        print(f"DEBUG: platform={parts[2]}")
        return parts[2]
    print("DEBUG: 3階層目が取得できませんでした")
    return None


def make_payload(rec: Dict[str, Any], start_price_usd: str, title_en: str,
                 platform: Optional[str] = None,
                 game_name: Optional[str] = None) -> Dict[str, Any]:
    pics = [u for u in rec.get("pictures", []) if u][:MAX_PICS]
    payload = {
        "CustomLabel": str(rec["vendor_item_id"]).strip(),
        "*Title": title_en,
        "*StartPrice": start_price_usd,
        "*Quantity": 1,
        "PicURL": "|".join(pics),
        "*Description": DESCRIPTION_TMPL.format(title=title_en),
        "category_id": CATEGORY_ID,
        "C:Brand": DEFAULT_BRAND_EN,
    }
    if platform and platform.strip():
        payload["platform"] = platform.strip()
    if game_name and game_name.strip():
        payload["game_name"] = game_name.strip()

    print("=== DEBUG: payload to be sent ===")
    for k, v in payload.items():
        print(f"{k}: {v}")
    print("=== END payload ===")
    return payload



# ===== メイン =====

def main() -> None:
    start = datetime.now()
    conn = get_sql_server_connection()

    try:
        # 1) アカウントポリシーの取得（谷川④）
        with conn.cursor() as cur:
            cur.execute("""
                SELECT fulfillment_policy_id, payment_policy_id, return_policy_id
                FROM [mst].[ebay_accounts]
                WHERE LTRIM(RTRIM(account)) = LTRIM(RTRIM(?))
            """, (EBAY_ACCOUNT,))
            row = cur.fetchone()

        acct_policies = {
            "fulfillment_policy_id": str(row[0]),
            "payment_policy_id": str(row[1]),
            "return_policy_id": str(row[2]),
            "merchant_location_key": "Default",
        }

        # 2) 候補1件のピックアップ（preset=ゲームA, 未出品）
        cand = pick_one_candidate(conn)
        if not cand:
            print("[INFO] 候補が見つかりません（preset=ゲームA / 未出品 / 在庫あり / 固定NG除外）")
            return

        vendor_name = cand["vendor_name"]
        vendor_item_id = cand["vendor_item_id"]
        title_jp = cand.get("title_jp") or ""
        title_en_existing = (cand.get("title_en") or "").strip() or None

        # 3) タイトル英訳（既存優先 → 未設定なら翻訳）
        title_en = prepare_title_en(conn, vendor_name, vendor_item_id, title_jp, title_en_existing)
        if not title_en:
            mark_listing_status(conn, vendor_name, vendor_item_id, None, "翻訳空返し", "")
            print(f"[SKIP] 翻訳取得不可: {vendor_item_id}")
            return

        # 4) 売価計算（DDP）
        cost_jpy = cand.get("price")
        start_price_usd = compute_start_price_usd_ddp(cost_jpy)
        if not start_price_usd:
            rng = f"{getattr(utils, 'LOW_USD_TARGET', '-')}-{getattr(utils, 'HIGH_USD_TARGET', '-')} USD"
            mark_listing_status(conn, vendor_name, vendor_item_id, None, "計算価格が範囲外", rng)
            print(f"[SKIP] 価格レンジ外: {vendor_item_id} cost={cost_jpy}")
            return
        
        # 4.5) Platform を category_path の3階層目から決定（テーブル由来のみ）
        platform = derive_platform_from_category_path(cand.get("amazon_category_path"))

        # 5) eBay ペイロード生成
        payload = make_payload(cand, start_price_usd, title_en, platform=platform, game_name=title_en)
        print(f"[POST] SKU={payload['CustomLabel']} Title='{payload['*Title']}' Price(USD)={payload['*StartPrice']}")

        # 6) 出品API呼び出し
        try:
            listing_id = post_one_item(payload, EBAY_ACCOUNT, acct_policies)
            if listing_id:
                print(f"[OK] 出品完了 listing_id={listing_id}")
                record_ebay_listing(conn, listing_id, EBAY_ACCOUNT, vendor_item_id, vendor_name)
                mark_listing_status(conn, vendor_name, vendor_item_id, None, "出品", "")
            else:
                mark_listing_status(conn, vendor_name, vendor_item_id, None, "出品失敗", "listing_id未返却")
                print("[ERR] 出品失敗: listing_id未返却")
                return
        except (ListingLimitError, ApiHandledError) as e:
            mark_listing_status(conn, vendor_name, vendor_item_id, None, "出品失敗", str(e))
            print(f"[ERR] APIハンドル例外: {e}")
            return
        except Exception as e:
            mark_listing_status(conn, vendor_name, vendor_item_id, None, "出品失敗(未分類)", str(e))
            print(f"[ERR] 想定外例外: {e}")
            return

        # 7) 完了通知（任意）
        try:
            elapsed = datetime.now() - start
            send_mail(
                "✅ eBay単発出品 完了 (谷川④)",
                f"SKU: {vendor_item_id}\nPrice(USD): {start_price_usd}\nListingID: {listing_id}\nElapsed: {elapsed}"
            )
        except Exception as e:
            print(f"[WARN] 完了メール送信に失敗: {e}")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
