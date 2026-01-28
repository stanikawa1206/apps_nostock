# -*- coding: utf-8 -*-
"""
ebay_common.py — 概要と関数一覧（公開関数のみ）
============================================================

■ 概要（端的に）
- Mercari の検索ページを自動で巡回し、商品IDや商品カード情報を取得する共通処理をまとめたモジュール。
- eBay向けツール群で「仕入れ候補収集」や「在庫チェック」に共通利用される。
- 検索URL生成、ページ送り、スクロール、ID抽出、在庫プレセット読込を一括サポート。

■ 関数一覧（補助関数は除外）
- page_url(base_search_url:str, page_idx_zero:int) -> str  
  検索ページのURLを page_token 付きで生成。例: page_token=v1:{idx}。

- has_no_results_banner(driver) -> bool  
  検索結果がゼロのとき（「該当する商品がありません」バナー）を検出。

- ensure_get(driver, url:str, max_retries:int=3, soft_stop:bool=True) -> WebDriver  
  get() 失敗時にリトライする安全版 driver.get()。Timeout時は window.stop() で中断フォールバック。

- extract_item_id(href:str) -> str|None  
  href 文字列から商品IDを抽出。  
  対応形式：`/item/m12345678`（個人）と `/shops/product/123456789`（Shops）。

- make_item_url(item_id:str) -> str  
  商品IDから個別商品のURLを生成。Shops ID でも item ページを参照可能な場合が多い。

- iterate_search(driver, base_search_url:str, preset:str, *, mode:'ids'|'cards'='ids', pause:float=0.45, stagnant_times:int=3, item_read_limit:int|None=None)
  -> Iterator  
  Mercari 検索結果をページごとに順次読み出す。  
  - mode='ids' → (page_idx, item_id, item_url, preset) を yield  
  - mode='cards' → (page_idx, [(item_id, title, price), ...], preset) を yield  
  ページ送り・スクロール・重複除外・結果ゼロ検出を自動で処理。

- make_search_url(vendor_name:str, brand_id:int, category_id:int, status:str, extra:str="") -> str  
  ブランドID・カテゴリID・状態などから Mercari 検索URLを構築。  
  vendor_name が「メルカリshops」なら item_types=beyond を付与。

- fetch_active_presets(conn) -> List[Dict]  
  mst.presets から is_active=1 の行を読み出す。  
  戻り値：[{preset, vendor_name, brand_id, category_id}, …]

■ 想定用途
- 「keepa_finder → Mercari検索 → 在庫確認 → eBay出品」の共通ルートで利用。
- 他のモジュール（publish_ebay_from_keepa.pyなど）からインポートして使う。

============================================================
"""


from __future__ import annotations
import time, random, re
from urllib.parse import quote
from typing import Iterable, Iterator, Literal, Tuple, List
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from apps.common.utils import (
    USD_JPY_RATE, PROFIT_RATE, EBAY_FEE_RATE,
    DOMESTIC_SHIPPING_JPY, INTL_SHIPPING_JPY, DUTY_RATE
)
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple

# ---- 既存の共通ツールを使う前提 ----
from apps.adapters.mercari_scraper import scroll_until_stagnant_collect_items

Mode = Literal["ids", "cards"]

def page_url(base_search_url: str, page_idx_zero: int) -> str:
    # Mercariの page_token=v1:{idx} 仕様
    return f"{base_search_url}&page_token={quote(f'v1:{page_idx_zero}', safe='')}"

def has_no_results_banner(driver) -> bool:
    # publish版/active版 両方をカバー（CSSと全文検索のフォールバック）
    try:
        el = driver.find_element(By.CSS_SELECTOR, "div[data-testid='no-result-banner']")
        if el.is_displayed():
            return True
    except Exception:
        pass
    try:
        txt = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        return ("該当する商品が" in txt) or ("出品された商品がありません" in txt)
    except Exception:
        return False

def ensure_get(driver, url: str, max_retries: int = 3, soft_stop: bool = True):
    err = None
    for attempt in range(1, max_retries + 1):
        try:
            driver.get(url)
            return driver
        except TimeoutException as e:
            if soft_stop:
                try:
                    driver.execute_script("window.stop();")
                    return driver
                except Exception:
                    pass
            err = e
        except WebDriverException as e:
            if any(s in str(e) for s in [
                "Timed out receiving message from renderer",
                "disconnected:",
                "cannot determine loading status",
                "chrome not reachable",
            ]):
                err = e
            else:
                raise
        except Exception as e:
            err = e
        # 再起動（呼び出し側で build_driver を持つケースがあるため単純リトライのみ）
        time.sleep(0.6 * attempt + random.uniform(0, 0.4))
    raise RuntimeError(f"ensure_get(): failed → {url} last_error={repr(err)}")

# ---- ID抽出の共通化（shops/personal両対応）----
_HREF_ID_PATTERNS = [
    re.compile(r"/item/(?P<iid>[a-z0-9]+)", re.IGNORECASE),            # 個人: /item/m123456...
    re.compile(r"/shops?/products?/(?P<iid>[0-9]+)", re.IGNORECASE),   # Shops: /shops/products/123456789
]

def extract_item_id(href: str) -> str | None:
    href = (href or "").strip().lower()
    for pat in _HREF_ID_PATTERNS:
        m = pat.search(href)
        if m:
            return m.group("iid")
    return None

def make_item_url(item_id: str) -> str:
    # item_id の形式で URL を作る（Shopsでも item ページに飛べるIDが多い。ダメなら呼び出し側で差替可）
    return f"https://jp.mercari.com/item/{item_id}"

# ---- 収集の本体 ----
def iterate_search(
    driver,
    base_search_url: str,
    preset: str,
    *,
    mode: Mode = "ids",
    pause: float = 0.45,
    stagnant_times: int = 3,
    item_read_limit: int | None = None,
) -> Iterator[Tuple[int, object, str]]:
    """
    mode='ids'   -> yield (page_idx, item_id, item_url, preset)
    mode='cards' -> yield (page_idx, items[(id,title,price)...], preset)
    """
    seen: set[str] = set()
    page_idx = 0

    while True:
        url = page_url(base_search_url, page_idx)
        driver = ensure_get(driver, url, max_retries=3)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        if has_no_results_banner(driver):
            break

        items = scroll_until_stagnant_collect_items(driver, pause=pause, stagnant_times=stagnant_times)

        if mode == "cards":
            # 既存の collector 仕様：(id,title,price) の配列を想定
            # ここではそのまま返す（呼び出し側でページ単位でUPSERT）
            yield (page_idx, items, preset)

        else:  # mode == "ids"
            # 画面の a[href] からID抽出（collectorの戻りに依らず二重化対策）
            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/item/'], a[href*='/products/']")
            raw_ids = []
            for a in anchors:
                try:
                    iid = extract_item_id(a.get_attribute("href"))
                    if iid:
                        raw_ids.append(iid)
                except Exception:
                    continue
            # collector 結果に id が含まれている場合はそれも併用
            for it in (items or []):
                try:
                    if isinstance(it, (list, tuple)) and len(it) >= 1:
                        raw_ids.append(str(it[0]).strip().lower())
                except Exception:
                    pass

            new_ids = [iid for iid in raw_ids if iid and iid not in seen]
            print(f"[DBG] page={page_idx} ids_found={len(new_ids)} anchors={len(anchors)} items_obj={len(items or [])} url={url}")
            for iid in new_ids:
                seen.add(iid)
                yield (page_idx, iid, make_item_url(iid), preset)
                if item_read_limit and len(seen) >= item_read_limit:
                    return

        if (not items) or (mode == "ids" and not new_ids):
            break

        page_idx += 1
        time.sleep(pause + random.uniform(0.15, 0.4))

# ebay_common.py
# -*- coding: utf-8 -*-
from typing import List, Dict

MERCARI_BASE_URL = (
    "https://jp.mercari.com/search?"   
    # ブランド・カテゴリ共通の検索ベース（brand_id, category_id は後付け）
    "d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"  # 出品形式：定額販売（固定価格）
    "&item_condition_id=1%2C2%2C3"           # 商品状態：新品・未使用に近い・目立った傷なし
    "&shipping_payer_id=2"                  # 送料負担：出品者（送料込み）
    "&sort=created_time&order=desc"         # 並び順：新着順（降順）
)

ITEM_TYPES = {
    "メルカリshops": "&item_types=beyond",
    "メルカリ":      "&item_types=mercari",
}

# apps/common/utils.py などに追加

def calc_cost_range_from_usd_range(
    mode: str,
    low_usd_target: Optional[float],
    high_usd_target: Optional[float],
) -> Tuple[Optional[int], Optional[int]]:
    """
    preset で指定した USDレンジ [low, high] に対して、
    compute_start_price_usd と同じ前提
      - USD_JPY_RATE
      - PROFIT_RATE
      - EBAY_FEE_RATE
      - DOMESTIC_SHIPPING_JPY / INTL_SHIPPING_JPY
      - DUTY_RATE
    で、
      「この範囲で売れるために許容される仕入れ円の min/max」
    を逆算して返す。

    low_usd_target / high_usd_target が None の場合は、その側は制約なし (None)。
    戻り値は (min_cost_jpy, max_cost_jpy) で、どちらも None あり。
    """

    rate = Decimal(str(USD_JPY_RATE))
    p    = Decimal(str(PROFIT_RATE))
    f    = Decimal(str(EBAY_FEE_RATE))
    denom = Decimal(1) - p - f
    if denom <= 0:
        raise ValueError("利益率と手数料率の合計が1.0以上です。")

    mode_u = mode.upper()
    if mode_u == "GA":
        ship = Decimal(str(DOMESTIC_SHIPPING_JPY))
        duty_factor = Decimal(1)      # GA は関税なし
    elif mode_u == "DDP":
        ship = Decimal(str(INTL_SHIPPING_JPY))
        duty_factor = Decimal(1) + Decimal(str(DUTY_RATE))
    else:
        raise ValueError(f"未知のmodeです: {mode}")

    def _cost_from_usd(usd_val: float) -> int:
        """1つの USD から、対応する仕入れ円（コスト）を逆算。"""
        U = Decimal(str(usd_val))

        # compute_start_price_usd の流れを逆算：
        # jpy_total = (cost + ship) * duty_factor / denom
        # usd      = jpy_total / rate
        # → jpy_total = U * rate
        # → cost + ship = jpy_total * denom / duty_factor
        # → cost       = jpy_total * denom / duty_factor - ship

        jpy_total = U * rate
        base = jpy_total * denom / duty_factor
        cost = base - ship

        # 小数を四捨五入して int 化
        c = int(cost.to_integral_value(rounding=ROUND_HALF_UP))
        return max(c, 0)

    min_cost: Optional[int] = None
    max_cost: Optional[int] = None

    if low_usd_target is not None:
        min_cost = _cost_from_usd(low_usd_target)
    if high_usd_target is not None:
        max_cost = _cost_from_usd(high_usd_target)

    # low > high になるパターンは一応補正
    if min_cost is not None and max_cost is not None and min_cost > max_cost:
        min_cost, max_cost = max_cost, min_cost

    return min_cost, max_cost


def make_search_url(*,
                    vendor_name: str,
                    brand_id: int,
                    category_id: int,
                    status: str,
                    mode: str = "GA",
                    low_usd_target: float = None,
                    high_usd_target: float = None,
                    extra: str = "") -> str:

    item_types = ITEM_TYPES.get(vendor_name, "")
    brand = f"&brand_id={int(brand_id)}" if brand_id else ""
    cat   = f"&category_id={int(category_id)}" if category_id else ""
    st    = f"&status={status}" if status else ""

    # === 円レンジの計算 ===
    min_cost, max_cost = calc_cost_range_from_usd_range(
        mode=mode,
        low_usd_target=low_usd_target,
        high_usd_target=high_usd_target,
    )

    # ★★★ デバッグ出力（重要） ★★★
    #print(
    #    f"[make_search_url DBG] vendor={vendor_name} preset_mode={mode} "
    #    f"USD={low_usd_target}〜{high_usd_target} → "
    #    f"JPY(min,max)=({min_cost}, {max_cost})"
    #)

    # === URL パラメータ ===
    price_q = ""
    if min_cost is not None:
        price_q += f"&price_min={min_cost}"
    if max_cost is not None:
        price_q += f"&price_max={max_cost}"

    return f"{MERCARI_BASE_URL}{brand}{cat}{st}{item_types}{price_q}{extra}"


def fetch_active_presets(conn) -> List[Dict]:
    """
    mst.v_presets から is_active=1 の行を読み込む（共通）
    """
    sql = """
        SELECT
            preset,
            vendor_name,
            brand_id,
            category_id,
            mode,
            low_usd_target,
            high_usd_target,
            category_id_ebay,
            department,
            default_brand_en,
            type_ebay,
            preset_group,
            max_page
          FROM [nostock].[mst].[v_presets] WITH (NOLOCK)
         WHERE ISNULL(is_active, 0) = 1
         ORDER BY preset
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out: List[Dict] = []
    for r in rows:
        out.append({
            "preset":           (r[0] or "").strip(),
            "vendor_name":      (r[1] or "").strip(),
            "brand_id":         int(r[2]) if r[2] is not None else 0,
            "category_id":      int(r[3]) if r[3] is not None else 0,
            "mode":             (r[4] or "").strip(),
            "low_usd_target":   float(r[5]) if r[5] is not None else None,
            "high_usd_target":  float(r[6]) if r[6] is not None else None,
            "category_id_ebay": (r[7] or "").strip(),
            "department":       (r[8] or "").strip(),
            "default_brand_en": (r[9] or "").strip(),
            "type_ebay":        (r[10] or "").strip() if r[10] is not None else "",
            "preset_group":     (r[11] or "").strip(),
            "max_page":         int(r[12]) if r[12] is not None else None,
        })
    return out
