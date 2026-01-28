# -*- coding: utf-8 -*-
"""
test_mercari_shops.py
- Mercari shops (item_types=beyond) 検証用
- 1,2ページを取得して (id, title, price_int) を print
- タイトル先頭の価格行を除去／価格は一度だけ抽出して整数で出力
"""

import sys, time, random, re, traceback
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
        sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
    except Exception:
        pass

sys.path.extend([r"D:\\apps_nostock", r"D:\\apps_nostock\\common"])
from scrape_utils import build_driver, safe_quit

BASE_URL = (
    "https://jp.mercari.com/search?"
    "brand_id=1326&category_id=243&status=on_sale&sort=created_time"
    "&item_condition_id=1%2C2%2C3&shipping_payer_id=2"
    "&price_min=50000&price_max=115000&beyond=shops"
    "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
    "&item_types=beyond"
)

PAUSE = 0.6
NO_RESULT_TEXT = "出品された商品がありません"

M_ITEM_RE  = re.compile(r"/item/(?P<m>m\d{8,})(?:[/?#]|$)")
SHOP_ID_RE = re.compile(r"/shops/product/(?P<p>[^/?#]+)")
YEN_LINE_RE = re.compile(r"^\s*(¥|￥)?\s*[\d,]+\s*$")
YEN_RE = re.compile(r"¥\s*([\d,]+)")

def add_or_replace_query(url: str, **params) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = str(v)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def page_url(base_url: str, idx_zero_based: int) -> str:
    return base_url if idx_zero_based == 0 else add_or_replace_query(base_url, page_token=f"v1:{idx_zero_based}")

def has_no_results_banner(driver) -> bool:
    try:
        txt = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        return NO_RESULT_TEXT in txt
    except Exception:
        return False

def dismiss_popups(driver):
    try:
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        for label in ("同意する", "許可しない", "閉じる", "OK", "同意して続行", "同意して閲覧"):
            try:
                for el in driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']"):
                    if el.is_displayed():
                        el.click(); time.sleep(0.2)
            except Exception:
                pass
    except Exception:
        pass

def click_show_more_if_any(driver):
    """もっと見る/さらに表示/続きを読み込む 等があれば押す（存在しなければ無視）"""
    labels = ("もっと見る", "さらに表示", "続きを読み込む", "もっと表示")
    for lab in labels:
        try:
            btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{lab}']")
            for b in btns:
                if b.is_displayed() and b.is_enabled():
                    b.click()
                    time.sleep(0.5)
        except Exception:
            pass

def incremental_scroll_collect(driver, max_loops=30, pause=PAUSE):
    """アンカー件数が伸びなくなるまでスクロール＋“もっと見る”クリック"""
    last = -1
    stagnant = 0
    for _ in range(max_loops):
        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/item/'], a[href*='/shops/product/']")
        if len(anchors) == last:
            stagnant += 1
        else:
            stagnant = 0
        if stagnant >= 2:
            break
        last = len(anchors)

        driver.execute_script("window.scrollBy(0, Math.max(800, window.innerHeight*0.8));")
        time.sleep(pause + random.uniform(0.15, 0.35))
        click_show_more_if_any(driver)

# URLからメルカリor　shopsの商品IDを取得
def extract_vendor_item_id(href: str) -> str | None:
    if not href:
        return None
    m1 = M_ITEM_RE.search(href)
    if m1:
        return m1.group("m")
    m2 = SHOP_ID_RE.search(href)
    if m2:
        return m2.group("p")
    return None

def sanitize_title(raw_text: str) -> str:
    """複数行の a.text から先頭の価格行などを除去して1行化"""
    if not raw_text:
        return ""
    lines = [ln.strip() for ln in raw_text.replace("\r", "").split("\n")]
    cleaned = []
    for ln in lines:
        if not ln:
            continue
        # 価格だけの行（¥ 108,600 など）はスキップ
        if YEN_LINE_RE.match(ln):
            continue
        cleaned.append(ln)
    title = " ".join(cleaned)
    # 価格の数字が末尾にダブって入っているケースを薄めに除去（後置の純数字）
    title = re.sub(r"\s+\d{2,}(?:,\d{3})*$", "", title).strip()
    return title

def extract_title_and_price_from_anchor(anchor):
    # title: aria-label → title属性 → a.text（整形）
    title = (anchor.get_attribute("aria-label") or "").strip()
    if not title:
        t2 = anchor.get_attribute("title")
        if t2 and t2.strip():
            title = t2.strip()
    if not title:
        title = sanitize_title(anchor.text or "")

    # price: 近傍から ¥xx,xxx を拾う（親3階層まで）
    price = None
    node = anchor
    for _ in range(3):
        try:
            txt = (node.text or "") + " " + (node.get_attribute("innerText") or "")
        except Exception:
            txt = anchor.text or ""
        m = YEN_RE.search(txt.replace("\u3000"," "))
        if m:
            try:
                price = int(m.group(1).replace(",", ""))
            except Exception:
                price = None
            break
        try:
            node = node.find_element(By.XPATH, "./..")
        except Exception:
            break

    return title, price

def main():
    driver = None
    try:
        # ブラウザを起動
        driver = build_driver()

        # 画面を広めにして、表示の崩れを起きにくくする
        try:
            driver.set_window_size(1280, 1600)
        except Exception:
            pass  # うまくいかない環境もあるので無視

        # 1ページ目・2ページ目を順番に見る（0が1ページ目の意味）
        for page_idx in (0, 1):
            url = page_url(BASE_URL, page_idx)
            print(f"\n[PAGE NAV] page={page_idx+1} url={url}")

            # ページを開いて、画面の読み込みを待つ
            try:
                driver.get(url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )
            except Exception:
                # 開けなかった場合は記録して次のページへ
                traceback.print_exc()
                print(f"[PAGE NAV] failed page={page_idx+1}")
                continue

            # 邪魔なポップアップを閉じる
            dismiss_popups(driver)

            # 「該当なし」などの案内が出ていたら、このページは飛ばす
            if has_no_results_banner(driver):
                print(f"[PAGE {page_idx+1}] no results banner detected")
                continue

            # ページ下まで少しずつスクロールして、商品を読み込ませる
            incremental_scroll_collect(driver, max_loops=32, pause=PAUSE)

            # 商品ページへのリンクだけを集める
            anchors = driver.find_elements(
                By.CSS_SELECTOR, "a[href*='/item/'], a[href*='/shops/product/']"
            )
            print(f"[DIAG] anchors: total={len(anchors)}")

            seen = set()   # 同じ商品を二度カウントしないための記録
            items = []     # (商品ID, タイトル, 価格) を入れる箱

            # 見つけたリンクから商品情報を抜き出す
            for a in anchors:
                try:
                    href = a.get_attribute("href") or ""
                    vid = extract_vendor_item_id(href)  # URLから商品IDを取り出す
                    if not vid or vid in seen:
                        continue  # IDが取れない、または重複ならスキップ
                    seen.add(vid)

                    # リンク周りの表示からタイトルと価格を拾う
                    title, price = extract_title_and_price_from_anchor(a)
                    items.append((vid, title, price))
                except Exception:
                    # 取り出しに失敗しても全体は止めない
                    continue

            # このページで取れた件数を表示
            print(f"[PAGE {page_idx+1}] scraped_count={len(items)}")

            # 1行につき1商品の簡単な出力（タブ区切り）
            for iid, title, price in items:
                p = "" if price is None else str(price)  # 価格が無いときは空欄
                # 出力形式: 商品ID[TAB]タイトル[TAB]価格(数字のみ)
                print(f"{iid}\t{title}\t{p}")

            # 次のページに行く前に、少し間をあける（アクセスが急にならないように）
            time.sleep(PAUSE + random.uniform(0.2, 0.5))

    finally:
        # 最後にブラウザをきちんと閉じる（念のため例外にも対応）
        try:
            if driver is not None:
                safe_quit(driver)
        except Exception:
            pass

if __name__ == "__main__":
    main()
