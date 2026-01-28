import sys
import time
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# utils を使うためのパス追加
sys.path.append(r"d:\apps_nostock\apps")
from common import utils  # type: ignore


JOOM_BASE = "https://www.joom.com"
MARKETPLACE_NAME = "joom"

# --------------------------------------------------
# JOOM 商品カード共通セレクタ
# --------------------------------------------------
# 今はページ全体から拾う元のセレクタをそのまま定義。
# 将来「検索結果グリッドだけ」に絞りたい場合は、
# 例: "div[data-block-id='productsGrid'] a.content___N4xbX[href*='/en/products/']"
# のように、前にコンテナのセレクタを足して調整してください。
CARD_SELECTOR = "a.content___N4xbX[href*='/en/products/']"


# --------------------------------------------------
# 「Show more」対応：商品カードを十分読み込む
# --------------------------------------------------
def load_cards_with_show_more(driver, min_count: int = 60):
    """
    商品カードを min_count 件以上読み込むまで
    「Show more」を押しながら増やしていく。
    ボタンが無くなったらその時点で終了。
    """
    wait = WebDriverWait(driver, 10)

    while True:
        cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
        print(f"[DEBUG] 現在のカード数: {len(cards)}")

        if len(cards) >= min_count:
            print(f"[DEBUG] min_count={min_count} 達成 → 終了")
            return cards

        try:
            # ★ Show more ボタンの XPATH をゆるくする
            show_more_btn = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//button[normalize-space()='Show more' "
                        "       or .//span[normalize-space()='Show more']]"
                    )
                )
            )
            print("[DEBUG] Show more ボタンをクリックします")
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});",
                show_more_btn
            )
            show_more_btn.click()

            # ロード待ち（必要に応じて 2→3 などに増やしてもOK）
            time.sleep(2)

        except Exception as e:
            print(f"[DEBUG] Show more ボタンが見つからない/クリックできない: {e}")
            print("[DEBUG] ここまで読み込んだカードだけで終了")
            return cards


# --------------------------------------------------
# DB: カテゴリ一覧取得
# --------------------------------------------------
def get_active_categories(conn):
    """
    ext.market_categories から is_active = 1 の categoryID を全部取る
    """
    sql = """
        SELECT categoryID
        FROM ext.market_categories
        WHERE is_active = 1
        ORDER BY categoryID
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [row[0] for row in rows]


# --------------------------------------------------
# DB: ext.market_items への UPSERT
# --------------------------------------------------
def upsert_market_item(
    conn,
    item_id: str,
    product_url: str,
    product_name: str | None,
    price_text: str | None,
    rating: float | None,
    review_count: int | None,
    purchase_text: str | None,   # ← ここは「元の文字列」をそのまま受け取る
    seller_id: str | None,
    seller_name: str | None,
    brand: str | None,
    category_id: str | None,
    page_index: int | None,
    image_url: str | None,
):
    """
    1 商品分を ext.market_items に MERGE で upsert する。
    sold カラムには「18 purchases」「More than 10000 purchases」など
    元のテキストをそのまま入れる。
    """

    # 価格（"¥2,403" → 2403.0）だけは数値化しておく
    price_value = None
    if price_text:
        m = re.search(r"([\d,]+)", price_text)
        if m:
            price_value = float(m.group(1).replace(",", ""))

    # sold に入れる文字列
    sold_text = purchase_text

    # ★ 「More than」を含む sold_text は非日本セラー疑いとして ABEND
    if sold_text is not None and "More than" in sold_text:
        print("===== ABEND: 非日本セラーと思われる sold_text を検出しました =====")
        print(f"item_id     : {item_id}")
        print(f"category_id : {category_id}")
        print(f"sold_text   : {sold_text}")
        print("========================================")
        raise RuntimeError("sold_text contains 'More than' → 非日本セラー疑いで終了")

    # ★ sold の最大文字数チェック（テーブル定義と合わせる：NVARCHAR(200) 前提）
    MAX_SOLD_LEN = 200
    if sold_text is not None and len(sold_text) > MAX_SOLD_LEN:
        print("===== ABEND: sold の文字列が長すぎます =====")
        print(f"item_id       : {item_id}")
        print(f"category_id   : {category_id}")
        print(f"len(sold_text): {len(sold_text)}")
        print("---- sold_text ----")
        print(sold_text)
        print("========================================")
        # ここで強制終了して原因を確認する
        raise RuntimeError("sold_text length > 200 (NVARCHAR(200) overflow)")

    # shopID と memo
    shop_id = seller_id
    memo = seller_name

    sql = """
    MERGE ext.market_items AS T
    USING (
        SELECT
            ? AS marketplace,
            ? AS itemID,
            ? AS product_url,
            ? AS product_name,
            ? AS price,
            ? AS rating,
            ? AS review_count,
            ? AS sold,
            ? AS categoryID,
            ? AS brand,
            ? AS shopID,
            ? AS memo,
            ? AS page,
            ? AS image_url
    ) AS S
        ON  T.marketplace = S.marketplace
        AND T.itemID      = S.itemID
    WHEN MATCHED THEN
        UPDATE SET
            T.product_url   = S.product_url,
            T.product_name  = S.product_name,
            T.price         = S.price,
            T.rating        = S.rating,
            T.review_count  = S.review_count,
            T.sold          = S.sold,          -- ← テキストのまま
            T.categoryID    = S.categoryID,
            T.brand         = S.brand,
            T.shopID        = S.shopID,
            T.memo          = S.memo,
            T.page          = S.page,
            T.image_url     = S.image_url,
            T.scraped_at    = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            marketplace,
            itemID,
            product_url,
            product_name,
            price,
            rating,
            review_count,
            sold,
            categoryID,
            brand,
            shopID,
            memo,
            page,
            image_url,
            scraped_at
        )
        VALUES (
            S.marketplace,
            S.itemID,
            S.product_url,
            S.product_name,
            S.price,
            S.rating,
            S.review_count,
            S.sold,
            S.categoryID,
            S.brand,
            S.shopID,
            S.memo,
            S.page,
            S.image_url,
            GETDATE()
        );
    """

    params = (
        MARKETPLACE_NAME,
        item_id,
        product_url,
        product_name,
        price_value,
        rating,
        review_count,
        purchase_text,  # ← sold_text と同じ中身だが「元のテキスト」をそのまま渡す
        category_id,
        brand,
        shop_id,
        memo,
        page_index,
        image_url,
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)


# --------------------------------------------------
# 1商品の詳細ページを開いて情報取得 + DB書き込み
# --------------------------------------------------
def scrape_product_detail(
    driver,
    conn,
    item_id: str,
    category_id: str | None,
    rank_index: int,
):
    """
    単一商品（item_id）の詳細ページを開いて情報取得。
    ext.market_items に upsert しつつ、
    review/purchase の有無を上位ロジックに返す。
    """

    url = f"{JOOM_BASE}/en/products/{item_id}"
    driver.get(url)

    wait = WebDriverWait(driver, 15)

    # ★ 追加：18歳確認画面が出たら「Yes, I am 18 or older」を押す
    try:
        age_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    # span の中にテキストがあるパターンと、button 直下にあるパターン両方見ておく
                    "//button[.//span[contains(normalize-space(), 'Yes, I am 18 or older')]"
                    " or contains(normalize-space(), 'Yes, I am 18 or older')]"
                )
            )
        )
        age_btn.click()
        time.sleep(1)  # 画面が切り替わるのを少しだけ待つ
    except TimeoutException:
        # 年齢確認がない商品ではそのまま進む
        pass

    # ----- 商品名 -----
    h1 = wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
    product_name = h1.text.strip()

    # ----- 価格 -----
    price_span = wait.until(
        EC.presence_of_element_located((By.XPATH, "//span[contains(text(),'¥')]"))
    )
    price_text = price_span.text.strip()

    # ----- 画像 URL -----
    image_url = None
    try:
        # 1) まずはギャラリー内の画像を優先して探す
        img_el = None
        try:
            img_el = driver.find_element(
                By.CSS_SELECTOR,
                "div[data-block-id='gallery'] img[src]"
            )
        except Exception:
            # 2) 見つからなければ、Joom の商品画像ドメインだけを対象にする
            imgs = driver.find_elements(
                By.CSS_SELECTOR,
                "img[src^='https://img.joomcdn.net/']"
            )
            if imgs:
                img_el = imgs[0]

        if img_el:
            cand = img_el.get_attribute("src") or ""

            # data: で始まるものはアイコン等なので捨てる
            if cand.startswith("data:"):
                # srcset に本物が入っている可能性があるので、そちらも試す
                srcset = img_el.get_attribute("srcset") or ""
                if srcset:
                    # "url1 100w, url2 200w, ..." の形式なので、最初の URL を抜く
                    first_part = srcset.split(",")[0].strip()
                    cand = first_part.split()[0]

            if cand.startswith("https://img.joomcdn.net/"):
                image_url = cand
            else:
                image_url = None
    except Exception:
        image_url = None

    # ----- レビュー/評価 -----
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

    review_count = None
    rating = None

    try:
        root = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[data-block-id='reviews']")
            )
        )

        # レビュー数
        try:
            count_span = root.find_element(By.CSS_SELECTOR, "span[class*='headerCount']")
            text = count_span.text.strip()
            if text.isdigit():
                review_count = int(text)
        except Exception:
            review_count = None

        # 評価
        try:
            rating_span = root.find_element(By.CSS_SELECTOR, "div[class*='rating'] span")
            t = rating_span.text.strip()
            rating = float(t)
        except Exception:
            try:
                filled = len(
                    root.find_elements(By.CSS_SELECTOR, ".ratingStars___WnCCa svg")
                )
                bg = len(
                    root.find_elements(By.CSS_SELECTOR, ".ratingBackground___aGvwa svg")
                ) or 5
                rating = (filled / bg) * 5
            except Exception:
                rating = None

    except Exception:
        review_count = None
        rating = None

    # ----- 購入数表示（"18 purchases" など） -----
    purchase_text = None
    try:
        elems = driver.find_elements(
            By.XPATH, "//*[contains(text(), 'purchases')]"
        )

        candidates: list[str] = []
        for el in elems:
            txt = (el.text or "").strip()
            if not txt:
                continue
            if "purchases" not in txt:
                continue

            # 長文説明を除外するためのフィルタ
            if len(txt) > 100:      # ここは好みで 80 とかでもOK
                continue
            if "\n" in txt:         # 複数行は説明文の可能性が高いので除外
                continue

            candidates.append(txt)

        if candidates:
            # 一番短いものを採用（たいてい "18 purchases" みたいなの）
            purchase_text = min(candidates, key=len)
        else:
            purchase_text = None

    except Exception:
        purchase_text = None

    # ----- セラーID & セラー名 -----
    seller_id = None
    seller_name = None
    try:
        store_link = driver.find_element(
            By.XPATH,
            "//a[contains(@href, '/en/stores/')]",
        )
        href = store_link.get_attribute("href")
        if href and "/en/stores/" in href:
            seller_id = href.split("/en/stores/")[1].split("?")[0]

        try:
            name_div = store_link.find_element(By.CSS_SELECTOR, "div[class*='nameText']")
            seller_name = name_div.text.strip()
        except Exception:
            seller_name = None
    except Exception:
        pass

    # ----- ブランド -----
    brand = None
    try:
        brand_div = driver.find_element(
            By.XPATH,
            "//a[contains(@href, '/en/search/')]"
            "//div[normalize-space(text()) != '' and not(contains(text(), 'purchases'))]",
        )
        brand = brand_div.text.strip()
    except Exception:
        brand = None

    # DB へ upsert
    upsert_market_item(
        conn=conn,
        item_id=item_id,
        product_url=url,
        product_name=product_name,
        price_text=price_text,
        rating=rating,
        review_count=review_count,      # ここを忘れずに
        purchase_text=purchase_text,
        seller_id=seller_id,
        seller_name=seller_name,
        brand=brand,
        category_id=category_id,
        page_index=rank_index,   # ← 一覧での並び順を保存
        image_url=image_url,
    )

    # 上位ロジック用に「レビュー/購入があるかどうか」を返す
    has_review = bool(
        (review_count is not None and review_count > 0)
        or (rating is not None and rating > 0)
    )
    has_purchase = bool(purchase_text)

    return has_review, has_purchase


def load_cards_with_scrolling(
    driver,
    min_count: int = 80,
    max_scroll: int = 20,
    sleep_sec: float = 2.0
):
    """
    一番下までスクロールして、商品カードが増えなくなるまで待つ方式。

    - min_count 以上になったら終了
    - max_scroll 回スクロールしても増えなければ終了
    """

    last_count = 0

    for i in range(max_scroll):
        # 現在のカード数
        cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
        curr_count = len(cards)
        print(f"[DEBUG] scroll#{i+1} カード数: {curr_count}")

        # 目標数に達したら終了
        if curr_count >= min_count:
            print(f"[DEBUG] min_count={min_count} 達成 → 終了")
            return cards

        # 前回から増えていなければ、これ以上読み込めないと判断して終了
        if curr_count == last_count:
            print("[DEBUG] カード数が増えないので終了")
            return cards

        last_count = curr_count

        # 一番下までスクロール
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(sleep_sec)

    # max_scroll 回スクロールしても終わらなかった場合
    print("[DEBUG] max_scroll に達したので終了")
    return driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)

def open_search_with_retry(driver, category_id: str, max_retry: int = 3) -> None:
    search_url = (
        f"{JOOM_BASE}/en/search/{category_id}"
        f"/f.merchantOrigin.tree.Japanese/s.salesCount.desc"
    )

    for attempt in range(1, max_retry + 1):
        print(f"[DEBUG] 検索URL open try {attempt}: {search_url}")
        driver.get(search_url)
        time.sleep(3)

        actual_url = driver.current_url
        print(f"[DEBUG] actual_url = {actual_url}")

        if "f.merchantOrigin.tree.Japanese" in actual_url:
            print("[DEBUG] Japan filter OK")
            return

        print("[WARN] Japan filter NG → retry")

    # ここに来た = max_retry 回とも失敗
    print("===== ABEND: Japan フィルタが反映されませんでした =====")
    print(f"category_id : {category_id}")
    print("=========================================================")
    raise RuntimeError("merchantOrigin.tree.Japanese not applied")


# --------------------------------------------------
# カテゴリ単位で検索結果 → 商品IDリスト取得
# （Show more 対応）
# --------------------------------------------------
# ※ どこか上の方で共通セレクタを定義しておくと楽です
# CARD_SELECTOR = "a.content___N4xbX[href*='/en/products/']"

def get_item_ids_from_category(driver, category_id: str):
    print(f"[DEBUG] 検索URL(論理): {JOOM_BASE}/en/search/{category_id}/f.merchantOrigin.tree.Japanese/s.salesCount.desc")

    # ★ ここで「Japan フィルタが効いた状態」になるまでリトライ
    open_search_with_retry(driver, category_id)

    wait = WebDriverWait(driver, 15)

    # 最初のカードが出るまで待つ（出なければそのカテゴリはスキップ）
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, CARD_SELECTOR)
            )
        )
    except TimeoutException:
        print("  → 商品カードが1件も表示されず（品切れ or ネット不調）、このカテゴリはスキップ")
        return []

    same_count_times = 0
    MAX_ROUNDS = 10

    for round_no in range(1, MAX_ROUNDS + 1):
        cards_before = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
        before_count = len(cards_before)
        print(f"[DEBUG] round#{round_no} 前 件数: {before_count}")

        # --- 1) Show more を優先して押す ---
        clicked = False
        try:
            show_more = driver.find_element(
                By.XPATH,
                "//*[self::a or self::button][.//span[normalize-space()='Show more']]"
            )
            if show_more.is_displayed() and show_more.is_enabled():
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});",
                    show_more
                )
                show_more.click()
                clicked = True
                print("[DEBUG] Show more クリック")
        except Exception:
            pass

        # --- 2) 自動ロード用にスクロール ---
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        cards_after = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
        after_count = len(cards_after)
        print(f"[DEBUG] round#{round_no} 後 件数: {after_count}")

        if after_count > before_count:
            same_count_times = 0
        else:
            same_count_times += 1
            print(f"[DEBUG] 件数増えず same_count_times={same_count_times}")
            if same_count_times >= 2:
                print("[DEBUG] 件数が増えないため読み込み終了")
                break

    # 最終的なカードから item_id 抜き出し
    final_cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)

    item_ids: list[str] = []
    for a in final_cards:
        href = a.get_attribute("href") or ""
        if "/en/products/" not in href:
            continue
        part = href.split("/en/products/")[1]
        item_id = part.split("?")[0]
        if item_id:
            item_ids.append(item_id)

    print(f"[DEBUG] 最終 item_id 件数: {len(item_ids)}")
    return item_ids



# --------------------------------------------------
# メイン：カテゴリごとに制御ロジック付きで商品を読み込む
# --------------------------------------------------
def main():
    conn = utils.get_sql_server_connection()
    categories = get_active_categories(conn)

    driver = webdriver.Chrome()

    try:
        for category_id in categories:
            print(f"=== Category: {category_id} ===")

            item_ids = get_item_ids_from_category(driver, category_id)
            if not item_ids:
                print("  → 商品ゼロ、スキップ")
                continue

            max_index = None  # 最後に「当たり」が出た位置 + 10
            HARD_LIMIT = 1000   # 何も当たりが出なかった場合の安全上限

            for idx, item_id in enumerate(item_ids, start=1):
                # 安全上限
                if idx > HARD_LIMIT:
                    print(f"  → HARD_LIMIT {HARD_LIMIT} 件に達したので終了")
                    break

                has_review, has_purchase = scrape_product_detail(
                    driver, conn, item_id, category_id, idx  # idx は page_index 用に渡している前提
                )

                # ★ 追加ロジック：
                # 最初の10件を見ても、レビュー / 購入が 1 件も無ければ
                # そのカテゴリは 10件だけ取得して終了する
                if max_index is None and idx >= 10 and not (has_review or has_purchase):
                    print("  → 最初の10件でレビュー/購入なし → 10件だけ取得して終了")
                    break

                # review か purchases がある商品なら、上限を「その位置 + 10」に延長
                if has_review or has_purchase:
                    new_max = idx + 10
                    if max_index is None or new_max > max_index:
                        max_index = new_max
                        print(f"  → 当たり item#{idx}（延長: {max_index} まで）")

                # 「当たり」が一度も出ていない間は、とりあえず進む
                if max_index is None:
                    continue

                # 最新の max_index を超えたら終了
                if idx >= max_index:
                    print(f"  → {idx} 件処理（最後の当たり +10 まで）、カテゴリ終了")
                    break

            conn.commit()

    finally:
        driver.quit()
        conn.close()


if __name__ == "__main__":
    main()
