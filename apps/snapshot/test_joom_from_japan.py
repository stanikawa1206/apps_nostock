# -*- coding: utf-8 -*-
"""
test_joom_from_japan.py

目的:
- 専用プロファイル(C:\ChromeDev)を使って Chrome を起動
- 指定カテゴリの検索結果ページで「From Japan」バッジを判定
- 先頭から順に見ていき、NOT From Japan が出たらその時点で情報を表示して即終了
"""

import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


JOOM_BASE = "https://www.joom.com"

# テスト対象カテゴリ（例）
TARGET_CATEGORY_ID = "c.1702995086130533533-178-2-14855-2635187613"

# 商品カード共通セレクタ
CARD_SELECTOR = "a.content___N4xbX[href*='/en/products/']"


def create_persona_chrome() -> webdriver.Chrome:
    """
    専用プロファイル(C:\\ChromeDev)を使って Chrome を起動する。
    初回起動時にこのブラウザで Joom にログインすると、
    その後もログイン状態が保持される。
    """
    options = webdriver.ChromeOptions()

    # 専用プロファイル用ディレクトリ（必要ならパスは変えてOK）
    options.add_argument(r"--user-data-dir=C:\ChromeDev")

    # 日本語環境寄せ（お好みで）
    options.add_argument("--lang=ja-JP")

    driver = webdriver.Chrome(options=options)
    return driver


def load_all_cards_with_show_more(driver, category_id: str):
    """
    指定カテゴリの検索結果ページを開き、
    Show more + スクロールで商品カードをできるだけ読み込む。
    戻り値: 最後に取得したカード要素リスト
    """
    url = f"{JOOM_BASE}/en/search/{category_id}/f.merchantOrigin.tree.Japanese/s.salesCount.desc"
    print(f"[INFO] URL: {url}")
    driver.get(url)

    # ここで実際に開かれたURLを確認
    time.sleep(3)  # SPAがURL書き換える時間を少しだけ待つ
    print(f"[INFO] current_url: {driver.current_url}")
    
    wait = WebDriverWait(driver, 15)

    # 最初のカードが表示されるまで待つ
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, CARD_SELECTOR)
            )
        )
    except TimeoutException:
        print("[WARN] 商品カードが1件も表示されませんでした。")
        return []

    same_count_times = 0
    MAX_ROUNDS = 10  # Show more + スクロールの最大試行回数

    for round_no in range(1, MAX_ROUNDS + 1):
        cards_before = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
        before_count = len(cards_before)
        print(f"[DEBUG] round#{round_no} 前 件数: {before_count}")

        clicked = False
        # Show more を優先して押す
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
            # ボタンが無ければ無視
            pass

        # ロード待ち＋自動ロード用のスクロール
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

    final_cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
    print(f"[INFO] 最終カード件数: {len(final_cards)}")
    return final_cards


def is_from_japan(card_el) -> bool:
    """
    検索結果の1カードについて、「From Japan」バッジがあるかどうかを判定する。

    判定方法:
    - カード内に japan_circle_v2 フラグ画像がある
      または
    - カード内のテキストに 'From Japan' を含む要素がある
    """

    # 1) 画像フラグで判定
    try:
        imgs = card_el.find_elements(
            By.CSS_SELECTOR,
            "img[src*='japan_circle_v2']"
        )
        if imgs:
            return True
    except Exception:
        pass

    # 2) テキスト 'From Japan' で判定
    try:
        elems = card_el.find_elements(
            By.XPATH,
            ".//*[contains(normalize-space(),'From Japan')]"
        )
        if elems:
            return True
    except Exception:
        pass

    return False


def check_from_japan_and_stop_on_first_non_japan(driver, category_id: str):
    """
    指定カテゴリの検索結果ページで、
    先頭から順にカードをチェックし：

      - From Japan → 情報を出して継続
      - NOT From Japan → 情報を出してその場で即終了
    """
    cards = load_all_cards_with_show_more(driver, category_id)
    if not cards:
        print("[WARN] カードが0件のため終了します。")
        return

    print("\n=== From Japan 判定開始 ===")
    for idx, card in enumerate(cards, start=1):
        href = card.get_attribute("href") or ""
        item_id = ""
        if "/en/products/" in href:
            part = href.split("/en/products/")[1]
            item_id = part.split("?")[0]

        if is_from_japan(card):
            print(f"[{idx:3d}] item_id={item_id} -> From Japan（継続）")
            continue

        # ここに来た = From Japan ではない
        print("\n===== NOT From Japan 検出 → 即終了 =====")
        print(f"index   : {idx}")
        print(f"item_id : {item_id}")
        print("status  : NOT From Japan")
        print("========================================")
        return  # ここで関数終了 → main も終了


def main():
    driver = create_persona_chrome()

    try:
        check_from_japan_and_stop_on_first_non_japan(driver, TARGET_CATEGORY_ID)
    finally:
        # このプロファイルはまた使う前提なので、ブラウザはあえて閉じない
        # 必要ならコメントアウトを外して閉じてもOK
        # driver.quit()
        pass


if __name__ == "__main__":
    main()
