import subprocess
import time
import urllib.error
import urllib.request

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PROFILE_DIR = r"D:\apps_nostock\selenium_profile"
DEBUG_PORT = 9222
LAUNCH_TIMEOUT_SEC = 30

TARGET_URL = "https://jp.mercari.com/mypage/purchases"

CURRENT_URL_RETRY_COUNT = 5
CURRENT_URL_RETRY_INTERVAL_SEC = 1


def _debugger_alive(port: int) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


def ensure_chrome_debugger(port: int = DEBUG_PORT, profile_dir: str = PROFILE_DIR,
                            timeout: int = LAUNCH_TIMEOUT_SEC) -> None:
    """デバッグポートで応答するChromeがなければ起動し、応答するまで待つ"""
    if _debugger_alive(port):
        print(f"OK: 起動済みのChrome(ポート{port})を利用します")
        return

    print(f"Chromeをリモートデバッグモードで起動します（ポート{port}）...")
    subprocess.Popen([
        CHROME_EXE,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
    ])

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _debugger_alive(port):
            print("OK: Chrome起動完了")
            return
        time.sleep(0.5)

    raise RuntimeError(f"Chromeの起動確認がタイムアウトしました（{timeout}秒）")


def _get_current_url_with_retry(driver, retries: int = CURRENT_URL_RETRY_COUNT,
                                 interval: float = CURRENT_URL_RETRY_INTERVAL_SEC) -> str:
    """ナビゲーション直後の一時的な execution context 喪失に備えてリトライする"""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return driver.current_url
        except TimeoutException as e:
            last_error = e
            print(f"  ({attempt}/{retries}) current_url取得失敗、リトライします: {e.msg}")
            time.sleep(interval)
    raise last_error


def _close_stale_tabs(driver, keep_handle):
    """keep_handle以外で、前回実行の結果タブ（TARGET_URL）や壊れたタブを閉じる"""
    for handle in driver.window_handles:
        if handle == keep_handle:
            continue
        try:
            driver.switch_to.window(handle)
            is_stale = driver.current_url.startswith(TARGET_URL)
        except Exception:
            # current_urlの取得自体に失敗するタブは使い物にならないので閉じる対象とみなす
            is_stale = True
        if is_stale:
            try:
                driver.close()
            except Exception:
                pass
    driver.switch_to.window(keep_handle)


def main():
    ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{DEBUG_PORT}"

    driver = webdriver.Chrome(options=options)

    # 既存タブを使い回すと、ログインのクロスオリジンリダイレクト
    # （mercari.com → login.mercari.com → OAuthコールバック）でレンダラープロセスが
    # 切り替わり、ChromeDriverのexecution contextが永久に迷子になることがあるため、
    # チェック専用の新規タブを開いて直接遷移する。
    # 全タブを閉じきってしまうとChromeごと終了するので、新規タブを先に作ってから
    # 前回実行の残骸タブを閉じる順序にしている。
    driver.switch_to.new_window('tab')
    check_tab = driver.current_window_handle
    _close_stale_tabs(driver, keep_handle=check_tab)

    driver.get(TARGET_URL)
    time.sleep(5)

    current_url = _get_current_url_with_retry(driver)
    page_title = driver.title
    print(f"URL: {current_url}")
    print(f"Title: {page_title}")

    if "login" in current_url or "sign_in" in current_url or "signin" in current_url:
        print("NG: ログインされていない可能性あり")
        return

    try:
        checkbox = driver.find_element(
            By.CSS_SELECTOR,
            '[data-testid="user-listing-inTransactionItemsCheckbox"]'
        )
        if not checkbox.is_selected():
            checkbox.click()
            time.sleep(3)
            print("OK: 「取引中の商品」にチェックを入れました")
        else:
            print("OK: 「取引中の商品」は既にチェック済みです")
    except Exception as e:
        print(f"WARN: 「取引中の商品」チェックボックスが見つかりません: {e}")

    body_text = driver.find_element("tag name", "body").text
    keywords = ["購入した商品", "購入履歴", "購入した", "mypage/purchases"]
    found = any(kw in body_text for kw in keywords)

    if found:
        print("OK: 購入履歴ページ表示確認成功")
    else:
        print("NG: 購入履歴ページを確認できない")
        print("--- body 先頭300文字 ---")
        print(body_text[:300])
        return

    links = driver.find_elements(By.TAG_NAME, "a")

    seen = set()
    transaction_urls = []

    for link in links:
        href = link.get_attribute("href")
        if href and "/transaction/" in href and href not in seen:
            seen.add(href)
            transaction_urls.append(href)

    print(f"取引URL数: {len(transaction_urls)}")
    for url in transaction_urls:
        print(url)


if __name__ == "__main__":
    main()
