"""
FurimaWatch タイムラインスクレイパー

処理フロー:
  1. ログイン済み Firefox プロファイルをコピーして起動
  2. ログイン状態を確認（未ログインなら Google ログインフローへ）
  3. タイムライン画面へ遷移
  4. 「次のページを読み込む」を繰り返して全件取得
  5. 各行をパース → Angular スコープから product_url を一括取得（クリック不要）
  6. DataFrame 化 → CSV 保存
  7. HTML・スクリーンショット保存

出力 CSV 列:
  no, title, price_yen, posted_at, source, alert, product_url

---- FurimaWatch の DOM 構造（確認済み）----
table > tbody:
  tr[0]         : ヘッダー行（<th> のみ、<td> なし）
  tr[ng-repeat] : データ行 × N
    td[0] : No.（FurimaWatch の seq 番号）
    td[1] : 画像（<img> のみ、text=空）
    td[2] : 商品情報
              <p> 価格   "1,120 円"
              <p> タイトル
              <p> 投稿日時 + 出品元（サービスアイコン + テキスト）
    td[3] : アラート名 + 「編集」ボタン
    td[4] : 「商品ページ」「公式商品ページ」「削除」ボタン（ng-click, href なし）
            product_url は Angular スコープ (timelineRow.item.itemUrl) から直接取得する
ページネーション:
  <button ng-click="loadNextPage()">次のページを読み込む</button>
"""
import csv
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)


# ===== 固定設定（環境に合わせてここだけ直す） =====
FIREFOX_BINARY      = r"C:\Program Files\Mozilla Firefox\firefox.exe"
GECKODRIVER_EXE     = r"D:\PGM\geckodriver.exe"
MASTER_PROFILE_NAME = "furimawatch_firefox"   # ログイン済み"金庫"プロファイル名
# ===============================================

# ===== デバッグ設定 =====
# 0 = 無制限（全件取得）
# 30 = デバッグ用（30件で打ち切り）
DEBUG_MAX_ROWS = 30
# =======================

BASE_URL     = "https://www.furimawatch.net"
LOGIN_URL    = f"{BASE_URL}/pc_latest/#!/login"
TIMELINE_URL = f"{BASE_URL}/pc_latest/#!/timeline"

TIMEOUT         = 20
LOAD_MORE_PAUSE = 2   # 「次のページを読み込む」クリック後の待機秒数

LOG_DIR         = Path(r"D:\apps_nostock\logs")
GECKODRIVER_LOG = LOG_DIR / "geckodriver.log"
HTML_OUTPUT     = LOG_DIR / "furimawatch_timeline.html"
PNG_OUTPUT      = LOG_DIR / "furimawatch_timeline.png"

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_OUTPUT = LOG_DIR / f"furimawatch_timeline_{ts}.csv"

COLUMNS = ["no", "title", "price_yen", "posted_at", "source", "alert", "product_url"]


# ---------------------------------------------------------------------------
# プロファイル管理
# ---------------------------------------------------------------------------

def get_firefox_profiles_ini_path() -> Path:
    return Path.home() / "AppData" / "Roaming" / "Mozilla" / "Firefox" / "profiles.ini"


def find_master_profile_dir(profile_name: str) -> Path:
    ini = get_firefox_profiles_ini_path()
    if not ini.exists():
        raise FileNotFoundError(f"profiles.ini が見つかりません: {ini}")

    text = ini.read_text(encoding="utf-8", errors="strict")
    blocks = text.split("\n\n")
    matches = []
    for b in blocks:
        lines = [ln.strip() for ln in b.splitlines() if ln.strip()]
        if not any(ln.startswith("Name=") for ln in lines):
            continue
        name   = next((ln.split("=", 1)[1] for ln in lines if ln.startswith("Name=")),       "")
        path   = next((ln.split("=", 1)[1] for ln in lines if ln.startswith("Path=")),       "")
        is_rel = next((ln.split("=", 1)[1] for ln in lines if ln.startswith("IsRelative=")), "1")
        if name == profile_name:
            matches.append((path, is_rel))

    if len(matches) == 0:
        raise RuntimeError(f"Firefoxプロファイル '{profile_name}' が profiles.ini に見つかりません")
    if len(matches) >= 2:
        raise RuntimeError(f"Firefoxプロファイル '{profile_name}' が複数見つかりました")

    path, is_rel = matches[0]
    base = Path.home() / "AppData" / "Roaming" / "Mozilla" / "Firefox"
    prof_dir = (base / path) if is_rel == "1" else Path(path)

    if not prof_dir.exists():
        raise FileNotFoundError(f"プロファイル実体が存在しません: {prof_dir}")

    return prof_dir


def copy_profile_dir(master_dir: Path) -> Path:
    temp_root = Path.home() / "AppData" / "Local" / "FurimaBotTempFirefoxProfiles"
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_dir = temp_root / f"temp_{uuid.uuid4().hex[:8]}"
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    shutil.copytree(master_dir, temp_dir)
    return temp_dir


# ---------------------------------------------------------------------------
# ドライバー生成（geckodriver ログ付き）
# ---------------------------------------------------------------------------

def build_driver_with_profile_dir(profile_dir: Path) -> webdriver.Firefox:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    options = webdriver.FirefoxOptions()
    options.binary_location = FIREFOX_BINARY
    options.add_argument("-profile")
    options.add_argument(str(profile_dir))
    service = FirefoxService(
        executable_path=GECKODRIVER_EXE,
        log_output=GECKODRIVER_LOG.open("w", encoding="utf-8"),
    )
    return webdriver.Firefox(service=service, options=options)


# ---------------------------------------------------------------------------
# ログインフロー
# ---------------------------------------------------------------------------

def step1_to_step3(driver: webdriver.Firefox) -> None:
    wait = WebDriverWait(driver, TIMEOUT)

    driver.get(LOGIN_URL)
    time.sleep(2)

    # 第1段階：ログインボタン（ログイン済みなら存在しない → スキップ）
    try:
        login_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space(.)='ログイン']"))
        )
    except TimeoutException:
        print("✓ 第1段階：ログインボタンなし → ログイン済みと判断、続行")
        print("  現在URL:", driver.current_url)
        return

    login_btn.click()
    print("✓ 第1段階：ログイン をクリック")
    time.sleep(2)

    # 第2段階：利用規約同意（出ていれば押す）
    terms_btns = driver.find_elements(By.XPATH, "//button[contains(., '利用規約に同意する')]")
    if terms_btns:
        terms_btns[0].click()
        print("✓ 第2段階：利用規約に同意する をクリック")
        time.sleep(2)
    else:
        print("✓ 第2段階：利用規約同意ボタンは非表示（スキップ）")

    # 第3段階：Google ログイン
    google_img = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#btnGoogleSignin"))
    )
    try:
        google_img.click()
    except Exception:
        parent = driver.execute_script("return arguments[0].parentElement;", google_img)
        driver.execute_script("arguments[0].click();", parent)

    print("✓ 第3段階：Googleでログイン をクリック")
    print("  ※ Google 認証は手動で完了してください")
    input("  認証完了後、Enter を押してください...")


# ---------------------------------------------------------------------------
# タイムライン遷移・ログイン状態確認
# ---------------------------------------------------------------------------

def navigate_to_timeline(driver: webdriver.Firefox) -> None:
    print(f"[INFO] タイムライン画面へ遷移: {TIMELINE_URL}")
    driver.get(TIMELINE_URL)
    time.sleep(3)
    print(f"[INFO] 現在URL: {driver.current_url}")


def assert_logged_in(driver: webdriver.Firefox) -> None:
    if "login" in driver.current_url.lower():
        raise RuntimeError(
            f"ログイン画面へリダイレクトされました。\n現在URL: {driver.current_url}"
        )
    print("[OK] ログイン確認")


# ---------------------------------------------------------------------------
# ページネーション（「次のページを読み込む」）
# ---------------------------------------------------------------------------

def _find_load_more_button(driver: webdriver.Firefox):
    """「次のページを読み込む」等のボタンを探して返す。なければ None。"""
    xpaths = [
        "//button[contains(normalize-space(.), '次のページを読み込む')]",
        "//button[contains(normalize-space(.), 'もっと見る')]",
        "//button[contains(normalize-space(.), '次へ')]",
        "//a[contains(normalize-space(.), '次のページを読み込む')]",
    ]
    for xpath in xpaths:
        elems = driver.find_elements(By.XPATH, xpath)
        visible = [e for e in elems if e.is_displayed()]
        if visible:
            return visible[0]
    return None


def load_all_rows(driver: webdriver.Firefox) -> None:
    """
    「次のページを読み込む」がなくなるまで、または DEBUG_MAX_ROWS に達するまで
    クリックし続けて行を展開する。

    DEBUG_MAX_ROWS = 0 なら無制限。
    """
    page = 1
    while True:
        # DEBUG_MAX_ROWS に達したら打ち切り
        if DEBUG_MAX_ROWS > 0:
            current_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr[ng-repeat]")
            if len(current_rows) >= DEBUG_MAX_ROWS:
                print(f"[INFO] DEBUG_MAX_ROWS ({DEBUG_MAX_ROWS}) 件に達したためページ読み込みを終了")
                break

        btn = _find_load_more_button(driver)
        if btn is None:
            print(f"[INFO] 「次のページを読み込む」ボタンなし → 全件取得完了（{page - 1} 回クリック）")
            break

        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.3)
            btn.click()
            print(f"[INFO] 「次のページを読み込む」クリック #{page}")
            page += 1
            time.sleep(LOAD_MORE_PAUSE)
        except StaleElementReferenceException:
            time.sleep(1)
            continue


# ---------------------------------------------------------------------------
# product_url 取得（Angular スコープから一括取得・クリック不要）
# ---------------------------------------------------------------------------

def _extract_item_urls_from_scope(driver: webdriver.Firefox) -> list[str]:
    """
    AngularJS スコープから全 ng-repeat 行の itemUrl を順番通りに一括取得する。

    timeline.js の jumpOfficialButtonClicked は item.itemUrl を window.open() するだけ。
    itemUrl はコントローラの reloadList() で以下のように構築済み:
      mercari → https://jp.mercari.com/item/{itemid}
      fril    → row.item.link (直接 URL)
      otamart → https://otamart.com/items/{itemid}
      yauc    → https://page7.auctions.yahoo.co.jp/jp/auction/{itemid}
      paypay  → https://paypayfleamarket.yahoo.co.jp/item/{itemid}

    クリック・新タブ不要。30 件でも一瞬で取得できる。
    """
    js = """
    var rows = document.querySelectorAll('tr[ng-repeat]');
    var urls = [];
    for (var i = 0; i < rows.length; i++) {
        try {
            var scope = angular.element(rows[i]).scope();
            urls.push(
                (scope && scope.timelineRow && scope.timelineRow.item)
                    ? (scope.timelineRow.item.itemUrl || "")
                    : ""
            );
        } catch(e) {
            urls.push("");
        }
    }
    return urls;
    """
    try:
        result = driver.execute_script(js)
        return result if isinstance(result, list) else []
    except Exception as e:
        print(f"[WARN] Angular スコープからの itemUrl 取得失敗: {e}")
        return []


# ---------------------------------------------------------------------------
# 行パース
# ---------------------------------------------------------------------------

def parse_rows(driver: webdriver.Firefox) -> list[dict]:
    """
    タイムラインテーブルの ng-repeat 行をパースして辞書リストを返す。

    DOM 構造（確認済み）:
      td[0]: No.（seq 番号）     → .text
      td[1]: 画像                → <img> のみ（text=空）
      td[2]: 商品情報             → <p>[0]=価格, <p>[1]=タイトル, <p>[2]=日時+出品元
      td[3]: アラート名+編集ボタン → .text の1行目がアラート名
      td[4]: 操作ボタン           → ng-click のみ（href なし）
    product_url:
      Angular スコープ (timelineRow.item.itemUrl) から一括取得済みの配列を参照。
      クリック・新タブ操作は一切不要。
    """
    # ヘッダー行（th のみ）は ng-repeat を持たないので除外
    tr_elems = driver.find_elements(By.CSS_SELECTOR, "table tbody tr[ng-repeat]")

    if not tr_elems:
        print("[WARN] ng-repeat 行が見つかりません。HTML を確認してください。")
        return []

    print(f"[INFO] {len(tr_elems)} 行を検出")

    # Angular スコープから itemUrl を一括取得（クリック不要・高速）
    item_urls = _extract_item_urls_from_scope(driver)
    print(f"[INFO] Angular スコープから itemUrl {len(item_urls)} 件取得")

    # DEBUG_MAX_ROWS で件数を制限
    if DEBUG_MAX_ROWS > 0:
        tr_elems  = tr_elems[:DEBUG_MAX_ROWS]
        item_urls = item_urls[:DEBUG_MAX_ROWS]
        print(f"[INFO] DEBUG_MAX_ROWS={DEBUG_MAX_ROWS} で {len(tr_elems)} 件に絞り込み")

    rows_data: list[dict] = []

    for i, (tr, product_url) in enumerate(zip(tr_elems, item_urls), start=1):
        try:
            cells = tr.find_elements(By.TAG_NAME, "td")

            print(f"  行{i:3d}: td数={len(cells)}", end="")

            if len(cells) < 5:
                print(" → td不足でスキップ")
                continue

            # ---- td[2]: 商品情報 ----
            p_tags = cells[2].find_elements(By.TAG_NAME, "p")

            price_raw = p_tags[0].text.strip() if len(p_tags) > 0 else ""
            title     = p_tags[1].text.strip() if len(p_tags) > 1 else ""
            dt_svc    = p_tags[2].text.strip() if len(p_tags) > 2 else ""

            # dt_svc 例: "2026-06-09 11:26:39\n                paypay"
            lines     = [ln.strip() for ln in dt_svc.splitlines() if ln.strip()]
            posted_at = lines[0] if lines else ""
            source    = lines[-1] if len(lines) > 1 else ""

            # 価格を数値文字列に正規化: "1,120 円" → "1120"
            price_yen = price_raw.replace("円", "").replace(",", "").replace(" ", "").strip()

            # ---- td[3]: アラート名（「編集」ボタン行は除く）----
            alert = cells[3].text.strip().split("\n")[0].strip()

            print(f" | {title[:25]!s:<25} | {price_yen:>6}円 | {source} | {product_url[:40]}")

            rows_data.append({
                "no":          i,
                "title":       title,
                "price_yen":   price_yen,
                "posted_at":   posted_at,
                "source":      source,
                "alert":       alert,
                "product_url": product_url,
            })

        except StaleElementReferenceException:
            print(" → StaleElement（DOM 再構築）スキップ")
        except Exception as e:
            print(f" → パース失敗: {e}")

    return rows_data


# ---------------------------------------------------------------------------
# スナップショット保存
# ---------------------------------------------------------------------------

def save_snapshot(driver: webdriver.Firefox) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    HTML_OUTPUT.write_text(driver.page_source, encoding="utf-8")
    print(f"[OK] HTML 保存: {HTML_OUTPUT}")
    driver.save_screenshot(str(PNG_OUTPUT))
    print(f"[OK] スクリーンショット: {PNG_OUTPUT}")


# ---------------------------------------------------------------------------
# CSV 出力
# ---------------------------------------------------------------------------

def save_csv(rows: list[dict]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.to_csv(CSV_OUTPUT, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
    print(f"[OK] CSV 保存: {CSV_OUTPUT}  ({len(df)} 件)")
    print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def scrape_timeline_to_csv(driver: webdriver.Firefox) -> None:
    navigate_to_timeline(driver)
    assert_logged_in(driver)
    load_all_rows(driver)
    save_snapshot(driver)

    print("[INFO] 行をパース中...")
    rows = parse_rows(driver)
    print(f"[INFO] パース完了: {len(rows)} 件")

    if not rows:
        print("[WARN] データが 0 件です。")
        print(f"       HTML を確認: {HTML_OUTPUT}")
        return

    save_csv(rows)


def main() -> None:
    master_dir = find_master_profile_dir(MASTER_PROFILE_NAME)
    temp_dir   = copy_profile_dir(master_dir)
    print(f"[INFO] プロファイルコピー: {master_dir.name} → {temp_dir}")
    print(f"[INFO] DEBUG_MAX_ROWS = {DEBUG_MAX_ROWS} ({'無制限' if DEBUG_MAX_ROWS == 0 else f'{DEBUG_MAX_ROWS}件で打ち切り'})")

    driver = None
    try:
        driver = build_driver_with_profile_dir(temp_dir)
        print("[INFO] Firefox 起動完了")

        step1_to_step3(driver)
        scrape_timeline_to_csv(driver)

    finally:
        if driver:
            driver.quit()
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"[INFO] 一時プロファイル削除: {temp_dir}")
        print(f"[INFO] geckodriver ログ: {GECKODRIVER_LOG}")


if __name__ == "__main__":
    main()
