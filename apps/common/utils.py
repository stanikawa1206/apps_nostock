# -*- coding: utf-8 -*-
from __future__ import annotations

"""
共通ユーティリティ集（Keepa以外）

重要な設計方針:
- import 時に外部API(OpenAI)の初期化で落ちないようにする
  → OpenAI は get_openai_client() 呼び出し時にのみ初期化（遅延初期化）
"""

import os
import re
import time
import unicodedata
import urllib.parse
import smtplib
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple, Dict, List

import requests
import pyodbc
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


# ------------------------------------------------------------
# .env 読み込み（ここは import 時にやってOK）
# ------------------------------------------------------------
load_dotenv()


# =========================
# 価格計算（GA用の想定値）
# =========================
USD_JPY_RATE = 155.0   # 為替レート
PROFIT_RATE = 0.10     # 利益率
EBAY_FEE_RATE = 0.17   # eBay手数料
DOMESTIC_SHIPPING_JPY = 470   # 国内GAセンターまで送料
INTL_SHIPPING_JPY = 3300      # 国際送料
DUTY_RATE = 0.15              # 関税


def compute_start_price_usd(
    cost_jpy: int,
    mode: str,
    low_usd_target: float,
    high_usd_target: float
) -> Optional[str]:
    """
    仕入れ円から開始価格USDを逆算。
    GA:  関税なし
    DDP: 関税 = 売価の DUTY_RATE %
    """
    cost = Decimal(cost_jpy)
    mode_up = mode.upper()

    if mode_up == "GA":
        ship = Decimal(DOMESTIC_SHIPPING_JPY)
        duty = Decimal("0")
    elif mode_up == "DDP":
        ship = Decimal(INTL_SHIPPING_JPY)
        duty = Decimal(str(DUTY_RATE))
    else:
        raise ValueError(f"未知のmodeです: {mode}")

    rate = Decimal(str(USD_JPY_RATE))
    p = Decimal(str(PROFIT_RATE))
    f = Decimal(str(EBAY_FEE_RATE))

    denom = Decimal("1") - p - f - duty
    if denom <= 0:
        raise ValueError("利益率＋手数料率＋関税率の合計が1.0以上です。")

    base = cost + ship
    jpy_total = base / denom
    usd = (jpy_total / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    low = Decimal(str(low_usd_target))
    high = Decimal(str(high_usd_target))
    if usd < low or usd > high:
        return None

    if mode_up == "GA":
        if Decimal("450.00") <= usd <= Decimal("525.00"):
            usd = Decimal("525.00")

    return f"{usd:.2f}"


# ============================================================
# OpenAI（遅延初期化）
# - import 時に OpenAI を初期化しない（DB接続だけ欲しい処理で落ちない）
# ============================================================
_openai_client = None

def get_openai_client():
    """
    OpenAI を使う関数だけが呼ぶ。
    import 時点では例外を出さない。
    """
    global _openai_client
    if _openai_client is not None:
        return _openai_client

    # ここで初めて import（環境差があってもDB用途の import を壊さない）
    from openai import OpenAI  # openai が無いなら、翻訳を呼んだ瞬間に分かりやすく落ちる

    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY が未設定です（.env を確認）")

    _openai_client = OpenAI(api_key=key)
    return _openai_client


# 危険ワード
_RISKY_KEYWORDS = [
    "クロコダイル", "ワニ", "カイマン", "かいまん",
    "パイソン", "ヘビ", "蛇", "リザード", "トカゲ",
    "オーストリッチ", "ダチョウ", "ミンク", "フォックス", "セーブル",
    "crocodile", "alligator", "caiman",
    "python", "lizard", "ostrich", "mink", "fox fur", "sable",
]

def _norm_for_match(s: str) -> str:
    """
    リスキー判定用の正規化:
    - NFKC（全角英数などの揺れを潰す）
    - lower
    - 記号はスペースに寄せて連結検知を安定させる
    """
    s = unicodedata.normalize("NFKC", s or "")
    s = s.lower()
    s = re.sub(r"[\u3000\s]+", " ", s)          # 全角/半角スペース整理
    s = re.sub(r"[^0-9a-z\u3040-\u30ff\u4e00-\u9fff ]+", " ", s)  # 記号を空白化
    s = re.sub(r"\s+", " ", s).strip()
    return s

def contains_risky_word(*texts: str) -> bool:
    """
    texts の中に危険ワード（エキゾチック素材など）が含まれていれば True
    """
    text = " ".join(t for t in texts if t)
    norm_text = _norm_for_match(text)

    for kw in _RISKY_KEYWORDS:
        if _norm_for_match(kw) in norm_text:
            return True

    return False



# ============================================================
#  eBay タイトル翻訳 — Premium → gpt-4o、それ以外 → gpt-4o-mini
# ============================================================
_PREMIUM_BRANDS_EN = {"HERMES", "LOUIS VUITTON", "CHANEL"}


def is_premium_brand(jp_title: str, description_jp: str | None, fixed_brand_en: str | None = None) -> bool:
    if fixed_brand_en:
        return fixed_brand_en in _PREMIUM_BRANDS_EN
    return False

def apply_hermes_stole_rules(title: str, jp_title: str, desc: str) -> str:
    jp_all = (jp_title or "") + (desc or "")

    is_hermes = "HERMES" in title.upper()
    is_carre = any(k in jp_all for k in ["カレ90", "カレ140", "カレ45"])

    stole_words = ("ストール", "ショール", "スカーフ", "マフラー")
    has_stole_word = any(w in jp_all for w in stole_words)
    has_size_cross = bool(re.search(r"\d+\s*[×x]\s*\d+", jp_all))

    is_stole = has_stole_word or has_size_cross

    if not (is_hermes and is_stole and not is_carre):
        return title

    remove_words = [
        r"\bFringe\b",
        r"\bElephant\b",
        r"\bHorse\b",
        r"\bAnimal\b",
        r"\bMade in France\b",
        r"\bFrance\b",
    ]

    t = title
    for pat in remove_words:
        t = re.sub(pat, "", t, flags=re.IGNORECASE)

    t = re.sub(r"\s{2,}", " ", t).strip(" -|/")
    return t

def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def translate_to_english(
    jp_title: str,
    description_jp: str | None = None,
    expected_brand_en: str | None = None,
) -> str:
    jp_title = (jp_title or "").strip()
    if not jp_title:
        return ""

    description_jp = description_jp or ""
    desc_block = description_jp if description_jp.strip() else "(no description)"

    # モデル選択
    if is_premium_brand(jp_title, description_jp, fixed_brand_en=expected_brand_en) or contains_risky_word(jp_title, description_jp):
        use_model = "gpt-4o"
    else:
        use_model = "gpt-4o-mini"

    brand_rule = ""
    if expected_brand_en:
        brand_rule = f"""
### BRAND RULE
- The brand must be "{expected_brand_en}".
- Do NOT output any other brand names.
""".strip()

    prompt = f"""
You are an expert eBay SEO title writer.

Create a concise, highly optimized eBay title (max 80 characters)
using BOTH the Japanese title and the product description.

{brand_rule}

### IMPORTANT FILTER RULE
Ignore any content related to:
- Returns / Refunds
- Shipping / Packaging
- Cleaning
- Notes / Disclaimers
These must NOT influence the generated title.

### TITLE STRUCTURE (in this order if possible)
BRAND + Line/Model + Size + Material + Color + Key Motif + Condition + Accessories

### OUTPUT RULES
- Output only the final English title (no quotes).
- Never output nonsense words (e.g., "Kale90").

Japanese title:
{jp_title}

Japanese description:
{desc_block}
""".strip()

    try:
        client = get_openai_client()
        resp = client.responses.create(model=use_model, input=prompt)
        title_en = _norm_spaces(resp.output_text or "")
        if not title_en:
            return ""

        title_en = apply_hermes_stole_rules(title_en, jp_title, desc_block)

        if len(title_en) > 80:
            cut = title_en[:80]
            if " " in cut:
                cut = cut[:cut.rfind(" ")]
            title_en = cut.strip()

        return title_en
    except Exception as e:
        print(f"[WARN] translate_to_english failed: {e}")
        return ""

# =========================
# 真贋・責任回避 表現の削除
# =========================

_AUTHENTICITY_CUT_PATTERNS = [
    r"cannot guarantee authenticity",
    r"authenticity cannot be guaranteed",
    r"authenticity is not guaranteed",
    r"authenticity not guaranteed",
    r"not purchased from an official store",
    r"purchase store is unknown",
    r"no receipt",
    r"please judge authenticity",
    r"i believe (this|it) is authentic",
    r"i am not sure if (this|it) is authentic",
]

def strip_authenticity_doubt(text: str) -> str:
    t = text or ""
    for pat in _AUTHENTICITY_CUT_PATTERNS:
        t = re.sub(pat, "", t, flags=re.IGNORECASE)
    # 空行整理
    t = re.sub(r"\n{2,}", "\n\n", t).strip()
    return t


# =========================
# 返品不可・防御文言の削除
# =========================

_NO_RETURN_CUT_PATTERNS = [
    r"no returns",
    r"returns are not accepted",
    r"return is not accepted",
    r"to prevent exchange",
    r"to avoid replacement",
]

def strip_no_return_policy(text: str) -> str:
    t = text or ""
    for pat in _NO_RETURN_CUT_PATTERNS:
        t = re.sub(pat, "", t, flags=re.IGNORECASE)
    t = re.sub(r"\n{2,}", "\n\n", t).strip()
    return t


def generate_ebay_description(
    title_en: str,
    description_jp: str,
    expected_brand_en: str | None = None,  # ★ DBの default_brand_en を渡す
) -> str:
    """
    eBay用 英語説明文生成
    - ブランドは推定しない（expected_brand_en を使う）
    - expected_brand_en がある場合は、そのブランドとして説明するよう指示する
    """
    title_en = (title_en or "").strip()
    description_jp = (description_jp or "").strip()

    description_jp = strip_authenticity_doubt(description_jp)
    description_jp = strip_no_return_policy(description_jp)

    if not description_jp:
        # 既存仕様に合わせた最小フォールバック（余計な保険は入れない）
        fb = (
            f"{title_en}\n\n"
            "Please contact us via eBay messages for details.\n"
            "Ships from Japan with tracking."
        )
        return fb.replace("\n", "<br>")

    brand_rule = ""
    if expected_brand_en:
        brand_rule = f"""
Brand rule:
- The brand is "{expected_brand_en}". Do not describe it as any other brand.
""".strip()

    system_prompt = f"""
You are a professional eBay listing writer.

Rewrite the Japanese product description into clear English.
- Use short sentences.
- Do not invent facts.
- If details are missing, say so briefly.

{brand_rule}
""".strip()

    user_content = f"""
Title:
{title_en}

Japanese description:
{description_jp}
""".strip()

    try:
        client = get_openai_client()
        response = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0.3,
        )
        desc = (response.output_text or "").replace("\r\n", "\n").strip()
        desc = re.sub(r"\n{3,}", "\n\n", desc).strip()
        return desc.replace("\n", "<br>")

    except Exception as e:
        print(f"[WARN] generate_ebay_description failed: {e}")
        fb = (
            f"{title_en}\n\n"
            "Please contact us via eBay messages for details.\n"
            "Ships from Japan with tracking."
        )
        return fb.replace("\n", "<br>")



# =========================
# DB接続（SQL Server）
# =========================
def get_sql_server_connection():
    driver = os.getenv("DB_DRIVER")
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

def build_driver(
    *,
    headless: bool = True,
    page_load_strategy: str = "eager",
):
    """共通 Selenium ChromeDriver"""
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--disable-notifications")
    opts.add_argument("--lang=ja-JP,ja")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119 Safari/537.36"
    )
    opts.page_load_strategy = page_load_strategy

    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_window_size(1400, 1000)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver


# SQLAlchemy Engine（文字列生成だけなので import 時点で落ちにくい）
def _build_odbc_params() -> str:
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
    server = os.getenv("SQLSERVER_SERVER", "192.168.100.105,1433")
    database = os.getenv("SQLSERVER_DATABASE", "nostock")
    uid = os.getenv("SQLSERVER_UID", "")
    pwd = os.getenv("SQLSERVER_PWD", "")

    s = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
    )
    return urllib.parse.quote_plus(s)

# engine は遅延生成（import時にDB/driver周りで落ちるのを防ぐ）
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(f"mssql+pyodbc:///?odbc_connect={_build_odbc_params()}")
    return _engine



DEEPL_ENDPOINT = "https://api-free.deepl.com/v2/translate"

def translate_ja_to_en_deepl(
    text: str,
    *,
    api_key: Optional[str] = None,
) -> str:
    """
    DeepL API (Free) を使って 日本語 → 英語 に翻訳する
    - text は空でない str 前提
    - 失敗時は例外をそのまま投げる（握りつぶさない）
    """

    if not text or not text.strip():
        return ""

    key = api_key or os.getenv("DEEPL_API_KEY_JOOM")
    if not key:
        raise RuntimeError("DEEPL_API_KEY_JOOM is not set")

    resp = requests.post(
        DEEPL_ENDPOINT,
        data={
            "auth_key": key,
            "text": text,
            "source_lang": "JA",
            "target_lang": "EN-US",
        },
        timeout=20,
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"DeepL API error: {resp.status_code} {resp.text}"
        )

    data = resp.json()
    translations = data.get("translations")
    if not translations:
        raise RuntimeError(f"DeepL response invalid: {data}")

    return translations[0]["text"].strip()

# =========================
# メール送信（Gmail）
# =========================
def send_mail(
    subject: str,
    body: str,
    sender_email: str | None = None,
    receiver_email: str | None = None,
    password: str | None = None,
    attachments: list[str] | None = None,
):
    """
    シンプルなテキストメール送信（添付対応）。
    - attachments: 添付したいファイルパスのリスト。未指定(None)なら本文のみ。
    """

    sender_email = sender_email or os.getenv("GMAIL_SENDER_EMAIL")
    receiver_email = receiver_email or sender_email
    password = password or os.getenv("GMAIL_APP_PASSWORD")

    if not sender_email or not password:
        raise RuntimeError("GMAIL_SENDER_EMAIL / GMAIL_APP_PASSWORD が未設定です")

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    # 添付（あれば）
    if attachments:
        for path in attachments:
            if not path:
                continue
            if not os.path.isfile(path):
                print(f"[WARN] 添付ファイルが見つかりません: {path}", flush=True)
                continue

            filename = os.path.basename(path)

            with open(path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())

            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{filename}"'
            )
            msg.attach(part)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        print("📧 完了通知メールを送信しました", flush=True)
    except Exception as e:
        print(f"❌ メール送信失敗: {e}", flush=True)




# =========================
# SKU → 元サイト在庫判定（ラクマ/ヤフオク）
# =========================
def check_status_from_sku(sku: str) -> Tuple[str, Optional[str]]:
    """
    戻り値: (status, category_path or None)
      status: "販売中" / "売り切れ" / "削除" / "未処理"
    """
    if sku.startswith("rkm_"):
        url = "https://item.fril.jp/" + sku[4:]
    elif sku.startswith("yha_"):
        url = "https://auctions.yahoo.co.jp/jp/auction/" + sku[4:]
    else:
        print(f"❓ 未対応のSKU形式: {sku}")
        return "未処理", None

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 404:
            print(f"📄 {url} → 削除（HTTP 404）")
            return "削除", None

        soup = BeautifulSoup(res.text, "html.parser")

        if "fril.jp" in url:
            if soup.find(string="SOLD OUT") or soup.find("div", class_="item-sold-out-label"):
                status = "売り切れ"
            elif soup.select_one("div.item_detail"):
                status = "販売中"
            else:
                return "未処理", None

            category_path = None
            for th in soup.find_all("th"):
                if "カテゴリ" in th.get_text(strip=True):
                    td = th.find_next_sibling("td")
                    if td:
                        category_list = [a.text.strip() for a in td.find_all("a")]
                        if category_list:
                            category_path = " > ".join(category_list)
                    break
            return status, category_path

        elif "auctions.yahoo.co.jp" in url:
            text_all = soup.get_text()
            if "このオークションは終了しています" in text_all or "開催終了" in text_all:
                status = "売り切れ"
            elif "ページが見つかりません" in text_all:
                return "削除", None
            else:
                status = "販売中"

            category_path = None
            category_dt = soup.find("dt", string="カテゴリ")
            category_dd = category_dt.find_next_sibling("dd") if category_dt else None
            if category_dd:
                category_links = category_dd.select('a[href*="category"]')
                category_list = [a.text.strip() for a in category_links if a.text.strip()]
                if category_list:
                    category_path = " > ".join(category_list)
            return status, category_path

        else:
            return "未処理", None

    except Exception as e:
        print(f"❌ URLチェックエラー: {url} → {e}")
        return "未処理", None
