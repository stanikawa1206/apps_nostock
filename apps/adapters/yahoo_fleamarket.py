# -*- coding: utf-8 -*-
"""
yahoo_fleamarket.py — Yahoo!フリマ（vendor_name='ＰａｙＰａｙフリマ'）の検索・商品詳細・在庫確認

Playwright/Selenium は使わず、サイト自身が使う JSON API を HTTP GET で呼ぶ（ログイン・Cookie 不要）。
  検索     /api/v1/search                 1ページ100件、offset は 9,900 件まで
  商品詳細 /api/item/v2/items/{id}         削除済みは 404
  ブランド /api/v1/brands/{id}/children    brandList にそのブランドの親から自分までの階層が入る

【ブランドと preset】（2026-11-10 実測）
  検索の brandIds は子ブランドの商品も返す（SEIKO で検索すると Grand Seiko・CREDOR・ALBA なども混ざる）。
  商品の preset は「検索した preset」ではなく、商品に付いたブランドから決める（resolve_item_preset）。
  - 商品ブランドの階層を下からたどり、同じカテゴリーの Yahoo!フリマ preset があるブランドの preset にする。
    例: SEIKO の検索で見つかった Grand Seiko の商品 → グランドセイコーの preset。
  - 子ブランドを Grand Seiko・CREDOR にせず SEIKO とだけ設定された商品は、タイトルにブランド名が
    明記されていれば付け替える（TITLE_BRAND_RULES。「GS」単独では判定しない）。
  - FURLA の下にある「ALBA」(124244) は出品しない（NG_BRAND_IDS。Yahoo!フリマ側のブランド構造の不備）。
  eBay の Brand・タイトル・説明文・高リスク判定は preset のブランド（presets_brand.default_brand_en）を使うため、
  preset を付け替えれば4つとも商品のブランドになる。
"""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import requests
from PIL import Image

VENDOR_NAME = "ＰａｙＰａｙフリマ"

BASE = "https://paypayfleamarket.yahoo.co.jp"
SEARCH_API = f"{BASE}/api/v1/search"
ITEM_API = f"{BASE}/api/item/v2/items/{{item_id}}"
BRAND_CHILDREN_API = f"{BASE}/api/v1/brands/{{brand_id}}/children"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
FETCH_TIMEOUT_SEC = 20

SEARCH_PAGE_SIZE = 100
# offset は 9,900 件目まで（それ以上は返らない）
SEARCH_OFFSET_LIMIT = 9900

# 商品の状態はメルカリの検索条件（新品・未使用に近い・目立った傷なし・やや傷）と同じ範囲
SEARCH_ITEM_CONDITIONS = "NEW,USED10,USED20,USED40"

# 商品の状態キー → メルカリと同じ item_condition_id
ITEM_CONDITION_MAP = {
    "new": 1,
    "used10": 2,   # 未使用に近い
    "used20": 3,   # 目立った傷や汚れなし
    "used40": 4,   # やや傷や汚れあり
    "used60": 5,   # 傷や汚れあり
    "used80": 6,   # 全体的に状態が悪い
}

# 出品しないブランド（Yahoo!フリマのブランドID）
NG_BRAND_IDS = {
    124244: "FURLA配下のALBA",
}

# 親ブランドだけが設定された商品を、タイトルのブランド名で子ブランドへ付け替える。
# {親のブランドID: [(子のブランドID, タイトルの正規表現), ...]}。タイトルは NFKC 正規化・小文字化してから照合する。
TITLE_BRAND_RULES = {
    101058: [  # SEIKO
        (102478, re.compile(r"グランドセイコー|grand\s*seiko")),  # Grand Seiko（「GS」単独では判定しない）
        (102541, re.compile(r"クレドール|credor")),               # CREDOR
    ],
}

# =========================
# 腕時計以外（部品・ベルト・空箱・冊子など）の判定
# Yahoo!フリマの腕時計カテゴリーには本体以外の商品が多い（2026-11-11 実測: 販売中 7,354件中 約6%。
# 高級ブランドのコマ・尾錠・箱・冊子・工具など）。タイトルで明確に分かるものだけを NG にする。
#   1) 本体以外であることの明記（空箱・ベルトのみ・部品取り 等）
#   2) 部品・付属品の語（コマ・尾錠・保証書・冊子・工具 等）があり、稼働・電池・クォーツ・本体などの記載が無い
#   3) 箱・ベルト・ケース等の語があり、時計本体を表す語（腕時計・文字盤 等）が無い（スマートウォッチは対象外）
# 「替えベルト付き」「箱あり」「コマ付属」のように後ろに 付き・あり 等が続く語は、本体に付いてくる物として数えない。
# =========================
_NON_WATCH_EXPLICIT = re.compile(
    r"空箱|(箱|ボックス|box|ケース|冊子|説明書|取説|保証書|ギャランティ|タグ|ベルト|バンド|ブレス|ストラップ|コマ|駒|文字盤"
    r"|ムーブメント|尾錠|バックル|ベゼル|ガラス|部品|パーツ)(だけ|のみ)|部品取り|純正部品|修理用|補修用|リペア用"
    r"|(ジェネリック|互換|社外)パーツ"
)
_ACCESSORY_PHRASE = re.compile(r"(時計|ウォッチ)\s*保護|保護\s*(ケース|カバー)|(ベゼル|リューズ)\s*ガード")
_PART_WORDS = [
    "部品", "パーツ", "コマ", "駒", "尾錠", "バックル", "中留", "クラスプ", "エンドピース", "弓カン", "弓菅", "バネ棒", "風防",
    "巻き芯", "巻芯", "パッキン", "交換用", "替えベルト", "替えバンド", "時計ベルト", "時計バンド", "腕時計ベルト",
    "ギャランティ", "保証書", "冊子", "プライスタグ", "工具", "ツール", "ドライバー", "巾着", "ポーチ", "保存袋",
    "ウォッチスタンド", "時計スタンド", "ディスプレイスタンド", "ワインディング", "巻き上げ機", "図鑑", "ムック",
    "ウォレット", "財布", "時計用", "ウォッチ用", "watch用",
]
_AMBIGUOUS_WORDS = [
    "ベルト", "バンド", "ブレスレット", "ブレス", "ストラップ", "箱", "ボックス", "box", "ケース", "収納", "スタンド",
    "充電器", "ケーブル", "リューズ", "竜頭", "りゅうず", "ガード", "プロテクター", "説明書", "取説",
]


def _word_re(words):
    return re.compile("(" + "|".join(sorted(map(re.escape, words), key=len, reverse=True)) + ")")


_PART_RE = _word_re(_PART_WORDS)
_AMBIGUOUS_RE = _word_re(_AMBIGUOUS_WORDS)
_FOLLOWED_BY_EXTRA = re.compile(r"^[^。、]{0,10}?(付き|付属|付|つき|あり|有り|有|無し|なし|欠品|交換済|セット|式|タイプ)")
_KOMA_IN_WORD = re.compile(r"コマ(?=[ァ-ヶー])")  # コマンダー 等
_LIVE_BODY = re.compile(r"本体|稼働|稼動|動作")
_STRONG_BODY = re.compile(
    r"本体|bt\d+%|バッテリー|稼働|稼動|動作|電池|クォーツ|クオーツ|quartz|自動巻|オートマチック|オート(?!バイ)|automatic"
    r"|手巻|ソーラー|solar|電波|メカニカル|スマートウォッチ|apple ?watch|series ?\d"
)
_WEAK_BODY = re.compile(
    r"(腕?時計|ウォッチ|watch)(?!\s*(用|ベルト|バンド|ケース|ボックス|box|箱|スタンド|尾錠|付属品|工具|ツール|ギャランティ"
    r"|保証書|冊子|ポーチ|巾着|バックル|コマ|駒|携帯))|文字盤|ダイヤル|アナログ|デジタル|クロノグラフ|chronograph"
)
SMARTWATCH_CATEGORY_ID = 40976

# 説明文: 本体が無いことの明記・「〜のみの出品」
_DESC_NO_BODY = re.compile(
    r"(時計)?本体(は|が)?(付属|含まれ|ついて|付いて|入って|ござい)(い)?(ません|おりません|ない)|本体(なし|無し)"
    r"|本体は(含み|付き|つき)ません|時計(本体)?ではありません"
)
_DESC_ONLY = re.compile(
    r"(ベルト|バンド|ブレス|ブレスレット|ストラップ|革ヒモ|コマ|駒|部品|パーツ|箱|ケース|ボックス|box|保証書|ギャランティ|冊子"
    r"|説明書|取扱説明書|尾錠|バックル|ベゼル|リューズ|竜頭|風防|文字盤|ムーブメント)(部分)?(のみ|だけ)(の)?"
    r"(出品|販売|お取引|お取り引き|です|となります|になります)"
)


def _part_hits(regex, t: str) -> List[str]:
    out = []
    for m in regex.finditer(t):
        if m.group(0) == "コマ" and _KOMA_IN_WORD.match(t, m.start()):
            continue
        if _FOLLOWED_BY_EXTRA.match(t[m.end():m.end() + 14]):
            continue
        out.append(m.group(0))
    return out


def non_watch_reason(title: Optional[str], category_id: Optional[int] = None) -> Optional[str]:
    """タイトルから腕時計本体ではないと明確に分かる場合はその理由、それ以外は None。"""
    t = _normalize_title(title)
    m = _NON_WATCH_EXPLICIT.search(t)
    if m:
        return f"本体以外の明記「{m.group(0)}」"
    m = _ACCESSORY_PHRASE.search(t)
    if m and not _STRONG_BODY.search(t):
        return f"付属品「{m.group(0)}」"
    parts = _part_hits(_PART_RE, t)
    if parts:
        # 冊子だけの場合は、商品名の AUTOMATIC 等ではなく稼働・本体の記載だけを本体の印にする
        body = _LIVE_BODY.search(t) if set(parts) == {"冊子"} else _STRONG_BODY.search(t)
        if not body:
            return f"部品・付属品「{parts[0]}」"
    if category_id != SMARTWATCH_CATEGORY_ID:
        amb = _part_hits(_AMBIGUOUS_RE, t)
        if amb and not _STRONG_BODY.search(t) and not _WEAK_BODY.search(t):
            return f"付属品のみ「{amb[0]}」"
    return None


def non_watch_reason_desc(description: Optional[str]) -> Optional[str]:
    """説明文に本体が無いことが明記されていればその理由。「付属品は箱のみです」は本体の商品なので数えない。"""
    t = _normalize_title(description)
    m = _DESC_NO_BODY.search(t)
    if m:
        return f"説明文「{m.group(0)}」"
    for m in _DESC_ONLY.finditer(t):
        if "付属" in t[max(0, m.start() - 12):m.start()]:
            continue
        return f"説明文「{m.group(0)}」"
    return None


# =========================
# Department（男女）の補正
# メンズ・レディースのカテゴリーの商品で、タイトルに性別が明記されていればそちらを使う。
# 兼用・ユニセックス・ペア・両方の記載がある場合は補正しない（カテゴリーの設定のまま）。
# =========================
_WOMEN = re.compile(r"レディース|レディス|ladies|lady's|ウィメンズ|women|婦人|女性用|女性向け")
_MEN = re.compile(r"メンズ|(?<!wo)men'?s\b|紳士|男性用|男性向け")
_UNISEX = re.compile(r"男女兼用|兼用|ユニセックス|unisex|ペア|ボーイズ")


def department_from_title(title: Optional[str], department: Optional[str]) -> Optional[str]:
    """department が Men / Women のときだけ、タイトルの性別で補正した値を返す（それ以外はそのまま）。"""
    if department not in ("Men", "Women"):
        return department
    t = _normalize_title(title)
    if _UNISEX.search(t):
        return department
    women, men = bool(_WOMEN.search(t)), bool(_MEN.search(t))
    if women and not men:
        return "Women"
    if men and not women:
        return "Men"
    return department


IMAGE_MIN_PX = 500
IMAGE_LIMIT = 20


class YahooHttpError(Exception):
    """API が 2xx / 404 以外を返した。"""

    def __init__(self, url: str, status_code: int, body_head: str):
        self.url = url
        self.status_code = status_code
        super().__init__(f"YAHOO_HTTP status={status_code} url={url} body_head={body_head!r}")


class YahooItemUnavailableError(Exception):
    """商品が販売中でない（state: '削除' / '売り切れ' / '判定不可'）。"""

    def __init__(self, message: str, state: str):
        super().__init__(message)
        self.state = state


_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})


def _get_json(url: str, params: Optional[dict] = None) -> Optional[dict]:
    """GET して JSON を返す。404 は None。それ以外の失敗は YahooHttpError（通信エラーは requests の例外のまま）。"""
    resp = _session.get(url, params=params, timeout=FETCH_TIMEOUT_SEC)
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise YahooHttpError(resp.url, resp.status_code, " ".join(resp.text[:300].split()))
    return resp.json()


def item_url(item_id: str) -> str:
    return f"{BASE}/item/{item_id}"


# =========================
# 検索
# =========================
def search_params(*, brand_id: int, category_id: int, status: str,
                  min_price: Optional[int], max_price: Optional[int], offset: int) -> dict:
    """status は 'open'（販売中）または 'sold'（売り切れ）。並び順は新着順。"""
    params = {
        "results": SEARCH_PAGE_SIZE,
        "offset": offset,
        "itemStatus": status,
        "brandIds": int(brand_id),
        "genreCategoryIds": int(category_id),
        "itemConditions": SEARCH_ITEM_CONDITIONS,
        "sort": "openTime",
        "order": "DESC",
    }
    if min_price is not None:
        params["minPrice"] = int(min_price)
    if max_price is not None:
        params["maxPrice"] = int(max_price)
    return params


def fetch_search_page(params: dict) -> Tuple[List[Dict[str, Any]], int]:
    """検索1ページ分。(商品の一覧, 総件数)。"""
    js = _get_json(SEARCH_API, params)
    if js is None or not isinstance(js.get("items"), list):
        raise YahooHttpError(SEARCH_API, 404 if js is None else 200, f"検索の応答に items が無い params={params}")
    items = []
    for it in js["items"]:
        brand = it.get("brand") or {}
        items.append({
            "vendor_item_id": str(it["id"]),
            "title_jp": it.get("title"),
            "price": int(it["price"]) if it.get("price") is not None else None,
            "brand_id": brand.get("id"),
            "brand_name": brand.get("name"),
            "item_condition_id": ITEM_CONDITION_MAP.get((it.get("condition") or "").lower()),
        })
    return items, int(js.get("totalResultsAvailable") or 0)


# =========================
# ブランドの階層と preset の決定
# =========================
# {ブランドID: 親から自分までのブランドID}。プロセス内で保持する。
_brand_path_cache: Dict[int, List[int]] = {}
# 子孫をたどり終えたブランドID（検索に使ったブランド）
_crawled_roots: set = set()


def _crawl_brand_tree(root_id: int) -> None:
    """
    root_id の子孫すべての階層を _brand_path_cache に入れる。
    子ブランドの一覧API は子のあるブランドにしか使えない（子の無いブランドは 404）ため、
    子のブランドの階層は親の階層に自分を足して作る。
    """
    js = _get_json(BRAND_CHILDREN_API.format(brand_id=root_id))
    if js is None:  # 子が無い
        _brand_path_cache.setdefault(root_id, [root_id])
        return
    root_path = [int(b["id"]) for b in js.get("brandList") or []] or [root_id]
    _brand_path_cache[root_id] = root_path
    queue = [(root_path, js)]
    while queue:
        parent_path, parent_js = queue.pop()
        for child in parent_js.get("brands") or []:
            child_path = parent_path + [int(child["id"])]
            _brand_path_cache[int(child["id"])] = child_path
            if child.get("hasChildren"):
                child_js = _get_json(BRAND_CHILDREN_API.format(brand_id=int(child["id"])))
                if child_js is not None:
                    queue.append((child_path, child_js))


def get_brand_path(brand_id: int, search_brand_id: int) -> List[int]:
    """
    商品のブランドの階層（親から自分まで）。search_brand_id は検索に使ったブランド
    （検索結果の商品のブランドはこのブランドかその子孫）。初めてのブランドなら子孫をたどってから引く。
    たどっても見つからないブランド（検索後に追加されたブランド等）は [brand_id] を返す。
    """
    brand_id, search_brand_id = int(brand_id), int(search_brand_id)
    if brand_id not in _brand_path_cache and search_brand_id not in _crawled_roots:
        _crawl_brand_tree(search_brand_id)
        _crawled_roots.add(search_brand_id)
    return _brand_path_cache.get(brand_id, [brand_id])


def _normalize_title(title: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", title or "").lower()


def resolve_item_preset(item_brand_id: Optional[int], title: Optional[str], category_id: int,
                        preset_by_brand: Dict[Tuple[int, int], str],
                        search_brand_id: int) -> Tuple[Optional[str], str]:
    """
    商品のブランドから preset を決める。戻り値は (preset, 説明)。出品しない商品は preset=None。
    preset_by_brand: {(Yahoo!フリマのブランドID, カテゴリーID): preset}（mst.presets_lookup の ＰａｙＰａｙフリマ 分）
    search_brand_id: 検索に使ったブランドID
    """
    nw = non_watch_reason(title, category_id)
    if nw:
        return None, f"腕時計以外: {nw}"

    if item_brand_id is None:
        return None, "ブランドなし"

    path = get_brand_path(int(item_brand_id), search_brand_id)
    for bid in path:
        if bid in NG_BRAND_IDS:
            return None, f"NGブランド {NG_BRAND_IDS[bid]}({bid})"

    resolved = next((bid for bid in reversed(path) if (bid, category_id) in preset_by_brand), None)
    if resolved is None:
        return None, f"preset のあるブランドが階層に無い brand_id={item_brand_id} path={path}"

    reason = f"brand_id={item_brand_id}" if resolved == item_brand_id else f"brand_id={item_brand_id}→{resolved}"
    rules = TITLE_BRAND_RULES.get(resolved)
    if rules:
        norm = _normalize_title(title)
        hits = []
        for child_id, pattern in rules:
            m = pattern.search(norm)
            if m and (child_id, category_id) in preset_by_brand:
                hits.append((m.start(), child_id))
        if hits:
            child_id = min(hits)[1]  # 両方あればタイトルで先に出てくる方
            reason += f"→{child_id}(タイトル)"
            resolved = child_id

    return preset_by_brand[(resolved, category_id)], reason


# =========================
# 商品詳細・在庫確認
# =========================
def fetch_item_json(item_id: str) -> dict:
    js = _get_json(ITEM_API.format(item_id=item_id))
    if js is None:
        raise YahooItemUnavailableError(f"item not found (404): {item_id}", state="削除")
    return js


def check_yahoo_item_status(item_id: str) -> Tuple[str, Optional[int]]:
    """
    check_remaining_ebay.py 用。戻り値は (status, price_jpy)。
    OPEN → 販売中、SOLD → 売り切れ、404 → YahooItemUnavailableError（呼び出し側で「削除」）。
    それ以外の状態（実物を確認できていない CLOSE・HOLD 等）は 判定不可（出品維持・次回再確認）。
    """
    js = fetch_item_json(item_id)
    status = js.get("status")
    if status == "SOLD":
        return "売り切れ", None
    if status == "OPEN" and js.get("price") is not None:
        return "販売中", int(js["price"])
    return "判定不可", None


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    """'2026-11-03T21:48:36+09:00' → このマシンの現地時刻（タイムゾーンなし。メルカリの fromtimestamp と同じ扱い）"""
    if not s:
        return None
    return datetime.fromisoformat(s).astimezone().replace(tzinfo=None)


def _valid_images(images: List[dict]) -> List[str]:
    """500px 以上の画像だけ。サイズが応答に無い（-1）画像は取得して確かめる。"""
    out: List[str] = []
    for im in images:
        url = (im or {}).get("url")
        if not url or not url.startswith("http"):
            continue
        w, h = im.get("width") or -1, im.get("height") or -1
        if w <= 0 or h <= 0:
            try:
                r = _session.get(url, timeout=10)
                r.raise_for_status()
                w, h = Image.open(BytesIO(r.content)).size
            except Exception as e:
                print(f"[IMAGE ERROR] {url} {e}", flush=True)
                continue
        if w >= IMAGE_MIN_PX and h >= IMAGE_MIN_PX:
            out.append(url)
        if len(out) >= IMAGE_LIMIT:
            break
    return out


def parse_detail_yahoo(item_id: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """
    parse_detail_personal / parse_detail_rakuma と同じ形の rec dict を返す。
    販売中でなければ YahooItemUnavailableError（state は 削除・売り切れ・判定不可）。
    """
    js = fetch_item_json(item_id)
    status = js.get("status")
    if status == "SOLD":
        raise YahooItemUnavailableError(f"status=SOLD: {item_id}", state="売り切れ")
    if status != "OPEN":
        raise YahooItemUnavailableError(f"status={status}: {item_id}", state="判定不可")

    images = _valid_images(js.get("images") or [])
    if not images:
        # メルカリと同じ文言（出品候補の条件で恒久的に除外される）
        raise Exception("no valid images (all <500px or error)")

    seller = js.get("seller") or {}
    rating = seller.get("rating") or {}
    return {
        "vendor_name": vendor_name,
        "title_jp": js.get("title"),
        "title_en": "",
        "price": int(js["price"]) if js.get("price") is not None else None,
        "vendor_created_at": _parse_dt(js.get("createDate")),
        # ヤフオク由来の商品（type=auction）には更新日時が無い → None（古い更新の判定対象外。ラクマと同じ）
        "vendor_updated_at": _parse_dt(js.get("updateDate")),
        "shipping_region": js.get("location"),
        "shipping_days": (js.get("deliverySchedule") or {}).get("text"),
        "seller_id": seller.get("id"),
        "seller_name": seller.get("nickname"),
        "rating_count": int(rating.get("total") or 0),
        "num_likes": int(js.get("likeCount") or 0),
        "images": images,
        "preset": preset,
        "description": (js.get("description") or "").strip(),
        "description_en": "",
        "item_attributes": [],
        "item_condition_id": ITEM_CONDITION_MAP.get(((js.get("condition") or {}).get("key") or "").lower()),
        "is_variant": False,
        "variants": [],
        "yahoo_category_id": ((js.get("categoryList") or [{}])[-1] or {}).get("id"),
    }


def non_watch_reason_rec(rec: Dict[str, Any]) -> Optional[str]:
    """出品前の確認用: 商品詳細のタイトル・説明文から腕時計以外と分かればその理由。"""
    return (non_watch_reason(rec.get("title_jp"), rec.get("yahoo_category_id"))
            or non_watch_reason_desc(rec.get("description")))
