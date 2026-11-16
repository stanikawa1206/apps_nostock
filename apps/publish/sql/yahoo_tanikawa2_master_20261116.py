# -*- coding: utf-8 -*-
"""
谷川② のデジカメ・ビデオカメラ・レンズ・ペン・製図用品に Yahoo!フリマ（Y）を追加するマスターの定義（2026-11-16）。

この1か所の定義から、SQL と戻しの SQL を作り（write）、本番DBを読むだけの検証をする（verify）。
    python -m apps.publish.sql.yahoo_tanikawa2_master_20261116 write
    python -m apps.publish.sql.yahoo_tanikawa2_master_20261116 verify

SQL は2段階:
  yahoo_tanikawa2_master_20261116.sql   … マスター（カテゴリー・ブランド・所属）と出品候補関数の修正。
                                           presets_price_ranges に Y の行が無いので lookup に Y の preset はまだ作られない
                                           （在庫取得・出品とも始まらない。verify の「段階1」で確認）
  yahoo_tanikawa2_enable_20261116.sql   … presets_price_ranges に Y の5行を追加 = 有効化
                                           （次のサイクル開始時の lookup 再生成から在庫取得、その後の 3:00 から出品）
  *_rollback.sql                        … それぞれの戻し（enable の戻し → master の戻しの順）

【方針】（ユーザー指示 2026-11-16）
- デジカメ・ペン: ラクマ専用（10/27 ラクマ拡充）のブランドも Y に含める
  → brand_category_group_vendors のラクマ専用の所属に Y の行を足す（メルカリは従来どおり対象外）。
- ビデオカメラ: アクションカメラ（Y 49419。GoPro・DJI・insta360 は Y ではこちら）も追加。eBay は 11724 Camcorders・Type Helmet/Action。
- レンズ: Y の交換レンズ(2465)は単焦点／ズームが1カテゴリー。Type は出品時にタイトルから決め、決められない商品は出品しない
  （apps/adapters/yahoo_fleamarket.py lens_type_from_title）。presets_categories.type_ebay は NULL。
- 製図用品: ブランドなし（brand_id=0）。製図板はメルカリと同じくカテゴリーで外す（Y の製図板カテゴリー 47524 を登録しない）。
  タイトルでの除外はしない（メルカリの 5317 その他製図用品と同じ扱い。11/16 ユーザー指示）。
- ペンのボールペン・万年筆の eBay カテゴリー: メルカリの presets_categories（5249 ボールペン・5262 万年筆）の
  ブランド別の行（brand_id → category_id_ebay）を Y の 47303・47315 に写す。
  メルカリに足りなかった組（モンブランのボールペン等）は PEN_ADDITIONS でメルカリにも足す（pen_brand_categories_20261116.sql）。
  - ペリカン: メルカリでは brand_id 13843「ペリカン」と 57582「Pelikan」の2つのブランド（商品はどちらか一方）。
    Y のブランドは1つ(101780)で、vendor_brand_map は (vendor_name, vendor_brand_id) が一意なので 13843 だけに対応させ、
    57582 の行は 13843 に写す（同じ eBay カテゴリーなので1行にまとめる。ラクマも 13843 だけ）。
- 優先順位は R → Y → MS → M のまま（mst.vendors は変更しない）。
- ブランドは、谷川② の価格帯・状態で Y の販売中が1件以上あるものだけ登録（谷川④ の腕時計と同じ。0件は対応不可）。
"""
import os
import sys
from collections import Counter, defaultdict
from types import SimpleNamespace

Y_VENDOR = "ＰａｙＰａｙフリマ"
Y_CODE = "Y"
M_VENDOR = "メルカリ"
ACCOUNT = "谷川②"
GROUPS = ["デジカメ", "ビデオカメラ", "レンズ", "ペン", "製図用品"]
NOTE = "Yahoo!フリマ 谷川② 2026-11-16"
HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# カテゴリー（ブランド別でないもの）: Y カテゴリーID → (グループ, 名前, 写す元のメルカリ category_id, type_ebay の上書き)
#   名前は preset 名（ブランド名＋カテゴリー名＋Y）になる。type_ebay・category_id_ebay・department・is_collectibles は
#   写す元のメルカリの行と同じ（上書きの指定があればその値）。
# ---------------------------------------------------------------------------
KEEP = object()
CATEGORIES = {
    # デジカメ（eBay 31388 Digital Cameras）
    47734: ("デジカメ", "ミラーレス一眼", 4006, KEEP),
    19801: ("デジカメ", "コンパクトデジタルカメラ", 4007, KEEP),
    47735: ("デジカメ", "デジタル一眼レフ", 4009, KEEP),
    # ビデオカメラ（eBay 11724 Camcorders）
    49429: ("ビデオカメラ", "ビデオカメラ本体", 4012, KEEP),
    49419: ("ビデオカメラ", "アクションカメラ", 4012, "Helmet/Action"),   # 11724 の Type 候補「Helmet/Action」
    # レンズ（eBay 3323 Lenses）。Type は出品時にタイトルから
    2465: ("レンズ", "交換レンズ", 846, None),
    # ペン（ブランド別でないもの）
    47299: ("ペン", "シャープペン", 5253, KEEP),      # 7284 Mechanical
    47312: ("ペン", "マーカー", 5257, KEEP),          # 28109 Art Pens & Markers
    47288: ("ペン", "色鉛筆", 5266, KEEP),            # 28108 Art Pencils & Charcoal
    47539: ("ペン", "筆ペン", 5269, KEEP),            # 28109
    # 製図用品（eBay 14017 Drafting Tools、ブランドなし）。製図板 47524 は登録しない
    47521: ("製図用品", "直線・方眼定規", 5312, KEEP),
    47522: ("製図用品", "コンパス", 5313, KEEP),
    47526: ("製図用品", "製図用ブラシ", 5314, KEEP),
    47523: ("製図用品", "字消し板", 5315, KEEP),
    47520: ("製図用品", "その他製図用品", 5317, KEEP),
    47528: ("製図用品", "三角スケール", 5318, KEEP),
    47529: ("製図用品", "三角定規", 5319, KEEP),
    47530: ("製図用品", "分度器", 5320, KEEP),
    47531: ("製図用品", "製図テンプレート", 5321, KEEP),
    47532: ("製図用品", "雲形定規", 5322, KEEP),
    47533: ("製図用品", "製図用シャープペンシル", 5323, KEEP),
}
# ペンのブランド別カテゴリー: Y カテゴリーID → (名前, 写す元のメルカリ category_id)
BRAND_DEPENDENT = {
    47303: ("ボールペン", 5249),
    47315: ("万年筆", 5262),
}
# 写すときの brand_id の付け替え（Y では1ブランドにまとめる）
BRAND_MERGE = {57582: 13843}   # Pelikan → ペリカン（Y 101780）

# メルカリ（vendor_group メルカリ = M・MS）のボールペン・万年筆に足りなかったブランド別の行（2026-11-16 調査）。
# (メルカリ category_id, brand_id) → eBay category_id。ブランド別の eBay カテゴリーが無いブランドは Other（7279 / 7281）。
# 件数はメルカリの販売中（谷川②のペンの価格帯 5,690〜131,550円）/ Y の販売中（同）。
OTHER_BALLPOINT, OTHER_FOUNTAIN = "7279", "7281"
PEN_ADDITIONS = {
    (5249, 1912): "29828",          # モンブラン ボールペン（eBay 29828 Montblanc）  M 203 / Y 430
    (5249, 57582): "165643",        # Pelikan ボールペン（13843 ペリカンと同じ 165643） M 70（13843 とは別のメルカリのブランド）
    (5249, 49148): OTHER_BALLPOINT,  # 三菱鉛筆／uni                                   M 400 / Y 180
    (5249, 8710): OTHER_BALLPOINT,   # ユニ（Y では三菱鉛筆の子。メルカリの 8710 は0件）  M 0 / Y 83
    (5249, 34953): OTHER_BALLPOINT,  # ゼブラ                                          M 240 / Y 27
    (5249, 42196): OTHER_BALLPOINT,  # ハローキティ（メルカリは vendor_brand_map 無効のため集めない） M 145 / Y 7
    (5249, 44990): OTHER_BALLPOINT,  # プラチナ                                        M 37 / Y 3
    (5249, 16649): OTHER_BALLPOINT,  # コクヨ                                          M 33 / Y 13
    (5249, 30428): OTHER_BALLPOINT,  # サクラクレパス                                  M 31 / Y 7
    (5249, 34791): OTHER_BALLPOINT,  # セーラー                                        M 27 / Y 4
    (5249, 39214): OTHER_BALLPOINT,  # トンボ                                          M 18 / Y 0
    (5249, 34088): OTHER_BALLPOINT,  # ステッドラー                                    M 16 / Y 6
    (5249, 25669): OTHER_BALLPOINT,  # OMAS                                            M 8 / Y 5
    (5249, 47727): OTHER_BALLPOINT,  # マーレン                                        M 7 / Y 1
    (5249, 30033): OTHER_BALLPOINT,  # コンクリン                                      M 2 / Y 9
    (5262, 13843): "14002",         # ペリカン 万年筆（57582 Pelikan と同じ 14002）  M 80（57582 は M 230 で既存）
    (5262, 44990): OTHER_FOUNTAIN,   # プラチナ 万年筆                                 M 364 / Y 93
    (5262, 42986): "165626",        # ビスコンティ 万年筆（eBay 165626 Visconti）      M 23 / Y 1
    (5262, 34088): OTHER_FOUNTAIN,   # ステッドラー 万年筆                             M 7 / Y 0
    (5262, 39214): OTHER_FOUNTAIN,   # トンボ 万年筆                                   M 2 / Y 2
}
# Y にだけ写し、メルカリには足さない組（メルカリのそのブランドIDでは0件。足すと空振りの M・MS preset ができる）
PEN_Y_ONLY = {(5249, 8710)}
# 足さなかった組（メルカリ・Y とも0件、または別ブランドの誤登録だけ）: ユニ・三菱鉛筆・コクヨの万年筆、ゼブラの万年筆（M 1件は ZEEBRA）、
#   Namiki・コンウェイ・スチュワートのボールペン（M 0件）
# eBay の Brand（presets_brand.default_brand_en）の誤記の修正
BRAND_EN_FIX = {47727: ("Marien", "Marlen")}   # マーレン（eBay カテゴリー 165623 も Marlen）

# ---------------------------------------------------------------------------
# ブランド: brand_id → (Yahoo!フリマのブランドID, Yahoo!フリマのブランド名, 谷川②の条件内の販売中件数 2026-11-16)
# 同じ名前の別ブランドがあるものは、文具・カメラの ID を選んだ（備考）。
# 既に Y の行があるブランド（CASIO・リコー・カルティエ等）と、販売中0件・Y に無いブランドはここに入れない。
# ---------------------------------------------------------------------------
BRANDS = {
    # カメラ・レンズ
    3276: (100537, "オリンパス", 189),
    3277: (100614, "キヤノン", 1489),
    3285: (101091, "SONY", 367),
    3290: (101345, "ニコン", 791),
    3291: (101460, "Panasonic", 243),
    3294: (101801, "ペンタックス", 109),       # Y ではリコー(102135)の子。リコーの検索の商品もペンタックスの preset になる
    3296: (102365, "FUJIFILM", 137),
    4194: (102073, "ライカ", 50),               # 同名: LEICA（シューケア）211733 ではない
    4196: (104459, "ケンコー・トキナー", 22),
    4197: (116853, "コシナ", 26),
    4198: (100774, "コニカミノルタ", 18),
    4199: (100880, "シグマ", 106),
    4203: (101276, "トキナー", 12),             # Y ではケンコー・トキナーの子
    4204: (100067, "TAMRON", 58),
    15358: (135365, "CONTAX", 3),
    15592: (116440, "ZEISS", 39),
    24997: (163295, "OM SYSTEM", 2),
    7202: (116363, "GoPro", 121),
    15577: (101536, "Victor(オーディオ)", 2),  # 同名: VICTOR（工具・ファッション）ではない
    21803: (136873, "Insta360", 123),
    37078: (116224, "DJI", 142),
    # ペン
    289: (102616, "WATERMAN", 90),
    522: (100707, "CROSS", 197),
    902: (102615, "PARKER", 226),               # 同名多数（PARKER（PORTER）154317 等）ではない
    1912: (102030, "MONTBLANC（筆記具、時計）", 728),
    3633: (107672, "フェリシモ", 9),
    8710: (121008, "uni（三菱鉛筆）", 19),       # Y では三菱鉛筆の子
    10505: (102632, "PILOT（文具）", 330),
    13330: (108197, "Miffy", 1),
    13843: (101780, "Pelikan（筆記具）", 136),  # pelican（stacksto.）141185 ではない。57582 Pelikan もこの ID（BRAND_MERGE）
    15344: (102619, "CARAN d'ACHE", 102),
    15531: (102621, "S.T.DUPONT", 16),
    16649: (100759, "KOKUYO", 2),
    25669: (159756, "OMAS", 8),
    29816: (100776, "コヒノール", 1),
    30026: (102627, "コンウェイ・スチュワート", 1),
    30033: (102625, "コンクリン", 9),
    30428: (102633, "サクラクレパス", 4),
    31766: (102620, "SHEAFFER", 25),
    32497: (115731, "シヤチハタ", 1),
    34088: (101016, "ステッドラー", 49),
    34791: (102631, "セーラー万年筆", 130),     # SAYLOR 144832 ではない
    34953: (115771, "ZEBRA", 6),
    35572: (104358, "ダーウェント", 4),
    39214: (117698, "トンボ鉛筆", 7),           # TONBOW（学生服）112795 ではない
    42196: (125889, "ハローキティ", 5),         # Y ではサンリオの子
    42986: (102629, "ビスコンティ", 1),
    43647: (101579, "ファーバーカステル", 20),
    44990: (118928, "プラチナ万年筆", 6),
    46802: (102638, "ぺんてる", 111),
    47469: (104089, "ホルベイン", 2),
    47727: (102624, "マーレン", 2),
    49148: (115772, "三菱鉛筆", 141),
    50591: (102618, "Montegrappa", 6),
    52017: (102109, "LAMY", 101),
    54175: (102280, "ロットリング", 23),
}
# 対応不可（登録しない）: 販売中0件 … シャープ・日立・JVC・ティファニー・プーマ・無印良品・スワロフスキー・スヌーピー・Namiki・
#   あかしや・ウィンザー＆ニュートン・カミオジャパン・呉竹・コピック・ショウワノート・ポケモン・アウロラ／Y に無い … ちいかわ
#   （ティファニー・スワロフスキーは他カテゴリー用の Y の行が既にあるので preset はできる。0件なのでジョブは空振りするだけ）


def q(sql_text):
    return sql_text.replace("'", "''")


def load(cur):
    """生成に使う本番マスター（読むだけ）。"""
    def rows(sql, *params):
        cur.execute(sql, *params)
        return cur.fetchall()
    m_cats = {r.category_id: r for r in rows(
        "SELECT * FROM mst.presets_categories WHERE vendor_group = ? AND brand_id = 0", M_VENDOR)}
    m_dep = rows("SELECT * FROM mst.presets_categories WHERE vendor_group = ? AND category_id IN (5249, 5262) ORDER BY category_id, brand_id",
                 M_VENDOR)
    bcgv = rows("SELECT brand_id, category_group, vendor_name FROM mst.brand_category_group_vendors "
                "WHERE category_group IN (" + ",".join("?" * len(GROUPS)) + ") ORDER BY category_group, brand_id", *GROUPS)
    y_vbm = rows("SELECT brand_id, vendor_brand_id FROM mst.vendor_brand_map WHERE vendor_name = ?", Y_VENDOR)
    m_price = rows("SELECT * FROM mst.presets_price_ranges WHERE preset_group = ? AND vendor_code = 'M' AND category_group IN ("
                   + ",".join("?" * len(GROUPS)) + ")", ACCOUNT, *GROUPS)
    return m_cats, m_dep, bcgv, y_vbm, m_price


def plan(cur):
    """登録する行（dict の list）を作る。"""
    m_cats, m_dep, bcgv, y_vbm, m_price = load(cur)
    cgc_y_other = cur.execute("SELECT DISTINCT category_group FROM mst.category_group_categories WHERE vendor_group = ?",
                              Y_VENDOR).fetchall()
    bcg_other = cur.execute("SELECT brand_id, category_group FROM mst.brand_category_groups").fetchall()
    bcgv_all = cur.execute("SELECT brand_id, category_group, vendor_name FROM mst.brand_category_group_vendors").fetchall()
    vendor_groups_of = defaultdict(set)
    for r in cur.execute("SELECT DISTINCT category_group, vendor_group FROM mst.category_group_categories").fetchall():
        vendor_groups_of[r.category_group].add(r.vendor_group)

    pc, cgc = [], []
    for cid, (group, name, src_id, type_override) in CATEGORIES.items():
        s = m_cats[src_id]
        pc.append(dict(category_id=cid, category_name_ja=name,
                       type_ebay=s.type_ebay if type_override is KEEP else type_override,
                       category_id_ebay=s.category_id_ebay, department=s.department, brand_id=0,
                       is_collectibles=s.is_collectibles, src=f"メルカリ {src_id} {s.category_name_ja}"))
        cgc.append(dict(category_group=group, category_id=cid, is_brand_dependent=0))
    # メルカリに足すペンの行（既存の同じカテゴリーの行と同じ列の値で、eBay カテゴリーだけ違う）
    additions = []
    have = {(int(r.category_id), r.brand_id): r.category_id_ebay for r in m_dep}
    for (src_id, brand_id), ebay in PEN_ADDITIONS.items():
        if (src_id, brand_id) in have and have[(src_id, brand_id)] != ebay:
            raise RuntimeError(f"メルカリに別の eBay カテゴリーで既にある組 {(src_id, brand_id)}（PEN_ADDITIONS を見直す）")
        tmpl = next(r for r in m_dep if int(r.category_id) == src_id)
        additions.append(dict(category_id=src_id, category_name_ja=tmpl.category_name_ja, type_ebay=None, category_id_ebay=ebay,
                              department=None, brand_id=brand_id, is_collectibles=tmpl.is_collectibles,
                              applied=(src_id, brand_id) in have))
    # メルカリに足す行（pen_brand_categories_20261116.sql。is_active=0 で入れて M・MS の preset は作らない）。適用済みの行は除く
    m_pc = [r for r in additions if (r["category_id"], r["brand_id"]) not in PEN_Y_ONLY and not r["applied"]]
    cols_pc = ("category_id", "brand_id", "category_id_ebay", "type_ebay", "department", "is_collectibles")
    # Y に写す元: メルカリの行（is_active=0 の行も含む）＋まだメルカリに無い追加分（Y だけの組を含む）
    m_all = [SimpleNamespace(**{k: getattr(r, k) for k in cols_pc}) for r in m_dep]
    m_all += [SimpleNamespace(**{k: r[k] for k in cols_pc}) for r in additions if not r["applied"]]
    for cid, (name, src_id) in BRAND_DEPENDENT.items():
        seen = {}
        for s in sorted((r for r in m_all if r.category_id == src_id), key=lambda r: r.brand_id):
            brand_id = BRAND_MERGE.get(s.brand_id, s.brand_id)
            if brand_id in seen:
                # まとめたブランド（ペリカン）は eBay カテゴリーが同じなら1行（違えば登録案の誤り）
                if seen[brand_id] != s.category_id_ebay:
                    raise RuntimeError(f"まとめるブランドの eBay カテゴリーが違う {src_id} {brand_id}: {seen[brand_id]} / {s.category_id_ebay}")
                continue
            seen[brand_id] = s.category_id_ebay
            pc.append(dict(category_id=cid, category_name_ja=name, type_ebay=s.type_ebay,
                           category_id_ebay=s.category_id_ebay, department=s.department, brand_id=brand_id,
                           is_collectibles=s.is_collectibles, src=f"メルカリ {src_id} brand_id={s.brand_id}"))
        cgc.append(dict(category_group="ペン", category_id=cid, is_brand_dependent=1))

    existing_y = {r.brand_id for r in y_vbm}
    vbm = [dict(brand_id=b, vendor_brand_id=yid, vendor_brand_name=yname, count=n)
           for b, (yid, yname, n) in BRANDS.items() if b not in existing_y]

    bcgv_y = [dict(brand_id=r.brand_id, category_group=r.category_group)
              for r in bcgv if r.vendor_name == "ラクマ"]
    bcgv_y = [r for r in bcgv_y if not any(x.brand_id == r["brand_id"] and x.category_group == r["category_group"]
                                           and x.vendor_name == Y_VENDOR for x in bcgv)]

    # vendor_brand_map はブランド共通なので、新しく Y の ID を入れたブランドが Y のある他のグループ（谷川④の腕時計）にも
    # 所属していると、そのグループの Y の preset ができてしまう（パナソニックの腕時計は谷川④の登録時に対応不可とした）。
    # その所属を、今使っている Y 以外の仕入先だけに限る行を足して、他のグループは今のままにする。
    other_y_groups = {r.category_group for r in cgc_y_other} - set(GROUPS)
    new_brands = {r["brand_id"] for r in vbm}
    keep_other = []
    for r in bcg_other:
        if r.brand_id in new_brands and r.category_group in other_y_groups:
            listed = [x.vendor_name for x in bcgv_all if x.brand_id == r.brand_id and x.category_group == r.category_group]
            if listed:
                continue  # 既に仕入先を限っている（Y は入っていない）
            for vg in sorted(vendor_groups_of[r.category_group] - {Y_VENDOR}):
                keep_other.append(dict(brand_id=r.brand_id, category_group=r.category_group, vendor_name=vg))

    price = [dict(category_group=r.category_group, low_usd_target=r.low_usd_target, high_usd_target=r.high_usd_target,
                  low_jpy_target=r.low_jpy_target, high_jpy_target=r.high_jpy_target, category_priority=r.category_priority)
             for r in sorted(m_price, key=lambda r: GROUPS.index(r.category_group))]
    return dict(pc=pc, cgc=cgc, vbm=vbm, bcgv=bcgv_y, keep_other=keep_other, price=price, m_pc=m_pc, pen_additions=additions)


# ---------------------------------------------------------------------------
# 出品候補関数の修正: presets_categories を (category_id, vendor_group) で1行にしてから結合する
# ---------------------------------------------------------------------------
FN_OLD_JOIN = """        INNER JOIN mst.presets_categories pc
            ON pc.category_id = pl.category_id
"""
FN_NEW_JOIN = """        -- 2026-11-16 presets_categories は (category_id, brand_id, vendor_group) ごとの行で、ペンのボールペン・万年筆は
        -- ブランド別に複数行ある。category_id だけで結合すると候補がその行数だけ重複していた（メルカリのペン72 preset で
        -- 1,260行）。使うのは is_collectibles だけ（同じ category_id・vendor_group の行で値は同じ）なので、
        -- 仕入先の vendor_group の (category_id) ごとに1行にしてから結合する（Yahoo!フリマの追加でも重複しない）。
        INNER JOIN (
            SELECT category_id, vendor_group, CAST(MAX(CAST(is_collectibles AS int)) AS bit) AS is_collectibles
            FROM mst.presets_categories
            GROUP BY category_id, vendor_group
        ) pc
            ON pc.category_id = pl.category_id
            AND pc.vendor_group = vd.vendor_group
"""


def fn_definitions(cur):
    old = cur.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.fn_take_one_candidates_base'))").fetchone()[0]
    if old.count(FN_OLD_JOIN) != 1:
        raise RuntimeError("fn_take_one_candidates_base の presets_categories 結合が想定と違う（定義が変わった？）")
    new = old.replace(FN_OLD_JOIN, FN_NEW_JOIN)
    return old, new


def as_create_or_alter(defn):
    i = defn.upper().index("FUNCTION")
    return "CREATE OR ALTER " + defn[i:]


def sqlv(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)) or v.__class__.__name__ == "Decimal":
        return str(v)
    return "N'" + q(str(v)) + "'"


def write(cur):
    p = plan(cur)
    old_fn, new_fn = fn_definitions(cur)
    out = []
    w = out.append
    w("-- 谷川② デジカメ・ビデオカメラ・レンズ・ペン・製図用品に Yahoo!フリマ を追加: 段階1 マスター（2026-11-16）")
    w("-- 生成: python -m apps.publish.sql.yahoo_tanikawa2_master_20261116 write（方針・ブランドIDの根拠はそのファイル）")
    w("-- この段階では presets_price_ranges に Y の行が無いので、lookup に Y の preset は作られない（在庫取得・出品は始まらない）。")
    w("-- 前提: Yahoo!フリマのグループ別判定（腕時計以外の判定を腕時計だけに・レンズの Type・ブランドなし検索）の")
    w("--       コードを本番（mouse・全VPS）に反映してから。古いコードはブランドなし（brand_id=0）を brandIds=0 で検索してしまう。")
    w(f"-- 件数: presets_categories {len(p['pc'])} / category_group_categories {len(p['cgc'])} / vendor_brand_map {len(p['vbm'])} / "
      f"brand_category_group_vendors {len(p['bcgv'])}+{len(p['keep_other'])} / 関数 dbo.fn_take_one_candidates_base")
    w("SET XACT_ABORT ON;")
    w("BEGIN TRAN;")
    w("")
    w("-- 1) カテゴリー（Yahoo!フリマ genreCategoryIds）と eBay カテゴリー・Type。写す元のメルカリの行を行末に記す")
    for r in p["pc"]:
        w("INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group) "
          f"VALUES ({r['category_id']}, {sqlv(r['category_name_ja'])}, {sqlv(r['type_ebay'])}, {sqlv(r['category_id_ebay'])}, "
          f"{sqlv(r['department'])}, {r['brand_id']}, {sqlv(r['is_collectibles'])}, {sqlv(Y_VENDOR)});  -- {r['src']}")
    w("")
    w("-- 2) グループ所属（ペンのボールペン・万年筆はブランド別 is_brand_dependent=1）。製図板 47524 は入れない")
    for r in p["cgc"]:
        w("INSERT INTO mst.category_group_categories (category_group, category_id, is_brand_dependent, vendor_group) "
          f"VALUES ({sqlv(r['category_group'])}, {r['category_id']}, {r['is_brand_dependent']}, {sqlv(Y_VENDOR)});")
    w("")
    w("-- 3) ブランドの Yahoo!フリマ ID（検索用。eBay の Brand は presets_brand.default_brand_en）。行末は谷川②の条件内の販売中件数")
    for r in p["vbm"]:
        w("INSERT INTO mst.vendor_brand_map (brand_id, vendor_name, vendor_brand_id, vendor_brand_name, is_primary, is_active, note, created_at) "
          f"VALUES ({r['brand_id']}, {sqlv(Y_VENDOR)}, {r['vendor_brand_id']}, {sqlv(r['vendor_brand_name'])}, 1, 1, {sqlv(NOTE)}, SYSDATETIME());"
          f"  -- {r['count']}件")
    w("")
    w("-- 4) ラクマ専用の所属（10/27 ラクマ拡充）を Yahoo!フリマ でも使う（メルカリは従来どおり使わない）")
    for r in p["bcgv"]:
        w("INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) "
          f"VALUES ({r['brand_id']}, {sqlv(r['category_group'])}, {sqlv(Y_VENDOR)}, {sqlv(NOTE + ': ラクマ専用の所属を Yahoo!フリマ でも使う')}, GETDATE());")
    w("")
    w("-- 4-2) 上で Y の ID を入れたブランドが、Y のある他のグループ（谷川④の腕時計）に所属している場合、その所属を")
    w("--      今の仕入先（メルカリ・ラクマ）だけに限る（行が無い所属は全仕入先共通なので、限らないと Y の preset ができる）")
    for r in p["keep_other"]:
        w("INSERT INTO mst.brand_category_group_vendors (brand_id, category_group, vendor_name, note, created_at) "
          f"VALUES ({r['brand_id']}, {sqlv(r['category_group'])}, {sqlv(r['vendor_name'])}, "
          f"{sqlv(NOTE + ': Y を追加しないため所属を今の仕入先に限る')}, GETDATE());")
    w("")
    w("COMMIT;")
    w("GO")
    w("")
    w("-- 5) 出品候補関数: presets_categories を (category_id, vendor_group) ごとに1行にして結合（候補の重複をなくす）")
    w(as_create_or_alter(new_fn).rstrip())
    w("GO")
    write_file("yahoo_tanikawa2_master_20261116.sql", out)

    rb = ["-- 段階1 マスターの戻し（2026-11-16）。先に enable の戻し（presets_price_ranges の Y 行の削除）を流すこと。",
          "-- コードは戻さなくてよい（新しいコードは Y の新グループが無くても谷川④の腕時計を従来どおり処理する）。",
          "SET XACT_ABORT ON;", "BEGIN TRAN;",
          "IF EXISTS (SELECT 1 FROM mst.presets_price_ranges WHERE vendor_code = 'Y' AND category_group IN ("
          + ",".join(sqlv(g) for g in GROUPS) + ")) THROW 50001, N'先に yahoo_tanikawa2_enable_20261116_rollback.sql を流す', 1;",
          f"DELETE FROM mst.brand_category_group_vendors WHERE vendor_name = {sqlv(Y_VENDOR)} AND category_group IN ("
          + ",".join(sqlv(g) for g in GROUPS) + ");",
          f"DELETE FROM mst.brand_category_group_vendors WHERE note = {sqlv(NOTE + ': Y を追加しないため所属を今の仕入先に限る')};",
          f"DELETE FROM mst.vendor_brand_map WHERE vendor_name = {sqlv(Y_VENDOR)} AND note = {sqlv(NOTE)};",
          f"DELETE FROM mst.category_group_categories WHERE vendor_group = {sqlv(Y_VENDOR)} AND category_id IN ("
          + ",".join(str(r["category_id"]) for r in p["cgc"]) + ");",
          f"DELETE FROM mst.presets_categories WHERE vendor_group = {sqlv(Y_VENDOR)} AND category_id IN ("
          + ",".join(sorted({str(r["category_id"]) for r in p["pc"]})) + ");",
          "COMMIT;", "GO", "", "-- 関数を修正前の定義に戻す", as_create_or_alter(old_fn).rstrip(), "GO"]
    write_file("yahoo_tanikawa2_master_20261116_rollback.sql", rb)

    en = ["-- 段階2 有効化（2026-11-16）: 谷川② の5グループに Yahoo!フリマ の価格帯の行を追加する。",
          "-- 価格帯・category_priority はメルカリ(M)の行と同じ。keep_* は NULL（Y の既存出品は無い）。",
          "-- 次の inventory_loop サイクル開始時の lookup 再生成から在庫取得が始まり、その後の 3:00 の出品から候補になる。",
          "-- 前提: 段階1（yahoo_tanikawa2_master_20261116.sql）とコードの本番反映。",
          "SET XACT_ABORT ON;", "BEGIN TRAN;"]
    for r in p["price"]:
        en.append("INSERT INTO mst.presets_price_ranges (preset_group, category_group, low_usd_target, high_usd_target, low_jpy_target, "
                  "high_jpy_target, category_priority, vendor_code, delete_flag, keep_low_usd_target, keep_high_usd_target) "
                  f"VALUES ({sqlv(ACCOUNT)}, {sqlv(r['category_group'])}, {r['low_usd_target']}, {r['high_usd_target']}, "
                  f"{r['low_jpy_target']}, {r['high_jpy_target']}, {r['category_priority']}, 'Y', 0, NULL, NULL);")
    en += ["COMMIT;"]
    write_file("yahoo_tanikawa2_enable_20261116.sql", en)
    write_file("yahoo_tanikawa2_enable_20261116_rollback.sql", [
        "-- 段階2 有効化の戻し: 谷川② の5グループの Yahoo!フリマ の価格帯の行を消す。",
        "-- 次の lookup 再生成で Y の preset が消える（Y の未終了出品が残っていれば delete_flag=1 にして残すこと。",
        "-- 行を消すと既存出品の在庫管理が止まる）。",
        "SET XACT_ABORT ON;", "BEGIN TRAN;",
        f"DELETE FROM mst.presets_price_ranges WHERE preset_group = {sqlv(ACCOUNT)} AND vendor_code = 'Y' AND category_group IN ("
        + ",".join(sqlv(g) for g in GROUPS) + ");",
        "COMMIT;"])
    write_pen(cur, p)
    print({k: len(v) for k, v in p.items()})


def write_pen(cur, p):
    """メルカリ（M・MS）のボールペン・万年筆の不足分。Y の段階1・2とは別に流せる（Y の行は写した値を持つので依存しない）。"""
    names = dict(cur.execute("SELECT brand_id, brand_name_ja FROM mst.presets_brand").fetchall())
    out = ["-- メルカリ（M・MS）のボールペン(5249)・万年筆(5262)のブランド別 eBay カテゴリーの不足分をマスターに足す（2026-11-16）。",
           "-- 生成: python -m apps.publish.sql.yahoo_tanikawa2_master_20261116 write（件数・根拠は PEN_ADDITIONS）",
           "-- is_active=0 で入れる = マスターとして持つだけで preset は作らない（M・MS の収集・出品は増えない。ユーザー指示 11/16）。",
           "-- 将来メルカリで再開するときは、その行を is_active=1 にする。",
           "-- 前提: presets_categories_is_active_20261116.sql（列の追加）と、lookup 生成が is_active=1 だけを使うコードの本番反映",
           "--       （古いコードの在庫管理ループが動いている間に流すと、次の lookup 再生成で M・MS の preset ができてしまう）。",
           "-- ラクマのペンは1カテゴリー（1604 ペン/マーカー → eBay 967）でブランド別にできないので変更しない。",
           "SET XACT_ABORT ON;", "BEGIN TRAN;",
           "IF COL_LENGTH('mst.presets_categories', 'is_active') IS NULL THROW 50001, N'先に presets_categories_is_active_20261116.sql を流す', 1;"]
    for r in p["m_pc"]:
        out.append("INSERT INTO mst.presets_categories (category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id, is_collectibles, vendor_group, is_active) "
                   f"VALUES ({r['category_id']}, {sqlv(r['category_name_ja'])}, NULL, {sqlv(r['category_id_ebay'])}, NULL, {r['brand_id']}, "
                   f"{sqlv(r['is_collectibles'])}, {sqlv(M_VENDOR)}, 0);  -- {names.get(r['brand_id'])}")
    for b, (old, new) in BRAND_EN_FIX.items():
        out.append(f"UPDATE mst.presets_brand SET default_brand_en = {sqlv(new)} WHERE brand_id = {b} AND default_brand_en = {sqlv(old)};"
                   f"  -- {names.get(b)} の eBay Brand の誤記")
    out.append("COMMIT;")
    write_file("pen_brand_categories_20261116.sql", out)
    rb = ["-- pen_brand_categories_20261116.sql の戻し（is_active=0 の行なので lookup には影響しない）。",
          "SET XACT_ABORT ON;", "BEGIN TRAN;"]
    for r in p["m_pc"]:
        rb.append(f"DELETE FROM mst.presets_categories WHERE vendor_group = {sqlv(M_VENDOR)} AND category_id = {r['category_id']} "
                  f"AND brand_id = {r['brand_id']} AND category_id_ebay = {sqlv(r['category_id_ebay'])} AND is_active = 0;")
    for b, (old, new) in BRAND_EN_FIX.items():
        rb.append(f"UPDATE mst.presets_brand SET default_brand_en = {sqlv(old)} WHERE brand_id = {b} AND default_brand_en = {sqlv(new)};")
    rb.append("COMMIT;")
    write_file("pen_brand_categories_20261116_rollback.sql", rb)
    write_file("presets_categories_is_active_20261116.sql", [
        "-- mst.presets_categories に is_active を足す（2026-11-16）。既存の行はすべて 1（今と同じ）。",
        "-- lookup 生成（daily_check.load_lookup_sources）は is_active=1 の行だけを使う。0 の行は「マスターとして持つが preset を作らない」。",
        "-- 古いコードは列を選ばないので、列を足しても今の動きは変わらない（コードより先に流す）。",
        "SET XACT_ABORT ON;",
        "IF COL_LENGTH('mst.presets_categories', 'is_active') IS NULL",
        "    ALTER TABLE mst.presets_categories ADD is_active BIT NOT NULL CONSTRAINT DF_presets_categories_is_active DEFAULT (1);"])
    write_file("presets_categories_is_active_20261116_rollback.sql", [
        "-- presets_categories_is_active_20261116.sql の戻し。先に is_active=0 の行（pen_brand_categories_20261116_rollback.sql）を消し、",
        "-- lookup 生成のコードを戻してから流す（新しいコードは列が無いと lookup 再生成で失敗する）。",
        "SET XACT_ABORT ON;",
        "IF EXISTS (SELECT 1 FROM mst.presets_categories WHERE is_active = 0) THROW 50001, N'is_active=0 の行が残っている', 1;",
        "ALTER TABLE mst.presets_categories DROP CONSTRAINT DF_presets_categories_is_active;",
        "ALTER TABLE mst.presets_categories DROP COLUMN is_active;"])


def write_file(name, lines):
    with open(os.path.join(HERE, name), "w", encoding="utf-8", newline="\r\n") as f:
        f.write("\n".join(lines) + "\n")
    print("wrote", name)


# ---------------------------------------------------------------------------
# 検証（本番DBは読むだけ。lookup の生成を登録前・段階1・段階2のマスターでメモリ上で行って比べる）
# ---------------------------------------------------------------------------
def ns(d, **kw):
    x = dict(d)
    x.update(kw)
    return SimpleNamespace(**x)


def apply_pen(src, p):
    """メルカリのペンの不足分（pen_brand_categories_20261116.sql）を足したもの。"""
    s = {k: list(v) for k, v in src.items()}
    # メルカリの行は is_active=0 で入れるので、load_lookup_sources（is_active=1 だけ）には現れない
    s["categories"] += [ns(r, vendor_group=M_VENDOR) for r in p["m_pc"] if r.get("is_active", 0) == 1]
    brands = []
    for b in s["brands"]:
        fix = BRAND_EN_FIX.get(b.brand_id)
        if fix and b.default_brand_en == fix[0]:
            b = SimpleNamespace(brand_id=b.brand_id, brand_name_ja=b.brand_name_ja, default_brand_en=fix[1], is_active=b.is_active)
        brands.append(b)
    s["brands"] = brands
    return s


def apply_plan(src, p, stage):
    """load_lookup_sources の結果に登録案を足したもの（stage=1 マスター、2 = マスター＋価格帯）。"""
    s = {k: list(v) for k, v in src.items()}
    s["categories"] += [ns(r, vendor_group=Y_VENDOR) for r in p["pc"]]
    s["cgc_rows"] += [ns(r, vendor_group=Y_VENDOR) for r in p["cgc"]]
    s["vbm_rows"] += [SimpleNamespace(brand_id=r["brand_id"], vendor_name=Y_VENDOR, vendor_brand_id=r["vendor_brand_id"], is_primary=True)
                      for r in p["vbm"]]
    s["bcgv_rows"] += [SimpleNamespace(brand_id=r["brand_id"], category_group=r["category_group"], vendor_name=Y_VENDOR) for r in p["bcgv"]]
    s["bcgv_rows"] += [SimpleNamespace(**r) for r in p["keep_other"]]
    if stage >= 2:
        s["price_ranges"] += [SimpleNamespace(preset_group=ACCOUNT, category_group=r["category_group"], vendor_code=Y_CODE,
                                              low_usd_target=r["low_usd_target"], high_usd_target=r["high_usd_target"],
                                              delete_flag=False, keep_low_usd_target=None, keep_high_usd_target=None)
                              for r in p["price"]]
    return s


def verify(cur):
    from apps.inventory import daily_check as dc
    from apps.adapters import yahoo_fleamarket as yf

    p = plan(cur)
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("OK   " if cond else "NG   ") + msg)
        ok = ok and cond

    # 主キー・一意制約（本番の行と合わせて）
    pk_pc = Counter((r["category_id"], r["brand_id"]) for r in p["pc"])
    check(max(pk_pc.values()) == 1, f"presets_categories 案の主キー重複なし（{len(p['pc'])}行）")
    exist_pc = {(r.category_id, r.brand_id) for r in cur.execute(
        "SELECT category_id, brand_id FROM mst.presets_categories WHERE vendor_group = ?", Y_VENDOR).fetchall()}
    check(not (set(pk_pc) & exist_pc), "presets_categories の Y の既存行と重ならない")
    used = {r.vendor_brand_id: r.brand_id for r in cur.execute(
        "SELECT vendor_brand_id, brand_id FROM mst.vendor_brand_map WHERE vendor_name = ?", Y_VENDOR).fetchall()}
    dup_y = [r for r in p["vbm"] if r["vendor_brand_id"] in used]
    check(not dup_y and len({r["vendor_brand_id"] for r in p["vbm"]}) == len(p["vbm"]),
          f"vendor_brand_map の Yahoo!フリマID が一意（既存の Y 行とも重ならない）{dup_y}")
    exist_bcgv = {(r.brand_id, r.category_group) for r in cur.execute(
        "SELECT brand_id, category_group FROM mst.brand_category_group_vendors WHERE vendor_name = ?", Y_VENDOR).fetchall()}
    check(not ({(r["brand_id"], r["category_group"]) for r in p["bcgv"]} & exist_bcgv), "brand_category_group_vendors の重複なし")
    check(47524 not in {r["category_id"] for r in p["pc"]}, "製図板 47524 を登録しない")
    check(len(p["price"]) == len(GROUPS), f"価格帯の元（メルカリの行）が5グループそろう {[r['category_group'] for r in p['price']]}")

    src = dc.load_lookup_sources(cur)
    base_rows, _ = dc.build_presets_lookup(src)
    s1_rows, _ = dc.build_presets_lookup(apply_plan(src, p, 1))
    s2_rows, _ = dc.build_presets_lookup(apply_plan(src, p, 2))
    cols = dc.LOOKUP_INSERT_COLUMNS
    base = {r[0]: r for r in base_rows}
    s1 = {r[0]: r for r in s1_rows}
    s2 = {r[0]: r for r in s2_rows}
    check(s1 == base, f"段階1（マスターだけ）: lookup は登録前と完全に同じ（{len(base)}行）")
    added = {k: v for k, v in s2.items() if k not in base}
    changed = [k for k in base if k in s2 and s2[k] != base[k]]
    lost = [k for k in base if k not in s2]
    check(not changed and not lost, f"段階2: 既存の lookup は変わらない（変化 {len(changed)}・消失 {len(lost)}）")
    i_vendor, i_group, i_brand, i_cat = cols.index("vendor_name"), cols.index("category_group"), cols.index("brand_id"), cols.index("category_id")
    i_ebay, i_type = cols.index("category_id_ebay"), cols.index("type_ebay")
    check(all(v[i_vendor] == Y_VENDOR and v[i_group] in GROUPS for v in added.values()),
          f"段階2: 追加は Y の5グループだけ（{len(added)}行）")
    by_group = Counter(v[i_group] for v in added.values())
    print("     追加の内訳:", dict(by_group))

    # Yahoo!フリマ の preset の決め方（{(Yahoo!フリマのブランドID, カテゴリーID): preset}）が一意
    keys = Counter((v[i_brand], v[i_cat]) for v in s2.values() if v[i_vendor] == Y_VENDOR)
    dups = [k for k, n in keys.items() if n > 1]
    check(not dups, f"Y の (ブランドID, カテゴリーID) が一意（preset の決め方が1つ）{dups[:5]}")
    check(not any(v[i_cat] == 47524 for v in added.values()), "製図板の preset なし")
    check(all(v[i_brand] == yf.NO_BRAND_ID for v in added.values() if v[i_group] == "製図用品"), "製図用品はブランドなし preset だけ")
    check(all(v[i_type] is None for v in added.values() if v[i_group] == "レンズ"), "レンズの type_ebay は NULL（出品時に決める）")

    # ペン: ブランド別 eBay カテゴリーがメルカリの規則と一致
    y_to_brand = {r["vendor_brand_id"]: r["brand_id"] for r in p["vbm"]}
    y_to_brand.update({vb: b for vb, b in used.items()})
    m_rule = {(BRAND_MERGE.get(r.brand_id, r.brand_id), int(r.category_id)): r.category_id_ebay for r in cur.execute(
        "SELECT brand_id, category_id, category_id_ebay FROM mst.presets_categories WHERE vendor_group = ? AND category_id IN (5249, 5262)",
        M_VENDOR).fetchall()}
    for r in p["pen_additions"]:   # 足す組（メルカリに足す行と Y だけの組）も同じ規則
        m_rule[(BRAND_MERGE.get(r["brand_id"], r["brand_id"]), r["category_id"])] = r["category_id_ebay"]
    to_m = {47303: 5249, 47315: 5262}
    bad, n_dep = [], 0
    for v in added.values():
        if v[i_cat] in to_m:
            n_dep += 1
            b = y_to_brand.get(v[i_brand])
            if m_rule.get((b, to_m[v[i_cat]])) != v[i_ebay]:
                bad.append((v[0], b, v[i_ebay], m_rule.get((b, to_m[v[i_cat]]))))
    check(not bad and n_dep > 0, f"ペンのボールペン・万年筆 {n_dep} preset の eBay カテゴリーがメルカリと同じ規則 {bad[:5]}")
    missing = sorted({(b, c) for (b, c) in m_rule if b in BRANDS or b in y_to_brand.values()}
                     - {(y_to_brand.get(v[i_brand]), to_m[v[i_cat]]) for v in added.values() if v[i_cat] in to_m})
    print(f"     メルカリにあって Y に無いペンの組（Y の販売中0件などで未登録のブランド）: {missing}")

    # 既存の谷川④ 腕時計の Y preset に影響しない（上の「既存の lookup は変わらない」に含まれるが、件数も表示）
    watch = [k for k, v in s2.items() if v[i_vendor] == Y_VENDOR and v[i_group] == "腕時計"]
    watch0 = [k for k, v in base.items() if v[i_vendor] == Y_VENDOR and v[i_group] == "腕時計"]
    check(watch == watch0, f"谷川④ 腕時計の Y preset は同じ（{len(watch0)}）")

    # preset の内訳（グループ×カテゴリー）
    detail = defaultdict(Counter)
    for v in added.values():
        detail[v[i_group]][v[i_cat]] += 1
    for g in GROUPS:
        print(f"     {g}: " + ", ".join(f"{c}={n}" for c, n in sorted(detail[g].items())))

    # メルカリのペンの不足分（pen_brand_categories_20261116.sql）: 単独で流した場合と、Y の段階2と合わせた場合
    pk_m = Counter((r["category_id"], r["brand_id"]) for r in p["m_pc"])
    exist_m = {(int(r.category_id), r.brand_id) for r in cur.execute(
        "SELECT category_id, brand_id FROM mst.presets_categories WHERE vendor_group = ?", M_VENDOR).fetchall()}
    check(max(pk_m.values()) == 1 and not (set(pk_m) & exist_m), f"メルカリのペンの追加 {len(p['m_pc'])}行は主キーが既存と重ならない")
    mp = {r[0]: r for r in dc.build_presets_lookup(apply_pen(src, p))[0]}
    i_en = cols.index("default_brand_en")
    m_added = {k: v for k, v in mp.items() if k not in base}
    m_changed = {k for k in base if k in mp and mp[k] != base[k]}
    m_lost = [k for k in base if k not in mp]
    check(not m_added, f"メルカリのペン: is_active=0 なので lookup に preset は増えない（M・MS の収集対象は今のまま）{sorted(m_added)[:5]}")
    # 参考: 将来メルカリで有効にした（is_active=1 にした）場合に増える preset
    future = {r[0]: r for r in dc.build_presets_lookup(dict(apply_pen(src, p), categories=list(src["categories"]) + [
        ns(r, vendor_group=M_VENDOR) for r in p["m_pc"]]))[0]}
    future_added = sorted(k for k in future if k not in base)
    print(f"     参考: メルカリで有効にすると増える M・MS の preset {len(future_added)}: {', '.join(future_added)}")
    only_brand_fix = all(
        [i for i in range(len(cols)) if mp[k][i] != base[k][i]] == [i_en] and mp[k][i_en] in {n for _, n in BRAND_EN_FIX.values()}
        for k in m_changed)
    check(not m_lost and only_brand_fix, f"メルカリのペン: 既存の lookup で変わるのは eBay Brand の誤記の修正だけ（{len(m_changed)}行 {sorted(m_changed)}）")
    m_bad = [(k, future[k][i_ebay]) for k in future_added
             if m_rule.get((BRAND_MERGE.get(future[k][i_brand], future[k][i_brand]), future[k][i_cat])) != future[k][i_ebay]]
    check(not m_bad, f"メルカリのペン: 将来有効にしたときの preset の eBay カテゴリーもブランドの行どおり {m_bad[:5]}")

    both = {r[0]: r for r in dc.build_presets_lookup(apply_plan(apply_pen(src, p), p, 2))[0]}
    y_both = {k: v for k, v in both.items() if v[i_vendor] == Y_VENDOR}
    y_s2 = {k: v for k, v in s2.items() if v[i_vendor] == Y_VENDOR}
    y_diff = {k for k in set(y_both) | set(y_s2) if y_both.get(k) != y_s2.get(k)}
    y_only_brand_fix = set(y_both) == set(y_s2) and all(
        [i for i in range(len(cols)) if y_both[k][i] != y_s2[k][i]] == [i_en] for k in y_diff)
    check(y_only_brand_fix, "メルカリのペンの SQL を流しても流さなくても Y の preset は同じ（違いは eBay Brand の誤記の修正だけ "
          f"{sorted(y_diff)}。Y は写した行だけを使う）")
    print("結果:", "全項目 OK" if ok else "NG あり")
    return ok


if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..")))
    sys.stdout.reconfigure(encoding="utf-8")
    from apps.common.utils import get_sql_server_connection
    conn = get_sql_server_connection()
    cur = conn.cursor()
    try:
        if sys.argv[1:] == ["write"]:
            write(cur)
        elif sys.argv[1:] == ["verify"]:
            sys.exit(0 if verify(cur) else 1)
        else:
            print(__doc__)
    finally:
        conn.rollback()
        conn.close()
