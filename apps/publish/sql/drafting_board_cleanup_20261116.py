# -*- coding: utf-8 -*-
"""
過去に誤って集めた製図板の既存データの整理（2026-11-16・1回だけ）。新しい除外ロジックは入れない。

経緯:
  preset「製図用ブラシM / 製図用ブラシMS」は 10/29 ごろまでメルカリの 5316（製図板）を検索していて、その後 5314（製図用ブラシ）に
  直された。新しい取り込みは止まったが、それまでに取り込んだ製図板が trx.vendor_item に残り、出品され続けている。
  5317（その他製図用品 = preset「その他M / その他MS」）に出品者が入れた製図板も同じく整理する。

対象の決め方（この1回だけ。結果は商品IDの一覧として SQL に固定する）:
  - preset が 製図用ブラシM/MS・その他M/MS の商品で、タイトルが製図板の本体（製図板・ライナーボード・平行定規・ドラフター等）
  - 付属品・小物の単品（ドラフタースケール・ドラフティングテープ・製図器セット・専用バッグのみ・マグネットプレート等）は対象外
  - メルカリの実際のカテゴリーが 5314（製図用ブラシ）と分かっている商品は対象外
  - 他の製図用品の preset（定規・コンパス・テンプレート等）は対象外
  判定に使ったメルカリの実際のカテゴリーは 2026-11-16 の検索（5314・5316・5317 の販売中・売り切れ。scratchpad mall.json）。

使い方:
    python -m apps.publish.sql.drafting_board_cleanup_20261116 write   … 一覧・SQL を作る（本番DBは読むだけ）
作るもの（このフォルダー）:
  drafting_board_cleanup_20261116_targets.csv   … 対象の一覧（商品ID・タイトル・URL・preset・実カテゴリー・変更前の状態・出品中の listing）
  drafting_board_cleanup_20261116_end_skus.txt  … eBay で終了する出品の SKU（= vendor_item_id）
  drafting_board_cleanup_20261116.sql           … trx.vendor_item を 出品不可flg=1・出品状況=NG(製図板) にする
  drafting_board_cleanup_20261116_rollback.sql  … vendor_item を変更前の値に戻す（eBay の終了は戻せない）

実施の順番:
  1) eBay の出品終了: python -m apps.publish.sql.drafting_board_end_listings_20261116
     （既存の apps/tests/delete_ebay_listigs_risk_items.py の処理をそのまま使う。EndItems・EndingReason=NotAvailable、
       成功分だけ trx.listings を is_deleted=1・delete_reason=特別、trx.vendor_item を 出品不可flg=1）
  2) drafting_board_cleanup_20261116.sql（出品中のまま残った行は更新しない）
再び候補に入らない理由: 出品候補（dbo.fn_take_one_candidates_base）は 出品不可flg=1 を除く。出品不可flg を 0 に戻すのは
  UNPUBLISHED 出品の再公開成功時（出品状況=保留中の行だけ）だけで、在庫の取り込み・価格変更では変わらない。
"""
import csv
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SCRATCH = os.environ.get("DRAFTING_SCRATCH", "")   # mall.json（メルカリの実カテゴリー）の場所
PRESETS = ["製図用ブラシM", "製図用ブラシMS", "その他M", "その他MS"]
NG_HEAD = "NG(製図板)"
NG_DETAIL = "過去の製図板(5316)収集の整理 2026-11-16"

# 製図板の本体を表す語
BOARD = re.compile(
    r"製図[板版台]|製図\s*ボード|ライナー\s*ボー[ドト]|liner\s*board|ドラフティング\s*ボード|drafting\s*board|drawing\s*boa"
    r"|ラピッド\s*ボード|rapid\s*A\d|TK-?system|TKシステム|TZ-plus|ドラパスボード|DRAPAS\s*BOARD|Drafcompo"
    r"|平行定規(?!用)|並行定規|ドラフター(?!\s*スケール|\s*用)|drafting\s*machine", re.I)
# 本体ではない物（付属品・小物の単品）
NON_BOARD = re.compile(
    r"ドラフター\s*スケール|ドラフター\s*用|製図用スケール|drafter\s*scale|ドラフティング\s*(テープ|ブラシ|ペン|セット)|製図用テープ"
    r"|製図器セット|スクライバー|航海用|ハンドラ|ローリング定規|金属定規|メタル定規|専用携帯バッグ|平行定規用|マグネットプレート"
    r"|コンパスセット|製図セット\s*定規\s*コンパス", re.I)
# 上の語で外れるが、タイトルを見て製図板の本体（＋付属品）と判断したもの
FORCE_INCLUDE = {
    "m40781952894",            # MUTOH アーム式ドラフター & ドラフタースケール
    "m60130671511",            # STAEDTLER 製図板 A2 鞄付 60cm金属定規セット
    "m62019285348",            # MAX ドラフティングマシンα MP-400FL
    "m83270856799",            # MAX ドラフティングマシーンMP-400FLⅡ
    "m74576810070",            # DRAPAS DXM-900N A1平行定規 マグネットプレート付
    "2JGgJEa6avAumkQDAFGo7e",  # Kurflo マグネット製図板(A2) 平行定規
    "2JTtPQwNdD7YMStpWaRpFz",  # MUTOH Drafcompo Jr. JR-A2S 製図板 + ドラフタースケール
}


def classify(title, mercari_cat, item_id):
    """(対象か, 理由)"""
    if item_id in FORCE_INCLUDE:
        return True, "製図板（付属品付き）"
    if mercari_cat == "5314":
        return False, "実カテゴリー5314（製図用ブラシ）"
    if NON_BOARD.search(title):
        return False, "付属品・小物"
    if not BOARD.search(title):
        return False, "製図板の語なし"
    return True, "製図板"


def item_url(vendor_name, item_id):
    if vendor_name == "メルカリshops":
        return f"https://jp.mercari.com/shops/product/{item_id}"
    return f"https://jp.mercari.com/item/{item_id}"


def load(cur):
    cur.execute(
        "SELECT v.vendor_name, v.vendor_item_id, v.preset, v.title_jp, v.status, v.出品不可flg, v.出品状況, v.出品状況詳細, "
        "CONVERT(varchar(19), v.created_at, 120), v.processing_at "
        "FROM trx.vendor_item v WHERE v.preset IN (" + ",".join("?" * len(PRESETS)) + ")", *PRESETS)
    items = cur.fetchall()
    cur.execute(
        "SELECT l.vendor_name, l.vendor_item_id, l.account, CAST(l.listing_id AS varchar(30)) FROM trx.listings l "
        "JOIN trx.vendor_item v ON v.vendor_name = l.vendor_name AND v.vendor_item_id = l.vendor_item_id "
        "WHERE l.is_deleted = 0 AND v.preset IN (" + ",".join("?" * len(PRESETS)) + ")", *PRESETS)
    active = {}
    for vn, iid, acct, lid in cur.fetchall():
        active.setdefault((vn, iid), []).append((acct, lid))
    return items, active


def plan(cur, mercari_cats):
    items, active = load(cur)
    targets, excluded = [], []
    for r in items:
        title = r[3] or ""
        if not BOARD.search(title) and r[1] not in FORCE_INCLUDE:
            continue   # 製図板と関係のない商品（定規・コンパス等）は一覧にも出さない
        mc = str(mercari_cats.get(r[1], ""))
        ok, why = classify(title, mc, r[1])
        row = dict(vendor_name=r[0], vendor_item_id=r[1], preset=r[2], title=title, status=r[4], flg=r[5],
                   jokyo=r[6], shosai=r[7], created_at=r[8], processing=r[9] is not None, mercari_cat=mc, reason=why,
                   url=item_url(r[0], r[1]), listings=active.get((r[0], r[1]), []))
        (targets if ok else excluded).append(row)
    return targets, excluded


def chunked_insert(target, rows, size=500):
    """INSERT ... VALUES は1文1,000行までなので、size 行ずつの文にする。"""
    out = []
    for i in range(0, len(rows), size):
        out.append(f"INSERT INTO {target} VALUES\n    " + ",\n    ".join(rows[i:i + size]) + ";")
    return "\n".join(out)


def sqlv(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    return "N'" + str(v).replace("'", "''") + "'"


def write(cur, mercari_cats):
    targets, excluded = plan(cur, mercari_cats)
    with open(os.path.join(HERE, "drafting_board_cleanup_20261116_targets.csv"), "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["区分", "理由", "仕入先", "商品ID", "タイトル", "URL", "preset", "メルカリの実カテゴリー(11/16検索)", "仕入先の状態",
                    "出品不可flg(変更前)", "出品状況(変更前)", "出品状況詳細(変更前)", "取り込み日時", "出品中(アカウント:listing_id)"])
        for kind, rows in (("対象", targets), ("対象外", excluded)):
            for r in rows:
                w.writerow([kind, r["reason"], r["vendor_name"], r["vendor_item_id"], r["title"], r["url"], r["preset"],
                            r["mercari_cat"] or "不明(検索範囲外)", r["status"], r["flg"], r["jokyo"], r["shosai"], r["created_at"],
                            " ".join(f"{a}:{l}" for a, l in r["listings"])])

    end = [r for r in targets if r["listings"]]
    with open(os.path.join(HERE, "drafting_board_cleanup_20261116_end_skus.txt"), "w", encoding="utf-8", newline="\n") as f:
        for r in end:
            f.write(r["vendor_item_id"] + "\n")

    values = chunked_insert("@t (vendor_name, vendor_item_id)",
                            [f"({sqlv(r['vendor_name'])}, {sqlv(r['vendor_item_id'])})" for r in targets])
    sql = [
        "-- 過去に誤って集めた製図板の trx.vendor_item を出品対象外にする（2026-11-16・1回だけ）。",
        "-- 生成: python -m apps.publish.sql.drafting_board_cleanup_20261116 write（対象の決め方はそのファイル）",
        f"-- 対象 {len(targets)}件（うち出品中 {len(end)}件は先に eBay で終了: drafting_board_end_listings_20261116.py）。",
        f"-- 出品不可flg=1（出品候補から外れる。既存の仕組み）、出品状況={NG_HEAD}、出品状況詳細={NG_DETAIL}。",
        "-- 出品中（trx.listings is_deleted=0）が残っている行と、出品処理中（processing_at あり）の行は更新しない（件数を表示）。",
        "SET XACT_ABORT ON;",
        "SET NOCOUNT ON;",
        "DECLARE @t TABLE (vendor_name NVARCHAR(100) NOT NULL, vendor_item_id NVARCHAR(200) NOT NULL, PRIMARY KEY (vendor_name, vendor_item_id));",
        values,
        "",
        "SELECT COUNT(*) AS 対象, ",
        "       SUM(CASE WHEN a.vendor_item_id IS NOT NULL THEN 1 ELSE 0 END) AS 出品中で更新しない,",
        "       SUM(CASE WHEN v.processing_at IS NOT NULL THEN 1 ELSE 0 END) AS 処理中で更新しない",
        "FROM @t t JOIN trx.vendor_item v ON v.vendor_name = t.vendor_name AND v.vendor_item_id = t.vendor_item_id",
        "LEFT JOIN (SELECT DISTINCT vendor_name, vendor_item_id FROM trx.listings WHERE is_deleted = 0) a",
        "  ON a.vendor_name = t.vendor_name AND a.vendor_item_id = t.vendor_item_id;",
        "",
        "BEGIN TRAN;",
        "UPDATE v SET 出品不可flg = 1, [出品状況] = " + sqlv(NG_HEAD) + ", [出品状況詳細] = " + sqlv(NG_DETAIL),
        "FROM trx.vendor_item v JOIN @t t ON t.vendor_name = v.vendor_name AND t.vendor_item_id = v.vendor_item_id",
        "WHERE v.processing_at IS NULL",
        "  AND NOT EXISTS (SELECT 1 FROM trx.listings l WHERE l.vendor_name = v.vendor_name AND l.vendor_item_id = v.vendor_item_id AND l.is_deleted = 0);",
        "SELECT @@ROWCOUNT AS 更新;",
        "COMMIT;",
    ]
    write_lines("drafting_board_cleanup_20261116.sql", sql)

    rb_values = chunked_insert(
        "@b (vendor_name, vendor_item_id, flg, jokyo, shosai)",
        [f"({sqlv(r['vendor_name'])}, {sqlv(r['vendor_item_id'])}, {sqlv(r['flg'])}, {sqlv(r['jokyo'])}, {sqlv(r['shosai'])})"
         for r in targets])
    rb = [
        "-- drafting_board_cleanup_20261116.sql の戻し: trx.vendor_item の 出品不可flg・出品状況・出品状況詳細を変更前（生成時点）の値に戻す。",
        "-- eBay で終了した出品は戻らない（出し直すなら新しい出品になる）。trx.listings の終了記録も戻さない。",
        "-- 出品状況が整理後のまま（NG(製図板)）の行だけ戻す（その後に別の処理で変わった行は触らない）。",
        "SET XACT_ABORT ON;",
        "SET NOCOUNT ON;",
        "DECLARE @b TABLE (vendor_name NVARCHAR(100) NOT NULL, vendor_item_id NVARCHAR(200) NOT NULL, flg BIT NULL, jokyo NVARCHAR(200) NULL, "
        "shosai NVARCHAR(MAX) NULL, PRIMARY KEY (vendor_name, vendor_item_id));",
        rb_values,
        "BEGIN TRAN;",
        "UPDATE v SET 出品不可flg = b.flg, [出品状況] = b.jokyo, [出品状況詳細] = b.shosai",
        "FROM trx.vendor_item v JOIN @b b ON b.vendor_name = v.vendor_name AND b.vendor_item_id = v.vendor_item_id",
        "WHERE v.[出品状況] = " + sqlv(NG_HEAD) + " AND v.[出品状況詳細] = " + sqlv(NG_DETAIL) + ";",
        "SELECT @@ROWCOUNT AS 戻した;",
        "COMMIT;",
    ]
    write_lines("drafting_board_cleanup_20261116_rollback.sql", rb)
    accounts = {}
    for r in end:
        for a, _ in r["listings"]:
            accounts[a] = accounts.get(a, 0) + 1
    print(f"対象 {len(targets)} / 対象外 {len(excluded)} / 出品中 {len(end)} {accounts}")
    return targets, excluded


def write_lines(name, lines):
    with open(os.path.join(HERE, name), "w", encoding="utf-8", newline="\r\n") as f:
        f.write("\n".join(lines) + "\n")
    print("wrote", name)


if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..")))
    sys.stdout.reconfigure(encoding="utf-8")
    from apps.common.utils import get_sql_server_connection
    if sys.argv[1:] != ["write"]:
        print(__doc__)
        sys.exit(0)
    # メルカリの実カテゴリーは一覧の参考表示用（無くても対象は変わらない。5314 のブラシは NON_BOARD でも外れる）
    cats = {}
    if SCRATCH and os.path.exists(os.path.join(SCRATCH, "mall.json")):
        with open(os.path.join(SCRATCH, "mall.json"), encoding="utf-8") as f:
            cats = {k: v[0] for k, v in json.load(f).items()}
    conn = get_sql_server_connection()
    try:
        write(conn.cursor(), cats)
    finally:
        conn.rollback()
        conn.close()
