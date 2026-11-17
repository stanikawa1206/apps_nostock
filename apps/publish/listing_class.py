# -*- coding: utf-8 -*-
"""
eBay 出品の区分（Collectibles／非Collectibles）ごとの管理（2026-11-17）。

eBay の月間無料出品枠は 非Collectibles（月 10,000件）と Collectibles（月 50,000件）で別なので、
当日の出品目標・当日の出品成功件数・月間無料出品残数・当日の終了理由を区分ごとに持つ:

    区分              月間無料出品残数               1日の出品目標               当日の終了理由
    非Collectibles    listing_left                   post_target                 close_reason
    Collectibles      listing_left_collectibles      post_target_collectibles    close_reason_collectibles

（すべて mst.ebay_accounts の列。アカウント共通の終了理由の列は持たない。eBay の LIMIT・BLOCKED は
アカウント全体の制限なので、未終了の両区分に同じ値を書く）

区分は商品のカテゴリー（mst.presets_categories.is_collectibles）で決まる。当日の出品成功件数は
trx.listings に区分の列を持たず、出品候補（dbo.fn_take_one_candidates_base）と同じ結合
（仕入先の vendor_group ごとの category_id）で、その日の出品から数える。
"""

NON_COLLECTIBLES = 0
COLLECTIBLES = 1
CLASSES = (NON_COLLECTIBLES, COLLECTIBLES)

LABEL = {NON_COLLECTIBLES: "非Collectibles", COLLECTIBLES: "Collectibles"}

# 区分ごとの mst.ebay_accounts の列名
LEFT_COL = {NON_COLLECTIBLES: "listing_left", COLLECTIBLES: "listing_left_collectibles"}
TARGET_COL = {NON_COLLECTIBLES: "post_target", COLLECTIBLES: "post_target_collectibles"}
CLOSE_COL = {NON_COLLECTIBLES: "close_reason", COLLECTIBLES: "close_reason_collectibles"}

# 当日の終了理由のうち、その日はもう出品しないもの（LIMIT は出品枠を確保すれば再開する一時停止）
FINISHED_REASONS = ("DONE", "EMPTY", "BLOCKED")


def class_of(is_collectibles) -> int:
    return COLLECTIBLES if is_collectibles else NON_COLLECTIBLES


def today_count_sql(account_expr: str, cls: int) -> str:
    """
    アカウントの当日（SQL Server の日付）の、区分 cls の出品成功件数を返すスカラー副問い合わせ。
    account_expr は SQL の式（'?' や 'A.account'）。is_deleted=0 だけを数えるのは従来の当日件数と同じ。
    カテゴリーが分からない出品（商品・preset が見つからない）は非Collectibles に数える。
    """
    cond = "= 1" if cls == COLLECTIBLES else "= 0"
    return f"""(
        SELECT COUNT(*)
        FROM trx.listings L
        LEFT JOIN trx.vendor_item V
            ON V.vendor_name = L.vendor_name AND V.vendor_item_id = L.vendor_item_id
        LEFT JOIN mst.presets_lookup PL ON PL.preset = V.preset
        LEFT JOIN mst.vendors VD ON VD.vendor_name = L.vendor_name
        LEFT JOIN (
            SELECT category_id, vendor_group, MAX(CAST(is_collectibles AS int)) AS is_col
            FROM mst.presets_categories
            GROUP BY category_id, vendor_group
        ) PC ON PC.category_id = PL.category_id AND PC.vendor_group = VD.vendor_group
        WHERE L.account = {account_expr}
          AND CAST(L.start_time AS DATE) = CAST(GETDATE() AS DATE)
          AND L.is_deleted = 0
          AND ISNULL(PC.is_col, 0) {cond}
    )"""


SQL_SELECT_CLASS_STATE = f"""
SELECT
    close_reason, close_reason_collectibles,
    post_target, post_target_collectibles,
    {today_count_sql('A.account', NON_COLLECTIBLES)} AS posted,
    {today_count_sql('A.account', COLLECTIBLES)} AS posted_collectibles
FROM mst.ebay_accounts A
WHERE A.account = ?
"""


def read_class_state(conn, account) -> dict:
    """{区分: {"close": 終了理由, "target": 当日の目標, "posted": 当日の出品成功件数}}（アカウントが無ければ {}）"""
    with conn.cursor() as cur:
        cur.execute(SQL_SELECT_CLASS_STATE, account)
        r = cur.fetchone()
    if not r:
        return {}
    return {
        NON_COLLECTIBLES: {"close": r[0], "target": r[2], "posted": r[4]},
        COLLECTIBLES: {"close": r[1], "target": r[3], "posted": r[5]},
    }


def set_class_close(conn, account, cls, reason) -> bool:
    """区分 cls の終了理由がまだ無い（NULL）ときだけ reason を書く。書いたら True。"""
    col = CLOSE_COL[cls]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE mst.ebay_accounts SET {col} = ? WHERE account = ? AND {col} IS NULL", reason, account)
        n = cur.rowcount
    conn.commit()
    return n > 0


# ===== 同時実行時に目標件数を超えないための処理枠 =====
# 同じアカウントを最大 MAX_PARALLEL_PC 台が並行して出品する。区分ごとに「当日の出品成功件数 + 処理中の件数 < 目標」の
# ときだけ1件を取りに行けるよう、処理中の1件ごとに SQL Server のアプリケーションロック（接続が持つ Session ロック）を
# 1つ持つ（枠番号 1〜slot_count）。空き枠の確認と取得は区分ごとの排他ロック（トランザクション内）で1台ずつ行う。
# 出品成功の記録（trx.listings への INSERT）は枠を持ったまま確定するので、件数と処理中の両方に数えられる時間はあっても
# どちらにも数えられない時間は無い（目標を超えない）。プロセスが落ちて接続が切れると枠は SQL Server が解放する。
SQL_RESERVE_SLOT = """
SET NOCOUNT ON;
DECLARE @gate INT, @posted INT, @target INT, @held INT = 0, @free INT = NULL, @i INT = 1, @got INT = NULL;
BEGIN TRANSACTION;  -- 区分の排他ロック（Transaction）は COMMIT で解放する。接続の autocommit に関係なく同じ動きにする
EXEC @gate = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 10000;
IF @gate >= 0
BEGIN
    SET @posted = {count_sql};
    SET @target = (SELECT ISNULL({target_col}, 0) FROM mst.ebay_accounts WHERE account = ?);
    WHILE @i <= ?
    BEGIN
        IF APPLOCK_TEST('public', CONCAT(?, @i), 'Exclusive', 'Session') = 0
            SET @held += 1;
        ELSE IF @free IS NULL
            SET @free = @i;
        SET @i += 1;
    END
    IF @free IS NOT NULL AND @posted + @held < @target
    BEGIN
        DECLARE @r INT;
        DECLARE @name NVARCHAR(255) = CONCAT(?, @free);
        EXEC @r = sp_getapplock @Resource = @name, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0;
        IF @r >= 0 SET @got = @free;
    END
END
COMMIT TRANSACTION;
SELECT @gate AS gate, @posted AS posted, @target AS target, @held AS held, @got AS slot;
"""


def _slot_prefix(account, cls):
    return f"publish_slot:{account}:{cls}:"


def reserve_class_slot(conn, account, cls, slot_count):
    """
    区分 cls の処理枠を1つ取る。取れたら枠の名前、取れなければ None（目標に達した・他の台の処理中で空きが無い）。
    戻り値2つ目は確認時の値 {"posted", "target", "held"}（取れなかった理由の判定・表示用）。
    """
    prefix = _slot_prefix(account, cls)
    sql = SQL_RESERVE_SLOT.format(count_sql=today_count_sql("?", cls), target_col=TARGET_COL[cls])
    with conn.cursor() as cur:
        cur.execute(sql, (f"publish_gate:{account}:{cls}", account, account, slot_count, prefix, prefix))
        gate, posted, target, held, slot = cur.fetchone()
    conn.commit()  # 枠（Session）は接続が持ち続ける
    info = {"posted": posted, "target": target, "held": held, "gate": gate}
    return (f"{prefix}{slot}" if slot is not None else None), info


def release_class_slot(conn, slot_name) -> None:
    if not slot_name:
        return
    with conn.cursor() as cur:
        cur.execute("EXEC sp_releaseapplock @Resource = ?, @LockOwner = 'Session'", slot_name)
    conn.commit()
