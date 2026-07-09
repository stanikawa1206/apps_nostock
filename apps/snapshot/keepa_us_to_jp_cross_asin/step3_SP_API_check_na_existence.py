# step3_SP_API_check_na_existence.py
# -*- coding: utf-8 -*-
#
# 【このプログラムの役割】
# Step2でJP情報が揃ったASINを対象に、米国（US）・カナダ（CA）の
# AmazonにもそのASINが存在するかをSP-API（公式API）で確認する。
# 存在する場合は現地の最安値も取得し、DBへ記録する。
# US・CAを交互に10件ずつ処理することで、APIのレート制限を回避する。
# ここで得た存在フラグと価格がStep5の輸出入判定に使用される。
#
import time
import sys

from my_utils import (
    get_sql_server_connection,
    get_spapi_access_token,
    get_spapi_items_batch,
    get_spapi_prices_batch
)

# ==========================================
# 設定
# ==========================================
BATCH_SIZE = 10
BASE_WAIT_TIME = 1.2

# 処理対象のマーケット設定
TARGET_MARKETS = [
    {"region": "US", "col_exist": "us_existence", "col_price": "us_lowest_price_d", "col_updated": "us_price_updated_at"},
    {"region": "CA", "col_exist": "ca_existence", "col_price": "ca_lowest_price_d", "col_updated": "ca_price_updated_at"}
]

# ==========================================
# 1バッチ（最大10件）の処理を行う関数
# ==========================================
# 指定された国（US or CA）に対して最大10件のASINを一括で確認する。
# カタログAPIで存在を確認し、存在するASINのみ価格APIを呼び出す。
# 結果（存在フラグと価格）をDBへ書き込む。
# エラー時はトークンを再取得して1回だけリトライする。
def process_batch(conn, region, batch, col_exist, col_price, col_updated, token):
    cursor = conn.cursor()

    # 存在する場合のUPDATE文（存在フラグ=1、現地価格を記録）
    sql_update_exist = f"""
        UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK)
        SET {col_exist} = 1, {col_price} = ?, {col_updated} = SYSDATETIME(), last_seen_at = SYSDATETIME()
        WHERE asin = ?
    """

    # 存在しない場合のUPDATE文（存在フラグ=2、価格は記録しない）
    sql_update_not_exist = f"""
        UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK)
        SET {col_exist} = 2, {col_updated} = SYSDATETIME(), last_seen_at = SYSDATETIME()
        WHERE asin = ?
    """
    current_retry = 0
    max_retries = 1

    while current_retry <= max_retries:
        try:
            # ① カタログAPIで存在確認
            items = get_spapi_items_batch(batch, region, token)
            found_asins = [item["asin"] for item in items if "asin" in item]

            # ② 存在するASINのみPricing APIを呼び出す
            price_map = {}
            if found_asins:
                price_map = get_spapi_prices_batch(found_asins, region, token)

            # ③ DBの更新処理（存在する場合と存在しない場合で別々のSQL）
            for asin in batch:
                if asin in found_asins:
                    price = price_map.get(asin)
                    cursor.execute(sql_update_exist, [price, asin])
                else:
                    cursor.execute(sql_update_not_exist, [asin])

            conn.commit()
            return token, True

        except Exception as e:
            print(f"  [{region} Error] {e}")
            if current_retry >= max_retries:
                print(f"  [{region} Critical] 最大リトライ回数を超えました。")
                return token, False

            # エラー時はトークンを再取得してリトライする
            print(f"  [{region}] トークンを再取得してリトライします...")
            try:
                token = get_spapi_access_token(region)
                current_retry += 1
                time.sleep(5)
            except:
                return token, False

    return token, False

# ==========================================
# DBから対象ASINを抽出する関数
# ==========================================
# Step2でJP価格が取得済みで、かつ指定の国（US or CA）の
# 存在確認がまだ行われていないASINをDBから抽出する。
# Keepaで新たに発見されたASINも再確認対象に含める。
def get_target_asins(conn, col_exist, col_updated):
    cursor = conn.cursor()
    
    sql_select = f"""
        SELECT asin FROM trx.amazon_cross_market_asin WITH (NOLOCK)
        WHERE step1_flag = 1
          AND jp_price_updated_at IS NOT NULL  -- Step2が完了している
          AND (
              ({col_exist} = 0 OR {col_exist} IS NULL)
              OR {col_updated} IS NULL
              OR {col_updated} < keepa_last_caught_at
          )
    """
    try:
        cursor.execute(sql_select)
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"DB接続/抽出エラー({col_exist}): {e}")
        return []

# ==========================================
# 実行エントリーポイント
# ==========================================
# プログラムの起動点。
# US・CAそれぞれの処理対象ASINをDBから取得し、
# 2つの国を交互に10件ずつ処理していく。
# どちらかの国でエラーが続いた場合はその国だけ処理を中断する。
def main():
    try:
        conn = get_sql_server_connection()
    except Exception as e:
        print(f"DB接続エラー: {e}")
        sys.exit(1)

    # US・CAそれぞれの処理対象ASINとトークンを準備する
    market_states = []
    for market in TARGET_MARKETS:
        region = market["region"]
        col_exist = market["col_exist"]
        col_updated = market["col_updated"] # 💡 修正: 引数不足を解消

        asins = get_target_asins(conn, col_exist, col_updated)
        print(f"[{region}] 存在確認＆価格取得対象: {len(asins)}件")

        # 処理対象がある場合はAPIトークンを取得する
        token = None
        if asins:
            try:
                token = get_spapi_access_token(region)
            except Exception as e:
                print(f"[{region}] 初期トークン取得失敗: {e}")
                asins = []

        market_states.append({
            "config": market,
            "asins": asins,
            "token": token,
            "current_index": 0,
            "total": len(asins),
            "is_active": len(asins) > 0
        })

    print(f"\n{'='*40}")
    print("=== 交互バッチ処理を開始します ===")
    print(f"{'='*40}")

    # US・CAを交互に処理し、両方完了したらループを抜ける
    while True:
        active_markets = [m for m in market_states if m["is_active"]]

        if not active_markets:
            break

        for state in active_markets:
            region = state["config"]["region"]
            idx = state["current_index"]
            # 現在の位置から最大10件を取り出してバッチ処理する
            batch = state["asins"][idx : idx + BATCH_SIZE]

            print(f"[{region}] Checking batch {idx} - {idx + len(batch)} / 対象総数: {state['total']}件")

            new_token, success = process_batch(
                conn,
                region,
                batch,
                state["config"]["col_exist"],
                state["config"]["col_price"],
                state["config"]["col_updated"], # 💡 修正: 引数追加
                state["token"]
            )

            state["token"] = new_token

            # 成功した場合は次のバッチへ進み、失敗した場合はその国の処理を中断する
            if not success:
                print(f"[{region}] エラーが継続したため、この国の処理を中断します。")
                state["is_active"] = False
            else:
                state["current_index"] += BATCH_SIZE

                if state["current_index"] >= state["total"]:
                    print(f"=== [{region}] 処理完了: {state['total']}件 ===")
                    state["is_active"] = False

            time.sleep(BASE_WAIT_TIME)

    conn.close()
    print("\n=== 全ての対象マーケットの処理が完了しました ===")

if __name__ == "__main__":
    main()
