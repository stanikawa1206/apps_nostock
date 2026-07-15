# step2_SP_API_amazon_jp_data.py
# -*- coding: utf-8 -*-
#
# 【このプログラムの役割】
# Step1でDBに登録されたASINに対して、日本AmazonのSP-API（公式API）を使い
# 商品タイトル・ブランド・梱包寸法・重量・現在の最安値を取得する。
# 取得したデータをDB（trx.amazon_cross_market_asin）へ書き込む。
# APIの認証トークンが切れた場合は自動で取り直して処理を再開する。
# ここで補完した情報はStep3以降の輸出入判定で使用される。
#
import time
import sys
from datetime import datetime

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
BASE_WAIT_TIME = 2.0

# Step2で処理が必要な商品だけを取得する。
#
# ・タイトルが未取得の商品
# ・Step2をまだ一度も実行していない商品
# ・Step1でKeepa情報が更新された後、まだStep2で最新情報を取得していない商品
#
# ※ タイトル未取得の条件は、
# SP-APIがタイトルだけ返さなかった場合の保険として残している。

SQL_SELECT = """
    SELECT asin
    FROM trx.amazon_cross_market_asin WITH (NOLOCK)
    WHERE step1_flag = 1
      AND (
          jp_price_updated_at IS NULL
          OR jp_price_updated_at < keepa_last_caught_at
      )
    ORDER BY last_seen_at DESC
"""

# UPDATE文にタイムスタンプ更新を追加
SQL_UPDATE = """
UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK)
SET jp_title = ?,
    jp_lowest_price_y = ?,
    jp_brand = ?,
    [length] = ?,
    [width] = ?,
    [height] = ?,
    [actual_weight] = ?,
    jp_price_updated_at = SYSDATETIME(),
    last_seen_at = SYSDATETIME()
WHERE asin = ?
"""

# ==========================================
# 単位変換ヘルパー関数
# ==========================================
# APIから返ってくる長さの値を、単位が何であっても
# センチメートル（cm）に統一して返す。
def convert_to_cm(value, unit):
    """各種長さの単位をセンチメートル(cm)に変換する"""
    if value is None or not unit:
        return None

    unit = unit.lower()
    if unit in ['centimeters', 'centimeter', 'cm']:
        return float(value)
    elif unit in ['millimeters', 'millimeter', 'mm']:
        return float(value) / 10.0
    elif unit in ['meters', 'meter', 'm']:
        return float(value) * 100.0
    elif unit in ['inches', 'inch', 'in']:
        return float(value) * 2.54
    else:
        return float(value)

# APIから返ってくる重量の値を、単位が何であっても
# グラム（g）に統一して返す。
def convert_to_g(value, unit):
    """各種重さの単位をグラム(g)に変換する"""
    if value is None or not unit:
        return None

    unit = unit.lower()
    if unit in ['grams', 'gram', 'g']:
        return float(value)
    elif unit in ['kilograms', 'kilogram', 'kg']:
        return float(value) * 1000.0
    elif unit in ['milligrams', 'milligram', 'mg']:
        return float(value) / 1000.0
    elif unit in ['pounds', 'pound', 'lb', 'lbs']:
        return float(value) * 453.592
    elif unit in ['ounces', 'ounce', 'oz']:
        return float(value) * 28.3495
    else:
        return float(value)

# ==========================================
# データ処理・DB保存関数
# ==========================================
# SP-APIから取得した商品情報と価格情報をDBへ保存する。
# タイトル・ブランド・寸法・重量・最安値を1件ずつ抽出し、
# DBをUPDATEする。失敗時は最大3回まで再試行する。
def process_items(cursor, items, price_map):
    """取得したアイテムリストと価格マップをDBに保存する"""
    count = 0
    for item in items:
        asin = item.get("asin")
        if not asin: continue

        summaries = item.get("summaries", [])
        if not summaries: continue

        summary = summaries[0]
        attr = item.get("attributes", {})

        # 1. タイトル、ブランド、価格の取得
        title = (summary.get("itemName") or "")[:255]
        brand = summary.get("brand") or attr.get("brand", [{}])[0].get("value")
        if brand: brand = str(brand).strip()
        price = price_map.get(asin)

        # 2. パッケージ寸法の取得と cm への変換
        pkg_dims = attr.get("item_package_dimensions", [{}])[0] if attr.get("item_package_dimensions") else {}
        length_cm = convert_to_cm(pkg_dims.get("length", {}).get("value"), pkg_dims.get("length", {}).get("unit"))
        width_cm  = convert_to_cm(pkg_dims.get("width", {}).get("value"), pkg_dims.get("width", {}).get("unit"))
        height_cm = convert_to_cm(pkg_dims.get("height", {}).get("value"), pkg_dims.get("height", {}).get("unit"))

        # 3. パッケージ重量の取得と g への変換
        pkg_weight = attr.get("item_package_weight", [{}])[0] if attr.get("item_package_weight") else {}
        weight_g = convert_to_g(pkg_weight.get("value"), pkg_weight.get("unit"))

        # 4. DBへの更新処理（失敗時は最大3回リトライ）
        for retry in range(3):
            try:
                # SQLのパラメータ順に合わせる (タイトル, 価格, ブランド, 長さ, 幅, 高さ, 重量, ASIN)
                cursor.execute(SQL_UPDATE, [
                    title, price, brand,
                    length_cm, width_cm, height_cm, weight_g,
                    asin
                ])
                cursor.connection.commit()
                count += 1
                break
            except Exception as e:
                if "timeout" in str(e).lower() or "deadlock" in str(e).lower():
                    time.sleep(1)
                    continue
                print(f"  [DB Update Error] {asin}: {e}")
                cursor.connection.rollback()
                break
    return count

# ==========================================
# メイン処理
# ==========================================
# プログラムの起動点。
# DBからJPデータが未取得または古いASINを抽出し、
# 10件ずつSP-APIへ問い合わせてDBを更新する。
# APIトークンが切れた場合は自動で再取得して最初から処理し直す。
def main():
    # 全体を無限ループで囲み、トークン切れ時にリスタートできるようにする
    while True:
        conn = None
        try:
            print("\n=== JPデータ更新処理を開始（または再開）します ===")
            conn = get_sql_server_connection()
            cursor = conn.cursor()

            # JP情報が未取得または古いASINをDBから取得する
            cursor.execute(SQL_SELECT)
            target_asins = [row[0] for row in cursor.fetchall()]

            all_asins = len(target_asins)
            print(f"更新対象: {len(target_asins)}件")
            if not target_asins:
                print("処理対象のASINがありませんでした。")
                break # 正常終了

            # JP用のAPIアクセストークンを取得する
            token = get_spapi_access_token("JP")
            total_processed = 0

            # 10件ずつバッチに分けてAPIを呼び出す
            for i in range(0, len(target_asins), BATCH_SIZE):
                batch = target_asins[i : i + BATCH_SIZE]

                try:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing batch {i} - {i+len(batch)} (all {all_asins})")
                    start_time = time.perf_counter()

                    # ① カタログ情報の取得 (寸法データもここに含まれます)
                    items = get_spapi_items_batch(batch, "JP", token)
                    if items is None:
                        raise ValueError("NEED_RESTART")

                    # ② 価格情報の取得
                    price_map = get_spapi_prices_batch(batch, "JP", token)

                    # ③ DB保存処理
                    if items:
                        processed_count = process_items(cursor, items, price_map)
                        total_processed += processed_count

                    execution_time = time.perf_counter() - start_time
                    print(f"  実行時間: {execution_time:.3f} 秒")

                except Exception as e:
                    error_msg = str(e)
                    # トークン切れが検知された場合は外側のループへ飛ばして再起動する
                    if "Unauthorized" in error_msg or "expired" in error_msg or str(e) == "NEED_RESTART":
                        print("!!! トークン期限切れ検知。プログラムの最初からやり直します !!!")
                        raise ValueError("NEED_RESTART") # 外側の例外処理へ飛ばす

                    print(f"  [Error Occurred] {error_msg}")
                    raise e # 予期せぬエラーはそのまま投げる

                time.sleep(BASE_WAIT_TIME)

            print(f"=== 正常終了: 合計 {total_processed}件 処理しました ===")
            break # 全て完了したらループを抜ける

        except Exception as e:
            if str(e) == "NEED_RESTART":
                print("5秒後に再起動します...")
                time.sleep(5)
                continue # ループの先頭に戻ってやり直す
            else:
                print(f"致命的なエラーのため終了します: {e}")
                sys.exit(1)

        finally:
            # やり直す場合でも終了する場合でも、DB接続は必ず閉じる
            if conn:
                conn.close()

if __name__ == "__main__":
    main()
