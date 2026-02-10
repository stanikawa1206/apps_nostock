import os
import requests
import boto3
from dotenv import load_dotenv
from pathlib import Path

# .envファイルを読み込む（apps/common/.env を明示的に指定）
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]  # .../apps_nostock
env_path = project_root / "apps" / "common" / ".env"

load_dotenv()

def test_upload():
    # --- 設定情報の取得 (画像94bcdeと完全に一致させる) ---
    # 画像を見ると .env のキーは R2_ENDPOINT / R2_BUCKET になっています
    endpoint    = os.getenv("R2_ENDPOINT")      
    access_key  = os.getenv("R2_ACCESS_KEY_ID")
    secret_key  = os.getenv("R2_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("R2_BUCKET")        
    public_base = os.getenv("R2_PUBLIC_BASE")

    # --- デバッグ表示 (Noneになっていないか確認) ---
    print(f"--- Environment Check ---")
    print(f"Endpoint: {endpoint}")
    print(f"Bucket: {bucket_name}")
    print(f"PublicBase: {public_base}")
    print(f"-------------------------")

    if not all([endpoint, access_key, secret_key, bucket_name]):
        print("❌ エラー: .env から読み込めていない値があります！")
        return

    # エンドポイントの末尾にバケット名が入っていないか確認 (画像92f6a4対策)
    # もしURLが /ebay-images で終わっていたら削除する
    if endpoint.endswith("/" + str(bucket_name)):
        endpoint = endpoint.replace("/" + str(bucket_name), "")

    # --- R2 クライアントの初期化 ---
    r2 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )

    # --- テスト用データ ---
    target_url = "https://static.mercdn.net/item/detail/orig/photos/m33008515431_1.jpg?1767788579"
    # test_key = "test_folder/m33008515431_1.jpg"
    test_key = "m33008515431/1.jpg"



    try:
        # 1. 画像のダウンロード
        print(f"Downloading image: {target_url}")
        res = requests.get(target_url, timeout=30)
        res.raise_for_status()
        print("Download success.")

        # 2. R2へのアップロード
        print(f"Uploading to R2 as: {test_key}...")
        r2.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=res.content,
            ContentType="image/jpeg",
        )
        print("Upload success!")

        # 3. 公開URLの表示
        final_url = f"{public_base}/{test_key}"
        print(f"\nCheck your browser with this URL:\n{final_url}")

    except requests.exceptions.RequestException as e:
        print(f"\n[Error] Failed to download image from Mercari: {e}")
    except Exception as e:
        print(f"\n[Error] R2 Upload failed: {e}")

if __name__ == "__main__":
    test_upload()