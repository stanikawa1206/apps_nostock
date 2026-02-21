import os
import boto3
from pathlib import Path
from dotenv import load_dotenv
from botocore.config import Config

def test_env_absolute_path():
    # 1. .env の場所を絶対パスで指定
    env_path = Path("/opt/apps_nostock/.env")
    
    print(f"--- .env 絶対パス読み込みテスト ---")
    print(f"Target path: {env_path}")

    # 2. 指定したパスから読み込み
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print("✅ .env ファイルが見つかりました。")
    else:
        print("❌ 失敗: 指定したパスに .env が存在しません。")
        return

    # 3. 値の取得
    endpoint    = os.getenv("R2_ENDPOINT")
    access_key  = os.getenv("R2_ACCESS_KEY_ID")
    secret_key  = os.getenv("R2_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("R2_BUCKET")

    if not secret_key:
        print("❌ 失敗: .env から値を取得できませんでした。")
        return

    print(f"取得確認: Endpoint={endpoint}")
    print(f"取得確認: Access Key={access_key[:5]}...")

    # 4. R2 クライアント作成
    r2 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(signature_version='s3v4')
    )

    # 5. アップロード試行
    try:
        print("アップロードを試行中...")
        r2.put_object(
            Bucket=bucket_name, 
            Key="env_path_test.txt", 
            Body="Absolute Path Test OK"
        )
        print("✅ 成功！絶対パス指定で .env から読み込んでアップロードできました。")
    except Exception as e:
        print(f"❌ 失敗: {e}")

if __name__ == "__main__":
    print("v3")
    test_env_absolute_path()