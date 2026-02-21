import os
import boto3
from dotenv import load_dotenv
from botocore.config import Config

load_dotenv()

def test_upload():
    r2_endpoint    = os.getenv("R2_ENDPOINT")
    r2_access_key  = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret_key  = os.getenv("R2_SECRET_ACCESS_KEY")
    if r2_secret_key:
            print(f"DEBUG: Key Length = {len(r2_secret_key)}")
            print(f"DEBUG: Key Raw = {repr(r2_secret_key)}")

    r2_bucket_name = os.getenv("R2_BUCKET")

    print(f"--- 接続設定確認 ---")
    print(f"Endpoint: {r2_endpoint}")
    print(f"Access Key: {r2_access_key}")
    print(f"Bucket: {r2_bucket_name}")
    print(f"------------------")

    # endpoint整形（昨日動いていたverのロジック）
    if r2_endpoint and r2_bucket_name and r2_endpoint.endswith("/" + r2_bucket_name):
        r2_endpoint = r2_endpoint.replace("/" + r2_bucket_name, "")

    r2 = boto3.client(
        "s3",
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        region_name="auto",
        config=Config(signature_version='s3v4') # 明示的にs3v4を指定
    )

    try:
        print("テストアップロードを開始します...")
        r2.put_object(
            Bucket=r2_bucket_name,
            Key="test_connection.txt",
            Body="OK",
            ContentType="text/plain",
        )
        print("✅ 成功！ R2との接続・アップロードは正常です。")
    except Exception as e:
        print(f"❌ 失敗: {e}")

if __name__ == "__main__":
    test_upload()