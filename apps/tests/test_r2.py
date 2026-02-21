import boto3
from botocore.config import Config

def test_direct():
    # .env を使わず、ここに直接値を書く（テストのため一時的に）
    endpoint    = "https://971a284140f61ec89f737806ab012d11.r2.cloudflarestorage.com"
    access_key  = "3da0759396fdfa1e89c9d1b42a02bbaa"
    secret_key  = "43db03620fc111e7512c7e3291af9f0a4d6b7ad68af18c3f8799c896b2120fc7"
    bucket_name = "ebay-images"

    print("--- 直接指定テスト開始 ---")
    
    r2 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(signature_version='s3v4')
    )

    try:
        r2.put_object(Bucket=bucket_name, Key="direct_test.txt", Body="OK")
        print("✅ 成功！直接指定なら動きます。")
    except Exception as e:
        print(f"❌ 失敗: {e}")

if __name__ == "__main__":
    print("v2")
    test_direct()