import os
import boto3
import socket
from pathlib import Path
from dotenv import load_dotenv
from botocore.config import Config

def test_vps_final():
    # 実行環境に合わせて .env のパスを切り替える
    if os.name == 'nt':  # Windowsの場合
        env_path = Path(r"D:\apps_nostock\.env")
    else:                # Linux/VPSの場合
        env_path = Path("/opt/apps_nostock/.env")
    
    print(f"--- 接続テスト開始 ---")
    print(f"OS: {os.name}, Path: {env_path}")

    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print("✅ .env を読み込みました")
    else:
        print(f"❌ .env が見つかりません: {env_path}")
        return

    # 取得時に .strip() を行うことで、目に見えない改行コードを消し去る
    # これが SignatureDoesNotMatch 対策の最重要ポイントです
    r_endpoint    = os.getenv("R2_ENDPOINT", "").strip()
    r_access_key  = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    # test_vps_final 関数の中身を以下のように修正
    r_secret_key = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    
    # 【重要チェック】
    # repr() を使うことで、末尾に '\r' などが残っていないか可視化します
    print(f"DEBUG: Secret Key Length = {len(r_secret_key)}")
    print(f"DEBUG: Secret Key Raw Value = {repr(r_secret_key)}") 

    if len(r_secret_key) != 64:
        print(f"⚠️ 警告: R2のシークレットキーは通常64文字ですが、{len(r_secret_key)}文字になっています。")
    r_bucket_name = os.getenv("R2_BUCKET", "").strip()


    if not r_secret_key:
        print("❌ .env の中身が空、あるいは読み込めていません")
        return

    print(f"確認: Endpoint = {r_endpoint}")

    # クライアント作成
    r2 = boto3.client(
        "s3",
        endpoint_url=r_endpoint,
        aws_access_key_id=r_access_key,
        aws_secret_access_key=r_secret_key,
        region_name="auto",
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
    )

    try:
        r2.put_object(Bucket=r_bucket_name, Key="final_test.txt", Body="Fixed")
        print("✅ 成功！ .env 経由でアップロードできました。")
    except Exception as e:
        print(f"❌ 失敗: {e}")

if __name__ == "__main__":
    test_vps_final()