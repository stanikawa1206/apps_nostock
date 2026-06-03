import os
import time
import requests
import gzip
from dotenv import load_dotenv

# .envファイルの読み込み
load_dotenv()

# --- 認証情報の読み込み ---
LWA_APP_ID = os.environ.get("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET")
REFRESH_TOKEN_JP = os.environ.get("REFRESH_TOKEN")       # 日本用
REFRESH_TOKEN_US = os.environ.get("REFRESH_TOKEN_US")    # 北米（US・CA）用

# --- 各国の設定リスト ---
TARGET_MARKETS = [
    {
        "country": "US",
        "marketplace_id": "ATVPDKIKX0DER",
        "endpoint": "https://sellingpartnerapi-na.amazon.com",
        "token": REFRESH_TOKEN_US
    },
    {
        "country": "CA",
        "marketplace_id": "A2EUQ1WTGCTBG2",
        "endpoint": "https://sellingpartnerapi-na.amazon.com",
        "token": REFRESH_TOKEN_US
    },
    {
        "country": "JP",
        "marketplace_id": "A1VC38T7YXB528",
        "endpoint": "https://sellingpartnerapi-fe.amazon.com", # JPはFE(Far East)エンドポイント
        "token": REFRESH_TOKEN_JP
    }
]

REPORT_TYPE = "GET_MERCHANT_LISTINGS_ALL_DATA"

# 💡 出力先のディレクトリを絶対パスに変更（Windowsのパスのため r を付与）
OUTPUT_DIR = r"X:\apps\snapshot\amazon\listings"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_access_token(refresh_token):
    """LWA Access Tokenの取得"""
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": LWA_APP_ID,
        "client_secret": LWA_CLIENT_SECRET
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

def create_report(token, endpoint, marketplace_id):
    """レポート作成リクエスト (Step 1)"""
    url = f"{endpoint}/reports/2021-06-30/reports"
    headers = {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }
    body = {
        "reportType": REPORT_TYPE,
        "marketplaceIds": [marketplace_id]
    }
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    return response.json()["reportId"]

def wait_for_report(token, endpoint, report_id):
    """レポート作成完了までポーリング (Step 2)"""
    url = f"{endpoint}/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": token}
    
    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        status = data.get("processingStatus")
        
        print(f"  -> Current Status: {status}")
        
        if status == "DONE":
            return data["reportDocumentId"]
        elif status in ["CANCELLED", "FATAL"]:
            raise Exception(f"Report generation failed: {status}")
        
        # 30秒待機
        time.sleep(30)

def download_and_save(token, endpoint, document_id, country_code):
    """レポートのダウンロードと解凍・保存 (Step 3)"""
    url = f"{endpoint}/reports/2021-06-30/documents/{document_id}"
    headers = {"x-amz-access-token": token}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    doc_data = response.json()
    download_url = doc_data["url"]
    compression = doc_data.get("compressionAlgorithm")

    file_response = requests.get(download_url)
    file_response.raise_for_status()

    # ファイル名に国コード (US/CA/JP) を含める
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(OUTPUT_DIR, f"listings_{country_code}_{timestamp}.tsv")

    if compression == "GZIP":
        content = gzip.decompress(file_response.content).decode('utf-8')
    else:
        content = file_response.text

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    print(f"  -> SUCCESS! File saved to: {file_path}")

def main():
    for market in TARGET_MARKETS:
        country = market["country"]
        token_src = market["token"]
        endpoint = market["endpoint"]
        marketplace_id = market["marketplace_id"]

        print(f"\n========== 🌍 対象国: {country} のレポート処理を開始 ==========")
        
        if not token_src:
            print(f"⚠️ {country}用のリフレッシュトークンが.envに設定されていないためスキップします。")
            continue

        try:
            # 1. 認証
            print("Accessing LWA Token...")
            access_token = get_access_token(token_src)
            
            # 2. レポート作成依頼
            print(f"Requesting report: {REPORT_TYPE}...")
            report_id = create_report(access_token, endpoint, marketplace_id)
            print(f"  -> Report ID: {report_id}")
            
            # 3. 完了待ち
            print("Waiting for report generation...")
            document_id = wait_for_report(access_token, endpoint, report_id)
            
            # 4. ダウンロードと保存
            print("Downloading and extracting file...")
            download_and_save(access_token, endpoint, document_id, country)
            
        except Exception as e:
            print(f"❌ {country} の処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()