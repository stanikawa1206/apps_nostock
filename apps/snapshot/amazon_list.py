import os
import time
import requests
import gzip
import io
from dotenv import load_dotenv

# .envファイルの読み込み
load_dotenv()

# --- 設定 ---
credentials = {
    "lwa_app_id": os.environ.get("LWA_CLIENT_ID"),
    "lwa_client_secret": os.environ.get("LWA_CLIENT_SECRET"),
    "refresh_token": os.environ.get("REFRESH_TOKEN_US"),
}

REGION_ENDPOINT = "https://sellingpartnerapi-na.amazon.com"
MARKETPLACE_ID = "ATVPDKIKX0DER"  # Amazon.com (US)
REPORT_TYPE = "GET_MERCHANT_LISTINGS_ALL_DATA"
OUTPUT_DIR = "listings"

# 出力ディレクトリの作成
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_access_token():
    """LWA Access Tokenの取得"""
    print("Accessing LWA Token...")
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": credentials["refresh_token"],
        "client_id": credentials["lwa_app_id"],
        "client_secret": credentials["lwa_client_secret"]
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

def create_report(token):
    """レポート作成リクエスト (Step 1)"""
    print(f"Requesting report: {REPORT_TYPE}...")
    url = f"{REGION_ENDPOINT}/reports/2021-06-30/reports"
    headers = {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }
    body = {
        "reportType": REPORT_TYPE,
        "marketplaceIds": [MARKETPLACE_ID]
    }
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    return response.json()["reportId"]

def wait_for_report(token, report_id):
    """レポート作成完了までポーリング (Step 2)"""
    url = f"{REGION_ENDPOINT}/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": token}
    
    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        status = data.get("processingStatus")
        
        print(f"Current Status: {status}")
        
        if status == "DONE":
            return data["reportDocumentId"]
        elif status in ["CANCELLED", "FATAL"]:
            raise Exception(f"Report generation failed: {status}")
        
        # 30秒待機（スロットリング回避とサーバー負荷軽減）
        time.sleep(30)

def download_and_save(token, document_id):
    """レポートのダウンロードと解凍・保存 (Step 3)"""
    print("Getting download URL...")
    url = f"{REGION_ENDPOINT}/reports/2021-06-30/documents/{document_id}"
    headers = {"x-amz-access-token": token}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    doc_data = response.json()
    download_url = doc_data["url"]
    compression = doc_data.get("compressionAlgorithm")

    print(f"Downloading file (Compression: {compression})...")
    file_response = requests.get(download_url)
    file_response.raise_for_status()

    # ファイル名の生成 (listings/listings_20240101_120000.tsv)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(OUTPUT_DIR, f"listings_{timestamp}.tsv")

    # GZIP圧縮されている場合の処理
    if compression == "GZIP":
        content = gzip.decompress(file_response.content).decode('utf-8')
    else:
        content = file_response.text

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    print("-" * 30)
    print(f"SUCCESS!")
    print(f"File saved to: {file_path}")
    print("-" * 30)

def main():
    try:
        # 1. 認証
        token = get_access_token()
        
        # 2. レポート作成依頼
        report_id = create_report(token)
        print(f"Report ID: {report_id}")
        
        # 3. 完了待ち
        document_id = wait_for_report(token, report_id)
        print(f"Document ID: {document_id}")
        
        # 4. ダウンロードと保存
        download_and_save(token, document_id)
        
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()