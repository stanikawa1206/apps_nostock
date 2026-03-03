import pyodbc
import requests
import base64
import os
from dotenv import load_dotenv
from apps.common.utils import get_sql_server_connection

load_dotenv()

# .env から認証情報を取得
CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

def test_takafumi_complete():
    account_name = "貴文"
    conn = get_sql_server_connection() #
    
    try:
        with conn.cursor() as cur:
            # 1. DBから最新の refresh_token を取得
            cur.execute("SELECT refresh_token FROM mst.ebay_accounts WHERE account = ?", (account_name,))
            row = cur.fetchone()
            if not row:
                print(f"❌ DBにアカウント '{account_name}' が見つかりません。")
                return
            
            db_refresh_token = row[0]

        print(f"--- Testing Account: {account_name} ---")
        
        # 2. refresh_token を使って access_token を新規発行する (Refresh Grant)
        print("Refreshing Access Token...")
        auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
        encoded_auth = base64.b64encode(auth_str.encode()).decode()
        
        token_url = "https://api.ebay.com/identity/v1/oauth2/token"
        headers_token = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded_auth}"
        }
        payload_token = {
            "grant_type": "refresh_token",
            "refresh_token": db_refresh_token,
            "scope": "https://api.ebay.com/oauth/api_scope/sell.inventory"
        }
        
        token_res = requests.post(token_url, headers=headers_token, data=payload_token)
        
        if token_res.status_code != 200:
            print(f"❌ Token Refresh Failed: {token_res.status_code}")
            print(f"Response: {token_res.text}")
            return
            
        new_access_token = token_res.json().get("access_token")
        print("✅ Access Token Refreshed Successfully.")

        # 3. 取得したばかりの access_token で Location API を叩く
        print("Checking eBay Inventory Locations...")
        location_url = "https://api.ebay.com/sell/inventory/v1/location"
        headers_loc = {
            "Authorization": f"Bearer {new_access_token}",
            "Accept": "application/json"
        }
        
        loc_res = requests.get(location_url, headers=headers_loc)
        
        if loc_res.status_code == 200:
            locations = loc_res.json()
            print("✨✨ SUCCESS!! API Connection Verified ✨✨")
            if not locations.get("locations"):
                print("⚠️  Warning: No locations found. (手動出品で拠点が作られていない可能性)")
            else:
                for loc in locations["locations"]:
                    print(f" - LocationKey: {loc.get('merchantLocationKey')}, Name: {loc.get('name')}")
        else:
            print(f"❌ Location API Error: {loc_res.status_code}")
            print(f"Response: {loc_res.text}")

    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_takafumi_complete()