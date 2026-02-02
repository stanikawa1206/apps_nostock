import os, sys, requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from requests_auth_aws_sigv4 import AWSSigV4

load_dotenv()

AWS_REGION = "us-east-1"
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID_US", "ATVPDKIKX0DER")
API_ENDPOINT = "https://sellingpartnerapi-na.amazon.com"

def get_auth():
    return AWSSigV4(
        "execute-api",
        region=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )

def get_lwa_token():
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": os.getenv("REFRESH_TOKEN_US"),
        "client_id": os.getenv("LWA_CLIENT_ID"),
        "client_secret": os.getenv("LWA_CLIENT_SECRET"),
    })
    r.raise_for_status()
    return r.json()["access_token"]

def get_open_event_group_id(token, auth):
    r = requests.get(f"{API_ENDPOINT}/finances/v0/financialEventGroups", 
                     headers={"x-amz-access-token": token}, auth=auth)
    if r.status_code == 200:
        groups = r.json().get("payload", {}).get("FinancialEventGroupList", [])
        for g in groups:
            if g.get("ProcessingStatus") == "Open":
                return g.get("FinancialEventGroupId")
    return None

def fetch_finances(token, auth, days=180):
    all_events_payloads = []
    open_group_id = get_open_event_group_id(token, auth)
    
    # 未確定分と過去分を両方狙う
    targets = []
    if open_group_id:
        targets.append({"FinancialEventGroupId": open_group_id})
    targets.append({"PostedAfter": (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')})

    for base_params in targets:
        next_token = None
        while True:
            params = base_params.copy()
            if next_token:
                params = {"NextToken": next_token} # NextTokenがある時は他のパラメータは送らない

            r = requests.get(f"{API_ENDPOINT}/finances/v0/financialEvents", 
                             headers={"x-amz-access-token": token}, params=params, auth=auth)
            
            if r.status_code != 200:
                print(f"[ERROR] {r.status_code}: {r.text}")
                break
            
            payload = r.json().get("payload", {})
            events = payload.get("FinancialEvents", {})
            if events:
                all_events_payloads.append(events)
            
            next_token = payload.get("NextToken")
            if not next_token:
                break
            
    return all_events_payloads

def parse_finance_events(events_list):
    rows = []
    for events in events_list:
        # 1. 出荷 (Shipments)
        for ev in events.get("ShipmentEventList", []):
            for item in ev.get("ShipmentItemList", []):
                rows.append({
                    "date": ev.get("PostedDate"),
                    "type": "Shipped",
                    "id": ev.get("AmazonOrderId"),
                    "amount": sum(float(c.get("ChargeAmount", {}).get("CurrencyAmount", 0)) for c in item.get("ItemChargeList", [])),
                    "fee": sum(float(f.get("FeeAmount", {}).get("CurrencyAmount", 0)) for f in item.get("ItemFeeList", [])),
                })
        
        # 2. 返金 (Refunds)
        for ev in events.get("RefundEventList", []):
            for item in ev.get("ShipmentItemAdjustmentList", []):
                rows.append({
                    "date": ev.get("PostedDate"),
                    "type": "Refund",
                    "id": ev.get("AmazonOrderId"),
                    "amount": sum(float(c.get("ChargeAmount", {}).get("CurrencyAmount", 0)) for c in item.get("ItemChargeAdjustmentList", [])),
                    "fee": sum(float(f.get("FeeAmount", {}).get("CurrencyAmount", 0)) for f in item.get("ItemFeeAdjustmentList", [])),
                })

        # 3. サービス手数料 (月額費用・広告等)
        for ev in events.get("ServiceFeeEventList", []):
            rows.append({
                "date": ev.get("PostedDate"),
                "type": "ServiceFee",
                "id": ev.get("FeeDescription", "N/A"),
                "amount": float(ev.get("FeeAmount", {}).get("CurrencyAmount", 0)),
                "fee": 0,
            })
            
    # 重複排除 (Order ID + Date + Type)
    unique_rows = { (r['id'], r['date'], r['type']): r for r in rows }.values()
    return sorted(list(unique_rows), key=lambda x: x['date'] or "", reverse=True)

if __name__ == "__main__":
    token = get_lwa_token()
    auth = get_auth()

    print(f"[INFO] Fetching records (180 days)...")
    payloads = fetch_finances(token, auth, days=180)
    finances = parse_finance_events(payloads)

    print(f"\n取得件数: {len(finances)} 件")
    for r in finances[:20]:
        print(f"{r['date']} | {r['type']:10} | {r['id']:20} | Amt: {r['amount']:8} | Fee: {r['fee']:8}")