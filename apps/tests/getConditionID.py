import requests
from apps.adapters.ebay_api import get_access_token_new
import json
CATEGORY_IDS = [
    "183454",  # トレカ
]

def get_conditions():

    token = get_access_token_new("BUZZ")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    for category_id in CATEGORY_IDS:

        print("\n==============================")
        print(f"category_id: {category_id}")

        url = (
            "https://api.ebay.com/sell/metadata/v1/"
            f"marketplace/EBAY_US/get_item_condition_policies"
            f"?filter=categoryIds:{{{category_id}}}"
        )

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"❌ エラー: {response.status_code}")
            print(response.text)
            continue

        data = response.json()

        print(json.dumps(data, indent=2, ensure_ascii=False))
        return

        for policy in data.get("itemConditionPolicies", []):

            print()
            print(f"ConditionID : {policy.get('conditionId')}")
            print(f"Name        : {policy.get('conditionDescription')}")

            for descriptor in policy.get("conditionDescriptors", []):

                print(
                    f"  DescriptorID={descriptor.get('conditionDescriptorId')}"
                    f" Name={descriptor.get('conditionDescriptorName')}"
                )

                for value in descriptor.get("conditionDescriptorValues", []):

                    print(
                        f"      ValueID={value.get('conditionDescriptorValueId')}"
                        f" Value={value.get('conditionDescriptorValueName')}"
                    )

if __name__ == "__main__":
    get_conditions()