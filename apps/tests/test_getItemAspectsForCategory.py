import requests
from apps.adapters.ebay_api import get_access_token_new

CATEGORY_IDS = list(dict.fromkeys([
    "183454",
]))

def get_item_aspects():
    token = get_access_token_new("BUZZ")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    for category_id in CATEGORY_IDS:
        print("\n==============================")
        print(f"category_id: {category_id}")

        url = f"https://api.ebay.com/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id={category_id}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"❌ エラー: {response.status_code}")
            print("👉 leafじゃない or USカテゴリじゃない")
            continue

        data = response.json()

        print("=== REQUIRED ONLY ===")
        for aspect in data.get("aspects", []):
            name = aspect.get("localizedAspectName")
            constraint = aspect.get("aspectConstraint", {})

            if constraint.get("aspectRequired"):
                mode = constraint.get("aspectMode")

                # 1行で表示
                line = f"{name} ← 必須 ({mode})"

                # SELECTION_ONLYなら候補も横並び
                if mode == "SELECTION_ONLY":
                    values = [v.get("localizedValue") for v in aspect.get("aspectValues", [])]
                    line += " | 候補: " + ", ".join(values)

                print(line)


if __name__ == "__main__":
    get_item_aspects()