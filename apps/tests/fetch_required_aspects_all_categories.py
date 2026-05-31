import csv
import requests
from apps.adapters.ebay_api import get_access_token_new

CATEGORY_MAPPING_CSV = r"D:\ebay\ebay-mercari-research\data\category_mapping.csv"
OUTPUT_CSV = r"D:\ebay\ebay-mercari-research\data\ebay_required_aspects.csv"


def load_categories():
    categories = []
    with open(CATEGORY_MAPPING_CSV, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        seen = set()
        for row in reader:
            cat_id = row["ebay_category_id"].strip()
            cat_name = row["ebay_category_name"].strip()
            if cat_id not in seen:
                seen.add(cat_id)
                categories.append((cat_id, cat_name))
    return categories


def fetch_required_aspects():
    token = get_access_token_new("BUZZ")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    categories = load_categories()
    rows = []

    for category_id, category_name in categories:
        print(f"\n--- category_id: {category_id} ({category_name}) ---")
        url = (
            f"https://api.ebay.com/commerce/taxonomy/v1/category_tree/0"
            f"/get_item_aspects_for_category?category_id={category_id}"
        )
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"  ERROR {response.status_code}: skipping")
            continue

        data = response.json()

        for aspect in data.get("aspects", []):
            name = aspect.get("localizedAspectName")
            constraint = aspect.get("aspectConstraint", {})

            if not constraint.get("aspectRequired"):
                continue

            mode = constraint.get("aspectMode", "")
            values = ""
            if mode == "SELECTION_ONLY":
                values = "|".join(
                    v.get("localizedValue", "") for v in aspect.get("aspectValues", [])
                )

            print(f"  REQUIRED: {name} ({mode})")
            rows.append({
                "category_id": category_id,
                "category_name": category_name,
                "aspect_name": name,
                "aspect_mode": mode,
                "values": values,
            })

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["category_id", "category_name", "aspect_name", "aspect_mode", "values"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n保存完了: {OUTPUT_CSV} ({len(rows)} 行)")


if __name__ == "__main__":
    fetch_required_aspects()
