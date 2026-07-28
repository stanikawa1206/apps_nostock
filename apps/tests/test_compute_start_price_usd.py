from apps.common.utils import compute_start_price_usd


def main():

    cost_jpy = 100000

    mode = "GA"
    low_usd_target = 80
    high_usd_target = 1700

    result = compute_start_price_usd(
        cost_jpy=cost_jpy,
        mode=mode,
        low_usd_target=low_usd_target,
        high_usd_target=high_usd_target
    )

    print("compute_start_price_usd result =", result)


if __name__ == "__main__":
    main()