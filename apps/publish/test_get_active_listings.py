from apps.publish.publish_manager import (
    run_get_active_listings_vps,
    run_get_active_listings_local,
)


def main():
    print("===== run_get_active_listings_vps START =====")
    run_get_active_listings_vps()
    print("===== run_get_active_listings_vps END =====")

    print("===== run_get_active_listings_local START =====")
    run_get_active_listings_local()
    print("===== run_get_active_listings_local END =====")


if __name__ == "__main__":
    main()
