def refresh_presets_lookup_temp_python(conn):
    cur = conn.cursor()
    print("presets_lookup_temp refresh start")

    cur.execute("""
    IF OBJECT_ID('mst.presets_lookup_temp', 'U') IS NULL
    BEGIN
        SELECT TOP 0 *
        INTO mst.presets_lookup_temp
        FROM mst.presets_lookup;
    END;

    TRUNCATE TABLE mst.presets_lookup_temp;
    """)

    vendors = cur.execute("""
        SELECT vendor_code, vendor_name
        FROM mst.vendors
    """).fetchall()

    brands = cur.execute("""
        SELECT brand_id, brand_name_ja, default_brand_en, is_active, is_listing_target
        FROM mst.presets_brand
        WHERE is_active = 1
    """).fetchall()

    categories = cur.execute("""
        SELECT category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id
        FROM mst.presets_categories
    """).fetchall()

    category_groups = cur.execute("""
        SELECT category_group, mode, low_usd_target, high_usd_target,
               low_jpy_target, high_jpy_target
        FROM mst.category_groups
        WHERE is_scrape_stopped = 0
    """).fetchall()

    cgc_rows = cur.execute("""
        SELECT category_group, category_id, is_brand_dependent
        FROM mst.category_group_categories
    """).fetchall()

    bcg_rows = cur.execute("""
        SELECT brand_id, category_group
        FROM mst.brand_category_groups
    """).fetchall()

    brand_by_id = {r.brand_id: r for r in brands}
    category_by_id = {r.category_id: r for r in categories}
    group_by_name = {r.category_group: r for r in category_groups}

    cgc_by_group = {}
    for r in cgc_rows:
        cgc_by_group.setdefault(r.category_group, []).append(r)

    cgc_by_category_id = {}
    for r in cgc_rows:
        cgc_by_category_id.setdefault(r.category_id, []).append(r)

    bcg_groups = {r.category_group for r in bcg_rows}

    rows = []

    # 1) 現行VIEWの1本目：brand_category_groupsあり、is_brand_dependent=0
    for bcg in bcg_rows:
        brand = brand_by_id.get(bcg.brand_id)
        if not brand:
            continue

        for cgc in cgc_by_group.get(bcg.category_group, []):
            if cgc.is_brand_dependent != 0:
                continue

            pc = category_by_id.get(cgc.category_id)
            cg = group_by_name.get(cgc.category_group)
            if not pc or not cg:
                continue

            for v in vendors:
                preset = (
                    (brand.brand_name_ja or "")
                    + (pc.category_name_ja or "")
                    + ("men" if pc.department == "Men" else "")
                    + (v.vendor_code or "")
                )

                rows.append((
                    preset,
                    v.vendor_name,
                    brand.brand_id,
                    pc.category_id,
                    cg.mode,
                    cg.low_usd_target,
                    cg.high_usd_target,
                    pc.category_id_ebay,
                    pc.department,
                    brand.default_brand_en,
                    pc.type_ebay,
                    cg.low_jpy_target,
                    cg.high_jpy_target,
                    cg.category_group,
                    brand.is_active,
                    brand.is_listing_target,
                ))

    # 2) 追加：brand_category_groupsに無いcategory_group、is_brand_dependent=0
    for cgc in cgc_rows:
        if cgc.is_brand_dependent != 0:
            continue

        if cgc.category_group in bcg_groups:
            continue

        pc = category_by_id.get(cgc.category_id)
        cg = group_by_name.get(cgc.category_group)
        if not pc or not cg:
            continue

        for v in vendors:
            preset = (
                (pc.category_name_ja or "")
                + ("men" if pc.department == "Men" else "")
                + (v.vendor_code or "")
            )

            rows.append((
                preset,
                v.vendor_name,
                pc.brand_id,          # 多分0
                pc.category_id,
                cg.mode,
                cg.low_usd_target,
                cg.high_usd_target,
                pc.category_id_ebay,
                pc.department,
                "",                   # default_brand_enなし
                pc.type_ebay,
                cg.low_jpy_target,
                cg.high_jpy_target,
                cg.category_group,
                1,
                1,
            ))

    # 3) 現行VIEWの2本目：is_brand_dependent=1
    for pc in categories:
        brand = brand_by_id.get(pc.brand_id)
        if not brand:
            continue

        for cgc in cgc_by_category_id.get(pc.category_id, []):
            if cgc.is_brand_dependent != 1:
                continue

            cg = group_by_name.get(cgc.category_group)
            if not cg:
                continue

            for v in vendors:
                preset = (
                    (brand.brand_name_ja or "")
                    + (pc.category_name_ja or "")
                    + ("men" if pc.department == "Men" else "")
                    + (v.vendor_code or "")
                )

                rows.append((
                    preset,
                    v.vendor_name,
                    brand.brand_id,
                    pc.category_id,
                    cg.mode,
                    cg.low_usd_target,
                    cg.high_usd_target,
                    pc.category_id_ebay,
                    pc.department,
                    brand.default_brand_en,
                    pc.type_ebay,
                    cg.low_jpy_target,
                    cg.high_jpy_target,
                    cg.category_group,
                    brand.is_active,
                    brand.is_listing_target,
                ))

    cur.fast_executemany = True
    cur.executemany("""
        INSERT INTO mst.presets_lookup_temp (
            preset,
            vendor_name,
            brand_id,
            category_id,
            mode,
            low_usd_target,
            high_usd_target,
            category_id_ebay,
            department,
            default_brand_en,
            type_ebay,
            low_jpy_target,
            high_jpy_target,
            category_group,
            is_active,
            is_listing_target
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    print(f"presets_lookup_temp refresh done: {len(rows):,} rows")

from apps.common.utils import send_mail, get_sql_server_connection


def main():

    conn = get_sql_server_connection()

    try:
        refresh_presets_lookup_temp_python(conn)

        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM mst.v_presets")
        view_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM mst.presets_lookup_temp")
        temp_count = cur.fetchone()[0]

        cur.execute("""
            SELECT category_group, COUNT(*)
            FROM mst.presets_lookup_temp
            GROUP BY category_group
            ORDER BY category_group
        """)
        for row in cur.fetchall():
            print(row.category_group, row[1])

        print(f"v_presets: {view_count:,}")
        print(f"temp    : {temp_count:,}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()