"""
Creates test tables in Redshift and populates them with synthetic data.
Uses Redshift-compatible SQL for data generation.
"""

import redshift_connector
from config_loader import load_config


def create_and_populate(conn, table_name: str, row_count: int, cfg: dict):
    cursor = conn.cursor()
    num_int = cfg["benchmark"]["num_int_cols"]
    num_float = cfg["benchmark"]["num_float_cols"]
    num_str = cfg["benchmark"]["num_string_cols"]
    str_len = cfg["benchmark"]["string_length"]

    # Build column definitions
    cols = []
    for i in range(num_int):
        cols.append(f"int_col_{i} BIGINT")
    for i in range(num_float):
        cols.append(f"float_col_{i} DOUBLE PRECISION")
    for i in range(num_str):
        cols.append(f"str_col_{i} VARCHAR({str_len})")

    col_defs = ", ".join(cols)

    # Drop if exists and create
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({col_defs})")
    conn.commit()

    # Build SELECT expressions — one for seeding (with row number), one for doubling
    seed_select_cols = []
    double_select_cols = []
    for i in range(num_int):
        seed_select_cols.append(f"(RANDOM() * 1000000)::BIGINT AS int_col_{i}")
        double_select_cols.append(f"(RANDOM() * 1000000)::BIGINT AS int_col_{i}")
    for i in range(num_float):
        seed_select_cols.append(f"RANDOM() * 1000.0 AS float_col_{i}")
        double_select_cols.append(f"RANDOM() * 1000.0 AS float_col_{i}")
    for i in range(num_str):
        seed_select_cols.append(
            f"LEFT(MD5(RANDOM()::TEXT || n::TEXT), {str_len}) AS str_col_{i}"
        )
        double_select_cols.append(
            f"LEFT(MD5(RANDOM()::TEXT), {str_len}) AS str_col_{i}"
        )

    seed_select_expr = ", ".join(seed_select_cols)
    double_select_expr = ", ".join(double_select_cols)

    # Seed using stv_blocklist (a system table with many rows) as a row source
    seed_count = min(row_count, 100000)
    print(f"  Seeding {seed_count} rows...")
    cursor.execute(
        f"INSERT INTO {table_name} "
        f"SELECT {seed_select_expr} "
        f"FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT {seed_count})"
    )
    conn.commit()

    # Verify seed
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    current = cursor.fetchone()[0]
    print(f"  Seeded: {current} rows")

    # Double up until we reach target
    while current < row_count:
        batch = min(current, row_count - current)
        print(f"  Doubling: {current:,} -> {current + batch:,} rows...")
        cursor.execute(
            f"INSERT INTO {table_name} "
            f"SELECT {double_select_expr} "
            f"FROM {table_name} LIMIT {batch}"
        )
        conn.commit()
        current += batch

    # Final count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    actual = cursor.fetchone()[0]
    print(f"  Table {table_name}: {actual:,} rows")


def main():
    cfg = load_config()
    rs = cfg["redshift"]

    conn = redshift_connector.connect(
        host=rs["host"],
        port=rs.get("port", 5439),
        database=rs["database"],
        user=rs["user"],
        password=rs["password"],
    )

    prefix = cfg["benchmark"]["test_table_prefix"]
    row_counts = cfg["benchmark"]["row_counts"]

    for count in row_counts:
        table_name = f"{prefix}_{count // 1000000}m"
        print(f"\nCreating {table_name} with {count:,} rows...")
        create_and_populate(conn, table_name, count, cfg)

    conn.close()
    print("\nDone. Test tables created.")


if __name__ == "__main__":
    main()
