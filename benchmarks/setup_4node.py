"""
Set up test data on the 4-node cluster.
Uses config_4node.yaml.
"""

import sys
import redshift_connector
import yaml
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)


def load_config():
    with open(Path(__file__).parent / "config_4node.yaml") as f:
        return yaml.safe_load(f)


def create_and_populate(conn, table_name, row_count, cfg):
    cursor = conn.cursor()
    num_int = cfg["benchmark"]["num_int_cols"]
    num_float = cfg["benchmark"]["num_float_cols"]
    num_str = cfg["benchmark"]["num_string_cols"]
    str_len = cfg["benchmark"]["string_length"]

    cols = []
    for i in range(num_int):
        cols.append(f"int_col_{i} BIGINT")
    for i in range(num_float):
        cols.append(f"float_col_{i} DOUBLE PRECISION")
    for i in range(num_str):
        cols.append(f"str_col_{i} VARCHAR({str_len})")

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({', '.join(cols)})")
    conn.commit()

    seed_select = []
    double_select = []
    for i in range(num_int):
        seed_select.append(f"(RANDOM() * 1000000)::BIGINT AS int_col_{i}")
        double_select.append(f"(RANDOM() * 1000000)::BIGINT AS int_col_{i}")
    for i in range(num_float):
        seed_select.append(f"RANDOM() * 1000.0 AS float_col_{i}")
        double_select.append(f"RANDOM() * 1000.0 AS float_col_{i}")
    for i in range(num_str):
        seed_select.append(f"LEFT(MD5(RANDOM()::TEXT || n::TEXT), {str_len}) AS str_col_{i}")
        double_select.append(f"LEFT(MD5(RANDOM()::TEXT), {str_len}) AS str_col_{i}")

    seed_expr = ", ".join(seed_select)
    double_expr = ", ".join(double_select)

    seed_count = min(row_count, 100000)
    print(f"  Seeding {seed_count} rows...")
    cursor.execute(
        f"INSERT INTO {table_name} SELECT {seed_expr} "
        f"FROM (SELECT ROW_NUMBER() OVER () AS n FROM stv_blocklist LIMIT {seed_count})"
    )
    conn.commit()

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    current = cursor.fetchone()[0]
    print(f"  Seeded: {current} rows")

    while current < row_count:
        batch = min(current, row_count - current)
        print(f"  Doubling: {current:,} -> {current + batch:,} rows...")
        cursor.execute(
            f"INSERT INTO {table_name} SELECT {double_expr} "
            f"FROM {table_name} LIMIT {batch}"
        )
        conn.commit()
        current += batch

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    actual = cursor.fetchone()[0]
    print(f"  Table {table_name}: {actual:,} rows")


def main():
    cfg = load_config()
    rs = cfg["redshift"]

    print(f"Connecting to 4-node cluster: {rs['host']}")
    conn = redshift_connector.connect(
        host=rs["host"],
        port=rs.get("port", 5439),
        database=rs["database"],
        user=rs["user"],
        password=rs["password"],
    )

    prefix = cfg["benchmark"]["test_table_prefix"]
    for count in cfg["benchmark"]["row_counts"]:
        table_name = f"{prefix}_{count // 1000000}m"
        print(f"\nCreating {table_name} with {count:,} rows...")
        create_and_populate(conn, table_name, count, cfg)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
