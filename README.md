# Arrowjet

**The fastest way to move data in and out of cloud databases.**

Arrowjet wraps each database's native bulk path in a simple Python API  - no boilerplate, no S3 scripts, no slow INSERT loops.

```bash
pip install arrowjet              # core (PostgreSQL COPY, MySQL LOAD DATA, BYOC Engine, CLI)
pip install arrowjet[redshift]    # + Redshift driver (arrowjet.connect())
pip install arrowjet[iceberg]     # + Apache Iceberg output (write_iceberg())
pip install arrowjet[full]        # + Redshift + PostgreSQL + MySQL + SQLAlchemy + Iceberg
```

Both `arrowjet` and `aj` work as CLI commands:

```bash
aj export --provider postgresql --query "SELECT * FROM orders" --to orders.parquet
aj transfer --from-profile pg --to-profile mysql --table orders
```

**Supported databases:**
- **PostgreSQL** / Aurora PostgreSQL / RDS PostgreSQL  - via COPY protocol
- **MySQL** / Aurora MySQL / RDS MySQL / MariaDB  - via LOAD DATA LOCAL INFILE
- **Amazon Redshift**  - via COPY/UNLOAD through S3

---

## Why Arrowjet

Standard database drivers move data row-by-row. For large datasets, this is the bottleneck  - not the database.

Arrowjet uses each database's native bulk path instead. There is no slow path.

### PostgreSQL  - Writes (COPY FROM STDIN)

| Approach | 1M rows | vs Arrowjet |
|---|---|---|
| `executemany` (batch 1000) | ~16 min | **850x slower** |
| Multi-row VALUES (batch 1000) | 8.4s | 7.4x slower |
| **Arrowjet** | **1.13s** | **baseline** |

### PostgreSQL  - Reads (COPY TO STDOUT)

| Approach | 1M rows | vs Arrowjet |
|---|---|---|
| `cursor.fetchall()` | 1.00s | 1.5x slower |
| **Arrowjet** | **0.65s** | **baseline** |

*Benchmarked on RDS PostgreSQL 16.6, EC2 same region.*

### MySQL - Writes (LOAD DATA LOCAL INFILE)

| Approach | 1M rows | vs Arrowjet |
|---|---|---|
| `executemany` (batch 1000) | 25.4s | 6.6x slower |
| Multi-row VALUES (batch 1000) | 25.7s | 6.7x slower |
| **Arrowjet** | **3.87s** | **baseline** |

*Benchmarked on RDS MySQL 8.0, EC2 same region.*

### Redshift  - Writes (COPY via S3)

| Approach | 1M rows | vs Arrowjet |
|---|---|---|
| `write_dataframe()` INSERT | 13.4 hours | 14,523x slower |
| `executemany` (batch 5000) | 27.1 hours | 29,296x slower |
| Manual COPY | 4.06s | 1.22x slower |
| **Arrowjet** | **3.33s** | **baseline** |

### Redshift  - Reads (UNLOAD via S3)

| Approach | 1M rows | 10M rows |
|---|---|---|
| `cursor.fetchall()` | ~11s | ~105s |
| **Arrowjet** | **~4s** | **~34s** |

![Read benchmark  - 10M rows, 4-node ra3.large cluster](https://github.com/arrowjet/arrowjet/blob/main/docs/images/read_benchmark.png?raw=true)

![Write benchmark  - 1M rows, 4-node ra3.large cluster](https://github.com/arrowjet/arrowjet/blob/main/docs/images/write_benchmark.png?raw=true)

---

## Quick Start  - PostgreSQL

No S3 bucket, no IAM role, no staging config. Just a psycopg2 connection.

```python
import arrowjet
import psycopg2

conn = psycopg2.connect(host="your-host", dbname="mydb", user="user", password="...")

engine = arrowjet.Engine(provider="postgresql")

# Bulk write  - 850x faster than executemany
engine.write_dataframe(conn, my_dataframe, "target_table")

# Bulk read  - 1.5x faster than cursor.fetchall()
result = engine.read_bulk(conn, "SELECT * FROM events")
df = result.to_pandas()
```

Works with any PostgreSQL: Aurora, RDS, self-hosted, Docker, Supabase, Neon.

## Quick Start  - MySQL

```python
import arrowjet
import pymysql

conn = pymysql.connect(host="your-host", database="mydb", user="user",
                        password="...", local_infile=True)

engine = arrowjet.Engine(provider="mysql")

# Bulk write - LOAD DATA LOCAL INFILE (6.6x faster than INSERT)
engine.write_dataframe(conn, my_dataframe, "target_table")

# Read  - cursor fetch to Arrow
result = engine.read_bulk(conn, "SELECT * FROM events")
df = result.to_pandas()
```

Works with any MySQL: Aurora MySQL, RDS MySQL, self-hosted, MariaDB, PlanetScale, TiDB.

## Quick Start  - Redshift

```python
import arrowjet

conn = arrowjet.connect(
    host="your-cluster.region.redshift.amazonaws.com",
    database="dev",
    user="awsuser",
    password="...",
    staging_bucket="your-staging-bucket",
    staging_iam_role="arn:aws:iam::123456789:role/RedshiftS3Role",
    staging_region="us-east-1",
)

# Bulk read  - UNLOAD -> S3 -> Parquet -> Arrow
result = conn.read_bulk("SELECT * FROM events WHERE date > '2025-01-01'")
df = result.to_pandas()

# Bulk write  - Arrow -> Parquet -> S3 -> COPY
conn.write_dataframe(my_dataframe, "target_table")

# Safe mode  - standard DBAPI for small queries
df = conn.fetch_dataframe("SELECT COUNT(*) FROM events")
```

---

## Bring Your Own Connection

Already have connection management? Arrowjet works with your existing connections.

```python
import arrowjet

# PostgreSQL  - no staging config needed
pg_engine = arrowjet.Engine(provider="postgresql")
pg_engine.write_dataframe(existing_pg_conn, df, "my_table")
result = pg_engine.read_bulk(existing_pg_conn, "SELECT * FROM my_table")

# MySQL  - no staging config needed
mysql_engine = arrowjet.Engine(provider="mysql")
mysql_engine.write_dataframe(existing_mysql_conn, df, "my_table")
result = mysql_engine.read_bulk(existing_mysql_conn, "SELECT * FROM my_table")

# Redshift  - needs S3 staging config
rs_engine = arrowjet.Engine(
    provider="redshift",
    staging_bucket="your-bucket",
    staging_iam_role="arn:aws:iam::123:role/RedshiftS3Role",
    staging_region="us-east-1",
)
rs_engine.write_dataframe(existing_rs_conn, df, "my_table")
result = rs_engine.read_bulk(existing_rs_conn, "SELECT * FROM my_table")
```

Works with `psycopg2`, `psycopg3`, `pymysql`, `redshift_connector`, ADBC, or any DBAPI connection.

---

## Cross-Database Transfer

Move data between any two supported databases in two lines. Arrow is the bridge.

```python
import arrowjet

pg_engine = arrowjet.Engine(provider="postgresql")
mysql_engine = arrowjet.Engine(provider="mysql")
rs_engine = arrowjet.Engine(
    provider="redshift",
    staging_bucket="your-bucket",
    staging_iam_role="arn:aws:iam::123:role/RedshiftS3",
    staging_region="us-east-1",
)

# PostgreSQL to MySQL
result = arrowjet.transfer(
    source_engine=pg_engine, source_conn=pg_conn,
    query="SELECT * FROM orders WHERE date > '2025-01-01'",
    dest_engine=mysql_engine, dest_conn=mysql_conn,
    dest_table="orders",
)
print(f"Transferred {result.rows:,} rows in {result.total_time_s}s")

# MySQL to Redshift
arrowjet.transfer(
    source_engine=mysql_engine, source_conn=mysql_conn,
    query="SELECT * FROM users",
    dest_engine=rs_engine, dest_conn=rs_conn,
    dest_table="users",
)
```

All 6 combinations work: PostgreSQL, MySQL, and Redshift in any direction.

### Transfer Benchmarks (100K rows, EC2 same region)

| Path | Total | Read | Write | Rows/sec |
|---|---|---|---|---|
| PostgreSQL to MySQL | 0.64s | 0.10s | 0.54s | 157K/s |
| MySQL to PostgreSQL | 0.70s | 0.58s | 0.12s | 143K/s |
| PostgreSQL to Redshift | 17.2s | 0.06s | 17.1s | 5.8K/s |
| Redshift to PostgreSQL | 6.5s | 6.4s | 0.12s | 15K/s |

PostgreSQL and MySQL transfers are sub-second. Redshift paths include S3 staging overhead.

---

## CLI

```bash
# PostgreSQL
arrowjet export --provider postgresql --query "SELECT * FROM users" --to ./users.parquet
arrowjet export --provider postgresql --query "SELECT * FROM users" --to ./users.csv --format csv

# MySQL
arrowjet export --provider mysql --query "SELECT * FROM orders" --to ./orders.parquet
arrowjet export --provider mysql --query "SELECT * FROM orders" --to ./orders.csv --format csv

# Redshift
arrowjet export --query "SELECT * FROM sales" --to ./out.parquet
arrowjet export --query "SELECT * FROM sales" --to s3://bucket/sales/
arrowjet import --from s3://bucket/sales/ --to sales_table
arrowjet import --from ./data.parquet --to sales_table

# Inspect data
arrowjet preview --file ./out.parquet
arrowjet preview --file ./out.parquet --max-width 30
arrowjet validate --table sales --row-count --schema --sample

# Configure and manage profiles
arrowjet configure
arrowjet profiles
arrowjet profiles --verbose

# Export with query from file
arrowjet export --provider postgresql --from-file query.sql --to ./out.parquet

# Dry-run (show SQL without executing)
arrowjet export --provider postgresql --query "SELECT * FROM users" --to ./out.parquet --dry-run

# Transfer between databases (inline credentials)
arrowjet transfer \
  --from-provider postgresql --from-host pg-host --from-password pgpass \
  --to-provider mysql --to-host mysql-host --to-password mypass \
  --query "SELECT * FROM orders" --to-table orders

# Transfer between databases (using profiles)
arrowjet transfer \
  --from-profile my-postgres \
  --to-profile my-mysql \
  --query "SELECT * FROM orders" --to-table orders

# Export to Apache Iceberg (queryable from Athena, Spark, Trino, DuckDB)
arrowjet export --provider postgresql \
  --query "SELECT * FROM orders" \
  --to /path/to/warehouse \
  --format iceberg \
  --iceberg-table analytics.orders
```

All commands read connection details from `~/.arrowjet/config.yaml` (set up with `arrowjet configure`).
Override per-command with `--host`, `--password`, `--profile`, `--provider`, etc.

See [docs/cli_reference.md](https://github.com/arrowjet/arrowjet/blob/main/docs/cli_reference.md) for full details.

---

## Authentication

### PostgreSQL

Password auth via psycopg2 connection parameters. IAM database auth for Aurora/RDS coming soon.

### Redshift

Three methods supported:

- **Password**  - default, standard credentials
- **IAM**  - `auth_type="iam"`, temporary credentials via `GetClusterCredentials` (provisioned) or `GetCredentials` (serverless)
- **Secrets Manager**  - `auth_type="secrets_manager"`, fetch credentials from a secret ARN

See [docs/configuration.md](https://github.com/arrowjet/arrowjet/blob/main/docs/configuration.md) for the full reference.

---

## Integrations

### Airflow

```python
@task
def export_from_postgres():
    import arrowjet, psycopg2
    conn = psycopg2.connect(host=..., dbname=..., ...)
    engine = arrowjet.Engine(provider="postgresql")
    result = engine.read_bulk(conn, "SELECT * FROM events")
    # write to S3, transform, etc.
    conn.close()
```

See [examples/airflow/](https://github.com/arrowjet/arrowjet/tree/main/examples/airflow) for Redshift examples with before/after comparison.

### dbt

```bash
bash examples/dbt/run_with_arrowjet.sh
```

See [examples/dbt/](https://github.com/arrowjet/arrowjet/tree/main/examples/dbt) for the full setup.

### SQLAlchemy (Redshift)

```python
from sqlalchemy import create_engine
engine = create_engine("redshift+arrowjet://user:pass@host:5439/dev")
```

---

## How It Works

Each database has a fast bulk path that most Python users don't know about:

| Database | Slow path (what most people use) | Fast path (what arrowjet uses) |
|---|---|---|
| PostgreSQL | `executemany()`, `to_sql()` | `COPY FROM STDIN` / `COPY TO STDOUT` |
| MySQL | `executemany()`, `to_sql()` | `LOAD DATA LOCAL INFILE` |
| Redshift | `INSERT`, `write_dataframe()` | `COPY` / `UNLOAD` via S3 |

Arrowjet wraps the fast path in a one-line API. There is no slow path.

---

## Apache Iceberg Output

Export data from any supported database directly to Apache Iceberg tables. The result is queryable from Athena, Redshift Spectrum, Spark, Trino, DuckDB, or any Iceberg-compatible engine.

```bash
pip install arrowjet[iceberg]
```

```python
import arrowjet

# Read from PostgreSQL, write to Iceberg
engine = arrowjet.Engine(provider="postgresql")
result = engine.read_bulk(pg_conn, "SELECT * FROM orders")

arrowjet.write_iceberg(
    result.table,
    table_name="analytics.orders",
    warehouse="/path/to/warehouse",   # local path or s3://bucket/warehouse
    mode="append",                    # "append" or "overwrite"
)

# Read it back
table = arrowjet.read_iceberg("analytics.orders", "/path/to/warehouse")
df = table.to_pandas()
```

Supports SQLite-based catalog (no server needed), REST catalog, and AWS Glue catalog.

---

## Requirements

- Python 3.10+
- **PostgreSQL:** `psycopg2` or `psycopg2-binary` (any PostgreSQL  - Aurora, RDS, self-hosted)
- **MySQL:** `pymysql` with `local_infile=True` (any MySQL  - Aurora, RDS, MariaDB, self-hosted)
- **Redshift:** `pip install arrowjet[redshift]` + S3 bucket + IAM role
- **Iceberg:** `pip install arrowjet[iceberg]` (pyiceberg + s3fs)
- **Redshift:** `pip install arrowjet[redshift]` + S3 bucket + IAM role

See [docs/iam_setup.md](https://github.com/arrowjet/arrowjet/blob/main/docs/iam_setup.md) for Redshift IAM configuration.

---

## Roadmap

- [x] Redshift (COPY/UNLOAD via S3)
- [x] PostgreSQL (COPY protocol)
- [x] MySQL (LOAD DATA LOCAL INFILE)
- [x] Cross-database transfer (`arrowjet.transfer()`)
- [x] Chunked `read_bulk` - iterator mode for memory-constrained environments (Lambda, notebooks)
- [x] `--dry-run` flag for export - show SQL without executing
- [x] `--from-file` flag for export - read query from a .sql file
- [x] `arrowjet profiles` - list configured connection profiles
- [x] Row count in S3-direct export
- [x] Truncate wide sample output in preview (`--max-width`)
- [x] Progress indicator for long-running exports
- [ ] `arrowjet benchmark` - CLI command to compare INSERT vs COPY speed on your own data
- [ ] CSV/JSON staging format support for COPY/UNLOAD
- [ ] Snowflake provider (COPY INTO via stages)
- [ ] BigQuery provider (GCS + Load API)
- [ ] Databricks provider (cloud storage + COPY INTO)
- [x] Apache Iceberg output format (`--format iceberg`)
- [ ] Data diff engine (`arrowjet.diff()`)
- [ ] IAM database auth for Aurora PostgreSQL / Aurora MySQL
- [ ] Data validation - row counts, null rates, duplicate detection
- [ ] Airflow provider package (`apache-airflow-providers-arrowjet`)
- [ ] Homebrew formula (`brew install arrowjet`)
- [ ] Docker image for CI/CD pipelines
- [ ] Observability dashboard for operation history and cost tracking

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started.

---

## License

MIT
