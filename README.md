# Arrowjet

**The fastest way to move data in and out of cloud databases.**

Arrowjet wraps each database's native bulk path in a simple Python API — no boilerplate, no S3 scripts, no slow INSERT loops.

```bash
pip install arrowjet              # core (PostgreSQL COPY, BYOC Engine, CLI)
pip install arrowjet[redshift]    # + Redshift driver (arrowjet.connect())
pip install arrowjet[full]        # + Redshift + SQLAlchemy
```

**Supported databases:**
- **PostgreSQL** / Aurora PostgreSQL / RDS PostgreSQL — via COPY protocol
- **Amazon Redshift** — via COPY/UNLOAD through S3

---

## Why Arrowjet

Standard database drivers move data row-by-row. For large datasets, this is the bottleneck — not the database.

Arrowjet uses each database's native bulk path instead. There is no slow path.

### PostgreSQL — Writes (COPY FROM STDIN)

| Approach | 1M rows | vs Arrowjet |
|---|---|---|
| `executemany` (batch 1000) | ~16 min | **850x slower** |
| Multi-row VALUES (batch 1000) | 8.4s | 7.4x slower |
| **Arrowjet** | **1.13s** | **baseline** |

### PostgreSQL — Reads (COPY TO STDOUT)

| Approach | 1M rows | vs Arrowjet |
|---|---|---|
| `cursor.fetchall()` | 1.00s | 1.5x slower |
| **Arrowjet** | **0.65s** | **baseline** |

*Benchmarked on RDS PostgreSQL 16.6, EC2 same region.*

### Redshift — Writes (COPY via S3)

| Approach | 1M rows | vs Arrowjet |
|---|---|---|
| `write_dataframe()` INSERT | 13.4 hours | 14,523x slower |
| `executemany` (batch 5000) | 27.1 hours | 29,296x slower |
| Manual COPY | 4.06s | 1.22x slower |
| **Arrowjet** | **3.33s** | **baseline** |

### Redshift — Reads (UNLOAD via S3)

| Approach | 1M rows | 10M rows |
|---|---|---|
| `cursor.fetchall()` | ~11s | ~105s |
| **Arrowjet** | **~4s** | **~34s** |

![Read benchmark — 10M rows, 4-node ra3.large cluster](https://github.com/arrowjet/arrowjet/blob/main/docs/images/read_benchmark.png?raw=true)

![Write benchmark — 1M rows, 4-node ra3.large cluster](https://github.com/arrowjet/arrowjet/blob/main/docs/images/write_benchmark.png?raw=true)

---

## Quick Start — PostgreSQL

No S3 bucket, no IAM role, no staging config. Just a psycopg2 connection.

```python
import arrowjet
import psycopg2

conn = psycopg2.connect(host="your-host", dbname="mydb", user="user", password="...")

engine = arrowjet.Engine(provider="postgresql")

# Bulk write — 850x faster than executemany
engine.write_dataframe(conn, my_dataframe, "target_table")

# Bulk read — 1.5x faster than cursor.fetchall()
result = engine.read_bulk(conn, "SELECT * FROM events")
df = result.to_pandas()
```

Works with any PostgreSQL: Aurora, RDS, self-hosted, Docker, Supabase, Neon.

## Quick Start — Redshift

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

# Bulk read — UNLOAD → S3 → Parquet → Arrow
result = conn.read_bulk("SELECT * FROM events WHERE date > '2025-01-01'")
df = result.to_pandas()

# Bulk write — Arrow → Parquet → S3 → COPY
conn.write_dataframe(my_dataframe, "target_table")

# Safe mode — standard DBAPI for small queries
df = conn.fetch_dataframe("SELECT COUNT(*) FROM events")
```

---

## Bring Your Own Connection

Already have connection management? Arrowjet works with your existing connections.

```python
import arrowjet

# PostgreSQL — no staging config needed
pg_engine = arrowjet.Engine(provider="postgresql")
pg_engine.write_dataframe(existing_pg_conn, df, "my_table")
result = pg_engine.read_bulk(existing_pg_conn, "SELECT * FROM my_table")

# Redshift — needs S3 staging config
rs_engine = arrowjet.Engine(
    provider="redshift",
    staging_bucket="your-bucket",
    staging_iam_role="arn:aws:iam::123:role/RedshiftS3Role",
    staging_region="us-east-1",
)
rs_engine.write_dataframe(existing_rs_conn, df, "my_table")
result = rs_engine.read_bulk(existing_rs_conn, "SELECT * FROM my_table")
```

Works with `psycopg2`, `psycopg3`, `redshift_connector`, ADBC, or any DBAPI connection.

---

## CLI

```bash
# PostgreSQL
arrowjet export --provider postgresql --query "SELECT * FROM users" --to ./users.parquet
arrowjet export --provider postgresql --query "SELECT * FROM users" --to ./users.csv --format csv

# Redshift
arrowjet export --query "SELECT * FROM sales" --to ./out.parquet
arrowjet export --query "SELECT * FROM sales" --to s3://bucket/sales/
arrowjet import --from s3://bucket/sales/ --to sales_table
arrowjet import --from ./data.parquet --to sales_table

# Inspect data
arrowjet preview --file ./out.parquet
arrowjet validate --table sales --row-count --schema --sample

# Configure profiles
arrowjet configure
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

- **Password** — default, standard credentials
- **IAM** — `auth_type="iam"`, temporary credentials via `GetClusterCredentials` (provisioned) or `GetCredentials` (serverless)
- **Secrets Manager** — `auth_type="secrets_manager"`, fetch credentials from a secret ARN

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
| Redshift | `INSERT`, `write_dataframe()` | `COPY` / `UNLOAD` via S3 |

Arrowjet wraps the fast path in a one-line API. There is no slow path.

---

## Requirements

- Python 3.10+
- **PostgreSQL:** `psycopg2` or `psycopg2-binary` (any PostgreSQL — Aurora, RDS, self-hosted)
- **Redshift:** `pip install arrowjet[redshift]` + S3 bucket + IAM role

See [docs/iam_setup.md](https://github.com/arrowjet/arrowjet/blob/main/docs/iam_setup.md) for Redshift IAM configuration.

---

## Roadmap

- [x] Redshift (COPY/UNLOAD via S3)
- [x] PostgreSQL (COPY protocol)
- [ ] MySQL / Aurora MySQL (LOAD DATA)
- [ ] Snowflake (COPY INTO via stages)
- [ ] Data validation (row counts, schema checks, null detection)

---

## License

MIT
