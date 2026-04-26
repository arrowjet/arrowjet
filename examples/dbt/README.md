# Arrowjet + dbt

## How Arrowjet Fits the dbt Ecosystem

dbt is a SQL transformation tool  - it excels at turning raw data into clean,
tested, documented tables inside your warehouse. What dbt doesn't do is move
large volumes of data in or out efficiently.

That's where arrowjet comes in:

| Stage | Without Arrowjet | With Arrowjet |
|---|---|---|
| Load raw data before `dbt run` | `dbt seed` uses INSERT (slow for large files) | `arrowjet write_bulk()` uses COPY (10-50x faster) |
| Export results after `dbt run` | `cursor.fetchall()` over wire protocol | `arrowjet read_bulk()` uses UNLOAD (2-3x faster) |

**The pattern:** dbt owns the SQL transforms. Arrowjet handles the bulk data movement at the edges.

```
raw data (CSV / Parquet / S3)
        ↓  arrowjet write_bulk (COPY)
  Redshift (raw tables)
        ↓  dbt run (SQL transforms)
  Redshift (clean tables)
        ↓  arrowjet read_bulk (UNLOAD)
  S3 / local Parquet / downstream systems
```

## Performance

Benchmarked on a 4-node Redshift cluster (ra3.large), EC2 same region, 10M rows:

| Approach | Time | vs Naive |
|---|---|---|
| Naive fetch (`cursor.fetchall`) | ~105s | baseline |
| Manual UNLOAD | ~58s | 1.8x faster |
| Arrowjet | ~33s | 3.1x faster |

For the dbt export use case (reading transformed results out of Redshift),
arrowjet is 3x faster than a naive fetch at 10M rows.

## Files

| File | Purpose |
|---|---|
| `dbt_project.yml` | dbt project config |
| `profiles.yml` | Redshift connection (reads from env vars) |
| `models/sales_summary.sql` | SQL model: aggregate raw data by day |
| `models/sources.yml` | Declares the raw source table |
| `macros/export_with_arrowjet.sql` | Hook macro (logs export intent) |
| `run_with_arrowjet.sh` | Shell script: `dbt run` + arrowjet export |

## Prerequisites

- Python 3.10+
- Redshift cluster (provisioned or serverless)
- A source table in Redshift (examples use `benchmark_test_1m`)
- S3 bucket for staging (same region as Redshift)
- IAM role that Redshift can assume for S3 access

```bash
pip install arrowjet dbt-redshift
```

## Setup

### For beginners: step by step

**1. Set environment variables**

```bash
export REDSHIFT_HOST="your-cluster.region.redshift.amazonaws.com"
export REDSHIFT_PASS="your-password"
export STAGING_BUCKET="your-staging-bucket"
export STAGING_IAM_ROLE="arn:aws:iam::123456789:role/YourRedshiftS3Role"
export STAGING_REGION="us-east-1"
```

Or use a `.env` file and load it:
```bash
set -a && source .env && set +a
```

**2. Verify dbt can connect**

```bash
dbt debug --profiles-dir examples/dbt --project-dir examples/dbt
```

**3. Run the example**

```bash
PYTHONPATH=src bash examples/dbt/run_with_arrowjet.sh
```

### For experienced users: quick start

```bash
set -a && source .env && set +a
PYTHONPATH=src bash examples/dbt/run_with_arrowjet.sh
```

## Expected Output

```
=== Step 1: dbt run ===
1 of 1 OK created sql table model public.sales_summary [SUCCESS in 6.05s]

=== Step 2: arrowjet export -> s3://your-bucket/dbt-exports/sales_summary/ ===
Exported 365 rows in 4.2s -> /tmp/dbt_sales_summary.parquet

Done.
```

## Cleanup

Stop the Redshift Serverless workgroup when not in use to avoid charges:

```bash
aws redshift-serverless update-workgroup \
  --workgroup-name your-workgroup \
  --base-capacity 0 \
  --region us-east-1
```

Or delete it entirely:

```bash
aws redshift-serverless delete-workgroup --workgroup-name your-workgroup --region us-east-1
aws redshift-serverless delete-namespace --namespace-name your-namespace --region us-east-1
```

## Known Limitations

**dbt Python models are not supported on Redshift** (only Snowflake, BigQuery, Databricks).
This means arrowjet cannot run inside a dbt Python model on Redshift today.

The workaround is the shell script pattern shown here  - arrowjet runs outside dbt,
wrapping `dbt run`. This works for all Redshift deployments (provisioned and serverless).

For the roadmap on contributing Python model support to dbt-redshift, see
`docs/future/dbt_python_models_redshift.md`.

## How dbt Hooks Work (and Why They Can't Call Python)

dbt `on-run-start` and `on-run-end` hooks only execute SQL statements or macros
that produce SQL. They cannot call Python or shell commands directly.

The `export_with_arrowjet` macro in this example logs the export intent  - the actual
arrowjet export runs in `run_with_arrowjet.sh` after `dbt run` completes.

If you need to trigger arrowjet from within dbt, use a CI/CD pipeline or an
Airflow DAG that chains `dbt run` and `arrowjet export` as separate steps.
