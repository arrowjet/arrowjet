# Arrowjet + Airflow Examples

## The Problem

Exporting data from Redshift in Airflow typically requires 30-50 lines of manual UNLOAD/S3/cleanup code. Arrowjet reduces this to ~8 lines with automatic staging, cleanup, and error handling.

## Examples

### Without Arrowjet (manual UNLOAD): ~50 lines
See [without_arrowjet.py](without_arrowjet.py) — manual UNLOAD, S3 management, cleanup, error handling.

### With Arrowjet (TaskFlow): ~8 lines
See [with_arrowjet.py](with_arrowjet.py) — same result, automatic everything. Two-task DAG: extract → verify.

### With Arrowjet (CLI via BashOperator): ~3 lines
See [with_arrowjet_cli.py](with_arrowjet_cli.py) — zero Python, just a CLI command.

## Performance

Benchmarked on a 4-node Redshift cluster (ra3.large), EC2 same region:

| Approach | 1M rows | 10M rows |
|---|---|---|
| Naive fetch (`cursor.fetchall`) | ~11s | ~108s |
| Manual UNLOAD (no arrowjet) | ~7s (1.7x) | ~58s (1.9x) |
| Arrowjet | ~4s (2.6x) | ~34s (3.1x) |

The gap widens with more data. At 10M rows, arrowjet is 3.1x faster than naive fetch.

| Metric | Manual UNLOAD | Arrowjet |
|---|---|---|
| Lines of code | ~50 | ~8 |
| S3 cleanup | Manual | Automatic |
| Error handling | Manual | Built-in |
| LIMIT workaround | Manual subquery | Automatic |

## Prerequisites

- Python 3.10+
- A Redshift cluster with a test table (the examples use `benchmark_test_1m`)
- An S3 bucket for staging (same region as Redshift)
- An IAM role that Redshift can assume for S3 access
- AWS credentials configured locally

## Setup

```bash
# 1. Create a venv and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install arrowjet apache-airflow

# 2. Set up environment variables
export REDSHIFT_HOST="your-cluster.region.redshift.amazonaws.com"
export REDSHIFT_PASS="your-password"
export STAGING_BUCKET="your-staging-bucket"
export STAGING_IAM_ROLE="arn:aws:iam::123456789:role/YourRedshiftS3Role"
export STAGING_REGION="us-east-1"

# 3. Initialize Airflow (one-time)
export AIRFLOW_HOME=$(pwd)/airflow_test
airflow db migrate
mkdir -p $AIRFLOW_HOME/dags
```

## Running

```bash
set -a && source .env && set +a
cp with_arrowjet.py $AIRFLOW_HOME/dags/
PYTHONPATH=src airflow dags test test_arrowjet_export 2025-01-01
```

Expected output:
```
Bulk read: 1,000 rows in 3.29s
Saved to /tmp/airflow_arrowjet_test.parquet
Verified: 1,000 rows, 15 columns
All good.
```

## Known Quirks

- Redshift UNLOAD doesn't support `LIMIT` directly — Arrowjet handles this automatically.
- Use Airflow 3.2+ (`dags test` has a bug in 3.0.x).
