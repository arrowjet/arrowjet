#!/bin/bash
# run_with_arrowjet.sh
#
# Wraps dbt run with arrowjet bulk export after transformation.
#
# Pattern:
#   1. dbt run  — SQL transforms execute in Redshift
#   2. arrowjet — bulk export the transformed result to S3 (UNLOAD)
#
# Usage:
#   set -a && source ../../.env && set +a
#   bash run_with_arrowjet.sh
#
# Requirements:
#   pip install arrowjet dbt-redshift

set -e

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_PROFILES_DIR="$SCRIPT_DIR"
DBT_PROJECT_DIR="$SCRIPT_DIR"
MODEL="sales_summary"

# ── Step 1: dbt run ───────────────────────────────────────────────────────────
echo "=== Step 1: dbt run ==="
dbt run --profiles-dir "$DBT_PROFILES_DIR" --project-dir "$DBT_PROJECT_DIR"

# ── Step 2: arrowjet export ───────────────────────────────────────────────────
echo ""
echo "=== Step 2: arrowjet export → s3://${STAGING_BUCKET}/dbt-exports/${MODEL}/ ==="

python3 - <<EOF
import os, time, arrowjet

conn = arrowjet.connect(
    host=os.environ["REDSHIFT_HOST"],
    database=os.environ.get("REDSHIFT_DATABASE", "dev"),
    user=os.environ.get("REDSHIFT_USER", "awsuser"),
    password=os.environ["REDSHIFT_PASS"],
    staging_bucket=os.environ["STAGING_BUCKET"],
    staging_iam_role=os.environ["STAGING_IAM_ROLE"],
    staging_region=os.environ.get("STAGING_REGION", "us-east-1"),
)

start = time.perf_counter()
result = conn.read_bulk("SELECT * FROM public.${MODEL}")
elapsed = time.perf_counter() - start

import pyarrow.parquet as pq
pq.write_table(result.table, "/tmp/dbt_${MODEL}.parquet")

print(f"Exported {result.rows:,} rows in {elapsed:.2f}s → /tmp/dbt_${MODEL}.parquet")
conn.close()
EOF

echo ""
echo "Done."
