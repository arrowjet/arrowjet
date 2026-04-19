# CLI Reference

## Overview

```
arrowjet <command> [options]
```

| Command | Description |
|---|---|
| `configure` | Set up a connection profile |
| `export` | Export query results to S3 or local file |
| `preview` | Inspect a Parquet file (schema, rows, sample) |
| `validate` | Check a Redshift table (row count, schema, sample) |

---

## arrowjet configure

Interactive setup — saves connection profile to `~/.arrowjet/config.yaml`.

```bash
arrowjet configure
arrowjet configure --profile production
```

| Option | Default | Description |
|---|---|---|
| `--profile` | `default` | Profile name |

### Config file format

```yaml
# ~/.arrowjet/config.yaml
default_profile: dev

profiles:
  dev:
    host: my-cluster.region.redshift.amazonaws.com
    database: dev
    user: awsuser
    password: mypass
    staging_bucket: my-staging-bucket
    staging_iam_role: arn:aws:iam::123:role/RedshiftS3
    staging_region: us-east-1

  production:
    host: prod-cluster.region.redshift.amazonaws.com
    database: prod
    user: etl_user
    password: ...
    staging_bucket: prod-staging
    staging_iam_role: arn:aws:iam::123:role/ProdRedshiftS3
    staging_region: us-east-1
```

---

## arrowjet export

Export query results from Redshift to S3 or local file.

```bash
arrowjet export --query "SELECT * FROM sales" --to ./sales.parquet
arrowjet export --query "SELECT * FROM sales" --to s3://bucket/sales/
arrowjet export --query "SELECT * FROM sales" --to ./sales.csv --format csv
```

| Option | Required | Default | Description |
|---|---|---|---|
| `--query` | Yes | — | SQL query to export |
| `--to` | Yes | — | Destination path (local or s3://) |
| `--format` | No | `parquet` | Output format: `parquet` or `csv` |
| `--profile` | No | default | Config profile name |
| `--host` | No | profile | Redshift host |
| `--database` | No | profile | Database name |
| `--user` | No | profile | Database user |
| `--password` | No | profile | Database password |
| `--staging-bucket` | No | profile | S3 staging bucket |
| `--iam-role` | No | profile | IAM role ARN for UNLOAD |
| `--region` | No | profile | AWS region |
| `--verbose` | No | off | Show detailed output |

### Routing behavior

- S3 destination + staging config → bulk mode (UNLOAD, fast)
- Local destination or no staging → safe mode (direct fetch)

---

## arrowjet preview

Inspect a Parquet file without loading it into a database.

```bash
arrowjet preview --file ./sales.parquet
arrowjet preview --file s3://bucket/sales/export.parquet
arrowjet preview --file ./sales.parquet --schema
arrowjet preview --file ./sales.parquet --rows 20
```

| Option | Required | Default | Description |
|---|---|---|---|
| `--file` | Yes | — | Parquet file path (local or s3://) |
| `--schema` | No | off | Show schema only (no sample data) |
| `--rows` | No | `10` | Number of sample rows to display |
| `--region` | No | `us-east-1` | AWS region for S3 files |

### Output

```
File: ./sales.parquet
Rows: 1,000,000
Columns: 15
Size: 248.0 MB (in memory)

Schema:
  id: int64
  amount: double
  customer: string

Sample (10 rows):
  id  amount  customer
   1   99.50  alice
   2  150.00  bob
  ...
```

---

## arrowjet validate

Validate a Redshift table — row count, schema, and sample data.

```bash
arrowjet validate --table sales
arrowjet validate --table sales --row-count
arrowjet validate --table sales --schema
arrowjet validate --table sales --sample --sample-rows 10
```

| Option | Required | Default | Description |
|---|---|---|---|
| `--table` | Yes | — | Table name |
| `--schema-name` | No | `public` | Schema name |
| `--row-count` | No | — | Show row count |
| `--schema` | No | — | Show table schema |
| `--sample` | No | — | Show sample rows |
| `--sample-rows` | No | `5` | Number of sample rows |
| `--profile` | No | default | Config profile name |
| `--host` | No | profile | Redshift host |
| `--database` | No | profile | Database name |
| `--user` | No | profile | Database user |
| `--password` | No | profile | Database password |

If no flags are specified, all three (row count + schema + sample) are shown.

---

## Config Resolution Order

All connection options are resolved in this order:

1. CLI flag (e.g., `--host myhost`)
2. Environment variable (e.g., `REDSHIFT_HOST=myhost`)
3. Profile config (`~/.arrowjet/config.yaml`)

### Environment Variables

| Variable | Maps to |
|---|---|
| `REDSHIFT_HOST` | `--host` |
| `REDSHIFT_DATABASE` | `--database` |
| `REDSHIFT_USER` | `--user` |
| `REDSHIFT_PASS` | `--password` |
| `STAGING_BUCKET` | `--staging-bucket` |
| `STAGING_IAM_ROLE` | `--iam-role` |
| `STAGING_REGION` | `--region` |

---

## Future Commands

| Command | Status | Description |
|---|---|---|
| `arrowjet transfer` | Planned | Move data between systems |
| `arrowjet ask` | Planned | AI natural language queries |
