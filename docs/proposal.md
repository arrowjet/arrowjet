# Arrowjet — High-Performance Data Movement Platform
## Full Project Proposal & Execution Plan

---

# 1. Executive Summary

## Vision
Build a **bulk data movement platform** starting with Redshift that:
- Delivers **order-of-magnitude improvements** for bulk workloads
- Provides **Arrow-native data access**
- Abstracts each database's fastest data paths behind a simple, explicit API
- Expands to Snowflake, BigQuery, and Databricks

> This is not a driver. It is a bulk data movement platform that uses drivers (ADBC, ODBC, JDBC) as supporting components for safe-mode compatibility, while the core value lives in the bulk engine (COPY/UNLOAD via cloud storage).

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Layer 3: Integration Layer                     │
│  (SQLAlchemy, Airflow, dbt, pandas, polars)     │
├─────────────────────────────────────────────────┤
│  Layer 2: Bulk Engine  ← core value             │
│  (COPY, UNLOAD, S3/GCS staging, Arrow, Parquet) │
├─────────────────────────────────────────────────┤
│  Layer 1: Driver Layer                          │
│  (ADBC, ODBC — safe mode, metadata, compat)     │
└─────────────────────────────────────────────────┘
```

## Core Idea
Redshift has two fundamentally different data paths:

| Path | Protocol | Format | Performance |
|------|--------|--------|------------|
| Standard fetch | PostgreSQL wire | Row-based | Limited |
| Bulk path | S3 (COPY/UNLOAD) | Columnar (Parquet) | High |

👉 Arrowjet exposes both paths explicitly: safe mode for tools, bulk mode for performance

---

# 2. Problem Statement

## Current Limitations

### Reads
- PostgreSQL wire protocol → **row-based**
- Requires:
  - row materialization
  - conversion to columnar (Arrow)
- CPU + memory overhead

### Writes
- INSERT / batch → slow
- leader node bottleneck

---

## Key Insight

> You cannot make Redshift fast by optimizing the wire protocol.

👉 You must **bypass it for data movement**

---

# 3. Solution Overview

## Hybrid Execution Engine

### Reads
```
Small → Direct fetch
Large → UNLOAD → S3 → Parquet → Arrow
```

### Writes
```
Small → INSERT
Large → Arrow → Parquet → S3 → COPY
```

---

## Key Advantage

| Operation | Traditional | This Product |
|----------|------------|------------|
| Large Read | row fetch | UNLOAD |
| Large Write | INSERT | COPY |
| Format | row → column | column → column |
| Parallelism | limited | full cluster |

---

# 4. Product Features

## Core Features

### 1. Smart Execution Router
- decides:
  - direct vs bulk
- based on:
  - row count
  - query type
  - config

---

### 2. Bulk Read Engine
- Executes:
```
UNLOAD ('SELECT ...') TO S3 FORMAT PARQUET
```
- Reads files → Arrow
- merges results

---

### 3. Bulk Write Engine
- Arrow input
- convert → Parquet
- upload → S3
- execute COPY

---

### 4. Dual API Surface

Two distinct interfaces for two distinct use cases:

```python
# Safe mode — DBAPI-compatible, what tools use
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE id = %s", (42,))
row = cursor.fetchone()

# Explicit bulk mode — user opts in, knows the tradeoffs
table = conn.read_bulk("SELECT * FROM events")
conn.write_bulk(arrow_table, "target_table")

# Optional auto mode — opt-in, guardrailed
cursor = conn.cursor(mode="auto")
```

Safe mode is the default. Bulk mode is explicit. Auto mode is opt-in.

---

### 5. Configurable Behavior

```
read_mode = auto | direct | unload
write_mode = auto | direct | copy
```

---

# 5. Architecture

## Components

### 1. Router
- central brain
- decides execution path

---

### 2. Staging Manager
- S3 paths
- lifecycle
- cleanup
- manifests

---

### 3. Parquet Layer
- Arrow ↔ Parquet
- compression
- streaming

---

### 4. S3 Engine
- multipart upload
- parallelism
- retries

---

### 5. Redshift Executor
- runs:
  - UNLOAD
  - COPY
- error handling

---

### 6. Result Layer
- returns Arrow
- hides source (fetch vs unload)

---

# 6. Execution Flow

## Direct Read
```
SQL → PG protocol → rows → Arrow
```

## Bulk Read
```
SQL → UNLOAD → S3 → Parquet → Arrow
```

## Bulk Write
```
Arrow → Parquet → S3 → COPY
```

---

# 7. Performance Expectations

## Realistic Targets

### Writes
- 10x – 50x faster (COPY vs INSERT)

### Reads
- Direct: 1.5x – 3x improvement
- Bulk: 3x – 15x improvement (large datasets)

## Important: Latency vs Throughput Tradeoff

Bulk mode (UNLOAD/COPY) optimizes for **total completion time**, not time-to-first-row.

| Metric | Direct fetch | Bulk (UNLOAD) |
|---|---|---|
| Time to first row | Fast (milliseconds) | Slow (seconds — S3 round-trip overhead) |
| Time to full result (large) | Slow (row-by-row transfer) | Fast (parallel, columnar) |

Users should expect: a bulk read of 10 rows will be **slower** than direct fetch. A bulk read of 10 million rows will be **much faster**. This tradeoff is inherent to the S3 staging approach and cannot be eliminated — only managed through correct mode selection.

---

# 8. MVP Plan

## Phase 1: Benchmarking
- compare:
  - JDBC
  - ODBC
  - Python connector

---

## Phase 2: Bulk Write (highest ROI)
- implement COPY pipeline

---

## Phase 3: Bulk Read
- UNLOAD + Parquet read

---

## Phase 4: Auto Mode
- heuristics
- thresholds

---

## Phase 5: Packaging
- Python
- shared libs

---

# 9. Risks & Mitigation

## Risk: hidden UNLOAD surprises
→ expose logs + config

## Risk: IAM complexity
→ simple config templates

## Risk: small query slowdown
→ conservative thresholds

---

# 10. Go-To-Market Positioning

## Strong Message

> "Fastest way to move data in and out of Redshift"

## Avoid

❌ "10x faster queries"
❌ "drop-in replacement for Tableau/BI"

## BI Tools (Tableau, Superset, etc.) — Future Vision

BI tools are distribution channels, not the product. Prioritizing BI compatibility early would force the engine into ODBC/JDBC constraints and optimize for compatibility over performance — killing differentiation.

Strategy: add BI compatibility after the core engine is strong, not before.

When ready, three options:
1. Smart ODBC/JDBC wrapper — detect heavy queries, internally switch to UNLOAD/COPY, return results as if normal query. No Tableau changes required.
2. Extract to external storage — arrowjet exports to S3 (Parquet), Tableau connects via Athena or external tables. Scalable but requires workflow change.
3. Hybrid — ODBC compatibility by default, externalized data access for advanced users.

Priority: do not build BI-first. Do not constrain the core engine to ODBC limitations. Add compatibility in Phase B or later.

## CLI Vision (sqlxport)

A command-line tool wrapping the arrowjet engine for ad-hoc bulk operations:

```bash
sqlxport export --query "SELECT * FROM events" --format parquet --dest s3://bucket/path/
sqlxport import --source s3://bucket/data.parquet --table target_table
sqlxport preview --query "SELECT * FROM events" --limit 1000
```

`arrowjet` = the engine. `sqlxport` = the CLI on top. This is a natural Tier 3 product — post-Phase A, good for open-source adoption and scripting workflows.

---

# 11. Final Strategy

## Key Principle

> Use PostgreSQL protocol as control plane
> Use S3 + Parquet as data plane

---

## One-line Vision

> A bulk data movement platform that uses drivers when needed.


---

# 12. Integration & Adoption Strategy

## The Problem

A high-performance driver alone is not enough. Most Redshift users interact with the database through established tools and frameworks — not raw driver APIs:

| User Persona | Primary Tool | Current Driver |
|---|---|---|
| Data Scientists | pandas / polars | psycopg2, SQLAlchemy |
| Analytics Engineers | dbt | dbt-redshift (psycopg2) |
| Pipeline Engineers | Airflow | RedshiftSQLOperator, S3ToRedshiftOperator |
| App Developers | SQLAlchemy ORM | psycopg2, redshift_connector |
| Data Platform Teams | Custom ETL scripts | JDBC, ODBC, boto3 |

None of these users will rewrite working code to adopt a new driver API — no matter how fast it is. ADBC adoption in the Python ecosystem is still early. pandas and polars technically support ADBC connections, but the vast majority of users are on SQLAlchemy or psycopg2. "ADBC support exists" does not mean "adoption is automatic."

> The risk is not building a bad product. The risk is building a great product nobody uses.

There is a deeper problem beyond reach: this product has two personalities.

1. A **safe SQL driver** — predictable, transactional, boring. What tools expect.
2. A **bulk data movement engine** — fast, S3-based, different semantics. What makes this product valuable.

These cannot be conflated. Tools like SQLAlchemy and dbt assume DBAPI behavior: row-based execution, transaction support, predictable metadata. If the bulk engine leaks into the compatibility surface — if SQLAlchemy asks "do you support transactions?" and the answer depends on whether the router chose UNLOAD — tools break, users lose trust, adoption dies.

## The Architecture: Three Layers

The product must be structured as three distinct layers:

```
┌─────────────────────────────────────────────────┐
│  Layer 1: Compatibility Layer                   │
│  (metadata + DBAPI + safe mode)                 │
│  What tools see. Boring, predictable, safe.     │
│  SQLAlchemy, dbt, BI tools interact here.       │
├─────────────────────────────────────────────────┤
│  Layer 2: Execution Router                      │
│  (decides direct vs bulk path)                  │
│  Internal logic. Invisible to tools.            │
├─────────────────────────────────────────────────┤
│  Layer 3: Bulk Engine                           │
│  (S3, Parquet, UNLOAD, COPY, Arrow)             │
│  The performance layer. Exposed only through    │
│  the explicit bulk API, never through Layer 1.  │
└─────────────────────────────────────────────────┘
```

### Layer 1: Compatibility Layer

This is what tools see. It must behave like a normal, predictable Redshift driver:

- Implements DBAPI 2.0 (PEP 249) for Python
- Metadata introspection always reflects safe-mode capabilities
- Transaction support: yes (because safe mode uses the PG wire protocol)
- No surprises: tools never encounter bulk engine behavior through this layer

This is critical because tools query metadata statically. SQLAlchemy will ask "what tables exist?" and "what types are supported?" at dialect initialization — long before any query runs. dbt inspects schemas before every run. BI tools like Superset enumerate catalogs on connection. If metadata responses vary based on execution path, these tools will produce inconsistent or broken behavior.

ADBC provides standardized metadata APIs (`GetObjects`, `GetTableSchema`, `GetTableTypes`) as first-class operations in the spec. The existing `redshift_connector` had to manually implement JDBC-style `DatabaseMetaData` methods (table listing, column introspection, type mapping) to support tool integrations. This gives us a structural advantage: less custom glue code, more standardized behavior. The metadata layer is implemented once, correctly, and always reflects safe-mode semantics.

> Metadata layer = safe mode only. Always. No exceptions.

### Layer 2: Execution Router

Internal only. Decides whether a given operation goes through direct fetch or bulk path. Tools never interact with this layer directly. Defined in detail in Section 15 (Auto Mode Decision Engine).

### Layer 3: Bulk Engine

The performance layer. Exposed to users only through the explicit product API (see below). Never surfaced through the compatibility layer.

## The Product API: Explicit Over Implicit

ADBC is the transport layer — internal plumbing for Arrow-native data movement. It is not the product API.

This product makes decisions that have cost, semantic, and performance implications: UNLOAD to S3, COPY from S3, temporary object lifecycle, KMS encryption. These cannot be hidden behind `cursor.execute()`. Users need to see and control what's happening.

### Prior Art: redshift_connector

The existing official Redshift Python driver (`amazon-redshift-python-driver`) already established that DBAPI alone is insufficient for Redshift users. It added purpose-built methods:

- `cursor.fetch_dataframe()` — return results as pandas DataFrame
- `cursor.fetch_numpy_array()` — return results as numpy array
- `cursor.write_dataframe(df, table)` — write a DataFrame to a table

Users already expect Redshift drivers to have convenience methods beyond raw DBAPI. We are not inventing a new pattern — we are extending it with bulk-aware operations.

### Our API Surface

```python
import redshift_adbc

conn = redshift_adbc.connect(
    host="...",
    database="dev",
    staging_bucket="s3://my-staging-bucket/tmp/",
    staging_cleanup_policy="on_success",
)

# ── Safe mode (DBAPI-compatible, what tools use) ──────────────
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE id = %s", (42,))
row = cursor.fetchone()                    # normal fetch, PG wire protocol
df = cursor.fetch_dataframe()              # pandas DataFrame (compat with redshift_connector)

# ── Explicit bulk mode (product API, user-facing) ─────────────
# Bulk read — user knows: this uses UNLOAD, hits S3, not transactional
table = conn.read_bulk("SELECT * FROM events WHERE date > '2025-01-01'")
# Returns Arrow Table. User opted in explicitly.

# Bulk write — user knows: this uses COPY, stages through S3
conn.write_bulk(arrow_table, "target_table")
# Arrow → Parquet → S3 → COPY. Explicit, no surprises.

# ── Auto mode (opt-in, with guardrails) ───────────────────────
cursor = conn.cursor(mode="auto")
cursor.execute("SELECT * FROM large_table")
table = cursor.fetch_arrow_table()
# Driver chose UNLOAD because: autocommit=True, no temp tables,
# estimated result > threshold. Logged at INFO level.
```

### Why Explicit Matters

| Method | User knows | Semantics |
|---|---|---|
| `cursor.execute()` + `fetchall()` | Normal SQL, safe | Transactional, PG wire |
| `cursor.fetch_dataframe()` | Convenience, safe | Same as fetchall, returns DataFrame |
| `conn.read_bulk(query)` | Uses UNLOAD + S3 | Not transactional, may cost S3 |
| `conn.write_bulk(table, target)` | Uses COPY + S3 | Stages through S3, bulk semantics |
| `cursor(mode="auto")` | Driver decides | Logged, guardrailed, opt-in |

The explicit bulk API is a trust contract. When a user calls `read_bulk`, they know what's happening: this will use UNLOAD, this will hit S3, this is not transactional, this may incur S3 costs. When they use `cursor.execute()`, they get safe, predictable behavior. No surprises either way.

This is not just naming — it's the difference between a driver and a product. A driver is transparent plumbing. This product makes decisions with cost and semantic implications. Users must be able to see and control those decisions.

## Tool Integration Strategy

With the three-layer architecture, tool integrations become compatibility layers with constrained behavior — not thin wrappers that expose the full engine.

### Phase 5: pandas / polars

- pandas `read_sql()` and polars `read_database()` support ADBC connections
- Works out of the box for safe mode (direct fetch → Arrow)
- Bulk mode available through the explicit API: `conn.read_bulk()` returns Arrow, which pandas/polars consume natively
- Not "zero integration work" — requires documentation, testing, and clear guidance on when to use safe vs bulk
- Effort: ~1-2 weeks

### Phase 5: SQLAlchemy Dialect (scoped, safe mode only)

This is the highest-leverage and hardest integration. SQLAlchemy expects DBAPI behavior — row-based execution, transaction support, predictable metadata. It is a semantic translation layer, not a thin wrapper.

Approach:
- Implement a `redshift+adbc://` dialect
- Dialect operates in safe mode only: no UNLOAD, no COPY, no auto-routing
- Metadata introspection backed by ADBC's `GetObjects` / `GetTableSchema` — standardized, less custom code than the existing `sqlalchemy-redshift` dialect which had to hand-roll JDBC-style metadata
- Clearly document: SQLAlchemy integration provides compatibility and metadata, not bulk performance
- Users who want bulk performance use the explicit API alongside SQLAlchemy, not through it

What this unlocks:
- Superset, and other SQLAlchemy-based tools
- Foundation for dbt adapter
- Drop-in replacement for existing `sqlalchemy-redshift` users (safe mode is semantically identical)

Effort: ~3-4 weeks (dialect implementation, metadata mapping, transaction handling, testing against SQLAlchemy test suite)

### Phase 6: Airflow Provider

Good fit because Airflow already embraces explicit COPY/S3 patterns:
- `S3ToRedshiftOperator` already wraps COPY
- New operator can use `write_bulk()` / `read_bulk()` directly — explicit semantics align with Airflow's operator model
- Each operator does one clear thing; no hidden routing
- Effort: ~2 weeks

### Phase 6: dbt Adapter (write optimization only)

dbt expects strict SQL semantics for query execution. Routing reads through UNLOAD would break assumptions.

Approach:
- Use the SQLAlchemy dialect (safe mode) for all query execution
- Use `write_bulk()` for `dbt seed` (loading CSV/data into tables) — this is where COPY shines
- Do NOT route `dbt run` queries through UNLOAD
- Effort: ~2-3 weeks (depends on SQLAlchemy dialect maturity)

## Effort Summary

| Integration | Effort | Mode | Dependency |
|---|---|---|---|
| pandas / polars | ~1-2 weeks | Safe + explicit bulk | Core product |
| SQLAlchemy dialect | ~3-4 weeks | Safe mode only | Core product |
| Airflow provider | ~2 weeks | Explicit bulk | Core product |
| dbt adapter | ~2-3 weeks | Safe + write bulk | SQLAlchemy dialect |

## Key Principles

1. **Metadata layer = safe mode only.** Tools must see a normal, predictable Redshift data layer. The bulk engine is invisible at the metadata level.
2. **Bulk performance is opt-in, not hidden.** Users access it through explicit APIs (`read_bulk`, `write_bulk`) or explicit mode selection (`mode="auto"`).
3. **Compatibility is not a wrapper — it's a semantic contract.** The SQLAlchemy dialect doesn't "wrap" the bulk engine. It constrains behavior to what SQLAlchemy expects.
4. **ADBC is the transport, not the product.** The product API is what users interact with. ADBC is the internal plumbing that makes Arrow-native transport possible.
5. **Match existing expectations.** The existing `redshift_connector` set the bar with `fetch_dataframe()` and `write_dataframe()`. Our API must meet that bar and extend it with bulk-aware operations.
6. **Meet users where they are.** The product without integrations is an engineering achievement that doesn't reach users. Both must ship.
7. **Don't force connection changes.** Users with existing connection management (Airflow, ETL scripts) should be able to use arrowjet's bulk engine without replacing their connection setup. The Engine API (`arrowjet.Engine`) accepts any DBAPI connection.

## Two Entry Points: Unified Connection vs BYOC Engine

The product offers two ways to use the bulk engine, serving different audiences:

### Path 1: Unified Connection (greenfield users)
```python
conn = arrowjet.connect(host=..., staging_bucket=..., ...)
result = conn.read_bulk("SELECT * FROM events")
conn.write_bulk(arrow_table, "target_table")
df = conn.fetch_dataframe("SELECT COUNT(*) FROM events")  # safe mode
```
Best for: new projects, scripts, notebooks. One entry point, both safe and bulk mode.

### Path 2: BYOC Engine (existing codebases)
```python
# User keeps their existing connection
conn = redshift_connector.connect(host=..., database=..., ...)

# Arrowjet just does the bulk part
engine = arrowjet.Engine(staging_bucket=..., staging_iam_role=..., staging_region=...)
result = engine.read_bulk(conn, "SELECT * FROM events")
engine.write_bulk(conn, arrow_table, "target_table")
```
Best for: Airflow DAGs, ETL scripts, dbt hooks — anywhere connection management already exists.

### Contract
The Engine requires only `conn.cursor()` and `cursor.execute(sql)` — standard DBAPI. Works with redshift_connector, psycopg2, ADBC dbapi, or any DBAPI-compatible connection. The bulk path only fires UNLOAD/COPY SQL — no result set fetching, no driver-specific features needed.

### Why Both
- `arrowjet.connect()` is the recommended path for new users — zero friction, everything included
- `arrowjet.Engine()` is the adoption path for existing codebases — zero rewiring, just add bulk


---

# 13. Bulk Staging Design

## Why This Section Exists

The Staging Manager is not a utility component — it is the core of the product. Every bulk read and bulk write flows through S3 staging. If staging is unreliable, the entire driver is unreliable. This section defines the contracts, policies, and failure modes.

## Namespace Scheme

Every staged operation gets a unique, collision-free path:

```
s3://<bucket>/<base-prefix>/<cluster-id>/<database>/<conn-uuid>/<stmt-uuid>/<attempt-n>/
```

This structure isolates:
- concurrent statements (different `stmt-uuid`)
- retries (different `attempt-n`)
- multiple connections (different `conn-uuid`)
- multiple users sharing a bucket (different `conn-uuid` + `cluster-id`)

The driver generates `conn-uuid` at connection time and `stmt-uuid` per operation. Attempt numbers increment on retry.

## Concurrency Controls

Namespace isolation prevents collisions, but does not prevent overload. Too many concurrent bulk operations can pressure the Redshift cluster, saturate S3 throughput, or exhaust staging storage.

| Control | Default | Purpose |
|---|---|---|
| `max_concurrent_bulk_ops` | 4 per connection | Limits parallel UNLOAD/COPY operations to prevent cluster pressure |
| `max_staging_bytes` | 10GB per operation | Prevents a single operation from consuming excessive S3 storage |
| `bulk_queue_behavior` | `wait` | When limit is reached: `wait` (queue until a slot opens) or `reject` (raise immediately) |

When `max_concurrent_bulk_ops` is reached, new bulk operations either queue (default) or raise an error, depending on configuration. Direct-mode operations are never throttled.

```python
conn = adbc_driver_redshift.connect(
    ...
    max_concurrent_bulk_ops=4,
    bulk_queue_behavior="wait",  # or "reject"
)
```

## Lifecycle States

Each staged operation transitions through defined states:

```
planned → files_staging → manifest_written → command_submitted → completed → cleanup_done
                                                                    ↓
                                                              cleanup_failed
```

The driver tracks state internally per operation. On unexpected termination, orphaned objects remain in S3 at their namespaced path — identifiable and cleanable by prefix.

## Cleanup Policy

Cleanup behavior is configurable per connection:

| Policy | Behavior |
|---|---|
| `always` | Delete staged files after every operation regardless of outcome |
| `on_success` (default) | Delete on success, preserve on failure for debugging |
| `never` | Leave all staged files (user manages lifecycle) |
| `ttl_managed` | Driver does not delete; relies on S3 lifecycle rules to expire objects |

Recommended production setup: `on_success` + S3 lifecycle rule with 24-48h TTL on the staging prefix as a safety net for orphans.

Configuration:

```python
conn = adbc_driver_redshift.connect(
    ...
    staging_cleanup_policy="on_success",
    staging_ttl_recommendation_days=2,  # logged as guidance, not enforced
)
```

## IAM and Account Model

Three deployment models must be supported:

### Model 1: Same account, same region (simplest)
- Redshift cluster IAM role has read/write to the staging bucket
- Driver uses the same or ambient credentials for S3 operations
- Single set of permissions

### Model 2: Same account, separate bucket ownership
- Staging bucket managed by a different team
- Requires explicit bucket policy granting:
  - Redshift cluster role: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the staging prefix
  - Driver credentials: same permissions for upload/download/cleanup

### Model 3: Cross-account bucket
- Bucket in a different AWS account
- Requires:
  - Bucket policy with cross-account principal allowance
  - Redshift cluster role with `sts:AssumeRole` or direct cross-account S3 access
  - Driver-side credentials with cross-account access

### Required permissions (minimum)

Redshift cluster role:
```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::<staging-bucket>",
    "arn:aws:s3:::<staging-bucket>/<base-prefix>/*"
  ]
}
```

Driver-side (for upload/download/cleanup):
```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::<staging-bucket>",
    "arn:aws:s3:::<staging-bucket>/<base-prefix>/*"
  ]
}
```

If SSE-KMS is used, add:
```json
{
  "Effect": "Allow",
  "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
  "Resource": "arn:aws:kms:<region>:<account>:key/<key-id>"
}
```

## Encryption

The driver must support and document:

| Mode | Who configures | Notes |
|---|---|---|
| SSE-S3 | Bucket default or driver | Simplest, no extra permissions |
| SSE-KMS (AWS-managed key) | Bucket default or driver | Requires `kms:Decrypt` / `kms:GenerateDataKey` |
| SSE-KMS (customer-managed key) | Explicit config | Key ARN must be provided; both Redshift role and driver need key access |

Responsibility split:
- Driver-side uploads (bulk write): driver sets encryption headers on `PutObject`
- Redshift UNLOAD: Redshift applies encryption based on UNLOAD command parameters or bucket default
- Redshift COPY: Redshift reads encrypted objects using its IAM role's KMS permissions

Configuration:

```python
conn = adbc_driver_redshift.connect(
    ...
    staging_encryption="SSE_KMS",
    staging_kms_key_id="arn:aws:kms:us-east-1:123456789:key/abc-def",
)
```

## Networking

Enterprise deployments often restrict S3 access paths. The driver must work in:

| Environment | Requirement |
|---|---|
| Public subnet | Default — no special config |
| Private subnet + VPC endpoint | S3 requests route through VPC endpoint; driver must not assume public S3 endpoints |
| Private subnet + NAT gateway | Works transparently but has cost/bandwidth implications |
| Region-locked | Staging bucket must be in the same region as the Redshift cluster for COPY/UNLOAD |

The driver should:
- Accept an optional `s3_endpoint_url` override for non-standard endpoints
- Validate at connection time that the staging bucket is reachable
- Log a clear error if COPY/UNLOAD fails due to network/permission issues (not just the Redshift error code)

## Failure Recovery

| Failure Point | Behavior |
|---|---|
| Upload fails mid-transfer | Abort multipart upload, clean up partial parts, retry or raise |
| UNLOAD fails | Log Redshift error, clean up any partial S3 output, raise |
| COPY fails | Log Redshift error, staged files preserved (per cleanup policy), raise with actionable message |
| Driver crashes mid-operation | Orphaned files remain at namespaced path; S3 lifecycle rule handles expiry |
| Cleanup fails | Log warning, do not raise — operation result is already returned to user |

---

# 14. Execution Semantics and Safety

## Why This Section Exists

Routing a query through UNLOAD instead of direct fetch is not a transparent optimization — it changes execution semantics. The driver must define exactly when bulk routing is safe and when it is not.

## The Core Problem

A normal `SELECT` via the PostgreSQL wire protocol:
- Executes within the current transaction
- Sees the current snapshot (including uncommitted changes in the same transaction)
- Can reference temporary tables and session-scoped objects
- Supports cursor-based incremental fetching

An `UNLOAD` command:
- Runs as a separate Redshift operation
- May not see uncommitted transaction state
- Cannot reference session-local temporary tables (in some configurations)
- Always materializes the full result to S3

These are different semantics. Silently swapping one for the other can produce incorrect results.

## Eligibility Rules for Auto-Routing

The driver will only consider bulk-read routing (UNLOAD) when ALL of the following are true:

| Condition | Rationale |
|---|---|
| Connection is in autocommit mode OR user has explicitly opted into `read_mode=unload` | Prevents snapshot visibility surprises inside transactions |
| Query does not reference temporary tables | Temp tables may not be visible to UNLOAD |
| Query is a SELECT (not a DML with RETURNING, not a cursor declaration) | UNLOAD only works with SELECT |
| Staging configuration is fully valid (bucket, permissions, region) | Cannot attempt bulk path without working staging |
| Result is expected to be fully materialized (no streaming/cursor semantics requested) | UNLOAD produces a complete result, not a stream |

If any condition fails, the driver falls back to direct fetch — silently, with a debug-level log entry.

## Mode Behavior

| Mode | Behavior |
|---|---|
| `read_mode=direct` | Always use PostgreSQL wire protocol. Semantically safe in all contexts. |
| `read_mode=unload` | Always use UNLOAD. User accepts bulk-export semantics. Fails if staging is not configured. |
| `read_mode=auto` (default) | Use direct unless all eligibility conditions are met AND the estimated result size exceeds the routing threshold. |

## Write-Side Semantics

Bulk write (COPY) has fewer semantic risks but still requires:

| Condition | Rationale |
|---|---|
| Target table exists and is not a temp table (unless explicitly allowed) | COPY into temp tables may have session-scoping issues |
| No active transaction that expects atomicity with other statements | COPY is its own transaction; mixing with other DML in a transaction may not behave as expected |
| Staging config is valid | Same as reads |

## Correctness Guarantee

> The driver will never silently change query semantics in `auto` mode. If there is any ambiguity about whether bulk routing is safe, the driver falls back to direct execution.

This is the primary design principle. Performance is secondary to correctness.

---

# 15. Auto Mode Decision Engine

## Why This Section Exists

A hardcoded row-count threshold is not sufficient to decide between direct fetch and UNLOAD. The break-even point depends on multiple variables. This section defines how the driver makes that decision.

## The Two Optimization Goals

Users care about different things depending on context:

| Goal | Metric | Best Path |
|---|---|---|
| Low latency (first rows fast) | Time to first row | Direct fetch |
| High throughput (full result fast) | Time to complete result | UNLOAD (for large results) |

UNLOAD always has fixed overhead (command submission, S3 write, S3 read). For small results, this overhead dominates. For large results, the parallelism wins.

The driver must be clear: auto mode optimizes for total completion time, not first-row latency.

## Cost Model

The routing decision is based on estimated total time for each path:

```
T_direct ≈ T_query + T_fetch_rows + T_row_to_arrow
T_unload ≈ T_query + T_unload_overhead + T_s3_write + T_s3_read + T_parquet_to_arrow
```

Where:
- `T_unload_overhead` is the fixed cost of UNLOAD setup (~2-5 seconds typically)
- `T_fetch_rows` scales linearly with row count and row width
- `T_s3_write` and `T_s3_read` scale with data volume but benefit from parallelism

## Decision Strategy (Phased)

### Phase 1: Conservative Defaults (MVP)

No estimation. Simple rules:

- Default to `direct` for all queries
- `auto` mode only activates UNLOAD when the user has provided an explicit hint:

```python
# User signals this is a bulk query
cursor.execute("SELECT * FROM large_table", bulk_hint=True)
```

Or via query-level override:

```python
cursor.set_options(read_mode="unload")
cursor.execute("SELECT * FROM large_table")
```

This avoids any risk of surprising behavior in the MVP.

### Phase 2: Heuristic Routing

Use observable signals to estimate result size before choosing a path:

- `EXPLAIN` output (estimated rows, estimated width)
- Query shape analysis (e.g., `SELECT *` from a known large table vs. `SELECT count(*)`)
- User-configured thresholds:

```python
conn = adbc_driver_redshift.connect(
    ...
    auto_mode_threshold_rows=100_000,       # estimated rows to trigger UNLOAD
    auto_mode_threshold_bytes=50_000_000,   # estimated bytes to trigger UNLOAD
)
```

Decision flow:

```
1. Run EXPLAIN on the query
2. Extract estimated_rows and estimated_width
3. estimated_bytes = estimated_rows × estimated_width
4. If estimated_bytes > threshold → UNLOAD
5. Else → direct fetch
```

Fallback: if EXPLAIN fails or returns no estimate, use direct fetch.

### Phase 3: Adaptive Routing (Post-MVP)

Learn from observed execution history:

- Track actual bytes transferred per query shape
- Track actual time for direct vs. UNLOAD paths
- Adjust thresholds per-connection or per-session based on observed performance
- Factor in observed S3 latency for the current region/endpoint

This is a future optimization. The MVP must work well without it.

## User Controls

Regardless of phase, users always have full override:

| Setting | Effect |
|---|---|
| `read_mode=direct` | Never use UNLOAD |
| `read_mode=unload` | Always use UNLOAD (fails if staging not configured) |
| `read_mode=auto` | Driver decides per-query |
| `auto_mode_threshold_rows` | Override row threshold for auto decisions |
| `auto_mode_threshold_bytes` | Override byte threshold for auto decisions |
| `bulk_hint=True` (per-query) | Signal that this specific query should prefer UNLOAD |

---

# 16. Cost and Operational Model

## Why This Section Exists

Bulk mode trades compute time for S3 operations. This is not free. The proposal must acknowledge the cost implications and give users visibility and control.

## Cost Dimensions

| Cost Source | When Incurred | Scale |
|---|---|---|
| S3 PUT requests | Every bulk write (upload Parquet files) | Per-file; depends on parallelism / chunk size |
| S3 GET requests | Every bulk read (download UNLOAD output) | Per-file; depends on UNLOAD parallelism |
| S3 LIST requests | Manifest listing, cleanup | Per-operation |
| S3 storage | Temporary staged files | Duration × size; depends on cleanup policy |
| KMS requests | If SSE-KMS is used | Per encrypt/decrypt operation |
| Data transfer | If cross-AZ or cross-region | Per-byte; can be significant for large datasets |
| Redshift compute | UNLOAD/COPY execution | Cluster time; uses cluster resources |

## Realistic Cost Example

A bulk read of 1GB of data with UNLOAD:
- ~10 Parquet files written to S3: 10 PUT requests (~$0.00005)
- ~10 GET requests to read them: (~$0.000004)
- Temporary storage for ~60 seconds: negligible
- Total S3 cost: < $0.001

The S3 cost is trivial for most workloads. The real cost consideration is:
- KMS requests at scale (thousands of operations/hour)
- Cross-region transfer if misconfigured
- Cluster compute time for UNLOAD (which uses Redshift resources)

## User Controls

```python
conn = adbc_driver_redshift.connect(
    ...
    # Hard limits
    max_staging_bytes=10_000_000_000,        # 10GB max staged data per operation
    disallow_cross_region_staging=True,       # fail if bucket region ≠ cluster region

    # Observability
    cost_logging="summary",                   # off | summary | verbose

    # Cleanup
    staging_cleanup_policy="on_success",
    staging_ttl_recommendation_days=2,
)
```

## Observability

The driver logs per-operation metrics at `INFO` level (when `cost_logging` is enabled):

```
[ADBC-Redshift] Bulk read completed:
  path: UNLOAD
  rows: 5,234,000
  staged_bytes: 847MB
  s3_files: 12
  s3_requests: 25 (12 PUT + 12 GET + 1 LIST)
  staging_duration: 4.2s
  total_duration: 6.8s
  cleanup: completed
```

This gives users and operators enough information to:
- Understand what the driver did and why
- Estimate cost impact
- Debug performance issues
- Decide whether to adjust mode or thresholds

## Metrics and Tracing Hooks

Logs are sufficient for debugging but not for production monitoring at scale. The driver exposes programmatic metrics for integration with monitoring systems:

### Counters (cumulative per connection)

| Metric | Description |
|---|---|
| `ops_direct_read` | Number of reads via direct fetch |
| `ops_bulk_read` | Number of reads via UNLOAD |
| `ops_direct_write` | Number of writes via INSERT |
| `ops_bulk_write` | Number of writes via COPY |
| `bytes_staged_total` | Total bytes written to S3 staging |
| `bytes_read_total` | Total bytes read from S3 staging |
| `s3_requests_total` | Total S3 API requests (PUT + GET + LIST + DELETE) |
| `cleanup_failures` | Number of failed cleanup attempts |

### Access

```python
metrics = conn.get_metrics()
print(metrics.ops_bulk_read)       # 42
print(metrics.bytes_staged_total)  # 128_849_018_880
```

### Tracing Hook (optional)

For integration with OpenTelemetry or custom tracing:

```python
def on_bulk_operation(event):
    # event contains: operation_type, duration, bytes_staged, s3_requests, path_chosen
    my_tracer.record(event)

conn = redshift_adbc.connect(
    ...
    on_bulk_operation=on_bulk_operation,
)
```

The tracing hook fires after every bulk operation completes (success or failure). It receives a structured event object, not raw log text.

## Operational Recommendations

| Concern | Recommendation |
|---|---|
| Orphaned files | Set S3 lifecycle rule on staging prefix (24-48h expiry) |
| Cost monitoring | Use S3 request metrics + CloudWatch to track staging bucket activity |
| Region alignment | Always place staging bucket in the same region as the Redshift cluster |
| Encryption | Use bucket default encryption (SSE-S3) unless compliance requires KMS |
| Access auditing | Enable S3 access logging or CloudTrail on the staging bucket |

## What the Driver Will Not Do

- It will not estimate dollar costs (too many variables, pricing changes)
- It will not enforce budget limits (that's a platform concern)
- It will not create or manage the staging bucket (user provides it)
- It will not modify bucket policies or IAM roles (user configures access)

The driver's responsibility is: use the staging infrastructure correctly, clean up after itself, and give users full visibility into what it did.


---

# 17. Open-Core Strategy & Acquisition Positioning

## Why Open-Core

The #1 failure mode of infrastructure tools is: great technology, zero adoption. Open-sourcing the core product eliminates this risk by making adoption frictionless. Users try it, contribute, build trust, and create a community — which is the real moat an acquirer pays for.

The open-core model splits the product into two tiers:

**Open Source** — the adoption engine:
- Explicit bulk APIs (`read_bulk`, `write_bulk`)
- Safe mode via PG ADBC driver (DBAPI, metadata, transactions)
- S3 staging subsystem with basic cleanup
- Basic per-operation logging
- pandas / polars compatibility
- SQLAlchemy dialect (safe mode only)
- Airflow provider (explicit bulk operators)
- dbt adapter (safe mode + write bulk)
- Single-database support (Redshift)

**Pro / Commercial** — the enterprise confidence layer:

1. **Observability & Cost Intelligence** — "Why did our pipeline cost €5k yesterday?"
   - Per-operation cost estimation (S3 requests + storage + Redshift compute)
   - Bytes scanned / transferred breakdown
   - Latency breakdown per phase (UNLOAD vs S3 transfer vs Parquet read)
   - Historical metrics API
   - Dashboard export / integration hooks

2. **Reliability & Recovery** — "Our pipeline must not fail at 3AM"
   - Automatic retries with backoff (COPY/UNLOAD failures)
   - Resume partial operations via checkpointing
   - Idempotent writes (safe to retry without duplicates)
   - Failure classification (IAM vs network vs data vs Redshift)
   - This is the strongest monetization lever — enterprises pay for reliability

3. **Staging Lifecycle Management** — "We can't leave terabytes of sensitive data in S3 accidentally"
   - TTL policies (auto cleanup after configurable hours/days)
   - Orphan detection + automated cleanup
   - Cross-account staging configuration
   - Encryption policy enforcement
   - Audit logs (who wrote what, where, when)

4. **Smart Routing (Auto Mode)** — "Smart defaults that reduce human error"
   - EXPLAIN-based estimation
   - Adaptive thresholds (learn from execution history)
   - Per-query-shape routing optimization

5. **Governance & Compliance** — "Security team approved this"
   - PII tagging (column-level on staged data)
   - Data classification
   - Audit trails
   - Policy enforcement (e.g., "no UNLOAD to non-encrypted bucket")

6. **Advanced Integration Features** — basic integrations stay OSS, advanced capabilities are Pro:
   - Airflow: retry policies, lineage tracking, cost-per-DAG
   - dbt: lineage integration, cost tracking per model
   - Terraform / CloudFormation templates for full infrastructure setup
   - SSO / IAM automation

7. **Multi-Database Expansion** — "One tool for all warehouses"
   - Snowflake, BigQuery, Databricks (future)
   - Same API, same staging model, different backends

8. **Support + SLAs**

The key rule: if it helps people start using the product → open source. If it helps companies trust it in production at scale → Pro. Integrations like SQLAlchemy, Airflow, and dbt are distribution channels that accelerate adoption — gating them behind Pro would kill the ecosystem spread that makes open-core valuable.

## M0 as Marketing

The benchmark results from Milestone 0 are not just an engineering artifact — they are the first public marketing asset. If the numbers are strong (10-50x write speedup, 3-15x read speedup), they should be published immediately as:
- A blog post with reproducible results
- A GitHub repo with benchmark scripts anyone can run
- Charts and visuals for social sharing

This becomes the HackerNews post, the Reddit r/dataengineering thread, the LinkedIn content that drives early awareness. Users will find the project before the core product is even finished. M0 is the launch of the public presence.

## Acquisition Positioning

A single-database open-source tool is a feature acquisition — low price, fast deal. A multi-database data movement platform with an active open-source community is a platform acquisition — higher price, more leverage.

The path:
1. Open source the Redshift core (Phase A) — build community and prove the model
2. Ship the Pro layer — prove monetization and enterprise demand
3. Ship OSS integrations (SQLAlchemy, Airflow, dbt) — prove ecosystem position
4. Expand to Snowflake and BigQuery — prove the platform thesis
5. At that point, the acquirer sees: "this team solved bulk data movement for 3+ warehouses with a single API, has an active OSS community, and a working commercial model"

That's a platform acquisition by Databricks, Confluent, dbt Labs, or one of the cloud providers — not an acqui-hire.

Acquirers don't buy code. They buy community, adoption, and platform position. The open-source core builds all three. The Pro layer proves the business model. The code being free is not a weakness — it's the entire reason the product is valuable.
