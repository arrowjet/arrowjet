# Arrowjet MVP — Product Specification

## Overview

Arrowjet is a developer-first data movement engine designed to make data transfer between systems fast, simple, and reliable.

The MVP focuses on a tight execution layer, not orchestration, UI, or scheduling.

## Core Value Proposition

Move data across systems in the fastest, simplest, and most reliable way — without pipelines, DAGs, or heavy tooling.

## Target Users

- Data Engineers
- Backend Engineers
- Small teams without full data stack
- Infra engineers dealing with data movement

---

## MVP Scope (Strict)

### 1. Export

Export data from a database to storage (e.g., S3, local filesystem).

```bash
arrowjet export \
  --source redshift \
  --query "SELECT * FROM sales" \
  --target s3://bucket/sales \
  --format parquet
```

Key Capabilities:
- Efficient export (UNLOAD / COPY where possible)
- Columnar formats (Parquet)
- Streaming / chunking (avoid memory issues)

### 2. Transfer

Move data between systems (warehouse → warehouse, DB → DB).

```bash
arrowjet transfer \
  --from redshift \
  --to snowflake \
  --table sales
```

Key Capabilities:
- Cross-warehouse abstraction
- Minimal configuration
- No intermediate scripts required

### 3. Preview / Validate

Quickly inspect and validate exported data.

```bash
arrowjet preview \
  --file s3://bucket/sales/*.parquet

arrowjet validate \
  --row-count \
  --schema \
  --sample
```

Key Capabilities:
- Schema inspection
- Row count validation
- Sample preview
- Engine-backed (DuckDB / Athena internally)

---

## Non-Goals (MVP)

The following are explicitly out of scope:

- ❌ Workflow orchestration (e.g., Airflow)
- ❌ Scheduling
- ❌ UI / dashboards
- ❌ Connectors marketplace
- ❌ AI layer (future phase)

---

## Architecture (MVP)

```
CLI / Python API
       ↓
  arrowjet engine
       ↓
Databases / Storage (S3, local)
```

### Two Entry Points

```python
# Path 1: Unified connection (new projects)
conn = arrowjet.connect(host=..., staging_bucket=..., ...)
result = conn.read_bulk(query)

# Path 2: Bring your own connection (existing codebases)
engine = arrowjet.Engine(staging_bucket=..., staging_iam_role=..., staging_region=...)
result = engine.read_bulk(existing_conn, query)
```

Path 1 is for greenfield users who want one entry point. Path 2 is for existing codebases (Airflow, ETL, dbt) where connection management already exists. Both use the same bulk engine underneath.

---

## Design Principles

- Execution-first — focus on performance and reliability
- Developer-first — simple CLI + Python API
- Minimal surface area — avoid feature creep
- Composable — can plug into orchestration tools later

---

## Differentiation

| Category | Limitation | Arrowjet Advantage |
|---|---|---|
| Orchestration tools | Don't move data efficiently | Fast execution layer |
| ETL tools | Rigid, expensive | Flexible, programmable |
| Scripts | Fragile, slow | Reliable, optimized |

---

## MVP Success Criteria

- Export large dataset without memory issues
- Transfer data between systems with minimal config
- Preview data in seconds after export
- CLI experience is simple and intuitive

---

## Future Extensions (Post-MVP)

- AI layer (intent → execution)
- Orchestration integrations (Airflow, etc.)
- Advanced validation & data quality
- Plugin system for formats and destinations

---

## Positioning Statement

> Arrowjet is the execution layer for modern data pipelines — making data movement fast, programmable, and intelligent.
