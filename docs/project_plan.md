# Arrowjet — Project Plan

---

## Milestone Tracker

| Milestone | Status | Key Result | Date |
|---|---|---|---|
| M0: Benchmark & Prototype | ✅ Complete | Writes: 1.06x of manual COPY (parity). Reads: 3.98x on 4-node cluster. | 2026-04-03 |
| M1: S3 Staging Subsystem | ✅ Complete | 39 tests passing (25 unit + 14 integration). Full cycle validated against real S3. | 2026-04-04 |
| M2: Bulk Write Engine | ✅ Complete | 53 tests (33 unit + 20 integration). COPY pipeline validated against Redshift. | 2026-04-04 |
| M3: Bulk Read Engine | ✅ Complete | 88 tests (53 unit + 22 integration). UNLOAD pipeline + eligibility checker validated. | 2026-04-04 |
| M4: Safe Mode + Unified Product | ✅ Complete | 22 integration tests. Unified connect() with safe + bulk mode. Phase A done. | 2026-04-05 |
| M5: Observability & Hardening | ✅ Complete | 155 tests. Metrics, tracing hooks, cost logging, retry logic, error classification. | 2026-04-05 |
| M6: Auto Mode | ✅ Complete | 19 new tests. Phase 1 (hints) + Phase 2 (EXPLAIN heuristics). read_auto() API. | 2026-04-05 |
| M7: SQLAlchemy Dialect | ✅ Complete | 29 tests. redshift+arrowjet:// dialect, SA 2.x compatible. | 2026-04-05 |
| CLI MVP | ✅ Complete | export/preview/validate/configure. ~/.arrowjet/config.yaml profiles. | 2026-04-05 |
| Package Rename | ✅ Complete | redshift_adbc → arrowjet. 203 tests passing. | 2026-04-05 |
| Safe Mode Benchmark | ✅ Complete | ADBC vs RS connector vs pyodbc. 1.64x ADBC advantage at 10M rows (EC2). | 2026-04-05 |
| M8: Adoption & Examples | 🔜 In Progress | Airflow ✅, dbt ✅, notebook/pandas pending, SQLAlchemy deferred (after BYOC) | |
| BYOC Engine API | ✅ Complete | arrowjet.Engine() — bring your own connection. 9 integration + 13 unit tests. | 2026-04-19 |
| Provider Abstraction | ✅ Complete | BulkProvider interface. RedshiftProvider. 32 unit tests. Multi-db ready. | 2026-04-19 |
| M9: PyPI & Launch | ✅ Complete | pip install arrowjet. https://pypi.org/project/arrowjet/ | 2026-04-19 |
| M10: CLI & Production Quality | 🔜 Next | Graceful cancellation, IAM auth, S3-direct export, dependency split | |

---

## Architectural Approach

**Arrowjet is a bulk data movement platform, not a driver.**

The project evolved from "build a better ADBC driver" to "build a bulk data movement platform that uses drivers when needed." This shift was validated by M0 benchmarks: the performance gains come from COPY/UNLOAD through cloud storage, not from optimizing the wire protocol. ADBC/ODBC/JDBC are supporting components for safe-mode compatibility — the core value is the bulk engine.

**Build the bulk engine first. Plug drivers in for compatibility second.**

---

## Primary Success Criterion

> By the end of Phase A (Milestone 4), the project must already be a usable Python product with explicit bulk APIs (`read_bulk`, `write_bulk`), safe mode compatibility via the PG ADBC driver, and enough documentation to onboard early users.

If Phase A delivers a working product, Phase B is valuable but optional. If Phase A doesn't deliver, Phase B is irrelevant.

---

## Open-Core Strategy

This project follows an open-core model to maximize adoption and acquisition value.

**Open Source (Phase A + ecosystem integrations)** — the adoption engine:
- Explicit bulk APIs (`read_bulk`, `write_bulk`)
- Safe mode via PG ADBC driver
- S3 staging subsystem with basic cleanup (`on_success`)
- Basic per-operation logging
- pandas / polars compatibility
- SQLAlchemy dialect (safe mode only)
- Airflow provider (explicit bulk operators)
- dbt adapter (safe mode + write bulk)
- Single-database support (Redshift)
- Quickstart docs, IAM guide, migration guide

**Commercial / Pro** — the enterprise confidence layer:

1. **Observability & Cost Intelligence** — "Why did our pipeline cost €5k yesterday?"
   - Per-operation cost estimation (S3 requests + storage + Redshift compute)
   - Bytes scanned / transferred breakdown
   - Latency breakdown (UNLOAD vs S3 vs Parquet read)
   - Historical metrics API (query past operations)
   - Dashboard export / integration hooks

2. **Reliability & Recovery** — "Our pipeline must not fail at 3AM"
   - Automatic retries (COPY/UNLOAD failures with backoff)
   - Resume partial operations (checkpoint-based)
   - Idempotent writes (safe to retry without duplicates)
   - Failure classification (IAM vs network vs data vs Redshift)
   - This is the strongest monetization lever

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

The principle: open source everything that helps people start using it. Commercialize everything that helps companies trust it in production at scale. Integrations are distribution channels, not revenue — gating them behind Pro kills the adoption that makes open-core work.

## Phase Structure

```
Phase A: Core Product — Open Source (M0–M4)       ~4.5 months
  Prove value → build bulk engine → add safe mode → ship + open source

Phase B: Enterprise & Ecosystem — Pro (M5–M9)     ~3.5+ months
  Observability → auto mode → SQLAlchemy → Airflow → dbt
```

---

# PHASE A — Core Product (Open Source)

This is the real project. Everything here ships as open source.

---

## Milestone 0: Benchmarking & Proof of Value (Weeks 1–3)

### Goals
- Quantify the performance gap with concrete numbers
- Build a minimal Python prototype proving `read_bulk` and `write_bulk`
- Show 10-50x wins over standard drivers — this is the pitch
- Produce publishable benchmark artifacts (this is your first marketing weapon)
- If the numbers don't hold, stop early and reassess

### Tasks

| # | Task | Output |
|---|---|---|
| 0.1 | Set up Redshift cluster + test datasets (1M, 10M, 100M rows) | Test environment |
| 0.2 | Set up S3 staging bucket (same region, IAM role configured) | Staging infrastructure |
| 0.3 | Benchmark: `redshift_connector` direct fetch + `fetch_dataframe()` | Baseline read perf |
| 0.4 | Benchmark: `redshift_connector` INSERT + `write_dataframe()` | Baseline write perf |
| 0.5 | Benchmark: JDBC driver (if Java path relevant) | Competitor baseline |
| 0.6 | Build prototype: `read_bulk(query)` — UNLOAD → S3 → Parquet → Arrow | Working bulk read |
| 0.7 | Build prototype: `write_bulk(table, target)` — Arrow → Parquet → S3 → COPY | Working bulk write |
| 0.8 | Benchmark prototype vs baselines at 1M, 10M, 100M rows | Performance gap quantified |
| 0.9 | Document results: speedup factors, latency profiles, resource usage | Benchmark report |
| 0.10 | Publish reproducible benchmark scripts to GitHub repo | Open benchmark suite |
| 0.11 | Write blog post: "Redshift reads: Nx faster with UNLOAD vs wire protocol" | GTM content |
| 0.12 | Create charts / visuals for benchmark results | Shareable assets |

### M0 Is Marketing, Not Just Engineering

The benchmark results are the first public artifact. If the numbers are strong, publish them immediately:
- Blog post with reproducible results
- GitHub repo with benchmark scripts anyone can run
- Charts showing the performance gap

This becomes your HackerNews post, your Reddit r/dataengineering thread, your LinkedIn content. Users will find you before you finish M1. Do not treat M0 as an internal engineering exercise — it is the launch of your public presence.

### Prototype Shape (minimal, throwaway)

```python
import pyarrow.parquet as pq
import boto3
import redshift_connector

def read_bulk(conn, query, staging_bucket, staging_prefix, iam_role):
    s3_path = f"s3://{staging_bucket}/{staging_prefix}/{uuid4()}/"
    unload_sql = f"UNLOAD ('{query}') TO '{s3_path}' IAM_ROLE '{iam_role}' FORMAT PARQUET"
    conn.cursor().execute(unload_sql)
    # download + pq.read_table() + merge + cleanup
    return arrow_table

def write_bulk(conn, arrow_table, target_table, staging_bucket, staging_prefix, iam_role):
    s3_path = f"s3://{staging_bucket}/{staging_prefix}/{uuid4()}/"
    # pq.write_table() + upload
    copy_sql = f"COPY {target_table} FROM '{s3_path}' IAM_ROLE '{iam_role}' FORMAT PARQUET"
    conn.cursor().execute(copy_sql)
    # cleanup
```

### Exit Criteria
- `read_bulk` is 3-15x faster than direct fetch for 10M+ rows on a production-sized cluster (4+ nodes)
- `write_bulk` is within 1.0x–1.25x of manual COPY throughput (parity or better)
- `write_bulk` is 10x+ faster than `write_dataframe` INSERT (pain baseline)
- All three write baselines documented: INSERT, manual COPY, our pipeline
- Benchmark results validated with at least 3 runs for stability
- Benchmark report proves the value proposition
- We know real-world UNLOAD/COPY overhead (setup time, S3 latency)

### Go/No-Go Gate
- Writes: if `write_bulk` is more than 1.5x slower than manual COPY, investigate overhead before proceeding
- Reads: if 4-node cluster does not show meaningful improvement over 1-node, reassess read-side value proposition

---

## Milestone 1: S3 Staging Subsystem (Weeks 4–7)

### Goals
- Replace prototype's ad-hoc S3 handling with production-grade staging
- This is ~40% of the engineering complexity — get it right early

### Tasks

| # | Task | Output |
|---|---|---|
| 1.1 | Design namespace scheme (`cluster/db/conn/stmt/attempt`) | Spec + path generation with tests |
| 1.2 | Implement staging lifecycle state machine | State tracking per operation |
| 1.3 | Implement S3 upload engine (multipart, parallel, retry on 503) | Resilient upload |
| 1.4 | Implement S3 download engine (parallel, streaming to Arrow) | Parallel download |
| 1.5 | Implement cleanup manager (always / on_success / never / ttl_managed) | Configurable cleanup |
| 1.6 | Implement orphan detection (list-by-prefix diagnostic) | Ops tooling |
| 1.7 | Implement configuration model (bucket, prefix, cleanup, encryption, endpoint, region) | Config surface |
| 1.8 | Implement SSE-S3 and SSE-KMS encryption on upload | Encrypted staging |
| 1.9 | Implement connection-time validation (bucket reachable, region match, permissions) | Fail-fast on misconfig |
| 1.10 | Implement concurrency controls (max concurrent bulk ops, queue/reject) | Backpressure |
| 1.11 | Integration test: full cycle under concurrency | End-to-end staging test |

### Dependencies
- AWS credentials / IAM role for S3 + Redshift
- Test S3 bucket in same region as cluster

### Exit Criteria
- Staging handles upload, download, cleanup reliably
- Concurrent operations isolated (no collisions)
- Encryption works (SSE-S3, SSE-KMS)
- Cleanup policies work on success and failure
- Backpressure prevents overload

---

## Milestone 2: Bulk Write Engine — Production Grade (Weeks 7–10)

### Goals
- Replace prototype `write_bulk` with production implementation
- Full error handling, observability, configuration

### Tasks

| # | Task | Output |
|---|---|---|
| 2.1 | Implement Arrow → Parquet conversion (compression, chunking) | Parquet writer |
| 2.2 | Wire Parquet output to staging upload engine | Staged files in S3 |
| 2.3 | Implement COPY command generation (IAM role, format, encryption, manifest) | COPY SQL builder |
| 2.4 | Implement `conn.write_bulk(arrow_table, target_table)` | Public API |
| 2.5 | Implement `conn.write_dataframe(df, target_table)` convenience | pandas compat |
| 2.6 | Handle COPY errors (parse, actionable message, cleanup) | Error handling |
| 2.7 | Add per-operation logging (bytes, files, duration, cleanup outcome) | Observability |
| 2.8 | Benchmark: production `write_bulk` vs prototype vs `redshift_connector` | Perf validation |
| 2.9 | Integration test: 1M, 10M, 100M rows; verify data integrity | Scale + correctness |

### Dependencies
- Milestone 1

### Exit Criteria
- `write_bulk()` works end-to-end with full error handling
- Within 1.0x–1.25x of manual COPY throughput (parity target)
- 10x+ faster than INSERT for 100K+ rows
- Cleanup works on success and failure
- Logging shows what happened
- Benchmarked against all three baselines: INSERT, manual COPY, our pipeline

---

## Milestone 3: Bulk Read Engine — Production Grade (Weeks 10–14)

### Goals
- Replace prototype `read_bulk` with production implementation
- Define and enforce execution eligibility rules

### Tasks

| # | Task | Output |
|---|---|---|
| 3.1 | Implement UNLOAD command generation (query, S3 path, format, encryption) | UNLOAD SQL builder |
| 3.2 | Implement S3 result discovery (list UNLOAD output files) | File listing |
| 3.3 | Implement parallel Parquet → Arrow reader with result merge | Parquet reader |
| 3.4 | Implement `conn.read_bulk(query)` | Public API |
| 3.5 | Implement eligibility checker (autocommit, no temp tables, SELECT only, staging valid) | Safety guard |
| 3.6 | Handle UNLOAD errors (permissions, query failures, partial output, cleanup) | Error handling |
| 3.7 | Add per-operation logging (rows, bytes, files, duration) | Observability |
| 3.8 | Benchmark: production `read_bulk` vs prototype vs `redshift_connector` | Perf validation |
| 3.9 | Integration test: 1M, 10M, 100M rows; verify data integrity | Scale + correctness |
| 3.10 | Test: behavior with temp tables, transactions, non-SELECT | Safety tests |

### Dependencies
- Milestone 1
- Can run in parallel with Milestone 2 if team size allows

### Exit Criteria
- `read_bulk()` works end-to-end with full error handling
- 3-15x faster than direct fetch for large datasets
- Eligibility rules prevent unsafe usage
- Cleanup works correctly

---

## Milestone 4: Safe Mode via PG ADBC Driver + Unified Product (Weeks 14–18)

### Goals
- Integrate the PG ADBC driver as the implementation for safe/direct mode
- Build the unified connection that exposes both safe and bulk interfaces
- Write enough documentation to onboard early users
- This is the Phase A finish line — after this, you have a shippable product

### Tasks

| # | Task | Output |
|---|---|---|
| 4.1 | Integrate `adbc-driver-postgresql` as dependency for safe mode | PG driver wired in |
| 4.2 | Implement unified `redshift_adbc.connect()` (initializes safe + bulk) | Single entry point |
| 4.3 | Implement `cursor.execute()` + `fetchall()` / `fetchone()` via PG wire | DBAPI cursor |
| 4.4 | Implement `cursor.fetch_dataframe()` / `fetch_numpy_array()` | Convenience methods |
| 4.5 | Implement `cursor.fetch_arrow_table()` via safe mode (ADBC native) | Arrow from safe path |
| 4.6 | Verify ADBC metadata APIs for Redshift (`GetObjects`, `GetTableSchema`) | Metadata validation |
| 4.7 | Ensure metadata always reflects safe-mode capabilities | Metadata contract |
| 4.8 | Implement `read_mode` / `write_mode` connection-level config (direct / unload / copy) | Mode selection |
| 4.9 | Test: safe mode matches `redshift_connector` behavior for standard queries | Compat validation |
| 4.10 | Test: safe mode works inside transactions, with temp tables, with cursors | Semantic safety |
| 4.11 | Python package structure (`redshift-adbc`), installable via pip | Package |
| 4.12 | Write quickstart guide (connect, safe query, bulk read, bulk write) | User docs |
| 4.13 | Write configuration reference | Reference docs |
| 4.14 | Write IAM setup guide (3 deployment models) | Ops docs |
| 4.15 | Write migration guide from `redshift_connector` | Adoption docs |

### Dependencies
- Milestones 2 and 3

### Exit Criteria
- `pip install redshift-adbc` works
- Single `connect()` gives access to both safe and bulk APIs
- Safe mode behaves like a normal Redshift driver
- Metadata is correct and always reflects safe-mode semantics
- No bulk engine behavior leaks through safe interface
- Documentation is sufficient for early adopters
- **Product is usable and shippable**

---

## Phase A Summary

| Milestone | Weeks | What You Have After |
|---|---|---|
| M0: Benchmark & Prototype | 1–3 | Proof that the idea works, concrete numbers |
| M1: Staging Subsystem | 4–7 | Production-grade S3 infrastructure |
| M2: Bulk Write (COPY) | 7–10 | `write_bulk()` — 10-50x faster writes |
| M3: Bulk Read (UNLOAD) | 10–14 | `read_bulk()` — 3-15x faster reads |
| M4: Safe Mode + Unified Product | 14–18 | Complete, shippable Python product |

**Phase A total: ~18 weeks (~4.5 months)**

After Phase A, you have:
- A working Python package on PyPI (open source, permissive license)
- Explicit bulk APIs that deliver order-of-magnitude speedups
- Safe mode that works like a normal driver
- Documentation for early adopters
- Benchmark data to back up the pitch

This is enough to: open source, build community, onboard early users, pitch internally, or seek investment. The OSS core is the adoption engine.

---

# PHASE B — Expansion

Prioritize based on user feedback and adoption signals from Phase A. Milestones are marked as OSS (open source) or Pro (commercial) based on the open-core strategy.

---

## Milestone 5: Observability & Hardening (Weeks 19–21) — Pro

### Goals
- Production-grade observability and edge case handling
- This is enterprise confidence — exactly what Pro customers pay for

### Tasks

| # | Task | Output |
|---|---|---|
| 5.1 | Implement `conn.get_metrics()` (counters: ops, bytes, requests, failures) | Metrics API |
| 5.2 | Implement optional tracing hook (`on_bulk_operation` callback) | Tracing integration |
| 5.3 | Implement `cost_logging` levels (off / summary / verbose) | Cost visibility |
| 5.4 | Add `max_staging_bytes`, `disallow_cross_region_staging` | Safety limits |
| 5.5 | Harden: connection drop during bulk op | Graceful cleanup |
| 5.6 | Harden: S3 throttling / 503 retries | Retry resilience |
| 5.7 | Harden: Redshift restart during UNLOAD/COPY | Error recovery |
| 5.8 | Harden: concurrent `write_bulk` to same table | Conflict handling |

---

## Milestone 6: Auto Mode (Weeks 21–24) — Pro

### Goals
- Optional smart routing — only after real usage data informs thresholds
- Keep it conservative; explicit mode remains the recommended path

### Tasks

| # | Task | Output |
|---|---|---|
| 6.1 | Implement `cursor(mode="auto")` interface | Opt-in auto |
| 6.2 | Phase 1 routing: explicit hints only (`bulk_hint=True`) | Conservative auto |
| 6.3 | Phase 2 routing: EXPLAIN-based size estimation | Heuristic routing |
| 6.4 | Threshold configuration (`auto_mode_threshold_rows`, `_bytes`) | User controls |
| 6.5 | Routing decision logging (INFO: why this path was chosen) | Observability |
| 6.6 | Test: never routes inside transactions; falls back on EXPLAIN failure | Safety tests |
| 6.7 | Benchmark: auto mode overhead vs always-direct | Overhead measurement |

### Note
Auto mode is a convenience feature, not a core value driver. Ship it only when you have enough real-world usage data to set sensible defaults. Until then, explicit `read_bulk` / `write_bulk` is the product.

---

## Milestone 7: SQLAlchemy Dialect (Weeks 24–28) — Open Source

### Goals
- Safe-mode-only SQLAlchemy dialect
- This is a distribution channel, not a monetization feature — it must be open source to maximize adoption
- High leverage (unlocks Superset, dbt, and the broader SQLAlchemy ecosystem) but also a time sink — scope carefully

### Tasks

| # | Task | Output |
|---|---|---|
| 7.1 | Implement `redshift+adbc://` dialect (safe mode only) | SQLAlchemy dialect |
| 7.2 | Metadata reflection (tables, columns, types, schemas) | Introspection |
| 7.3 | Transaction handling | Transaction compat |
| 7.4 | Run SQLAlchemy dialect test suite | Compat validation |
| 7.5 | Test with Superset (basic connection + query) | Tool validation |
| 7.6 | Document: supported features, gaps, bulk API alongside SQLAlchemy | User docs |

### Honest Warning
SQLAlchemy dialects are notorious for consuming more time than estimated. Budget extra time for edge cases in type mapping, reflection, and transaction semantics. The test suite will surface surprises.

### Note on Bulk Integration
The dialect operates in safe mode only — all queries go through the wire protocol via `redshift_connector`. This is intentional: SQLAlchemy expects DBAPI semantics (transactions, cursors, row-based results) and routing transparently through UNLOAD would break them.

For bulk reads alongside SQLAlchemy, use the hybrid pattern (documented in M8 examples):
```python
# SQLAlchemy for query building
sql = str(select(my_table).where(...).compile(dialect=engine.dialect))
# Arrowjet for bulk execution
result = conn.read_bulk(sql)
```
This is a usage pattern, not a dialect feature. No dev work needed in the dialect itself.

---

## Milestone 8: Adoption & Examples — Open Source

### Goals
- Show real users how to use Arrowjet in their existing workflows
- Before/after comparisons (code + performance)
- No new library code — just examples, benchmarks, and docs

### Tasks

| # | Task | Output |
|---|---|---|
| 8.1 | Airflow example: without_arrowjet.py (manual UNLOAD/COPY, ~50 lines) | ✅ Done |
| 8.2 | Airflow example: with_arrowjet.py (TaskFlow API, two-task DAG) | ✅ Done |
| 8.3 | Airflow example: BashOperator with CLI | ✅ Done |
| 8.4 | dbt example: shell script wrapping dbt run + arrowjet export | ✅ Done |
| 8.5 | dbt example: profiles.yml + dbt_project.yml + SQL model | ✅ Done |
| 8.6 | Benchmark: three-way comparison (naive/manual UNLOAD/arrowjet) on EC2 | ✅ Done — 1M: 2.6x, 10M: 3.1x |
| 8.7 | Jupyter notebook quickstart | 🔜 Next |
| 8.8 | Blog post draft: "Nx faster Redshift data movement" | ⬜ Pending |
| 8.9 | pandas example: read_sql vs read_bulk before/after | 🔜 Next |
| 8.10 | SQLAlchemy example: create_engine redshift+arrowjet:// + hybrid pattern (SA for query building, arrowjet for bulk read) | ⬜ Deferred (after BYOC Engine API) |
| 8.11 | AWS Glue example: Python Shell job using arrowjet | ⬜ Deferred (post-M9) |

### Structure
```
examples/
  airflow/
    without_arrowjet.py
    with_arrowjet.py
    with_arrowjet_cli.py
    README.md
  dbt/
    dbt_project.yml
    profiles.yml
    macros/arrowjet_sync.sql
    README.md
  notebooks/
    quickstart.ipynb
```

---

## BYOC Engine API: Bring Your Own Connection — Open Source ✅ Complete

### Goals
- Let users use arrowjet's bulk engine with any existing DBAPI connection
- Lower adoption barrier: no need to replace connection management
- Driver-agnostic: works with redshift_connector, psycopg2, ADBC, or any DBAPI-compatible connection
- Coexists with `arrowjet.connect()` — two entry points for two audiences

### Result
`arrowjet.Engine` implemented and tested. 13 unit tests + 9 integration tests against real Redshift.

```python
engine = arrowjet.Engine(staging_bucket=..., staging_iam_role=..., staging_region=...)
result = engine.read_bulk(existing_conn, query)
engine.write_bulk(existing_conn, arrow_table, "target_table")
engine.write_dataframe(existing_conn, df, "target_table")
```

---

## Provider Abstraction — Open Source ✅ Complete

### Goals
- Make BulkReader and BulkWriter database-agnostic
- No if-elses in the engine — each database is a plugin
- Clean interface for adding Snowflake, BigQuery, Databricks

### Result
`BulkProvider` ABC with `build_export_sql()` and `build_import_sql()`.
`RedshiftProvider` implements it (UNLOAD/COPY, LIMIT wrapping, KMS).
32 unit tests. Adding a new database = implement `BulkProvider`, pass to engine.

```python
# Adding Snowflake (future)
class SnowflakeProvider(BulkProvider):
    def build_export_sql(self, query, staging_path, ...):
        return ExportCommand(sql=f"COPY INTO '{staging_path}' FROM ({query}) ...", ...)
    def build_import_sql(self, target_table, staging_path, ...):
        return ImportCommand(sql=f"COPY INTO {target_table} FROM '{staging_path}' ...", ...)
```

### Tasks

| # | Task | Output |
|---|---|---|
| BYOC.1 | Implement `arrowjet.Engine` class with staging config | Engine with read_bulk/write_bulk accepting external conn |
| BYOC.2 | Extract shared bulk logic from ArrowjetConnection into Engine | No code duplication between the two paths |
| BYOC.3 | Add `write_dataframe()` convenience on Engine | pandas compat for BYOC path |
| BYOC.4 | Unit tests for Engine with mock connections | Verify driver-agnostic contract |
| BYOC.5 | Integration test: Engine with redshift_connector | Validate against real Redshift |
| BYOC.6 | Integration test: Engine with psycopg2 | Validate driver-agnostic claim |
| BYOC.7 | Update examples to show both paths (Airflow, notebook) | Adoption docs |
| BYOC.8 | Update README, migration guide, quickstart | User-facing docs |

### Priority
High — this directly reduces adoption friction. Should ship before or alongside M9 (PyPI launch). The Engine API makes the Airflow/dbt examples even simpler since users don't need to change their connection setup.

### Exit Criteria
- `arrowjet.Engine` works with redshift_connector, psycopg2, and ADBC connections
- Same bulk performance as `arrowjet.connect()` path
- Examples show both entry points
- Docs clearly explain when to use which

---

## Milestone 9: PyPI & Launch — Open Source

### Goals
- Make `pip install arrowjet` real
- Polish README with demo/GIF
- Publish blog post

### Tasks

| # | Task | Status | Output |
|---|---|---|---|
| 9.1 | Polish README (install, quickstart, before/after) | ✅ Done | User-facing README |
| 9.2 | Publish to PyPI | ✅ Done | pip install arrowjet 0.1.0 |
| 9.3 | Fix README links (relative → absolute GitHub URLs) | ✅ Done | arrowjet 0.1.1 pending publish |
| 9.4 | Push to arrowjet/arrowjet GitHub org | ✅ Done | https://github.com/arrowjet/arrowjet |
| 9.5 | Publish blog post | ⬜ Pending | HackerNews / Reddit / LinkedIn |
| 9.6 | Submit to awesome-python / awesome-data-engineering | ⬜ Pending | Discovery |
| 9.7 | Publish arrowjet 0.1.1 to PyPI (README link fix) | ⬜ Pending | pip install arrowjet==0.1.1 |

---

## Milestone 10: CLI & Production Quality — Open Source

### Goals
- Fix issues discovered during real-world CLI testing
- Make arrowjet production-grade for first users
- No new features — polish and correctness only

### Tasks

| # | Task | Output |
|---|---|---|
| 10.1 | **S3-direct export** — UNLOAD directly to destination S3 path, no client roundtrip | ✅ Done |
| 10.2 | **Graceful Ctrl+C** — catch KeyboardInterrupt, cancel Redshift query, clean up staged files, exit cleanly | ✅ Done |
| 10.3 | **IAM authentication** — support `auth: iam` in config profile (no password, uses AWS credentials) | ✅ Done |
| 10.4 | **Secrets Manager auth** — support `auth: secrets_manager` with secret ARN | ⬜ Pending |
| 10.5 | **Optional dependencies** — split `redshift-connector` and `adbc-driver-postgresql` into optional extras (`pip install arrowjet[redshift]`) | ✅ Done |
| 10.6 | **Connection validation on configure** — test the connection after `arrowjet configure` and report success/failure | ✅ Done |
| 10.7 | **Progress indicator** — show progress for long-running exports (rows/s, elapsed time) | ⬜ Pending |
| 10.8 | **Show connection context in output** — every command shows which cluster/database it's using | ✅ Done |
| 10.9 | **`arrowjet profiles` command** — list configured profiles, show which is default | ⬜ Pending |
| 10.10 | **`--dry-run` flag for export** — show UNLOAD SQL without executing | ⬜ Pending |
| 10.11 | **`--from-file` flag for export** — read query from a SQL file | ⬜ Pending |
| 10.12 | **Row count in S3-direct export** — show rows exported after UNLOAD completes | ⬜ Pending |
| 10.13 | **Preview without full download** — use Parquet metadata for schema/row count, only fetch first row group for sample | ⬜ Pending |
| 10.14 | **Truncate wide sample output** — limit column width in sample display, truncate long strings | ⬜ Pending |
| 10.15 | **`arrowjet import` command** — load data into Redshift via COPY from S3 or local Parquet/CSV | ✅ Done |

### Priority
High — these are real gaps found during first-user testing. Fix before community outreach.

---

## Phase B Summary

| Milestone | Weeks | Priority | Tier |
|---|---|---|---|
| M5: Observability & Hardening | 19–21 | High (production readiness) | Pro |
| M6: Auto Mode | 21–24 | Medium (convenience, not core value) | Pro |
| M7: SQLAlchemy Dialect | 24–28 | High (ecosystem unlock, time-risky) | Open Source |
| CLI MVP | — | High (zero-code adoption) | Open Source |
| M8: Adoption & Examples | — | High (reach real users) | Open Source |
| BYOC Engine API | ✅ Complete | High (adoption friction reduction) | Open Source |
| Provider Abstraction | ✅ Complete | High (multi-database foundation) | Open Source |
| M9: PyPI & Launch | — | High (discoverability) | Open Source |
| M10: CLI & Production Quality | — | High (first-user polish) | Open Source |

**Phase B total: ~15 weeks (~3.5 months)**

---

## Full Timeline

| Phase | Milestones | Duration | Tier | What You Have |
|---|---|---|---|---|
| Phase A | M0–M4 | ~4.5 months | Open Source | Shippable data movement layer with bulk + safe APIs |
| Phase B | M5–M6 | ~6 weeks | Pro | Observability, hardening, auto mode |
| Phase B | M7–M9 | ~9 weeks | Open Source | SQLAlchemy, Airflow, dbt integrations |
| Total | M0–M9 | ~8 months (1 eng) / ~6 months (2 eng) | Full product | OSS data movement platform + Pro enterprise layer |

---

## Parallelization (with 2 engineers)

| Parallel pair | Saves |
|---|---|
| M2 (bulk write) + M3 (bulk read) | ~4 weeks |
| M5 (hardening) + M6 (auto mode) | ~2 weeks |
| M8 (Airflow) + M7 (SQLAlchemy, partial) | ~2 weeks |

With 2 engineers: Phase A ~3.5 months, Phase B ~2.5 months, total ~6 months.

---

## Key Decision: When Does the PG Driver Enter?

The PG ADBC driver enters at Milestone 4 — after the bulk engine is proven and production-grade.

- M0–M3: No PG driver dependency. Pure bulk engine development.
- M4: PG driver integrated as a component for safe mode.
- M5+: Router and integrations build on both paths.

The architecture is shaped by the bulk engine (the value), not by the PG driver (the compatibility layer).

---

## Key Risks

| Risk | Mitigation | Milestone |
|---|---|---|
| Prototype doesn't show expected speedup | Investigate dataset size, network, UNLOAD config; go/no-go gate at M0 | M0 |
| S3 staging adds unexpected latency | Benchmark per-region; enforce same-region bucket | M1 |
| PG ADBC driver has Redshift-specific bugs | File upstream; maintain local patches if needed | M4 |
| SQLAlchemy dialect consumes more time than planned | Scope tightly; document gaps honestly; don't chase 100% compat | M7 |
| IAM too complex for users | Provide CloudFormation/Terraform templates | M4 |
| Auto mode makes wrong routing decisions | Conservative defaults; always allow override; log decisions | M6 |
| Redshift UNLOAD behavior varies across versions | Test against provisioned + serverless | M3 |
| Phase B stretches beyond estimate | Treat Phase B milestones as independent; ship each as it completes | M5–M9 |


---

## Future Roadmap

Items below are not scheduled. Prioritize based on adoption signals and user feedback.

### Near-term: CLI MVP (arrowjet CLI)

Priority: ✅ Complete — delivered as part of Phase B.

### Deferred: Custom Airflow Operator

Priority: Deferred — revisit after M8 examples validate adoption.

Original scope:
- Custom `ArrowjetReadOperator` and `ArrowjetWriteOperator`
- Native Airflow operator with connection management
- Test in sample DAG

Why deferred: examples using existing Airflow primitives (TaskFlow, PythonOperator, BashOperator) achieve the same adoption with less engineering risk. Build custom operators only if users request better DX after trying the examples.

Why deferred: dbt adapters are complex to build and maintain. dbt Labs has opinions about adapter behavior. Pre-run hooks and profiles.yml integration (in M8 examples) provide adoption without the maintenance burden. Build the full adapter only if dbt users adopt via examples and ask for native support.

Priority: Low-Medium — required for full dbt adoption, but complex.

The only way to inject arrowjet's UNLOAD path into `dbt run` query execution is at the driver level. Two approaches:

1. **Wrap redshift_connector** — a Python wrapper that intercepts large queries and routes them through UNLOAD. dbt-redshift would need to accept a custom connection class.
2. **Native ADBC driver for Redshift** — build a proper Redshift ADBC driver (Rust/C++ with Python bindings) that dbt can use via an ADBC adapter. This is the strongest long-term moat.

Why this matters: without driver-level integration, arrowjet only helps dbt users at the edges (hooks, CLI). The core `dbt run` query execution stays slow for large result sets. Driver-level integration would make every `dbt run` faster transparently.

Track alongside dbt Fusion (dbt's new Rust execution engine) — if Fusion exposes execution hooks, that may be a lower-effort path than a full ADBC driver.

Prerequisite: validate adoption via hook-based examples first. Build the driver only if dbt users adopt and ask for deeper integration.

Priority: Deferred — revisit after M8 examples validate adoption.

Original scope:
- Full dbt adapter using SQLAlchemy dialect (safe mode for queries)
- Wire `dbt seed` to `write_bulk()` for COPY-based loading
- Test `dbt run` + `dbt seed` against Redshift

### Deferred: dbt Adapter

### Medium-term: Multi-database expansion

Priority: Medium — strategic for platform positioning and acquisition.

- Snowflake (internal stages + COPY INTO)
- BigQuery (GCS + Load API / Storage Read API)
- Databricks (cloud storage + COPY INTO)

Requires: storage backend abstraction in M1 staging subsystem (S3 → S3/GCS/Azure Blob).

### Long-term: AI Data Query Layer

Priority: Low — separate product that uses Arrowjet as backend. Not core to the data movement platform.

Concept: natural language → SQL → Athena/Redshift → results + summary. Uses Bedrock for SQL generation, Glue for schema discovery.

Architecture: React UI → API Gateway → Lambda → Bedrock → Athena → S3

This is a potential Pro feature or standalone product. Do not pursue until the core platform has traction.
