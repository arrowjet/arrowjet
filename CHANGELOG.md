# Changelog

All notable changes to arrowjet are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [0.1.1] — 2026-04-19

### Added
- **S3-direct export**: `arrowjet export --to s3://...` now runs UNLOAD directly to the destination — no roundtrip through the client machine. Data goes Redshift → S3 only.
- **IAM authentication**: `arrowjet configure` now offers `auth: iam` option. Uses AWS credentials (`~/.aws/credentials` or instance role) — no password needed.
- **Connection context**: every command now shows which cluster and database it's connected to (e.g. `Connected: my-cluster / dev`).
- **Connection validation on configure**: `arrowjet configure` tests the connection after saving the profile and reports success or failure.
- **Graceful Ctrl+C**: interrupting a long-running export now exits cleanly with a warning about any partial files, instead of hanging or showing a traceback.
- **Optional dependencies**: Redshift drivers (`redshift-connector`, `adbc-driver-postgresql`) are now optional. Install with `pip install arrowjet[redshift]` for full functionality. Core install (`pip install arrowjet`) is lighter for BYOC Engine users.

### Fixed
- README links now point to absolute GitHub URLs (previously broken on PyPI).
- `pyproject.toml` URLs updated to `github.com/arrowjet/arrowjet`.

---

## [0.1.0] — 2026-04-19

### Added
- Initial release.
- `arrowjet.connect()` — unified connection with safe mode (ADBC PG driver) and bulk mode (COPY/UNLOAD via S3).
- `arrowjet.Engine()` — bring your own connection (BYOC). Works with `redshift_connector`, `psycopg2`, ADBC, or any DBAPI connection.
- `read_bulk()` — UNLOAD → S3 → Parquet → Arrow. 2.6x faster than `cursor.fetchall()` at 1M rows, 3.1x at 10M rows (4-node ra3.large, EC2 same region).
- `write_bulk()` / `write_dataframe()` — Arrow/pandas → Parquet → S3 → COPY. Matches manual COPY performance.
- `fetch_dataframe()`, `fetch_arrow_table()`, `fetch_numpy_array()` — safe mode convenience methods.
- SQLAlchemy dialect: `redshift+arrowjet://` (safe mode, SQLAlchemy 2.x compatible).
- CLI: `arrowjet configure`, `arrowjet export`, `arrowjet preview`, `arrowjet validate`.
- Auto mode (`read_auto()`): EXPLAIN-based routing between direct fetch and UNLOAD.
- Observability: `ConnectionMetrics`, `TracingHook`, `CostLogger`.
- Hardening: retry on transient errors, connection health checks, error classification.
- Provider abstraction: `BulkProvider` interface for multi-database support. `RedshiftProvider` included.
- Airflow examples: before/after comparison with benchmarks.
- dbt examples: shell script wrapping `dbt run` + bulkflow export.
