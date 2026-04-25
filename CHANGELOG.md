# Changelog

All notable changes to arrowjet are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [0.2.0] — 2026-04-24

### Added
- **IAM authentication** — `arrowjet.connect(auth_type="iam")` fetches temporary credentials via `GetClusterCredentials` (provisioned) or `GetCredentials` (serverless). No password needed.
- **Secrets Manager authentication** — `arrowjet.connect(auth_type="secrets_manager", secret_arn="...")` fetches credentials from AWS Secrets Manager.
- **`arrowjet.auth.redshift` module** — standalone credential resolver with `ResolvedCredentials` object (`.as_kwargs()` for redshift_connector/psycopg2, `.as_uri()` for ADBC/SQLAlchemy). Works with any DBAPI driver.
- **CLI `--auth-type` flag** — all CLI commands (export, import, validate, configure) support `--auth-type password|iam|secrets_manager` and `--secret-arn`.
- **Serverless support** — IAM auth auto-detects provisioned vs serverless from the endpoint hostname.
- **Write benchmark results** — six-lane comparison at 1M rows: executemany (27.1h), VALUES (195.8s), parallel VALUES (148.4s), manual COPY (4.06s), arrowjet (3.33s).
- **Benchmark charts in README** — read and write benchmark visualizations now visible on GitHub and PyPI.

### Changed
- CLI connection handling centralized in `config.py` — all commands use the same auth resolution path.
- `arrowjet.connect()` now accepts `auth_type`, `db_user`, `aws_region`, `aws_profile`, `secret_arn` parameters (all optional, backward compatible).
- Connection URI encoding fixed for special characters in IAM temporary passwords.

---

## [0.1.3] — 2026-04-21

### Fixed
- License metadata corrected to MIT (matches LICENSE file). Previously pyproject.toml incorrectly stated Apache-2.0.

---

## [0.1.2] — 2026-04-19

### Added
- **`arrowjet import` command** — load data into Redshift via COPY from S3 or local Parquet/CSV. S3-direct path: data goes S3 → Redshift with no client roundtrip.
- **CLI test suite** — 31 unit tests + 14 integration tests covering all CLI commands (export, import, validate, preview, configure). Includes negative tests for missing credentials, invalid tables, and missing required options.

### Changed
- README CLI section updated with `import` command, S3-direct export, `--schema-name`, and `--profile` usage examples.

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
