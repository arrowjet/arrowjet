# M4 Work Log: Safe Mode + Unified Product

## Date Started: 2026-04-05

---

## Step 1: ADBC Driver Integration

Installed `adbc-driver-postgresql` (1.10.0) — ships pre-compiled C++ binary as `.so` on macOS (Mach-O arm64). Python wrapper via `adbc-driver-manager`.

Verified connectivity to Redshift via ADBC:
```bash
pip install adbc-driver-postgresql adbc-driver-manager
```

### Redshift-specific quirks discovered:
- ADBC PG driver uses `$1` parameter style, not `%s` (PostgreSQL native)
- `autocommit` not exposed as a property on ADBC Connection object
- `information_schema.schemata` returns truncated results on Redshift — use `pg_namespace` instead

---

## Step 2: Unified Connection

Created `src/arrowjet/connection.py` — single `connect()` entry point:

- Safe mode: ADBC PG driver for DBAPI cursor, metadata, transactions
- Bulk mode: `redshift_connector` for COPY/UNLOAD commands (ADBC experimental on Redshift)
- Both modes through same connection object

---

## Step 3: Packaging

- `pyproject.toml` — pip-installable as `arrowjet`
- `README.md` — quickstart guide
- `docs/configuration.md` — full parameter reference
- `docs/iam_setup.md` — 3 IAM deployment models
- `docs/migration_guide.md` — redshift_connector → Arrowjet

---

## Step 4: Tests

22 integration tests:

| Category | Tests | Status |
|---|---|---|
| Safe mode (cursor, dataframe, arrow, numpy) | 7 | ✅ |
| Bulk mode (read, write, dataframe, both modes) | 5 | ✅ |
| Metadata (tables, columns, schemas) | 4 | ✅ |
| Compatibility (types, nulls, rows) | 3 | ✅ |
| Transactions (autocommit, commit, numpy) | 3 | ✅ |

---

## Task Status

| # | Task | Status |
|---|---|---|
| 4.1 | Integrate ADBC driver | ✅ |
| 4.2 | Unified connect() | ✅ |
| 4.3 | cursor.execute/fetchall/fetchone | ✅ |
| 4.4 | fetch_dataframe / fetch_numpy_array | ✅ |
| 4.5 | fetch_arrow_table | ✅ |
| 4.6 | Verify ADBC metadata APIs | ✅ (with Redshift quirk fixes) |
| 4.7 | Metadata reflects safe-mode only | ✅ |
| 4.8 | Mode selection (has_bulk) | ✅ |
| 4.9 | Compat test vs redshift_connector | ✅ |
| 4.10 | Transactions, autocommit | ✅ |
| 4.11 | pip-installable package | ✅ (pyproject.toml) |
| 4.12 | Quickstart guide | ✅ (README.md) |
| 4.13 | Configuration reference | ✅ |
| 4.14 | IAM setup guide | ✅ |
| 4.15 | Migration guide | ✅ |

---

## Phase A Complete

M0-M4 all done. Arrowjet is a usable Python product with:
- Explicit bulk APIs (read_bulk, write_bulk)
- Safe mode via ADBC PG driver
- Automatic S3 staging with cleanup
- Documentation for early adopters

Date completed: 2026-04-05
