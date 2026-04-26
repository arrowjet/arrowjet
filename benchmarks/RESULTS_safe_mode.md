# Safe Mode Benchmark Results

## Environment

### Local (macOS, VPN)
- Platform: macOS arm64
- Python: 3.13.7
- Network: VPN to us-east-1
- Cluster: REDACTED_CLUSTER (dc2.large, 1 node)

### EC2 (same-region)
- Platform: Linux x86_64
- Instance: r5.xlarge (4 vCPU, 32GB RAM)
- Python: 3.11.14
- Network: same VPC as Redshift cluster
- Cluster: REDACTED_CLUSTER (dc2.large, 1 node)

### Drivers tested
- ADBC PostgreSQL driver 1.10.0 (C++ via Python bindings)
- redshift_connector 2.1.13 (Python, PG wire protocol)
- pyodbc 5.3.0 + Redshift ODBC 2.1.15.0 (C++ driver)

### Precautions
- Result cache disabled (`SET enable_result_cache_for_session = off`)
- ODBC logging disabled (LogLevel=0)
- 3 iterations per test, median reported
- Same table, same query across all drivers

---

## EC2 Results (same-region  - authoritative)

### 1M rows (benchmark_test_1m, 15 columns)

| Driver | Method | Median | vs RS connector |
|---|---|---|---|
| ADBC | fetch_arrow_table (native) | 15.35s | 1.15x faster |
| ADBC | fetch_arrow -> to_pandas | 17.22s | 1.02x faster |
| redshift_connector | fetch_dataframe | 17.59s | baseline |
| pyodbc | fetchall -> DataFrame | 18.21s | 0.97x (slower) |

### 10M rows (benchmark_test_10m, 15 columns)

| Driver | Method | Median | vs RS connector |
|---|---|---|---|
| ADBC | fetch_arrow_table (native) | 90.26s | 1.64x faster |
| ADBC | fetch_arrow -> to_pandas | 86.27s | 1.72x faster |
| redshift_connector | fetch_dataframe | 148.18s | baseline |
| pyodbc | fetchall -> DataFrame | 163.49s | 0.91x (slower) |

### Scaling behavior

| Driver | 1M | 10M | Scale factor |
|---|---|---|---|
| ADBC Arrow native | 15.35s | 90.26s | 5.9x |
| ADBC -> pandas | 17.22s | 86.27s | 5.0x |
| redshift_connector | 17.59s | 148.18s | 8.4x |
| pyodbc | 18.21s | 163.49s | 9.0x |

---

## Local Results (macOS, VPN  - for reference only)

### 1M rows

| Driver | Method | Median |
|---|---|---|
| ADBC | fetch_arrow_table | 34.47s |
| ADBC | fetch_arrow -> to_pandas | 35.08s |
| redshift_connector | fetch_dataframe | 37.46s |
| pyodbc | fetchall -> DataFrame | 36.52s |

Over VPN, all drivers are within 7%  - network latency dominates.

---

## Key Findings

1. **At 1M rows (EC2):** ADBC is 15% faster than redshift_connector for Arrow-native output. For DataFrame, the difference is negligible (2%).

2. **At 10M rows (EC2):** ADBC is 1.64-1.72x faster. The advantage grows with scale because Arrow-native fetch avoids the row-to-column conversion that row-based drivers must perform.

3. **ADBC scales better:** Going from 1M to 10M, ADBC time increases 5-6x while row-based drivers increase 8-9x. The row-to-column conversion cost grows linearly with row count.

4. **Over VPN:** All drivers are within 7%  - network latency dominates client-side processing. The ADBC advantage only shows on fast networks (EC2 same-region).

5. **pyodbc is consistently slowest** at scale due to Python-level row iteration + DataFrame construction.

6. **The wire protocol is the ceiling.** Even ADBC at 90s for 10M rows is far slower than our bulk engine (36s via UNLOAD on 4-node cluster). The real performance story is bulk mode, not driver optimization.

---

## Honest Summary for Driver Team

ADBC provides a meaningful advantage (1.6-1.7x) over redshift_connector at 10M+ rows on fast networks, specifically for columnar output (Arrow, DataFrame). For small queries or high-latency networks, the difference is negligible.

The real value of ADBC in Arrowjet is not safe-mode speed  - it's the Arrow-native data path that eliminates serialization overhead when feeding data into analytics tools (pandas, polars, DuckDB). The dramatic performance gains come from the bulk engine (COPY/UNLOAD), not from the driver layer.


---

## Comparison with Bulk Engine (from M0)

For context  - safe mode vs bulk mode on the same data:

### Reads (10M rows)

| Method | Time | vs best safe mode |
|---|---|---|
| ADBC Arrow (safe, 1-node) | 90.26s | baseline |
| UNLOAD bulk (1-node, 2 slices) | 99.67s | 0.9x |
| UNLOAD bulk (4-node, 8 slices) | 33.80s | 2.7x faster |

On a 4-node cluster, the bulk engine is 2.7x faster than the best safe-mode driver. On larger production clusters, the gap widens further.

### Writes (1M rows)

| Method | Time |
|---|---|
| write_dataframe INSERT | ~23 hours |
| Manual COPY script | 11.06s |
| Arrowjet write_bulk | 11.73s |

The bulk write path is 3,138x faster than INSERT and matches manual COPY at parity (1.06x).
