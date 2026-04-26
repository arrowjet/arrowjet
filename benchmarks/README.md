# M0: Redshift Bulk Data Movement  - Benchmark & Prototype

Proves the core value proposition: UNLOAD/COPY through S3 is 10-50x faster than wire protocol for bulk workloads.

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Copy `config.example.yaml` to `config.yaml` and fill in your Redshift + S3 details.

## Usage

```bash
# 1. Generate test data in Redshift
python setup_test_data.py

# 2. Run benchmarks
python benchmark.py

# 3. View results
# Results are saved to results/ as JSON + charts
```

## What This Measures

| Benchmark | Baseline | Prototype |
|---|---|---|
| Read 1M rows | `redshift_connector` fetch | `read_bulk` (UNLOAD -> S3 -> Parquet -> Arrow) |
| Read 10M rows | `redshift_connector` fetch | `read_bulk` |
| Read 100M rows | `redshift_connector` fetch | `read_bulk` |
| Write 1M rows | `redshift_connector` INSERT | `write_bulk` (Arrow -> Parquet -> S3 -> COPY) |
| Write 10M rows | `redshift_connector` INSERT | `write_bulk` |
| Write 100M rows | `redshift_connector` INSERT | `write_bulk` |

## Write Benchmark Results (1M rows, 4-node ra3.large, EC2 same region)

Six-lane comparison of every common INSERT approach vs COPY:

| Approach | Time | vs Arrowjet |
|---|---|---|
| `write_dataframe()` INSERT | 48,361s (13.4h) | 14,523x slower |
| `executemany` (batch 5000) | 97,556s (27.1h) | 29,296x slower |
| Multi-row VALUES (batch 5000) | 195.8s | 58.8x slower |
| Parallel VALUES (4 threads, batch 5000) | 148.4s | 44.6x slower |
| Manual COPY | 4.06s | 1.22x slower |
| **Arrowjet `write_bulk`** | **3.33s** | **baseline** |

Key takeaways:
- Every INSERT-based approach is orders of magnitude slower than COPY
- `executemany` is actually *slower* than `write_dataframe`  - row-by-row round trips dominate
- Parallelizing INSERT across 4 threads helps (148s vs 196s) but is still 45x slower than COPY
- Arrowjet matches manual COPY performance with a one-line API
