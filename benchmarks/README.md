# M0: Redshift Bulk Data Movement — Benchmark & Prototype

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
| Read 1M rows | `redshift_connector` fetch | `read_bulk` (UNLOAD → S3 → Parquet → Arrow) |
| Read 10M rows | `redshift_connector` fetch | `read_bulk` |
| Read 100M rows | `redshift_connector` fetch | `read_bulk` |
| Write 1M rows | `redshift_connector` INSERT | `write_bulk` (Arrow → Parquet → S3 → COPY) |
| Write 10M rows | `redshift_connector` INSERT | `write_bulk` |
| Write 100M rows | `redshift_connector` INSERT | `write_bulk` |
