# AWS Service Integration Opportunities

## Overview

Arrowjet's bulk data movement engine (COPY/UNLOAD via S3) overlaps with several AWS managed services. Integrating with or positioning alongside these services could accelerate adoption and strengthen the acquisition case.

---

## AWS DMS (Database Migration Service)

**What it does:** Moves data between databases — source to target, one-time or ongoing CDC replication. Supports heterogeneous migrations (Oracle → Redshift, MySQL → Redshift, etc.). Uses COPY under the hood for Redshift targets.

**Where arrowjet adds value:**
- Post-migration validation — bulk read from Redshift to compare row counts, checksums, sample data against source
- Incremental export — after DMS loads data, bulk export transformed results downstream via UNLOAD
- Testing pipelines — fast bulk reads for data quality checks after migration completes
- Python-native access to migrated data without writing custom scripts

**Integration angle:** Arrowjet as the Python companion to DMS — DMS handles the infrastructure migration, arrowjet handles the programmatic data access layer after data lands in Redshift.

**Acquisition relevance:** DMS team is a potential acquirer or integration partner. If arrowjet becomes the standard Python library for Redshift bulk access, DMS engineers would naturally reach for it.

---

## AWS Glue

**What it does:** Managed ETL service. Runs Spark jobs for data transformation. Has a Redshift connector that uses JDBC under the hood — slow for large datasets.

**Where arrowjet adds value:**
- Replace Glue's JDBC-based Redshift connector with COPY/UNLOAD for large-scale ETL
- Glue Python shell jobs could use arrowjet directly (no Spark overhead for simple bulk moves)
- Faster Redshift reads in Glue ETL scripts without spinning up a full Spark cluster

**Integration angle:** Arrowjet as a lightweight alternative to Glue's Redshift connector for Python-based ETL. "Use Glue for orchestration, arrowjet for the Redshift data movement."

**Acquisition relevance:** Glue team owns the Redshift connector. If arrowjet demonstrates significantly better performance, it's a natural acquisition target for the Glue team to absorb.

---

## AWS AppFlow

**What it does:** Managed data integration service for SaaS-to-AWS data flows (Salesforce → Redshift, ServiceNow → S3, etc.). No-code/low-code. Uses S3 as intermediate staging.

**Where arrowjet adds value:**
- AppFlow lands data in S3 — arrowjet can pick it up and COPY it into Redshift efficiently
- Post-AppFlow transformation: bulk read from Redshift after AppFlow loads, transform, bulk write back
- Python SDK complement: AppFlow has no Python bulk access layer for Redshift

**Integration angle:** Arrowjet as the "last mile" for AppFlow → Redshift pipelines. AppFlow handles the SaaS extraction, arrowjet handles the Redshift loading and reading.

**Acquisition relevance:** AppFlow team is less likely to acquire but could be an integration partner — AppFlow + arrowjet as a recommended pattern in AWS documentation.

---

## Investigation Priorities

| Service | Priority | Effort | Acquisition Signal |
|---|---|---|---|
| Glue | High | Medium | Strong — owns Redshift connector |
| DMS | Medium | Low | Medium — adjacent team |
| AppFlow | Low | Low | Weak — different domain |

**Next steps:**
1. Benchmark arrowjet vs Glue's Redshift connector on the same dataset
2. Build a Glue Python shell job example using arrowjet
3. Document DMS + arrowjet as a post-migration validation pattern
4. Reach out to Glue/DMS teams internally if pursuing acquisition path
