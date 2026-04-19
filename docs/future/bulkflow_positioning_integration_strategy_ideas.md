# Arrowjet — Positioning & Integration Strategy

## 1. Core Principle

Arrowjet should NOT be positioned as a standalone product competing with ETL/DMS tools.

Instead:

> **Arrowjet = programmable data movement execution layer**

Comparable to:
- Stripe → payments
- DuckDB → query execution
- Snowflake → data warehouse

Arrowjet should sit *underneath* orchestration, transformation, and AI systems.

---

## 2. Strategic Direction

### ❌ Avoid
- “Better DMS” positioning
- “Another ETL tool”
- Competing directly with Airflow/dbt/Fivetran

### ✅ Embrace
- Infrastructure primitive
- Execution engine
- Developer-first abstraction

---

## 3. Key Integration Opportunities

## 3.1 Airflow Integration (High Priority)

### Problem
Airflow users write repetitive, error-prone code for data movement.

### Solution
Provide a native operator.

```python
from arrowjet.airflow import ArrowjetOperator

ArrowjetOperator(
    task_id="export_users",
    source="postgres",
    query="SELECT * FROM users",
    destination="s3://bucket/users/",
    mode="incremental"
)
```

### Value
- Eliminates boilerplate PythonOperators
- Standardizes data movement
- Improves reliability and readability

### Positioning
> "dbt-like abstraction for data movement inside Airflow"

---

### ⚡ Low-friction adoption (use native Airflow primitives FIRST)

**Goal:** let users keep their existing DAG patterns while Arrowjet runs under the hood.

#### Option A — PythonOperator
```python
from airflow.operators.python import PythonOperator
import arrowjet

def run_sync():
    arrowjet.export(
        source="postgres",
        query="SELECT * FROM orders",
        to="s3://bucket/orders/"
    )

PythonOperator(task_id="sync_orders", python_callable=run_sync)
```

#### Option B — TaskFlow API (recommended)
```python
from airflow.decorators import task
import arrowjet

@task
def sync_orders():
    arrowjet.export(
        source="postgres",
        query="SELECT * FROM orders",
        to="s3://bucket/orders/"
    )
```

#### Option C — BashOperator (CLI)
```python
from airflow.operators.bash import BashOperator

BashOperator(
    task_id="sync_orders",
    bash_command="arrowjet export --source postgres --query 'SELECT * FROM orders' --to s3://bucket/orders/"
)
```

### Strategy
- Phase 1: integrate via existing primitives (no new mental model)
- Phase 2: introduce `ArrowjetOperator` for better DX

> **Key idea:** Arrowjet feels like an engine, not another Airflow framework.

---

## 3.2 dbt Integration (High Leverage)

### Problem
dbt does not handle ingestion or data movement.

### Solution
Extend dbt sources with Arrowjet sync.

```yaml
sources:
  - name: users
    arrowjet:
      from: postgres
      to: s3
      mode: cdc
```

### Value
- Completes the pipeline (ingestion → transform)
- Seamless integration with existing workflows

### Positioning
> "The ingestion layer dbt never built"

---

### ⚡ Deeper adoption tricks (dbt-native feel)

#### 1. Pre-run hooks (no new concepts)
```yaml
on-run-start:
  - "arrowjet export --source postgres --query 'SELECT * FROM users' --to s3://bucket/users/"
```

#### 2. Macros wrapper
```sql
{% macro arrowjet_sync(table) %}
  {{ run_query("!arrowjet export --table " ~ table) }}
{% endmacro %}
```

#### 3. Sources auto-materialization
- Arrowjet ensures data exists before dbt models run
- dbt remains unchanged → Arrowjet becomes invisible infra

### Strategy
- Do NOT fork dbt behavior
- Attach via hooks/macros

> **Key idea:** dbt users should not feel they are using a new tool.

---

## 3.3 Developer SDK (Foundation Layer)

### Problem
Moving data programmatically is fragmented and inconsistent.

### Solution
Provide a simple, powerful API.

```python
import arrowjet

arrowjet.export(
    query="SELECT * FROM orders",
    to="s3://bucket/orders.parquet"
)
```

### Value
- Fast adoption by developers
- Enables embedding into other systems

### Positioning
> "Stripe-like SDK for data movement"

---

## 3.4 AI / Agent Backend (Future Layer)

### Problem
AI systems can decide *what* data is needed but cannot reliably execute data movement.

### Solution
Arrowjet executes AI-generated intents.

Example:

Prompt:
"Fetch last 30 days of transactions and store as parquet"

Execution:

```python
arrowjet.export(
    query="SELECT * FROM transactions WHERE date > NOW() - INTERVAL '30 days'",
    to="s3://analytics/transactions/"
)
```

### Value
- Bridges AI → data execution
- Opens new category

### Positioning
> "Execution layer for AI-driven data workflows"

---

### ⚡ Deeper adoption tricks (AI-native)

#### 1. Function/tool interface
Expose Arrowjet as a callable tool:
```json
{
  "name": "arrowjet_export",
  "params": {"query": "...", "destination": "..."}
}
```

#### 2. Intent → plan → execution
- LLM generates plan
- Arrowjet executes deterministically

#### 3. Safe execution layer
- validate queries
- enforce limits
- retries / idempotency

### Strategy
- Arrowjet is NOT the AI
- Arrowjet is the **trusted executor**

> **Key idea:** AI decides, Arrowjet guarantees execution.

---

## 4. Architecture Vision

```
          ┌───────────────────────┐
          │       AI / LLM        │
          └──────────┬────────────┘
                     │
          ┌──────────▼────────────┐
          │   Airflow / dbt       │
          └──────────┬────────────┘
                     │
          ┌──────────▼────────────┐
          │       Arrowjet        │
          │ (execution engine)    │
          └──────────┬────────────┘
                     │
          ┌──────────▼────────────┐
          │ Databases / Storage   │
          └───────────────────────┘
```

---

## 5. MVP Definition (Strict Scope)

### MUST HAVE
1. CLI + Python SDK
2. PostgreSQL → S3 export (Parquet/CSV)
3. Incremental / basic CDC support
4. Simple config (no multiple sources of truth)

### SHOULD HAVE
5. Airflow Operator

### LATER
6. dbt integration
7. AI layer

---

## 6. Example End-to-End Usage

### Step 1: Airflow DAG

```python
ArrowjetOperator(
    task_id="sync_orders",
    source="postgres",
    query="SELECT * FROM orders",
    destination="s3://data/orders/"
)
```

### Step 2: dbt Transformation

```sql
SELECT * FROM {{ source('orders') }}
WHERE amount > 100
```

### Step 3: AI Query (future)

"Show me high-value customers"

→ triggers Arrowjet + dbt pipeline

---

## 7. Final Positioning

> **Arrowjet is the programmable data movement layer used by Airflow, dbt, and AI systems.**

---

## 8. Key Takeaway

Arrowjet wins by:
- Sitting underneath existing tools
- Providing a clean abstraction
- Becoming a dependency, not a competitor

---

## 9. Next Steps

1. Build minimal SDK + CLI
2. Add Airflow operator
3. Validate with real usage
4. Expand to dbt
5. Explore AI integration

