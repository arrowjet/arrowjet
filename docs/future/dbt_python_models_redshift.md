# Contributing Python Model Support to dbt-redshift

## The Opportunity

dbt Python models work on Snowflake, BigQuery, and Databricks — but not Redshift. This is a gap that affects every Redshift user who wants to use Python transformations inside dbt. Contributing this support would:

1. Make arrowjet the reason Python models work on Redshift — direct adoption driver
2. Establish credibility in the dbt ecosystem as a serious contributor
3. Create a path for arrowjet to run natively inside dbt pipelines (the holy grail)
4. Strengthen the acquisition case — AWS/dbt Labs would notice a contributor who added Redshift Python model support

This is a high-effort, high-reward play. Not for now — but worth planning carefully.

---

## How dbt Python Models Work (on supported platforms)

On Snowflake, BigQuery, and Databricks, dbt Python models work by:
1. Compiling the Python model code
2. Submitting it to the platform's native Python execution environment (Snowpark, Dataproc, Spark)
3. The platform runs the code, writes the result as a table
4. dbt tracks it like any other model (lineage, tests, docs)

The key: each platform has a **managed Python runtime** that can run arbitrary code, import packages, and write DataFrames back to the warehouse.

Redshift has no equivalent today.

---

## Feasibility Assessment

### Option A: Redshift Stored Procedures (plpythonu) — Low Feasibility

Redshift supports Python stored procedures via `plpythonu`. However:
- Sandboxed — no network access, no pip install, no S3 calls
- Row-level execution model — not DataFrame-based
- Can't import arrowjet or boto3
- dbt would need to wrap Python model code as a stored procedure — awkward fit

**Verdict:** Not viable for arrowjet integration. Could work for pure data transformation logic but defeats the purpose.

### Option B: AWS Lambda as Execution Backend — Medium Feasibility

Architecture:
1. dbt compiles the Python model
2. dbt-redshift submits it to a Lambda function (via API Gateway or direct invoke)
3. Lambda runs the Python code with full package access (arrowjet, pandas, etc.)
4. Lambda writes the result back to Redshift via arrowjet's `write_bulk()`
5. dbt tracks the resulting table

Challenges:
- Lambda has a 15-minute timeout and 10GB memory limit — fine for most models
- Lambda deployment package must include arrowjet and dependencies (~200MB with pyarrow)
- Lambda needs IAM access to both Redshift and S3
- dbt-redshift would need a new configuration block for Lambda ARN
- Cold starts add latency (~2-5s)
- dbt Labs would need to accept this architecture — not guaranteed

**Verdict:** Technically feasible. Architecturally sound. The main risk is dbt Labs' acceptance.

### Option C: AWS Glue Python Shell as Execution Backend — Medium-High Feasibility

Architecture:
1. dbt compiles the Python model
2. dbt-redshift submits it as a Glue Python Shell job
3. Glue runs the code with full package access
4. Results written back to Redshift via arrowjet
5. dbt polls for completion

Advantages over Lambda:
- No timeout constraints (Glue jobs can run for hours)
- Native AWS service — easier for enterprise users to trust
- Glue already has Redshift connectors — familiar territory for AWS teams
- Better fit for large-scale Python transformations

Challenges:
- Glue job startup time (~30-60s) — too slow for small models
- More complex IAM setup
- Cost: Glue charges per DPU-hour

**Verdict:** Best fit for large-scale use cases. Could be positioned as "dbt Python models for heavy workloads on Redshift."

### Option D: EC2/ECS Sidecar — High Feasibility, High Complexity

Run a persistent Python execution service alongside dbt. dbt-redshift submits Python models to this service via HTTP. The service runs them and writes results back.

This is essentially what Databricks does (Spark cluster) but self-managed. Too complex for an open-source contribution.

**Verdict:** Not worth pursuing as an open-source contribution.

---

## Recommended Approach: Lambda Backend (Option B)

Lambda is the right starting point because:
- Serverless — no infrastructure to manage
- Fast enough for most dbt Python models
- Arrowjet runs natively inside Lambda
- Can be extended to Glue for heavy workloads later

### Implementation Plan

**Phase 1: Proof of Concept (2-3 weeks)**
- Fork `dbt-labs/dbt-redshift`
- Add `python_model_backend: lambda` config option to `profiles.yml`
- Implement `PythonModelAdapter` that:
  - Wraps the Python model code in a Lambda-compatible handler
  - Invokes the Lambda function with the compiled code
  - Polls for completion
  - Reads the result table name back
- Test with a simple Python model (no arrowjet yet)

**Phase 2: Arrowjet Integration (1-2 weeks)**
- Add arrowjet to the Lambda deployment package
- Expose `dbt.ref()` as a arrowjet `read_bulk()` call for large upstream models
- Expose `return df` as a arrowjet `write_bulk()` call for the output
- Test end-to-end: SQL model → Python model (with arrowjet) → downstream SQL model

**Phase 3: Infrastructure Tooling (1-2 weeks)**
- Provide a CloudFormation/Terraform template for the Lambda function
- Document IAM requirements
- Add `dbt run-operation setup_python_backend` to automate Lambda deployment

**Phase 4: Contribute to dbt-redshift (ongoing)**
- Open a GitHub issue on `dbt-labs/dbt-redshift` to discuss the approach
- Submit PR with full implementation
- Engage with dbt Labs maintainers
- Write a blog post: "Python models on Redshift — how we built it"

---

## Challenges

| Challenge | Severity | Mitigation |
|---|---|---|
| dbt Labs may reject the PR | High | Engage early via GitHub issue before building |
| Lambda cold starts slow down dbt runs | Medium | Use provisioned concurrency for active projects |
| Lambda package size (pyarrow + arrowjet) | Medium | Use Lambda layers to separate dependencies |
| IAM complexity for enterprise users | Medium | Provide CloudFormation template |
| Glue vs Lambda debate | Low | Support both backends, let users choose |
| dbt's internal Python model API may change | Low | Pin to dbt-redshift version, track upstream changes |

---

## The Acquisition Angle

If this contribution lands in `dbt-redshift`:
- Every Redshift + dbt user who wants Python models will encounter arrowjet
- dbt Labs becomes a distribution channel for arrowjet
- AWS (who maintains dbt-redshift alongside dbt Labs) would notice
- This is the strongest possible signal to the Redshift team that arrowjet has ecosystem reach

The contribution itself is the marketing. A merged PR in `dbt-labs/dbt-redshift` is worth more than any blog post.

---

## Prerequisites Before Starting

1. Validate the shell-based dbt + arrowjet example (M8) — prove adoption first
2. Get 10+ GitHub stars or real users on arrowjet — show there's demand
3. Open a GitHub discussion on `dbt-labs/dbt-redshift` to gauge maintainer interest
4. Confirm Lambda approach is acceptable to dbt Labs before building

---

## Priority

**Low-Medium** — do not start until M9 (PyPI launch) is complete and there are real users. The contribution is only worth making if there's demonstrated demand. Without users, dbt Labs won't merge it.
