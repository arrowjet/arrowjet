# AI Data Query — Concept (Parked)

Status: Parked. Not scheduled. Revisit when core platform has traction.

## Concept

Natural language interface over warehouse data:
- User asks a question in plain English
- System generates SQL, executes it safely, returns results + summary
- Backed by Arrowjet engine for data access

## Why It's Interesting

- Lowers the barrier from "write SQL" to "ask a question"
- Natural Pro/enterprise feature (governance, audit, guardrails)
- Differentiator if integrated into the Arrowjet platform

## Why It's Parked

- Separate product, not core to data movement
- Requires Bedrock, Glue, Athena, React UI — different stack
- Doesn't strengthen the bulk engine or drive adoption
- Risk of scope creep away from the core value proposition

## Architecture (if pursued)

```
React UI → API Gateway → Lambda (TypeScript) → Bedrock (Claude) → Athena → S3
                                                                    ↑
                                                              Glue (schema)
```

## Core Flow

1. User prompt ("top 10 customers last month")
2. Fetch schema from Glue Data Catalog
3. LLM generates SQL
4. Validate (SELECT only, LIMIT 100, timeout)
5. Execute in Athena
6. LLM summarizes results
7. Return SQL + rows + summary

## Guardrails

- Only SELECT queries
- Auto-add LIMIT 100
- IAM restrictions (read-only)
- Query timeout
- Basic SQL validation before execution

## Arrowjet Integration Points

- Use Arrowjet bulk engine for large result export
- Use Arrowjet staging for intermediate data
- Add Redshift as a query target alongside Athena
- Engine selector: Athena for ad-hoc, Redshift for warehouse queries

## When to Revisit

- After Phase B (M5-M9) is complete
- After CLI MVP has users
- After multi-database expansion validates the platform thesis
- When there's a clear enterprise customer asking for it
