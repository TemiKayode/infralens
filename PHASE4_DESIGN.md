# InfraLens Phase 4 — LLM Copilot

## 1. Overview

The LLM Copilot is a Python FastAPI microservice that translates natural-language
observability questions into valid IQL queries, executes them via the API gateway,
analyses the results, and produces a structured root-cause analysis (RCA) report.

**Key design goals:**
- **Correctness by construction** — grammar-based constrained decoding ensures every
  generated query is syntactically valid IQL before it leaves the LLM.
- **Schema-aware** — GBNF grammar is generated at runtime from the live catalog, so
  column names and table names are always in scope.
- **Feedback loop** — accepted / rejected query pairs are stored in SQLite and used to
  fine-tune the system prompt over time.

---

## 2. Architecture

```
User (natural language question)
        │
        ▼
  FastAPI endpoint: POST /api/v1/copilot/query
        │
        ├─ 1. Build GBNF grammar from live catalog (schema + table names)
        │
        ├─ 2. Call llama.cpp via llama-cpp-python with constrained decoding
        │       → guaranteed syntactically valid IQL
        │
        ├─ 3. Forward IQL to API gateway  POST /api/v1/query
        │       → JSON result rows
        │
        ├─ 4. RCA pipeline
        │       • Anomaly detection (z-score on numeric columns)
        │       • Correlation analysis (Pearson on time-series columns)
        │       • LLM-generated narrative (second unconstrained inference pass)
        │
        └─ 5. Return CopilotResponse { iql, rows, rca }
                │
                └─ POST /api/v1/copilot/feedback  → store in SQLite feedback DB
```

---

## 3. Constrained Decoding

Grammar-based constrained decoding uses llama.cpp's GBNF (Generalized BNF) grammar
format.  At inference time the sampler is restricted to tokens that extend the current
prefix into a string matching the grammar.

### GBNF template (simplified)

```
root         ::= select-stmt
select-stmt  ::= "SELECT " projections " FROM " table-name ( " WHERE " expr )?
                 ( " GROUP BY " expr-list )? ( " ORDER BY " order-list )?
                 ( " LIMIT " integer )? ";"
table-name   ::= "logs" | "metrics" | "traces"
projections  ::= "*" | proj-list
proj-list    ::= expr ( ", " expr )*
expr         ::= column-name | function-call | literal | "(" expr " " binop " " expr ")"
column-name  ::= "timestamp_ns" | "severity_text" | "body" | "service_name" | ...
function-call ::= ("count" | "sum" | "avg" | "time_bucket" | "rate" | "now") "(" expr-list? ")"
```

The grammar is generated dynamically from the catalog so that:
- `table-name` lists only existing tables.
- `column-name` lists only columns of the selected table.

---

## 4. RCA Pipeline

The RCA pipeline runs after query execution:

1. **Anomaly detection** — for each numeric column, compute mean ± 3σ; flag rows outside
   that band as anomalies.
2. **Rate-of-change** — compute first derivative of time-ordered numeric columns;
   surface inflection points.
3. **Correlation** — Pearson correlation matrix across numeric columns; report pairs
   with |r| > 0.8.
4. **Narrative** — second LLM pass (no grammar constraint, short context window):
   > "Given these anomalies and correlations, what is the most likely root cause?"

---

## 5. Feedback Loop

Every query interaction is stored in SQLite:

```sql
CREATE TABLE feedback (
    id          INTEGER PRIMARY KEY,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    nl_question TEXT    NOT NULL,
    iql_query   TEXT    NOT NULL,
    accepted    BOOLEAN NOT NULL,  -- user thumbs up/down
    rca_text    TEXT,
    latency_ms  INTEGER
);
```

At startup, the 20 most recent accepted queries are loaded into the system prompt as
few-shot examples, steering the model toward the user's preferred query patterns.

---

## 6. API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/copilot/query`    | POST | Natural-language → IQL + RCA |
| `/api/v1/copilot/feedback` | POST | Store acceptance signal |
| `/api/v1/copilot/history`  | GET  | Recent query history |
| `/healthz`                 | GET  | Health check |

### Request body (POST /api/v1/copilot/query)
```json
{
  "question": "Show me error rate per service over the last hour",
  "context":  { "time_range_minutes": 60 }
}
```

### Response
```json
{
  "iql": "SELECT service_name, rate(count(*), interval '1m') AS rps FROM logs ...",
  "rows": [...],
  "rca": {
    "anomalies": [...],
    "correlations": [...],
    "narrative": "Service auth-svc shows a 3× spike at 14:23 UTC correlating with ..."
  },
  "latency_ms": 1240
}
```

---

## 7. Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `fastapi` | 0.115 | HTTP framework |
| `uvicorn` | 0.32  | ASGI server |
| `llama-cpp-python` | 0.3 | llama.cpp bindings + GBNF constrained decoding |
| `httpx` | 0.27 | Async HTTP client (calls API gateway) |
| `numpy` | 2.1  | Anomaly detection, correlation |
| `scipy` | 1.14 | Pearson correlation |
| `aiosqlite` | 0.20 | Async SQLite feedback store |
| `pydantic` | 2.9  | Request/response validation |

---

## 8. Deployment

- Containerised: `services/llm-copilot/Dockerfile`
- Model: Llama-3.2-3B-Instruct-GGUF (Q4_K_M quantisation, ~2 GB)
- Resources: 4 vCPU, 8 GB RAM minimum; GPU optional (CUDA / Metal via llama.cpp)
- Port: `:8081`
