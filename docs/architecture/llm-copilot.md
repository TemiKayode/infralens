# LLM Copilot

**Service:** `services/llm-copilot`

The LLM Copilot adds AI-powered natural language querying and automated root cause
analysis (RCA) to InfraLens. It runs as a standalone Python FastAPI service and
communicates with the API Gateway over HTTP.

---

## Design Goals

1. **Guaranteed valid IQL output** — the model cannot produce syntactically invalid
   queries because the grammar constrains the decoding token-by-token.
2. **CPU-friendly** — runs on Llama 3.2 3B (Q4_K_M) which fits in 2.5 GB RAM, no GPU
   required for moderate query volume.
3. **Graceful degradation** — if no model is loaded, the service enters stub mode and
   returns template-based IQL so all API endpoints remain functional.
4. **Continuous improvement** — a feedback loop stores corrections in SQLite and
   injects them as few-shot examples in future prompts.

---

## Component Overview

```
POST /v1/nl2iql ──► NL2IQL pipeline ──► GBNF-constrained LLM ──► IQL string
POST /v1/rca    ──► RCA pipeline    ──► IQL queries + stats  ──► LLM narrative
POST /v1/feedback ─► SQLite feedback store
```

---

## Natural Language → IQL (NL2IQL)

**File:** `services/llm-copilot/copilot/llm.py`

### GBNF constrained decoding

GBNF (Grammar-Based Next-token Filtering) is a llama.cpp feature that restricts the
model's token sampling to sequences that conform to a context-free grammar. InfraLens
generates the grammar dynamically from the live catalog (column names, table names)
so the model literally cannot produce:
- A column name that does not exist.
- An invalid IQL keyword sequence.
- Unbalanced parentheses.
- A malformed `INTERVAL` literal.

**File:** `services/llm-copilot/copilot/grammar.py`

The grammar is regenerated on every request (fast, pure Python string generation) and
passed to `llama_cpp.Llama.__call__(grammar=...)`:

```python
grammar_str = build_iql_grammar(catalog)
# grammar_str is a valid GBNF string like:
# root ::= select-stmt
# select-stmt ::= "SELECT" ws projections ws "FROM" ws table ...
# table ::= "logs" | "metrics" | "traces"
# column ::= "timestamp_ns" | "service_name" | "body" | ...
response = llm(prompt, grammar=LlamaGrammar.from_string(grammar_str))
```

### Prompt structure

```
[SYSTEM]
You are an IQL query generator for InfraLens observability data.
Available tables: logs, metrics, traces.
Available columns: timestamp_ns, service_name, body, severity_number, ...
Current time: {now_iso}

[FEW-SHOT EXAMPLES from feedback SQLite, if any]
Q: show errors in the last hour
A: SELECT timestamp_ns, body, service_name FROM logs
   WHERE severity_number >= 17
   AND timestamp_ns >= now() - INTERVAL '1 hour'
   ORDER BY timestamp_ns DESC LIMIT 100

[USER]
Q: {user_question}
A:
```

The model completes the `A:` line under grammar constraints.

### Confidence scoring

After generation, the copilot re-scores the output by computing the mean log-probability
of the generated tokens. Outputs where the model was uncertain (mean log-prob < threshold)
receive a `"confidence"` value below 0.7 in the response, signalling the caller to
treat the result with more scrutiny.

---

## Root Cause Analysis (RCA)

**File:** `services/llm-copilot/copilot/rca.py`

The RCA pipeline detects anomalies and correlates them across metric pairs, then
generates a human-readable narrative.

### Step 1 — Anomaly detection

For the target metric and service, the copilot queries InfraLens via the API Gateway:

```sql
SELECT time_bucket('1 minute', timestamp_ns) AS bucket,
       avg(value) AS avg_val,
       stddev(value) AS std_val
FROM metrics
WHERE service_name = '{service}'
  AND metric_name = '{metric}'
  AND timestamp_ns >= now() - INTERVAL '{window} minutes'
GROUP BY bucket
ORDER BY bucket
```

It then computes a rolling Z-score:

```python
z = (value - rolling_mean) / rolling_std
anomalies = [(ts, z, val) for ts, z, val in series if abs(z) > z_threshold]
```

The default Z-score threshold is 3.0 (configurable via `RCA_Z_THRESHOLD` env var).

### Step 2 — Cross-metric correlation

For each anomalous time window, the pipeline fetches the top-N related metrics
(configurable, default 5) and computes Pearson correlation with a lag sweep:

```python
from scipy.stats import pearsonr
for lag_seconds in range(-60, 61, 5):
    shifted = target_series.shift(lag_seconds)
    r, p_value = pearsonr(candidate_series, shifted)
    if abs(r) > 0.7 and p_value < 0.05:
        correlations.append({"metric": name, "pearson_r": r, "lag_seconds": lag_seconds})
```

The strongest correlation (highest |r|) is reported as the likely root cause.

### Step 3 — LLM narrative

The anomaly list and correlation list are injected into a structured prompt. The LLM
generates a plain-English narrative summary (unconstrained, not grammar-bound):

```
Given the following anomaly data:
- 4.2σ spike in http.request.duration at 2024-12-01 14:35 UTC
Correlations found:
- db.query.duration: r=0.91, lag=12s
Write a 2-3 sentence root cause analysis.
```

---

## Feedback Loop

**File:** `services/llm-copilot/copilot/rca.py` (feedback portions)

When a user submits a correction via `POST /v1/feedback`, the original question,
generated IQL, corrected IQL, and rating are stored in a SQLite table:

```sql
CREATE TABLE feedback (
    id           INTEGER PRIMARY KEY,
    question     TEXT NOT NULL,
    generated    TEXT NOT NULL,
    corrected    TEXT NOT NULL,
    rating       INTEGER,         -- 1-5
    created_at   INTEGER          -- unix seconds
);
```

On the next NL2IQL request, the pipeline queries:

```sql
SELECT question, corrected FROM feedback
WHERE rating >= 4
ORDER BY created_at DESC
LIMIT 5
```

These are injected as few-shot examples in the prompt. Over time, user corrections
teach the model the specific naming conventions and query patterns of the deployment.

---

## Stub Mode

If `COPILOT_MODEL_PATH` is not set or the file does not exist, the copilot runs in
stub mode. In stub mode:
- `POST /v1/nl2iql` returns a template-based IQL derived from keyword matching on
  the question (no LLM involved).
- `POST /v1/rca` runs the anomaly detection and correlation steps but skips the LLM
  narrative, returning a structured JSON result without the `narrative` field.
- All endpoints return HTTP 200 — the stub is not an error state.

This allows the full stack to be tested without a 2 GB model file.

---

## API Reference

### `POST /v1/nl2iql`

Request:
```json
{"question": "show me error logs from the payment service in the last hour"}
```

Response:
```json
{
  "iql": "SELECT timestamp_ns, body FROM logs WHERE service_name = 'payment' AND severity_number >= 17 AND timestamp_ns >= now() - INTERVAL '1 hour' ORDER BY timestamp_ns DESC LIMIT 100",
  "explanation": "Filtering for ERROR severity (≥17) in the payment service over the last hour.",
  "confidence": 0.94,
  "stub": false
}
```

### `POST /v1/rca`

Request:
```json
{
  "service": "payment",
  "window_minutes": 30,
  "metric": "http.request.duration"
}
```

Response:
```json
{
  "anomalies": [
    {"timestamp_ns": 1733059500000000000, "z_score": 4.2, "value": 892.3}
  ],
  "correlations": [
    {"metric": "db.query.duration", "pearson_r": 0.91, "lag_seconds": 12}
  ],
  "narrative": "A 4.2σ latency spike in the payment service at 14:35 UTC correlates strongly (r=0.91) with increased database query latency with a 12-second lag, suggesting the database is the root cause."
}
```

### `POST /v1/feedback`

Request:
```json
{
  "question": "show payment errors",
  "generated_iql": "SELECT * FROM logs WHERE body LIKE '%error%'",
  "corrected_iql": "SELECT timestamp_ns, body FROM logs WHERE service_name = 'payment' AND severity_number >= 17",
  "rating": 5
}
```

Response: `204 No Content`

---

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `COPILOT_MODEL_PATH` | `""` | Path to the GGUF model file; empty = stub mode |
| `COPILOT_N_GPU_LAYERS` | `0` | GPU layers to offload (0 = CPU only) |
| `COPILOT_GATEWAY_URL` | `http://localhost:8080` | InfraLens API Gateway URL |
| `COPILOT_FEEDBACK_DB_PATH` | `./feedback.db` | SQLite feedback database path |
| `COPILOT_PORT` | `8081` | FastAPI listen port |
| `RCA_Z_THRESHOLD` | `3.0` | Z-score threshold for anomaly detection |
| `RCA_MAX_CORRELATIONS` | `5` | Number of candidate metrics to correlate |

---

## Model Selection

The copilot is tested with **Llama 3.2 3B Instruct (Q4_K_M)**. Any GGUF-format model
supported by llama-cpp-python will work. Larger models produce higher-quality IQL at
the cost of inference latency:

| Model | Size | Inference latency (CPU) | Recommended for |
|-------|------|------------------------|-----------------|
| Llama 3.2 3B Q4_K_M | ~2 GB | ~500 ms | Development, low traffic |
| Llama 3.1 8B Q4_K_M | ~5 GB | ~1.5 s | Production, < 10 req/s |
| Llama 3.1 70B Q4_K_M | ~40 GB | ~8 s | High-accuracy, GPU required |

Download links are in the main [README](../../README.md#enable-the-llm-copilot).
