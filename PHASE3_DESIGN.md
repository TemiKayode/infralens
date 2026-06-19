# InfraLens Phase 3 — Query Engine

## 1. Query Language (IQL — InfraLens Query Language)

IQL is a SQL superset with temporal extensions.  Every valid SQL SELECT is valid IQL.
Temporal extensions are expressed as built-in functions, not new keywords, so any
standard SQL parser can tokenise them; only the function evaluation changes.

### Example queries

```sql
-- 1. Basic log search
SELECT timestamp_ns, severity_text, body
FROM   logs
WHERE  severity_number >= 13
  AND  timestamp_ns BETWEEN now() - interval '5 minutes' AND now()
LIMIT  100;

-- 2. Error rate over time (temporal)
SELECT time_bucket('1m', timestamp_ns)   AS minute,
       count(*)                           AS total,
       rate(count(*), interval '1m')      AS req_per_sec
FROM   logs
WHERE  severity_number >= 17
  AND  timestamp_ns >= now() - interval '1h'
GROUP  BY minute
ORDER  BY minute ASC;

-- 3. Latency percentile from traces
SELECT service_name,
       histogram_quantile(0.95, duration_ns, 50) AS p95_ns,
       histogram_quantile(0.99, duration_ns, 50) AS p99_ns
FROM   traces
WHERE  timestamp_ns >= now() - interval '15m'
GROUP  BY service_name;

-- 4. Cross-signal join: errors correlated with slow spans
SELECT l.body, t.name, t.duration_ns
FROM   logs   l
JOIN   traces t ON l.trace_id = t.trace_id
WHERE  l.severity_number >= 17
  AND  t.duration_ns > 500000000  -- 500 ms
  AND  l.timestamp_ns >= now() - interval '30m';
```

### Temporal built-ins

| Function | Signature | Description |
|----------|-----------|-------------|
| `time_bucket` | `(width: interval, ts: uint64) → uint64` | Truncate timestamp to bucket boundary |
| `rate` | `(value: float64, window: interval) → float64` | Samples/second over window |
| `delta` | `(value: float64, window: interval) → float64` | Value change over window |
| `histogram_quantile` | `(q: float64, col: any, buckets: int) → float64` | Estimated quantile from raw values |
| `now` | `() → uint64` | Current time as nanoseconds |
| `interval` | `(str: text) → int64` | Parse interval string to nanoseconds |

---

## 2. Architecture

```
IQL text
   │
   ▼
 Lexer ──► token stream
   │
   ▼
 Parser (recursive descent) ──► AST
   │
   ▼
 Binder (resolve column refs against Catalog) ──► typed AST
   │
   ▼
 Optimizer
   ├─ Rule-based pass (predicate pushdown, projection pushdown, const folding)
   └─ Cost-based pass (row-count stats from zone maps)
   │
   ▼
 Planner ──► PhysicalPlan
   │
   ▼
 Executor
   ├─ Local:  TableScan → Filter → Project → Aggregate → Sort → Limit
   └─ Distributed: ScatterGather (Phase 2) → Merge → Aggregate → Sort → Limit
```

---

## 3. Catalog

The catalog stores:
- Table definitions (one per signal type per node):
  `logs`, `metrics`, `traces`
- Column types (derived from Arrow schemas in `infralens-common`)
- Partition metadata (which partitions exist, their time ranges, SSTable counts)
- Statistics per partition (min/max per column, row count, null count from zone maps)

The catalog is stored in-memory and refreshed from disk on startup and after compaction.

---

## 4. Optimizer

### Rule-based rewrites (applied in order)

1. **Constant folding** — `now() - interval '5m'` evaluated at plan time.
2. **Predicate pushdown** — move `WHERE` predicates as close to the scan as possible.
3. **Projection pushdown** — only read columns referenced in SELECT + WHERE + JOIN.
4. **Partition pruning** — use zone maps to skip partitions outside the time range.
5. **Limit pushdown** — pass `LIMIT N` to each shard scan (avoids over-fetching).

### Cost-based selection

For joins: choose between nested-loop (small tables), hash-join (medium), and
sort-merge join (pre-sorted inputs) based on estimated cardinalities from zone maps.

---

## 5. Execution

Phase 3 uses a **vectorised, pull-based** executor operating on Arrow `RecordBatch`es
(default batch size: 8 192 rows).  Each operator implements:

```rust
trait PhysicalOperator: Send {
    fn schema(&self) -> Arc<Schema>;
    fn poll_next(&mut self) -> Result<Option<RecordBatch>>;
}
```

---

## 6. Go API Gateway

The API gateway (`services/api-gateway/`) is a Go service that:
1. Accepts HTTP queries: `POST /api/v1/query` with `{"query": "SELECT ..."}`.
2. Calls the Rust query engine via gRPC (`InternalService::QueryShard`).
3. Streams Arrow IPC results back to the client as NDJSON or Apache Arrow Flight.
4. Handles authentication (API-key or JWT) and rate limiting.

Port: `:8080` (configurable).

---

## 7. New crates / services

| Component | Location | Language |
|-----------|----------|----------|
| `infralens-query` | `crates/infralens-query/` | Rust |
| API gateway | `services/api-gateway/` | Go 1.23 |
