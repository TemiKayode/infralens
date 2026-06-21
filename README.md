# InfraLens

[![Build](https://github.com/TemiKayode/infralens/actions/workflows/ci.yml/badge.svg)](https://github.com/TemiKayode/infralens/actions)
[![Rust](https://img.shields.io/badge/rust-1.79%2B-orange)](https://www.rust-lang.org/)
[![Go](https://img.shields.io/badge/go-1.23%2B-00add8)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

**A production-grade distributed observability platform built from first principles.**

InfraLens ingests logs, metrics, and traces via OpenTelemetry, stores them in a custom columnar LSM storage engine (Parquet-format SSTables, bloom filters, zone maps), and queries them with IQL — a SQL-like language with a hand-written lexer, recursive-descent parser, cost-based optimizer, and vectorised Apache Arrow executor. An AI copilot translates natural language to IQL and performs automated root cause analysis.

> Built to understand how observability platforms actually work under the hood — from WAL to vectorised Arrow execution — not just as a user of existing tools.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        InfraLens                                │
│                                                                 │
│  ┌──────────────┐    ┌──────────────────────────────────────┐   │
│  │   Ingest     │    │         Storage Engine (Rust)        │   │
│  │              │    │                                      │   │
│  │ OTLP/gRPC   ─┼──► │  MemTable ──► SSTable (Parquet)     │   │
│  │ OTLP/HTTP   ─┼──► │     │            bloom filter       │   │
│  │              │    │    WAL            zone maps         │   │
│  │  normalise   │    │     │          compaction daemon    │   │
│  │  → channel   │    │     └──────────────────────────────  │   │
│  └──────────────┘    └───────────────┬──────────────────────┘   │
│                                      │                          │
│  ┌───────────────────────────────────▼──────────────────────┐   │
│  │                   Query Engine (Rust)                     │   │
│  │                                                           │   │
│  │   IQL text ──► Lexer ──► Parser ──► AST                  │   │
│  │                                      │                   │   │
│  │                               Logical Planner            │   │
│  │                                      │                   │   │
│  │                            Cost-Based Optimizer          │   │
│  │                     (predicate pushdown, partition       │   │
│  │                      pruning, zone-map skipping)        │   │
│  │                                      │                   │   │
│  │                        Vectorised Arrow Executor         │   │
│  └───────────────────────────────────┬───────────────────────┘   │
│                                      │                           │
│  ┌───────────────────────────────────▼──────────────────────┐    │
│  │                  Cluster Layer (Rust)                     │    │
│  │                                                           │    │
│  │   consistent-hash ring + etcd membership                 │    │
│  │   scatter/gather RPC for distributed queries             │    │
│  └───────────────────────────────────┬───────────────────────┘   │
│                                      │                           │
│  ┌───────────────┐  ┌────────────────▼────────────────────────┐  │
│  │  LLM Copilot  │  │        Go API Gateway (chi)             │  │
│  │  (Python)     │  │                                         │  │
│  │               │  │  JWT + API-key auth                     │  │
│  │  llama.cpp    │  │  NDJSON streaming                       │  │
│  │  GBNF decode  │  │  Prometheus /metrics                    │  │
│  │  NL → IQL     │  │  structured logging                     │  │
│  │  root cause   │  │  rate limiting                          │  │
│  └───────────────┘  └─────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Why I built this

Most engineers use Prometheus, Grafana, or Datadog as black boxes — you configure exporters, write PromQL, and get dashboards. I wanted to understand what's actually inside:

- How does a time-series storage engine decide what to write to disk and when to compact?
- What does a cost-based query optimizer actually do when it sees `WHERE time > now() - 1h`?
- How do distributed systems split a query across shards and merge results?

InfraLens is the answer. Every layer — WAL, MemTable, SSTable compaction, bloom filters, zone maps, lexer, parser, AST, optimizer, Apache Arrow executor, etcd coordination — was built from scratch to make those decisions explicit and learnable.

---

## Query Language (IQL)

InfraLens Query Language is SQL-like with time-series extensions:

```sql
-- Basic span query
SELECT service, operation, latency_ms
FROM traces
WHERE time > now() - 1h
  AND latency_ms > 200
ORDER BY latency_ms DESC
LIMIT 100;

-- Aggregate with time bucketing
SELECT time_bucket('5m', time) AS bucket,
       service,
       avg(latency_ms) AS p50,
       percentile(latency_ms, 0.95) AS p95,
       count(*) AS requests
FROM traces
WHERE time > now() - 6h
GROUP BY bucket, service
ORDER BY bucket DESC;

-- Error rate by service
SELECT service,
       count(*) FILTER (WHERE status_code >= 500) AS errors,
       count(*) AS total,
       round(errors * 100.0 / total, 2) AS error_rate_pct
FROM spans
WHERE time > now() - 30m
GROUP BY service
HAVING error_rate_pct > 1.0;
```

The optimizer applies **predicate pushdown** (filters evaluated before scan), **partition pruning** (skips SSTables outside the time range using zone maps), and **bloom filter skipping** (skips SSTables that can't contain a given value) — keeping queries fast on large datasets.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Storage Engine | Rust (8 crates: WAL, MemTable, SSTable, compaction, bloom, query engine, cluster, ingest) |
| Query Engine | Rust · Apache Arrow (vectorised execution) · Parquet |
| API Gateway | Go (chi router) · JWT + API-key auth · NDJSON streaming · Prometheus metrics |
| LLM Copilot | Python · llama.cpp · GBNF constrained decoding · SQLite (feedback loop) |
| Protocol | gRPC (OTLP) · HTTP/2 · protox (pure-Rust proto compilation) |
| Cluster Coordination | etcd (WAL replication + membership) |
| Deployment | Docker Compose · Kubernetes + Helm · Tilt (live-reload dev) |
| Observability | Prometheus metrics · Grafana dashboards |

---

## Quick Start

**Requirements:** Rust 1.79+, Docker, Go 1.23+ (optional, for gateway dev), Python 3.11+ (optional, for copilot)

### Single-node (Docker Compose)

```bash
git clone https://github.com/TemiKayode/infralens.git
cd infralens

# Start the full stack
docker compose up -d

# Send a test OTLP trace (gRPC)
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d @examples/trace_sample.json

# Query via the API gateway
curl -X POST http://localhost:8080/query \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"iql": "SELECT * FROM traces WHERE time > now() - 5m LIMIT 10"}'
```

Results stream as NDJSON:
```json
{"service":"api","operation":"GET /users","latency_ms":43,"status_code":200}
{"service":"db","operation":"SELECT users","latency_ms":12,"status_code":200}
```

### Kubernetes (Helm)

```bash
helm install infralens ./helm/infralens \
  --set replicaCount=3 \
  --set storage.class=gp3 \
  --namespace observability --create-namespace
```

### Live-reload development (Tilt)

```bash
tilt up
# UI at http://localhost:10350
```

---

## Crate Structure

```
infralens/
├── crates/
│   ├── wal/          # Write-Ahead Log — durability before MemTable flush
│   ├── memtable/     # In-memory sorted write buffer
│   ├── sstable/      # Parquet-format on-disk storage + bloom filters + zone maps
│   ├── compaction/   # Background merge and level management
│   ├── ingest/       # OTLP/gRPC + HTTP receivers, schema normalisation
│   ├── query/        # Lexer → Parser → AST → Planner → Optimizer → Arrow Executor
│   ├── cluster/      # Consistent-hash ring, etcd membership, scatter/gather
│   └── infralens/    # Top-level binary, config, service wiring
├── gateway/          # Go API gateway (chi, JWT, Prometheus, NDJSON)
├── copilot/          # Python LLM service (llama.cpp, GBNF, anomaly detection)
├── helm/             # Kubernetes Helm chart
├── docker-compose.yml
└── Tiltfile
```

---

## LLM Copilot

With an Ollama or compatible endpoint configured, the copilot enables:

```bash
# Natural language → IQL
curl -X POST http://localhost:8080/copilot/query \
  -d '{"prompt": "Show me the slowest API endpoints in the last hour"}'

# Automated root cause analysis
curl -X POST http://localhost:8080/copilot/rca \
  -d '{"service": "checkout", "window": "15m"}'
```

The copilot uses GBNF-constrained decoding to ensure the LLM output is always valid IQL — no hallucinated syntax. A SQLite feedback loop stores accepted/rejected suggestions to improve future responses.

---

## Configuration

InfraLens uses layered TOML config with environment variable overrides:

```toml
# infralens.toml
[storage]
data_dir = "/var/lib/infralens"
wal_sync_interval_ms = 100
compaction_threshold = 4       # SSTable files before compaction
bloom_false_positive_rate = 0.01

[query]
max_concurrent_queries = 32
scan_parallelism = 4

[cluster]
etcd_endpoints = ["http://etcd:2379"]
node_id = "node-1"

[gateway]
port = 8080
jwt_secret_env = "INFRALENS_JWT_SECRET"
```

---

## License

MIT
