# Architecture Overview

InfraLens is a horizontally scalable observability platform built from scratch in Rust,
Go, and Python. It ingests logs, metrics, and distributed traces via the OpenTelemetry
Protocol (OTLP), stores them in a custom columnar LSM engine on local disk, and serves
queries through a SQL-like language called IQL.

---

## High-Level Component Map

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             Clients / SDKs                                  │
│         (OTel SDKs, otel-collector, curl, your instrumented services)       │
└────────────────────────┬──────────────────────────────┬─────────────────────┘
                         │ OTLP/gRPC :4317              │ OTLP/HTTP :4318
                         ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       InfraLens Server  (Rust)                              │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Ingest Pipeline                                                     │   │
│  │  OTLP/gRPC receiver ──►  Normaliser ──►  bounded mpsc channel       │   │
│  │  OTLP/HTTP receiver ──/                         │                   │   │
│  └─────────────────────────────────────────────────┼───────────────────┘   │
│                                                    ▼                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Storage Engine  (LSM — columnar Parquet)                            │   │
│  │                                                                      │   │
│  │  WAL (per-partition) ──► MemTable (BTreeMap) ──► FlushWorker        │   │
│  │                                                       │              │   │
│  │                           ┌──────────────────────────┘              │   │
│  │                           ▼                                          │   │
│  │           SSTable (.parquet + .bloom + .zonemap)                    │   │
│  │           CompactionWorker (merge L0 → L1)                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Query Engine  (IQL)                                                 │   │
│  │  Lexer ──► Parser ──► AST ──► Planner ──► Optimizer ──► Executor   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Cluster Layer                                                        │   │
│  │  Consistent-hash ring ──► etcd membership ──► scatter/gather RPC    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ internal gRPC :5317
                    ┌─────────────▼──────────────┐
                    │   API Gateway  (Go / chi)   │
                    │   JWT + API-key auth        │
                    │   NDJSON streaming          │
                    │   :8080                     │
                    └─────────────┬───────────────┘
                                  │ HTTP
                    ┌─────────────▼───────────────┐
                    │   LLM Copilot  (Python)      │
                    │   GBNF-constrained decoding  │
                    │   RCA pipeline               │
                    │   Feedback loop (SQLite)     │
                    │   :8081                      │
                    └─────────────────────────────-┘
```

---

## Component Responsibilities

### infralens-server (Rust binary)

The single-process entry point. It owns:
- The Tokio async runtime.
- gRPC server (tonic) for OTLP and internal scatter/gather.
- HTTP server (axum) for OTLP/HTTP and `/healthz`.
- Prometheus metrics endpoint at `:9090/metrics`.
- Startup wiring of all subsystem crates.

### infralens-ingest

Receives raw OTLP protobuf payloads, normalises them to the internal `InternalRecord`
schema (Arrow-compatible), and pushes them into a bounded `mpsc` channel that provides
natural back-pressure. The bounded channel depth (`ingest.buffer_depth` in config)
is the primary admission-control knob.

### infralens-storage

Custom LSM engine. Three signal types (logs, metrics, spans) share the same engine;
they are separated at the partition level. See [storage-engine.md](storage-engine.md).

### infralens-query

IQL query engine with full SQL-like syntax, predicate pushdown, partition pruning, and
vectorised Arrow execution. See [query-engine.md](query-engine.md).

### infralens-cluster

Distributed coordination layer: consistent-hash ring for shard assignment, etcd-backed
node discovery, and WAL replication. See [cluster.md](cluster.md).

### infralens-rpc

Internal gRPC server (port 5317) that the API Gateway connects to. Implements
scatter/gather: fan out a query to all ring owners, merge results.

### api-gateway (Go)

Thin HTTP layer over the internal gRPC query API. Handles:
- JWT bearer-token authentication.
- API-key authentication.
- Request routing (chi router).
- NDJSON streaming of query results to HTTP clients.
- Rate limiting (configurable per-IP).

### llm-copilot (Python)

FastAPI service that translates natural language to IQL and performs automated root
cause analysis. See [llm-copilot.md](llm-copilot.md).

---

## Data Flow: Write Path

```
Client SDK
    │ OTLP/gRPC or OTLP/HTTP
    ▼
Normaliser                     converts OTLP proto → InternalRecord
    │ mpsc channel (bounded, back-pressure)
    ▼
StorageEngine::write_batch()
    │
    ├─► WAL::append()          fsync to wal.log (configurable interval)
    │
    └─► MemTable::insert()     BTreeMap<(timestamp_ns, record_id), Row>
            │
            │  when MemTable exceeds memtable_size_bytes
            ▼
        FlushWorker
            │  Arrow → Parquet (parquet_row_group_size rows per group)
            │  build BloomFilter for service_name / trace_id
            │  build ZoneMap (min/max timestamp per column)
            ▼
        SSTable on disk (.parquet + .bloom + .zonemap)
            │
            │  when L0 file count ≥ l0_compaction_trigger
            ▼
        CompactionWorker        merge-sort N L0 files → 1 L1 file
```

---

## Data Flow: Read Path

```
HTTP client
    │ POST /v1/query  {"query": "SELECT ..."}
    ▼
API Gateway (Go)
    │ auth check → gRPC QueryRequest
    ▼
infralens-rpc (scatter/gather)
    │ fan out to ring owners (single node in dev)
    ▼
infralens-query
    │ Lexer → Parser → AST → Planner → Optimizer
    │
    │ Optimizer:
    │   1. Constant folding (evaluate now())
    │   2. Predicate pushdown to Scan
    │   3. Projection pushdown (only read needed columns)
    │   4. Partition pruning (skip hour buckets outside time range)
    │
    ▼
Executor (pull-based, Arrow batches of 8192 rows)
    │
    │ Scan reads .zonemap → skip files with no overlap
    │ Scan reads .bloom  → skip files that can't have a key
    │ Scan reads .parquet column-by-column
    ▼
Arrow RecordBatches merged and serialised to NDJSON
    │
    ▼
API Gateway streams NDJSON rows to HTTP client
```

---

## On-Disk Layout

```
{data_dir}/
└── partitions/
    └── 2024120114/             ← YYYYMMDDHH (1-hour bucket, configurable)
        ├── logs/
        │   ├── wal.log
        │   ├── 0000001.parquet
        │   ├── 0000001.bloom
        │   └── 0000001.zonemap
        ├── metrics/
        │   └── ...
        └── spans/
            └── ...
```

Each partition is an independent unit: it can be archived to object storage (MinIO/S3),
reopened read-only, or dropped entirely without touching adjacent partitions.

---

## Technology Choices

| Choice | Rationale |
|--------|-----------|
| Rust for the core | Memory-safe, zero-cost abstractions, Tokio async ecosystem |
| Apache Parquet | Column-oriented, widely supported, good compression |
| Apache Arrow | Zero-copy columnar in-memory format; native Parquet integration |
| etcd | Battle-tested distributed KV, strong consistency, watch API |
| Go for the gateway | Fast compile, excellent HTTP primitives, small binary |
| Python for copilot | llama-cpp-python bindings, numpy/scipy for statistics |
| GBNF constrained decoding | Guarantees syntactically valid IQL without post-processing |
| protox (pure Rust) | Eliminates `protoc` build dependency — `cargo build` just works |

---

## Further Reading

- [Storage Engine](storage-engine.md) — WAL, MemTable, SSTable, bloom filters, zone maps, compaction
- [Query Engine](query-engine.md) — IQL lexer/parser, logical planner, optimizer rules, Arrow executor
- [Cluster](cluster.md) — Consistent-hash ring, etcd membership, WAL replication, scatter/gather
- [LLM Copilot](llm-copilot.md) — GBNF grammar generation, RCA pipeline, feedback loop
