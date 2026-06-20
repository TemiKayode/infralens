# InfraLens — Design Decisions

This document explains every major technical choice in InfraLens, the alternatives considered, and why the chosen path was taken. It is intended for contributors, technical reviewers, and anyone evaluating the project.

---

## 1. Core Language: Rust

**Chosen over:** Go, Java/Scala, Python, C++

### Why Rust

Observability backends have two ruthless requirements: **high write throughput** and **low, predictable tail latency**. These are in direct conflict with garbage-collected runtimes.

| Runtime | Write throughput | GC pause impact | p99 predictability |
|---|---|---|---|
| Java (JVM) | High (JIT-warmed) | Yes — multi-ms STW pauses corrupt p99 | Poor without careful GC tuning |
| Go | High | Yes — sub-ms but cumulative at scale | Moderate |
| Python | Low (GIL) | Ref-counting + cyclic GC | Poor |
| **Rust** | **Highest** | **None — no GC** | **Excellent** |
| C++ | Highest | None | Excellent but unsafe |

Rust gives C++-class performance with memory safety enforced at compile time. The Tokio async runtime provides M:N threading without the overhead of OS-thread-per-connection, and the `async/await` model maps directly to the ingest pipeline (receive → decode → channel → WAL → MemTable).

The compiler's borrow checker eliminates an entire class of concurrency bugs (use-after-free, data races) that are common in high-throughput systems written in C++.

### Tradeoffs accepted

- Longer compile times (mitigated by `sccache` and Docker layer caching).
- Steeper learning curve for new contributors.
- Smaller crate ecosystem than npm or PyPI, though it covers all required domains.

---

## 2. Storage Format: Apache Parquet + Apache Arrow

**Chosen over:** ClickHouse, TimescaleDB, OpenTSDB, custom binary files, MessagePack

### Why Parquet

Observability data is **append-heavy and read-rarely** — a 1,000:1 write-to-read ratio is common. The read pattern when it does occur is highly selective: "show me p99 latency for service X over the last hour" touches 1–2 columns out of 30+.

Parquet is a columnar file format designed exactly for this access pattern:

- **Column skipping**: a query for `severity_number` never reads the `body` column bytes.
- **Predicate pushdown**: zone maps and bloom filters on each row group allow the reader to skip entire file chunks before decoding.
- **Compression**: columnar layout means the Zstd or Snappy compressor sees repetitive values (same severity level repeated 10,000 times) and achieves 10–20× compression ratios vs row-oriented formats.
- **Ecosystem portability**: any tool that speaks Parquet — DuckDB, Spark, Polars, Pandas, AWS Athena — can query InfraLens data files directly without any export step. This is a strong escape hatch.

### Why not ClickHouse

ClickHouse is excellent but it is a **complete database system**, not a storage format. Embedding ClickHouse would mean:
- ~500 MB binary dependency.
- A separate cluster to operate.
- Tight coupling to a single vendor's query engine.

InfraLens stores Parquet files that any query engine can read. The query engine is swappable.

### Why not TimescaleDB

TimescaleDB is PostgreSQL with time-series extensions. PostgreSQL is a **row-oriented** MVCC engine — its page format stores all columns of a row together. Scanning 100 million log records for severity > ERROR requires reading all 30+ columns of every row to extract one. Columnar layout avoids this entirely.

### Why Apache Arrow in memory

Arrow is the **in-memory columnar format** that Parquet serialises to/from. Benefits:
- Zero-copy reads: a Parquet page is memory-mapped and the Arrow reader returns a buffer pointing directly into the mmap'd region — no allocation.
- SIMD vectorisation: Arrow's kernels exploit AVX2/AVX-512 for filter and aggregation operations.
- IPC: Arrow IPC frames are the wire format for query results between the Rust server and Go gateway — no serialisation step.

---

## 3. Storage Engine: WAL + MemTable + SSTable (LSM-inspired)

**Chosen over:** direct Parquet writes, RocksDB, LMDB, B-tree index

### Why an LSM-inspired engine

Writing directly to Parquet on every ingest request is untenable: Parquet requires knowing the full column set up front (it writes column footers), and random small writes produce pathologically fragmented files.

The LSM-tree pattern solves this:

```
Ingest → WAL (durable, append-only) → MemTable (in-memory, sorted)
       → flush when full → SSTable (Parquet file)
       → compact periodically → fewer, larger files
```

**WAL** provides durability before the write is acknowledged. If the process crashes mid-flush, the WAL replays on restart — no data loss.

**MemTable** batches small writes into large columnar batches before flushing. A 64 MiB MemTable typically accumulates 50,000–500,000 records before flushing, making each Parquet file large enough to compress and read efficiently.

**SSTable compaction** merges overlapping time-range files, rebuilds bloom filters, and removes tombstones — keeping the number of files per partition bounded.

### Why not RocksDB

RocksDB is a general-purpose key-value store with an LSM engine. Using it would:
- Pull in a large C++ dependency compiled via `cxx`.
- Force all values into byte blobs — losing native columnar structure.
- Require a separate serialisation/deserialisation step for Arrow batches.

Building the storage engine directly on Parquet and Arrow keeps the stack cohesive and eliminates the impedance mismatch between a KV store and a columnar database.

---

## 4. Ingestion Protocol: OTLP (OpenTelemetry Protocol)

**Chosen over:** StatsD, Prometheus remote write, custom binary protocol, Kafka

### Why OTLP

OTLP is the OpenTelemetry wire protocol. It carries **logs, metrics, and traces** in a single unified schema — the only standardised protocol that covers all three signal types.

| Protocol | Logs | Metrics | Traces | Structured metadata | Semantic conventions |
|---|---|---|---|---|---|
| StatsD | No | Yes (counters only) | No | No | No |
| Prometheus remote write | No | Yes | No | Labels only | Partial |
| Zipkin | No | No | Yes | Limited | No |
| **OTLP** | **Yes** | **Yes** | **Yes** | **Yes** | **Yes** |

Every major language has an OpenTelemetry SDK that emits OTLP out of the box. Adding InfraLens to an existing application requires changing one line of configuration — not instrumenting a new client library.

OTLP also defines a **JSON encoding** alongside the binary Protobuf encoding, which makes debugging and demo ingestion trivial (as seen in `demos/payloads/`).

### Why not Kafka as the ingest protocol

Kafka is a **message bus**, not an ingest protocol. It could sit in front of InfraLens as a durability buffer for very high cardinality deployments, but it adds:
- Another cluster to operate.
- Consumer group lag monitoring.
- Schema registry complexity.

InfraLens's WAL already provides the durability guarantee that Kafka provides. In high-throughput production deployments, a Kafka layer can be added transparently without changing the ingest API.

---

## 5. Cluster Coordination: etcd

**Chosen over:** ZooKeeper, Consul, Raft-in-process, gossip (memberlist/SWIM)

### Why etcd

The cluster layer needs **linearisable membership state**: when a node joins or leaves, all other nodes must agree before routing decisions are made. Eventual consistency is insufficient here — a split-brain scenario would cause duplicate writes.

| Option | Consistency | Operational complexity | Binary size |
|---|---|---|---|
| ZooKeeper | Linearisable | High (JVM, separate ensemble) | ~150 MB JVM |
| Consul | Linearisable + AP mode | Medium (full service mesh) | ~100 MB |
| Raft-in-process | Linearisable | High (custom Raft is error-prone) | 0 (embedded) |
| **etcd** | **Linearisable** | **Low (single static binary)** | **~20 MB** |

etcd exposes a gRPC API with lease TTLs — exactly the primitives needed: a node takes a lease, heartbeats it every 5 seconds, and the lease auto-expires in 15 seconds if the node dies. Other nodes watch the key space and update the hash ring in real time.

### Why not Consul

Consul provides service discovery, health checking, key-value, and service mesh. InfraLens needs only the KV and lease primitives. Consul's additional features add operational surface area without benefit.

---

## 6. Inter-service Communication: gRPC (tonic)

**Chosen over:** REST/JSON, NATS, RabbitMQ, raw TCP framing

### Why gRPC

The ingest-to-storage path and the query scatter/gather path both need:
1. **Strongly typed contracts** — payload shape is enforced by Protobuf schemas, not ad-hoc JSON.
2. **Streaming** — query results are streamed as Arrow IPC frames, not buffered into a single JSON response.
3. **Performance** — binary Protobuf encoding is 5–10× smaller and faster to decode than JSON.

gRPC provides all three. The `tonic` crate generates type-safe Rust client and server stubs from `.proto` files, eliminating an entire category of runtime marshalling errors.

### Why tonic over other Rust gRPC crates

`tonic` is the de-facto standard Rust gRPC implementation, built on Tokio and `hyper`. It integrates with `tower` middleware (tracing, rate limiting, authentication) and is actively maintained by the same team that maintains Tokio.

---

## 7. API Gateway: Go + chi

**Chosen over:** Rust (axum), Node.js/Express, Nginx + Lua, Envoy

### Why Go for the gateway

The gateway is **I/O-bound and logic-light**: receive a JSON query, validate JWT, fan out to Rust shards, aggregate Arrow IPC frames, return NDJSON. Go's goroutine model is simpler to write for this pattern than Rust's async/await — there are no lifetimes to reason about across async boundaries.

Go also has faster iteration cycles than Rust for the gateway's business logic layer (auth, routing, protocol translation), which changes more often than the storage engine.

### Why chi over gin/fiber/echo

`chi` is a thin wrapper over Go's standard `net/http`. It adds only routing and middleware composition — no magic, no custom context types. This means:
- `chi` handlers are 100% compatible with any `net/http` middleware.
- There is no framework lock-in: switching to vanilla `net/http` requires removing `chi.NewRouter()`.
- The codebase is readable by anyone who knows Go's standard library.

---

## 8. Cold Storage: MinIO (S3-compatible)

**Chosen over:** Local filesystem only, AWS S3 (vendor), Azure Blob, GCS

### Why S3-compatible object storage

Compacted Parquet files are ideal for object storage: they are immutable after compaction, large (100 MB–1 GB each), and accessed infrequently. Object storage provides:
- **Unlimited scale**: no filesystem capacity planning.
- **Durability**: 11-nines durability in AWS S3; MinIO replicates locally.
- **Cost**: cold object storage is 10–100× cheaper per GB than SSD.

### Why MinIO specifically

MinIO is **S3-compatible** — the same AWS SDK works against both. Running MinIO locally in Docker means:
- Zero cloud dependency during development.
- Production deployment requires only changing the endpoint URL and credentials.
- No S3 egress costs while testing.

---

## 9. LLM Copilot: Python + llama-cpp-python

**Chosen over:** OpenAI API, Rust `llm` crate, vLLM, Ollama

### Why local inference (llama.cpp)

Observability data frequently contains PII — IP addresses, user IDs, error messages with personal details. Sending this data to an external API:
- Violates GDPR/CCPA data residency requirements in many deployments.
- Incurs per-token cost at scale.
- Creates a hard dependency on external availability.

`llama-cpp-python` runs inference entirely on-premise using CPU-only quantised models (Q4_K_M = ~2 GB, runs on any laptop). GPU layers can be enabled via `COPILOT_N_GPU_LAYERS` when available.

### Why Python for the copilot

Python has the richest ML/AI ecosystem: HuggingFace, scipy, numpy, pandas. The RCA pipeline's z-score anomaly detection and Pearson correlation use scipy — libraries with no Rust equivalent of comparable maturity.

The copilot runs as a separate Docker service and communicates with the main stack over HTTP, so language choice is fully isolated.

### Why GBNF constrained decoding

Free-form LLM output for NL→IQL conversion produces invalid queries ~40% of the time even with careful prompting. GBNF (Grammar-Based Format constraints, a llama.cpp feature) constrains the model to produce **only syntactically valid IQL** — the grammar is generated at runtime from the live table catalog, making hallucinated column names impossible by construction.

This is preferable to:
- **Fine-tuning**: requires a labelled dataset and GPU compute.
- **Few-shot prompting**: output is unconstrained, validation is an extra round-trip.
- **Function calling**: requires OpenAI-format models; not portable.

---

## 10. Query Language: IQL (InfraLens Query Language)

**Chosen over:** PromQL, LogQL, InfluxQL, raw SQL, GraphQL

### Why a custom language

No existing query language covers all three signal types with a unified syntax:

| Language | Logs | Metrics | Traces | SQL-familiar | Temporal functions |
|---|---|---|---|---|---|
| PromQL | No | Yes | No | No | Partial |
| LogQL | Yes | Via metrics | No | No | No |
| InfluxQL | No | Yes | No | Partial | Partial |
| SQL | Via UDFs | Via UDFs | Via UDFs | Yes | No |
| **IQL** | **Yes** | **Yes** | **Yes** | **Yes** | **Yes** |

IQL is SQL with temporal extensions (`time_bucket`, `rate`, `delta`, `histogram_quantile`, `now()`, `INTERVAL`). Engineers who know SQL learn IQL in minutes. The `FROM` clause specifies the signal type (`FROM logs`, `FROM metrics`, `FROM traces`) instead of a table, making the unified model explicit.

The entire pipeline — lexer → parser → AST → logical planner → optimizer → physical executor — is implemented in the `infralens-query` crate with no runtime dependencies, making it embeddable and testable in isolation.

---

## Summary Table

| Decision | Chosen | Key reason |
|---|---|---|
| Core language | Rust | Zero GC, predictable p99 latency |
| Storage format | Parquet + Arrow | Columnar compression, ecosystem portability |
| Storage engine | LSM (WAL + MemTable + SSTable) | Append-optimised, durable, compaction-controlled |
| Ingest protocol | OTLP | Only unified logs+metrics+traces standard |
| Cluster coordination | etcd | Linearisable, single binary, gRPC leases |
| Inter-service RPC | gRPC/tonic | Typed contracts, streaming, binary efficiency |
| API gateway | Go + chi | I/O-bound proxy, fast iteration, stdlib-compatible |
| Cold storage | MinIO (S3-compatible) | On-premise S3, zero vendor lock-in |
| LLM inference | llama.cpp (local) | Data stays on-premise, no token cost, CPU-only |
| LLM output | GBNF constrained decoding | Guaranteed syntactically valid IQL output |
| Query language | IQL | Unified SQL-flavoured language for all three signals |
