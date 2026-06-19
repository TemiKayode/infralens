# InfraLens Phase 1 — Ingest & Storage Core: Design Document

## 1. Overview

Phase 1 establishes the two foundational subsystems of InfraLens:

1. **OTLP Ingest Pipeline** — receives telemetry over gRPC (port 4317) and HTTP (port 4318),
   validates and normalises the data, then writes it through a bounded channel to the storage layer.
2. **Columnar LSM Storage Engine** — persists all three signal types (logs, metrics, traces) using a
   write-optimised LSM variant that flushes to Apache Parquet on disk, with bloom filters and
   zone-maps for pruned reads.

Both components are written in Rust. The binary is a single server process that exposes both
endpoints and runs the background flush/compaction workers as `tokio` tasks.

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        infralens-server                          │
│                                                                  │
│   ┌─────────────────┐        ┌──────────────────────────────┐   │
│   │  OTLP/gRPC      │        │  OTLP/HTTP                   │   │
│   │  tonic server   │        │  axum server                 │   │
│   │  :4317          │        │  :4318                       │   │
│   └────────┬────────┘        └──────────────┬───────────────┘   │
│            │                                │                    │
│            └──────────┬─────────────────────┘                   │
│                       ▼                                          │
│            ┌──────────────────────┐                              │
│            │   Normalizer         │  OTLP proto → InternalRecord │
│            └──────────┬───────────┘                              │
│                       ▼                                          │
│            ┌──────────────────────┐                              │
│            │  IngestPipeline      │  bounded mpsc channel (BP)   │
│            │  batch assembler     │  configurable buffer depth   │
│            └──────────┬───────────┘                              │
│                       ▼                                          │
│            ┌──────────────────────────────────────────────────┐  │
│            │              StorageEngine                        │  │
│            │                                                  │  │
│            │  MemTable (active)  →  ImmutableMemTable  →  │   │  │
│            │  WAL (per partition)                        │   │  │
│            │                                             ▼   │  │
│            │                               SSTable (Parquet) │  │
│            │                               + Bloom + ZoneMap │  │
│            │                                                  │  │
│            │  Background tasks:                               │  │
│            │    - FlushWorker   (memtable → parquet)          │  │
│            │    - CompactWorker (merge small SSTables)        │  │
│            └──────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Internal Data Model

All telemetry is normalised from OTLP protobuf into Rust structs before touching storage.
These structs are shared across the ingest and storage crates via `infralens-common`.

### 3.1 Shared Value Type

```
AnyValue = String | Bool | Int64 | Float64 | Bytes | Array(AnyValue) | Map(String→AnyValue)
```

Stored in Parquet columns as JSON-encoded text in Phase 1. Phase 2 will switch to Arrow
`Map<Utf8, Dense Union>` for schema-on-read.

### 3.2 Signal Types

| Field              | LogRecord       | MetricPoint         | SpanRecord          |
|--------------------|-----------------|---------------------|---------------------|
| Primary timestamp  | timestamp_ns    | timestamp_ns        | start_time_ns       |
| Partition key      | timestamp_ns    | timestamp_ns        | start_time_ns       |
| Dedup key          | sequence_id     | sequence_id         | span_id (128-bit)   |
| Label set          | attributes      | attributes          | attributes          |
| Resource info      | resource_attrs  | resource_attrs      | resource_attrs      |

---

## 4. Write-Ahead Log (WAL)

### Design

Each signal type within each partition has a dedicated WAL file. A single background goroutine
drains the WAL and calls `fsync` at configurable intervals (default: 100 ms) to bound durability
lag without a write-per-entry `fsync` penalty.

### Entry Format

```
┌────────────┬───────────┬────────────┬─────────────────────┐
│  CRC32 (4) │  LEN  (4) │  TYPE (1)  │  DATA (LEN bytes)   │
└────────────┴───────────┴────────────┴─────────────────────┘
```

- `CRC32`: IEEE CRC32 over `[TYPE || DATA]`. Detects torn writes on recovery.
- `LEN`: byte length of `DATA`.
- `TYPE`: `0x01` = LogRecord, `0x02` = MetricPoint, `0x03` = SpanRecord, `0xFF` = Checkpoint.
- `DATA`: `bincode`-serialised payload.

### Recovery

On startup, each partition replays its WAL sequentially:
1. Read header; verify CRC. If mismatch, truncate file at this offset (partial write).
2. Deserialise record and re-insert into fresh MemTable.
3. On `Checkpoint` record, all prior entries are confirmed flushed; reset MemTable.

### Rotation

When the MemTable exceeds `memtable_size_bytes` (default 64 MiB), it becomes immutable.
A new WAL segment is started. After the flush to Parquet succeeds and is fsync'd, a
`Checkpoint` is written to the old WAL segment and the segment file is deleted.

---

## 5. MemTable

### Structure

```
BTreeMap<RowKey, Vec<u8>>
  where RowKey = (timestamp_ns: u64, signal_type: u8, sequence: u64)
  and   Vec<u8> = bincode-serialised InternalRecord
```

Keys are ordered by `(timestamp, signal_type, sequence)`, giving natural time-order iteration
for SSTable writes.

### Concurrency

The active MemTable is protected by a `parking_lot::RwLock`. Writes hold an exclusive lock
only while inserting the BTreeMap entry (μs-scale). Flushes read from the immutable snapshot
concurrently while the new active MemTable accepts writes.

### Size Accounting

`AtomicUsize` tracks approximate byte size (key size + value size). This avoids traversal for
size checks. The actual MemTable may overshoot by one batch if multiple writers race.

---

## 6. SSTable / Columnar Flush

### File Layout

```
{data_dir}/partitions/{YYYYMMDDHH}/{signal}/
├── 000001.parquet     ← columnar data (Arrow IPC via Parquet)
├── 000001.bloom       ← serialised BloomFilter for primary key set
└── 000001.zonemap     ← ZoneMap (min/max timestamp, per-column min/max)
```

### Parquet Schema

Separate schemas per signal type (see `infralens-common/src/schema.rs`). Attributes and
resource fields are stored as JSON `Utf8` columns in Phase 1 for simplicity. Phase 2 will
use Arrow `Map` and `Dictionary` types.

**Encoding & Compression:**
- `timestamp_ns` columns: `DELTA_BINARY_PACKED` (excellent for monotone sequences).
- String columns: `PLAIN` with `ZSTD(3)` page compression.
- Binary columns: `PLAIN` with `ZSTD(3)`.
- Row-group size: 1 million rows or 128 MiB, whichever is smaller.

### Bloom Filter

64 KiB per SSTable, 2 % false-positive rate. Hash functions: SipHash-1-3 with two independent
seeds (equivalent to double hashing). Serialised as `[num_bits: u64][num_hashes: u32][bits: bytes]`.

Bloom keys:
- Logs: `SHA256(attributes_json)[..8]` (8-byte fingerprint).
- Metrics: metric name (UTF-8 bytes).
- Spans: trace_id (16 bytes).

### Zone Map

Stored in a separate `.zonemap` file (bincode-serialised):
```
ZoneMap {
    min_timestamp_ns: u64,
    max_timestamp_ns: u64,
    per_column: HashMap<String, ColumnStats>,  // min/max as JSON Value
}
```

The query engine uses zone maps for time-range pruning without opening the Parquet file.

---

## 7. Compaction

### Trigger

Background `CompactionWorker` polls every 30 s. Compaction is triggered when a partition's
signal sub-directory has > `L0_FILE_LIMIT` (default: 4) SSTable files.

### Strategy

Phase 1 uses **levelled-tiered hybrid**:
- **L0 → L1**: merge all L0 files into a single L1 file; output sorted by `(timestamp, sequence)`.
- **L1 → L2** (future): further merge when L1 exceeds `l1_size_bytes`.

Merge reads all Parquet files into Arrow RecordBatches, does a k-way merge-sort in memory (or
spills to disk for large datasets), and writes a single output Parquet. Old files are deleted
after the new file is fsync'd.

---

## 8. Ingest Pipeline & Back-pressure

### Channel Design

```
OTLP Handler
    │
    │  tokio::sync::mpsc::Sender<IngestBatch>  (bounded: config.buffer_depth)
    ▼
IngestProcessor (single task)
    │
    │  calls StorageEngine::write_*()
    ▼
StorageEngine
```

`buffer_depth` defaults to 4096 batches. If the channel is full, `try_send` fails immediately
and the gRPC/HTTP handler returns `ResourceExhausted` (HTTP 429). This is the back-pressure
signal to the upstream SDK which should apply its own retry/buffer logic.

### Batching

The `IngestProcessor` task reads from the channel in chunks of up to `max_batch_size` (default: 1000)
records, assembling Arrow RecordBatches before writing to the MemTable. This amortises the
`RwLock` acquisition cost.

---

## 9. Self-Observability

Every component emits:
- **Structured logs** via `tracing` to stdout in JSON format.
- **Metrics** via `metrics` crate with a Prometheus exporter on `:9090/metrics`.
- **Spans** via `opentelemetry_sdk` (dogfooded into InfraLens itself in Phase 5).

Key metrics:
- `infralens_ingest_records_total{signal, status}` — records received/rejected.
- `infralens_ingest_backpressure_total` — channel-full events.
- `infralens_storage_memtable_size_bytes` — current memtable size.
- `infralens_storage_sstable_count{partition, signal}` — file count per partition/signal.
- `infralens_storage_flush_duration_seconds` — histogram of flush latency.
- `infralens_storage_compaction_duration_seconds` — histogram of compaction latency.
- `infralens_wal_write_duration_seconds` — WAL write latency histogram.

---

## 10. Trade-offs & Future Work

| Decision | Phase 1 choice | Phase 2+ upgrade |
|----------|---------------|-----------------|
| Attribute encoding | JSON text | Arrow Map + Dictionary |
| Schema | Fixed per signal | Dynamic schema inference |
| Sharding | Single node | Consistent hash across nodes |
| Cold storage | Local NVMe only | Auto-tier to S3 (Parquet in object store) |
| Query | No query engine yet | Full SQL/temporal engine (Phase 3) |
| Compaction | Simple merge | Levelled compaction with space amplification bounds |
| Replication | None | Raft-based replication groups (Phase 2) |

---

## 11. File Structure

```
infralens/
├── Cargo.toml                          # workspace
├── docker-compose.yml
├── config/
│   ├── default.toml
│   └── development.toml
├── proto/
│   └── opentelemetry/proto/
│       ├── common/v1/common.proto
│       ├── resource/v1/resource.proto
│       ├── logs/v1/logs.proto
│       ├── metrics/v1/metrics.proto
│       ├── trace/v1/trace.proto
│       └── collector/
│           ├── logs/v1/logs_service.proto
│           ├── metrics/v1/metrics_service.proto
│           └── trace/v1/trace_service.proto
└── crates/
    ├── infralens-common/               # shared types & config
    ├── infralens-proto/                # generated OTLP proto/tonic stubs
    ├── infralens-storage/              # WAL, MemTable, SSTable, Engine
    ├── infralens-ingest/               # OTLP gRPC+HTTP receivers, normaliser
    └── infralens-server/               # binary entry point
```

---

## 12. Running Phase 1

```bash
# Start dependencies (MinIO for future S3 tier)
docker compose up -d

# Build (requires Rust 1.79+)
cargo build --release

# Run with development config
INFRALENS_ENV=development ./target/release/infralens-server

# Run tests
cargo test --workspace

# Property tests (slower, thorough)
cargo test --workspace -- --include-ignored proptest
```
