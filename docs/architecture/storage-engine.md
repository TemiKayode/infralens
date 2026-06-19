# Storage Engine

**Crate:** `crates/infralens-storage`

InfraLens uses a custom Log-Structured Merge-tree (LSM) storage engine that writes
data in Apache Parquet format. The design is optimised for write-heavy observability
workloads where data is almost always queried by time range.

---

## Design Goals

1. **High write throughput** — OTLP data arrives in bursts; writes must not block the
   ingest receivers.
2. **Column-oriented reads** — queries typically touch 2–4 columns out of 10+; reading
   entire rows is wasteful.
3. **Time-range pruning without a separate index** — zone maps encode `(min, max)`
   timestamps per file and allow entire files to be skipped at open time.
4. **No external dependencies at runtime** — the engine is pure Rust, no C libraries,
   no external processes.

---

## Component Overview

```
write_batch(records)
      │
      ▼
  WAL::append()          Durability: append-only log, fsync on interval
      │
      ▼
  MemTable::insert()     Hot in-memory write buffer (BTreeMap, sorted by time)
      │
      │  threshold: memtable_size_bytes (default 64 MiB)
      ▼
  FlushWorker            Converts MemTable → SSTable on disk
      │
      │  per file: .parquet + .bloom + .zonemap
      ▼
  L0 SSTables            Unsorted at partition level, sorted within each file
      │
      │  threshold: l0_compaction_trigger (default 4 files)
      ▼
  CompactionWorker       Merge-sorts L0 → L1 (reduces read amplification)
      │
      ▼
  L1 SSTable             Globally sorted; bloom + zone map cover entire file
```

---

## Write-Ahead Log (WAL)

**File:** `crates/infralens-storage/src/wal.rs`

The WAL provides crash durability. Every incoming batch is serialised to the WAL before
being applied to the MemTable. On restart, the WAL is replayed to reconstruct any
in-flight MemTable entries that had not yet been flushed.

Key properties:
- One WAL file per partition directory (`wal.log`).
- Records are length-prefixed and CRC32-checksummed (via `crc32fast`).
- Serialisation uses `bincode` for compactness.
- `fsync` is called on a background interval (`wal_sync_interval_ms`, default 100 ms).
  Reducing this interval improves durability at the cost of IOPS.

### WAL recovery

On startup, `StorageEngine::open()` scans each partition directory for a `wal.log`. If
the MemTable for that partition is empty and the WAL is non-empty, every WAL record is
re-applied to the MemTable. Truncated records (identified by CRC mismatch) at the end
of the file are silently dropped — they represent an incomplete write at crash time.

---

## MemTable

**File:** `crates/infralens-storage/src/memtable.rs`

The MemTable is an in-memory write buffer. It holds unordered (by insertion) but
internally sorted (by timestamp) records until it reaches the flush threshold.

Implementation details:
- Backed by a `BTreeMap<(timestamp_ns, record_id), InternalRecord>`.
- Ordering by `(timestamp_ns, record_id)` produces a time-sorted flush — Parquet reads
  benefit from sorted row groups because statistics are tight.
- Size is tracked in bytes (estimated) rather than row count to avoid surprises with
  variable-length body strings.
- The active MemTable is protected by a `RwLock`; readers can scan concurrently with
  new inserts, and only the brief "make immutable" handoff to the flush worker requires
  an exclusive lock.

---

## SSTable (Parquet + Bloom + ZoneMap)

**File:** `crates/infralens-storage/src/sstable.rs`

Each flush produces three files with the same basename:

| Extension | Format | Purpose |
|-----------|--------|---------|
| `.parquet` | Apache Parquet | Column-oriented record storage |
| `.bloom` | Custom binary | Key existence filter (false-positive ~2%) |
| `.zonemap` | Custom binary | Per-column `(min, max)` statistics |

### Parquet layout

- Row groups of `parquet_row_group_size` rows (default 1,000,000).
- Column encoding: `DELTA_BINARY_PACKED` for integer columns (timestamps, severity),
  `PLAIN` or `DELTA_LENGTH_BYTE_ARRAY` for strings.
- Snappy compression per column chunk.
- Schema matches the Arrow schema defined in `infralens-common`.

### Bloom filter

**File:** `crates/infralens-storage/src/bloom.rs`

A classic double-hashing Bloom filter (`k = 7`, `m` sized for a 2% false-positive rate
at the expected number of rows). Keys indexed: `service_name`, `trace_id`.

Usage pattern: before opening a Parquet file for a query that filters on `service_name`,
the query engine checks the bloom file — if the key is definitely absent, the file is
skipped entirely.

### Zone map

**File:** `crates/infralens-storage/src/zone_map.rs`

A zone map stores `(min, max)` values for selected columns across the entire file:
- `timestamp_ns` (always indexed)
- `severity_number`
- `value` (metrics only)

Zone maps are checked before opening any file during a scan. A query with
`WHERE timestamp_ns >= now() - INTERVAL '1 hour'` skips all files whose
`zonemap.timestamp_max < now() - 1 hour`. This is the primary accelerator for
time-series workloads.

---

## Compaction

**File:** `crates/infralens-storage/src/compaction.rs`

Compaction is a background Tokio task that runs on a configurable interval
(`compaction_interval_secs`, default 30 s). It triggers when the number of L0 files
in a partition reaches `l0_compaction_trigger` (default 4).

### Algorithm

1. Collect all L0 `.parquet` files for a partition.
2. Open them as Arrow `RecordBatch` streams (parallel reads via Tokio tasks).
3. Merge-sort all batches by `(timestamp_ns, record_id)` using a k-way merge.
4. Write a new L1 `.parquet` file with a fresh `.bloom` and `.zonemap`.
5. Atomically replace the L0 files: write to a temp file, rename into place, delete L0s.

Benefits of compaction:
- Fewer files per partition → lower `open()` overhead for queries.
- Globally sorted output → tighter zone-map ranges → better pruning.
- Deduplicated bloom filter covers the full merged key set.

### Read amplification without compaction

Without compaction, a query must open every L0 file whose zone map overlaps the time
range. With 4 L0 files each covering an overlapping hour, read amplification is 4×.
After compaction to one L1 file, it drops to 1×.

---

## Partitioning

**File:** `crates/infralens-storage/src/partition.rs`

Data is partitioned by time bucket (`partition_hours`, default 1 hour). A record with
`timestamp_ns = 1733059200000000000` belongs to partition `2024120114` (2024-12-01 14:00 UTC).

Each partition is an independent directory with its own WAL and SSTable set. Partition
pruning in the query engine is a simple directory-name comparison:

```
partition_key = floor(timestamp_ns / (partition_hours * 3_600_000_000_000))
```

Queries that constrain `timestamp_ns` can skip entire partition directories before
opening any files.

---

## Arrow Schema

**File:** `crates/infralens-common/src/schema.rs`

All three signal types share one Arrow schema with a `signal_type` discriminator column:

| Column | Arrow Type | Description |
|--------|-----------|-------------|
| `timestamp_ns` | `Int64` | Unix timestamp in nanoseconds |
| `signal_type` | `Utf8` | `"log"` \| `"metric"` \| `"span"` |
| `service_name` | `Utf8` | From OTel resource `service.name` |
| `severity_number` | `Int32` | OTel severity (1–24); NULL for metrics/spans |
| `severity_text` | `Utf8` | `"INFO"`, `"ERROR"`, etc. |
| `body` | `Utf8` | Log body / span name |
| `metric_name` | `Utf8` | Metric name; NULL for logs/spans |
| `value` | `Float64` | Metric value; NULL for logs/spans |
| `trace_id` | `Utf8` | 16-byte hex; NULL for logs/metrics |
| `span_id` | `Utf8` | 8-byte hex; NULL for logs/metrics |
| `duration_ns` | `Int64` | Span duration; NULL for logs/metrics |
| `attributes_json` | `Utf8` | JSON blob of remaining OTel attributes |

Using a single schema across all signal types simplifies the flush path and allows
cross-signal queries (e.g., correlate a log spike with a metric anomaly in one query).

---

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `storage.data_dir` | `./data` | Root directory for all partitions |
| `storage.memtable_size_bytes` | `67108864` | Flush threshold (64 MiB) |
| `storage.l0_compaction_trigger` | `4` | Compact after this many L0 files |
| `storage.compaction_interval_secs` | `30` | Compaction background task interval |
| `storage.partition_hours` | `1` | Time bucket width |
| `storage.parquet_row_group_size` | `1000000` | Rows per Parquet row group |
| `storage.wal_sync_interval_ms` | `100` | WAL fsync interval |
