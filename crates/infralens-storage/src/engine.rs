//! `StorageEngine` — the public API of the storage crate.
//!
//! Responsibilities:
//!  1. Accept writes for all three signal types.
//!  2. Route each write to the correct partition's WAL + MemTable.
//!  3. Detect when a MemTable is full, freeze it, and schedule a flush.
//!  4. Run background flush and compaction workers as `tokio` tasks.

use crate::{
    compaction::compact_partition,
    error::{Result, StorageError},
    memtable::{ImmutableMemTable, MemTable},
    partition::{Partition, PartitionKey},
    sstable,
    wal::{Wal, ENTRY_LOG, ENTRY_METRIC, ENTRY_SPAN},
};
use dashmap::DashMap;
use infralens_common::{
    config::StorageConfig,
    model::{IngestBatch, LogRecord, MetricPoint, SignalType, SpanRecord},
};
use parking_lot::Mutex;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// ── Internal messages to the flush worker ────────────────────────────────────

struct FlushRequest {
    imm:         ImmutableMemTable,
    partition:   Arc<Partition>,
}

// ── Engine ────────────────────────────────────────────────────────────────────

pub struct StorageEngine {
    config:     StorageConfig,
    partitions: DashMap<u64, Arc<Partition>>,
    /// Active MemTable per partition key.
    memtables:  DashMap<u64, Arc<MemTable>>,
    /// Active WAL per partition (one WAL covers all signals in a partition).
    wals:       DashMap<u64, Arc<Mutex<Wal>>>,
    flush_tx:   mpsc::Sender<FlushRequest>,
    closed:     AtomicBool,
    bucket_ns:  u64,
}

impl StorageEngine {
    pub async fn open(config: StorageConfig) -> Result<Arc<Self>> {
        let data_dir = std::path::PathBuf::from(&config.data_dir);
        std::fs::create_dir_all(&data_dir)?;

        let bucket_ns = config.partition_hours * 3_600 * 1_000_000_000;

        let (flush_tx, flush_rx) = mpsc::channel::<FlushRequest>(64);

        let engine = Arc::new(Self {
            config: config.clone(),
            partitions: DashMap::new(),
            memtables:  DashMap::new(),
            wals:       DashMap::new(),
            flush_tx,
            closed:     AtomicBool::new(false),
            bucket_ns,
        });

        // Spawn the flush worker.
        let eng_flush = Arc::clone(&engine);
        tokio::spawn(async move {
            flush_worker(flush_rx, eng_flush).await;
        });

        // Spawn the compaction worker.
        let eng_compact = Arc::clone(&engine);
        let compact_interval = Duration::from_secs(config.compaction_interval_secs);
        let l0_trigger = config.l0_compaction_trigger;
        tokio::spawn(async move {
            compaction_worker(eng_compact, compact_interval, l0_trigger).await;
        });

        info!(data_dir = %data_dir.display(), "StorageEngine opened");
        Ok(engine)
    }

    // ── Write path ────────────────────────────────────────────────────────────

    pub async fn write_batch(&self, batch: IngestBatch) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed);
        }
        match batch {
            IngestBatch::Logs(records)    => self.write_logs(&records).await,
            IngestBatch::Metrics(records) => self.write_metrics(&records).await,
            IngestBatch::Spans(records)   => self.write_spans(&records).await,
        }
    }

    async fn write_logs(&self, records: &[LogRecord]) -> Result<()> {
        for record in records {
            let pkey = PartitionKey::for_timestamp(record.timestamp_ns, self.bucket_ns);
            let mem  = self.get_or_create_memtable(pkey);

            // WAL write first.
            let data = bincode::serialize(record)?;
            self.get_or_create_wal(pkey).lock().append(ENTRY_LOG, &data)?;

            mem.write_log(record)?;
            self.maybe_freeze(pkey, &mem).await?;
        }
        metrics::counter!("infralens_ingest_records_total", "signal" => "log", "status" => "ok")
            .increment(records.len() as u64);
        Ok(())
    }

    async fn write_metrics(&self, records: &[MetricPoint]) -> Result<()> {
        for record in records {
            let pkey = PartitionKey::for_timestamp(record.timestamp_ns, self.bucket_ns);
            let mem  = self.get_or_create_memtable(pkey);

            let data = bincode::serialize(record)?;
            self.get_or_create_wal(pkey).lock().append(ENTRY_METRIC, &data)?;

            mem.write_metric(record)?;
            self.maybe_freeze(pkey, &mem).await?;
        }
        metrics::counter!("infralens_ingest_records_total", "signal" => "metric", "status" => "ok")
            .increment(records.len() as u64);
        Ok(())
    }

    async fn write_spans(&self, records: &[SpanRecord]) -> Result<()> {
        for record in records {
            let pkey = PartitionKey::for_timestamp(record.start_time_ns, self.bucket_ns);
            let mem  = self.get_or_create_memtable(pkey);

            let data = bincode::serialize(record)?;
            self.get_or_create_wal(pkey).lock().append(ENTRY_SPAN, &data)?;

            mem.write_span(record)?;
            self.maybe_freeze(pkey, &mem).await?;
        }
        metrics::counter!("infralens_ingest_records_total", "signal" => "span", "status" => "ok")
            .increment(records.len() as u64);
        Ok(())
    }

    // ── MemTable lifecycle ────────────────────────────────────────────────────

    fn get_or_create_memtable(&self, key: PartitionKey) -> Arc<MemTable> {
        self.memtables
            .entry(key.0)
            .or_insert_with(|| MemTable::new(self.config.memtable_size_bytes))
            .clone()
    }

    fn get_or_create_wal(&self, key: PartitionKey) -> Arc<Mutex<Wal>> {
        // Check fast path first to avoid nested DashMap entry() calls.
        if let Some(w) = self.wals.get(&key.0) { return w.clone(); }
        let partition = self.get_or_create_partition(key);
        let wal_path  = partition.logs.wal_path();
        self.wals.entry(key.0).or_insert_with(|| {
            match Wal::open(&wal_path) {
                Ok(w)  => Arc::new(Mutex::new(w)),
                Err(e) => panic!("cannot open WAL at {}: {e}", wal_path.display()),
            }
        }).clone()
    }

    fn get_or_create_partition(&self, key: PartitionKey) -> Arc<Partition> {
        self.partitions.entry(key.0).or_insert_with(|| {
            let data_dir = std::path::PathBuf::from(&self.config.data_dir);
            match Partition::open(&data_dir, key) {
                Ok(p)  => Arc::new(p),
                Err(e) => panic!("cannot open partition {}: {e}", key.dir_name()),
            }
        }).clone()
    }

    async fn maybe_freeze(&self, key: PartitionKey, mem: &Arc<MemTable>) -> Result<()> {
        if !mem.is_full() { return Ok(()); }

        // Swap in a fresh active MemTable.
        let frozen = mem.clone();
        let fresh  = MemTable::new(self.config.memtable_size_bytes);
        self.memtables.insert(key.0, fresh);

        let partition = self.get_or_create_partition(key);
        let imm       = ImmutableMemTable::from_active(frozen);

        match self.flush_tx.try_send(FlushRequest { imm, partition }) {
            Ok(_)  => debug!(partition = key.dir_name(), "Flush scheduled"),
            Err(e) => warn!("Flush queue full, dropping flush request: {e}"),
        }
        Ok(())
    }

    // ── Manual flush ──────────────────────────────────────────────────────────

    /// Force-flush all non-empty memtables. Useful for graceful shutdown.
    pub async fn flush_all(&self) -> Result<()> {
        let keys: Vec<u64> = self.memtables.iter().map(|e| *e.key()).collect();
        for raw_key in keys {
            let key = PartitionKey(raw_key);
            if let Some(mem) = self.memtables.remove(&raw_key).map(|(_, m)| m) {
                if mem.is_empty() { continue; }
                let partition = self.get_or_create_partition(key);
                let imm       = ImmutableMemTable::from_active(mem);
                let _ = self.flush_tx.send(FlushRequest { imm, partition }).await;
            }
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.flush_all().await?;
        // Give workers time to drain.
        tokio::time::sleep(Duration::from_millis(500)).await;
        info!("StorageEngine closed");
        Ok(())
    }

    // ── Query support ─────────────────────────────────────────────────────────

    /// Return Parquet file paths for a given signal type across specified partitions.
    /// If `partition_filter` is empty all known partitions are searched.
    pub fn sstable_paths_for_signal(
        &self,
        signal: u8,
        partition_filter: &[u64],
    ) -> Vec<std::path::PathBuf> {
        use infralens_common::model::SignalType;
        let sig = match signal {
            0 => SignalType::Log,
            1 => SignalType::Metric,
            2 => SignalType::Span,
            _ => return vec![],
        };

        let keys: Vec<u64> = if partition_filter.is_empty() {
            self.partitions.iter().map(|e| *e.key()).collect()
        } else {
            partition_filter.to_vec()
        };

        let mut paths = Vec::new();
        for key in keys {
            if let Some(partition) = self.partitions.get(&key) {
                let sp = partition.signal_partition(sig);
                for meta in sp.sstables.lock().iter() {
                    paths.push(meta.dir.join(format!("{:06}.parquet", meta.seq)));
                }
            }
        }
        paths
    }
}

// ── Background workers ────────────────────────────────────────────────────────

async fn flush_worker(mut rx: mpsc::Receiver<FlushRequest>, _engine: Arc<StorageEngine>) {
    while let Some(req) = rx.recv().await {
        let start = std::time::Instant::now();
        if let Err(e) = flush_immutable(req).await {
            error!(error = %e, "Flush failed");
        }
        let elapsed = start.elapsed().as_secs_f64();
        metrics::histogram!("infralens_storage_flush_duration_seconds").record(elapsed);
    }
    info!("Flush worker exiting");
}

async fn flush_immutable(req: FlushRequest) -> Result<()> {
    let FlushRequest { imm, partition } = req;
    let mem = &imm.0;

    // Flush each signal type independently.
    flush_signal_logs(&partition, mem).await?;
    flush_signal_metrics(&partition, mem).await?;
    flush_signal_spans(&partition, mem).await?;

    // Write WAL checkpoint.
    // (WAL is keyed by partition; we load it from the partition's log dir.)
    let wal_path = partition.logs.wal_path();
    if wal_path.exists() {
        match Wal::open(&wal_path) {
            Ok(mut w) => { let _ = w.checkpoint(); }
            Err(e)    => warn!(error = %e, "WAL checkpoint failed"),
        }
    }

    Ok(())
}

async fn flush_signal_logs(partition: &Arc<Partition>, mem: &Arc<MemTable>) -> Result<()> {
    let rows = mem.drain_signal(SignalType::Log as u8);
    if rows.is_empty() { return Ok(()); }

    let records: Vec<LogRecord> = rows
        .iter()
        .filter_map(|(_, data)| bincode::deserialize(data).ok())
        .collect();

    let sp  = &partition.logs;
    let seq = sp.next_seq();
    let meta = sstable::write_logs(&records, &sp.dir, seq)?;
    sp.add_sstable(meta);

    metrics::gauge!("infralens_storage_sstable_count",
        "signal" => "log",
        "partition" => partition.key.dir_name()
    ).set(sp.sstable_count() as f64);
    Ok(())
}

async fn flush_signal_metrics(partition: &Arc<Partition>, mem: &Arc<MemTable>) -> Result<()> {
    let rows = mem.drain_signal(SignalType::Metric as u8);
    if rows.is_empty() { return Ok(()); }

    let records: Vec<MetricPoint> = rows
        .iter()
        .filter_map(|(_, data)| bincode::deserialize(data).ok())
        .collect();

    let sp  = &partition.metrics;
    let seq = sp.next_seq();
    let meta = sstable::write_metrics(&records, &sp.dir, seq)?;
    sp.add_sstable(meta);
    Ok(())
}

async fn flush_signal_spans(partition: &Arc<Partition>, mem: &Arc<MemTable>) -> Result<()> {
    let rows = mem.drain_signal(SignalType::Span as u8);
    if rows.is_empty() { return Ok(()); }

    let records: Vec<SpanRecord> = rows
        .iter()
        .filter_map(|(_, data)| bincode::deserialize(data).ok())
        .collect();

    let sp  = &partition.spans;
    let seq = sp.next_seq();
    let meta = sstable::write_spans(&records, &sp.dir, seq)?;
    sp.add_sstable(meta);
    Ok(())
}

async fn compaction_worker(engine: Arc<StorageEngine>, interval: Duration, l0_trigger: usize) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        if engine.closed.load(Ordering::Acquire) { break; }

        let keys: Vec<u64> = engine.partitions.iter().map(|e| *e.key()).collect();
        for raw_key in keys {
            if let Some(partition) = engine.partitions.get(&raw_key) {
                let start = std::time::Instant::now();
                let ops   = compact_partition(&partition, l0_trigger).await;
                if ops > 0 {
                    let elapsed = start.elapsed().as_secs_f64();
                    metrics::histogram!("infralens_storage_compaction_duration_seconds")
                        .record(elapsed);
                }
            }
        }
    }
    info!("Compaction worker exiting");
}
