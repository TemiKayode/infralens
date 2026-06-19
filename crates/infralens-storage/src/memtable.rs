//! In-memory sorted table (MemTable).
//!
//! Keys are `(timestamp_ns, signal_type, sequence)` — naturally ordered by time
//! so that SSTable flushes produce sorted Parquet files without an extra sort step.

use crate::error::{Result, StorageError};
use infralens_common::model::{LogRecord, MetricPoint, SignalType, SpanRecord};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::debug;

// ── Row key ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowKey {
    pub timestamp_ns:  u64,
    pub signal_type:   u8,
    pub sequence:      u64,
}

// ── MemTable ──────────────────────────────────────────────────────────────────

pub struct MemTable {
    inner:         RwLock<BTreeMap<RowKey, Vec<u8>>>,
    size_bytes:    AtomicUsize,
    max_size:      usize,
    sequence:      AtomicU64,
}

impl MemTable {
    pub fn new(max_size_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            inner:      RwLock::new(BTreeMap::new()),
            size_bytes: AtomicUsize::new(0),
            max_size:   max_size_bytes,
            sequence:   AtomicU64::new(0),
        })
    }

    // ── Writers ───────────────────────────────────────────────────────────────

    pub fn write_log(&self, record: &LogRecord) -> Result<()> {
        let data = bincode::serialize(record)?;
        self.insert(record.timestamp_ns, SignalType::Log as u8, data)
    }

    pub fn write_metric(&self, record: &MetricPoint) -> Result<()> {
        let data = bincode::serialize(record)?;
        self.insert(record.timestamp_ns, SignalType::Metric as u8, data)
    }

    pub fn write_span(&self, record: &SpanRecord) -> Result<()> {
        let data = bincode::serialize(record)?;
        self.insert(record.start_time_ns, SignalType::Span as u8, data)
    }

    fn insert(&self, timestamp_ns: u64, signal_type: u8, data: Vec<u8>) -> Result<()> {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        let key = RowKey { timestamp_ns, signal_type, sequence: seq };
        let size = std::mem::size_of::<RowKey>() + data.len();

        let mut guard = self.inner.write();
        guard.insert(key, data);
        drop(guard);

        self.size_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(())
    }

    // ── State queries ─────────────────────────────────────────────────────────

    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes() >= self.max_size
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Drain all entries for a given signal type, in key order.
    /// Used by the flush worker to produce sorted iterators for SSTable writes.
    pub fn drain_signal(&self, signal_type: u8) -> Vec<(RowKey, Vec<u8>)> {
        let guard = self.inner.read();
        guard
            .iter()
            .filter(|(k, _)| k.signal_type == signal_type)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Return all entries, consuming the MemTable contents.
    /// Only called from the flush worker on an immutable (frozen) snapshot.
    pub fn iter_all(&self) -> Vec<(RowKey, Vec<u8>)> {
        let guard = self.inner.read();
        guard.iter().map(|(k, v)| (*k, v.clone())).collect()
    }
}

impl std::fmt::Debug for MemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTable")
            .field("len", &self.len())
            .field("size_bytes", &self.size_bytes())
            .finish()
    }
}

// ── ImmutableMemTable ─────────────────────────────────────────────────────────

/// A frozen snapshot of a MemTable that is being written to disk.
/// The type wrapper makes it clear in signatures that this is read-only.
pub struct ImmutableMemTable(pub Arc<MemTable>);

impl ImmutableMemTable {
    pub fn from_active(mem: Arc<MemTable>) -> Self {
        debug!(len = mem.len(), size_bytes = mem.size_bytes(), "MemTable frozen");
        ImmutableMemTable(mem)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infralens_common::model::{AnyValue, InstrumentationScope, LogRecord};
    use std::collections::HashMap;

    fn make_log(ts: u64) -> LogRecord {
        LogRecord {
            timestamp_ns:          ts,
            observed_timestamp_ns: ts,
            trace_id:              None,
            span_id:               None,
            severity_number:       9,
            severity_text:         "INFO".to_string(),
            body:                  Some(AnyValue::String("test message".to_string())),
            attributes:            HashMap::new(),
            resource_attributes:   HashMap::new(),
            scope:                 InstrumentationScope::default(),
            schema_url:            String::new(),
        }
    }

    #[test]
    fn size_accounting() {
        let mem = MemTable::new(64 * 1024 * 1024);
        assert_eq!(mem.size_bytes(), 0);
        mem.write_log(&make_log(1000)).unwrap();
        assert!(mem.size_bytes() > 0);
    }

    #[test]
    fn is_full_threshold() {
        // Use a very small threshold so one write fills it.
        let mem = MemTable::new(1); // 1 byte — will be full after first write
        mem.write_log(&make_log(1000)).unwrap();
        assert!(mem.is_full());
    }

    #[test]
    fn drain_signal_isolation() {
        let mem = MemTable::new(64 * 1024 * 1024);

        let log = make_log(1000);
        mem.write_log(&log).unwrap();

        // Should see the log when draining logs.
        let logs = mem.drain_signal(SignalType::Log as u8);
        assert_eq!(logs.len(), 1);

        // Should NOT see it when draining metrics.
        let metrics = mem.drain_signal(SignalType::Metric as u8);
        assert!(metrics.is_empty());
    }

    #[test]
    fn keys_are_time_ordered() {
        let mem = MemTable::new(64 * 1024 * 1024);
        mem.write_log(&make_log(3000)).unwrap();
        mem.write_log(&make_log(1000)).unwrap();
        mem.write_log(&make_log(2000)).unwrap();

        let rows = mem.drain_signal(SignalType::Log as u8);
        let timestamps: Vec<u64> = rows.iter().map(|(k, _)| k.timestamp_ns).collect();
        assert_eq!(timestamps, vec![1000, 2000, 3000]);
    }
}
