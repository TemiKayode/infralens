//! Partition management.
//!
//! A partition covers one configurable time bucket (default: 1 hour).
//! Each partition has three signal sub-directories: logs/, metrics/, traces/.
//! Within each sub-directory there is:
//!   - An active WAL file
//!   - An active MemTable (in the engine, keyed by partition key)
//!   - Zero or more SSTable files (.parquet + .bloom + .zonemap)

use crate::error::Result;
use crate::sstable::SSTableMeta;
use infralens_common::model::SignalType;
use parking_lot::Mutex;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// An opaque identifier for a partition, derived from the start timestamp of its
/// time bucket.  Stored on disk as the directory name `{YYYYMMDDHH}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionKey(pub u64); // epoch-ns of bucket start

impl PartitionKey {
    /// Compute the partition key for a given timestamp with `bucket_ns` granularity.
    pub fn for_timestamp(timestamp_ns: u64, bucket_ns: u64) -> Self {
        PartitionKey(timestamp_ns - (timestamp_ns % bucket_ns))
    }

    /// Human-readable directory name (UTC hour bucket).
    pub fn dir_name(&self) -> String {
        let secs = (self.0 / 1_000_000_000) as i64;
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
            .unwrap_or_default();
        dt.format("%Y%m%d%H").to_string()
    }
}

/// Per-signal sub-partition state.
pub struct SignalPartition {
    pub dir:          PathBuf,
    pub sstables:     Mutex<Vec<SSTableMeta>>,
    pub next_seq:     AtomicU64,
}

impl SignalPartition {
    pub fn open(dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        // Scan existing SSTables to find max sequence number.
        let mut max_seq = 0u64;
        let mut sstables = Vec::new();

        for entry in fs::read_dir(&dir)?.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("zonemap") {
                let seq: u64 = path.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                max_seq = max_seq.max(seq);

                if let Ok(zm) = crate::zone_map::ZoneMap::read_from_file(&path) {
                    sstables.push(SSTableMeta {
                        seq,
                        signal_type: SignalType::Log, // refined by caller
                        dir:         dir.clone(),
                        zone_map:    zm,
                    });
                }
            }
        }

        sstables.sort_by_key(|m| m.seq);

        Ok(Self {
            dir,
            sstables: Mutex::new(sstables),
            next_seq: AtomicU64::new(max_seq + 1),
        })
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn add_sstable(&self, meta: SSTableMeta) {
        self.sstables.lock().push(meta);
    }

    pub fn sstable_count(&self) -> usize {
        self.sstables.lock().len()
    }

    /// Return SSTable metas whose zone-maps overlap the requested time range.
    pub fn candidates(&self, start_ns: u64, end_ns: u64) -> Vec<SSTableMeta> {
        self.sstables
            .lock()
            .iter()
            .filter(|m| m.zone_map.overlaps_time_range(start_ns, end_ns))
            .cloned()
            .collect()
    }

    pub fn wal_path(&self) -> PathBuf {
        self.dir.join("wal.log")
    }
}

/// A full partition (all three signals) under a single time-bucket directory.
pub struct Partition {
    pub key:     PartitionKey,
    pub dir:     PathBuf,
    pub logs:    SignalPartition,
    pub metrics: SignalPartition,
    pub spans:   SignalPartition,
}

impl Partition {
    pub fn open(base_dir: &Path, key: PartitionKey) -> Result<Self> {
        let dir = base_dir.join("partitions").join(key.dir_name());
        fs::create_dir_all(&dir)?;

        Ok(Self {
            key,
            logs:    SignalPartition::open(dir.join("logs"))?,
            metrics: SignalPartition::open(dir.join("metrics"))?,
            spans:   SignalPartition::open(dir.join("spans"))?,
            dir,
        })
    }

    pub fn signal_partition(&self, sig: SignalType) -> &SignalPartition {
        match sig {
            SignalType::Log    => &self.logs,
            SignalType::Metric => &self.metrics,
            SignalType::Span   => &self.spans,
        }
    }
}
