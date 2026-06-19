//! Zone maps store per-SSTable column statistics (min/max timestamp, cardinality hints).
//! The query engine uses these for time-range pruning before opening any Parquet file.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
// bincode is used for serialisation of ZoneMap to/from files.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min_value: Option<String>, // JSON-encoded
    pub max_value: Option<String>,
    pub null_count: u64,
    pub row_count:  u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneMap {
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    pub row_count:        u64,
    pub columns:          HashMap<String, ColumnStats>,
}

impl ZoneMap {
    pub fn new() -> Self {
        Self {
            min_timestamp_ns: u64::MAX,
            max_timestamp_ns: 0,
            row_count:        0,
            columns:          HashMap::new(),
        }
    }

    /// Extend the time range covered by this zone map.
    pub fn observe_timestamp(&mut self, ts_ns: u64) {
        if ts_ns < self.min_timestamp_ns { self.min_timestamp_ns = ts_ns; }
        if ts_ns > self.max_timestamp_ns { self.max_timestamp_ns = ts_ns; }
        self.row_count += 1;
    }

    /// Returns true if the zone map's time range overlaps `[start, end]`.
    pub fn overlaps_time_range(&self, start_ns: u64, end_ns: u64) -> bool {
        self.min_timestamp_ns <= end_ns && self.max_timestamp_ns >= start_ns
    }

    pub fn write_to_file(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let bytes = bincode::serialize(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        std::fs::write(path, bytes)
    }

    pub fn read_from_file(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        bincode::deserialize(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    }
}

impl Default for ZoneMap {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_range_overlap() {
        let mut zm = ZoneMap::new();
        zm.observe_timestamp(1_000_000);
        zm.observe_timestamp(5_000_000);

        assert!(zm.overlaps_time_range(0, 2_000_000));
        assert!(zm.overlaps_time_range(4_000_000, 6_000_000));
        assert!(!zm.overlaps_time_range(6_000_000, 9_000_000));
        assert!(!zm.overlaps_time_range(0, 500_000));
    }
}
