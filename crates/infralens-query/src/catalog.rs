//! Schema catalog — maps table names to Arrow schemas + partition metadata.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use arrow::datatypes::{DataType, Field, Schema};
use infralens_common::schema::{log_schema, metric_schema, span_schema};
use infralens_storage::zone_map::ZoneMap;

use crate::error::{QueryError, Result};

// ── Column descriptor ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name:     String,
    pub data_type: DataType,
    pub nullable: bool,
}

// ── Table descriptor ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TableDesc {
    pub name:    String,
    pub schema:  Arc<Schema>,
    pub columns: Vec<ColumnDesc>,
    /// Time-range stats derived from zone maps; used for partition pruning.
    pub stats:   Vec<TableStats>,
}

#[derive(Debug, Clone)]
pub struct TableStats {
    pub partition_key:   u64,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    pub row_count:        u64,
}

impl TableDesc {
    fn from_schema(name: impl Into<String>, schema: Arc<Schema>) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| ColumnDesc {
                name:      f.name().clone(),
                data_type: f.data_type().clone(),
                nullable:  f.is_nullable(),
            })
            .collect();
        Self { name: name.into(), schema, columns, stats: vec![] }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnDesc> {
        self.columns.iter().find(|c| c.name == name)
    }
}

// ── Catalog ───────────────────────────────────────────────────────────────────

pub struct Catalog {
    inner: RwLock<CatalogInner>,
}

struct CatalogInner {
    tables: HashMap<String, TableDesc>,
}

impl Catalog {
    pub fn new() -> Arc<Self> {
        let mut tables = HashMap::new();
        tables.insert("logs".into(),    TableDesc::from_schema("logs",    log_schema()));
        tables.insert("metrics".into(), TableDesc::from_schema("metrics", metric_schema()));
        tables.insert("traces".into(),  TableDesc::from_schema("traces",  span_schema()));
        Arc::new(Self { inner: RwLock::new(CatalogInner { tables }) })
    }

    pub fn get_table(&self, name: &str) -> Result<TableDesc> {
        let inner = self.inner.read().unwrap();
        inner.tables.get(name)
            .cloned()
            .ok_or_else(|| QueryError::Bind(format!("unknown table '{name}'")))
    }

    /// Update statistics for a table from a fresh zone-map scan.
    pub fn update_stats(&self, table: &str, zone_maps: Vec<(u64, ZoneMap)>) {
        let mut inner = self.inner.write().unwrap();
        if let Some(desc) = inner.tables.get_mut(table) {
            desc.stats = zone_maps
                .into_iter()
                .map(|(key, zm)| TableStats {
                    partition_key:    key,
                    min_timestamp_ns: zm.min_timestamp_ns,
                    max_timestamp_ns: zm.max_timestamp_ns,
                    row_count:        zm.row_count,
                })
                .collect();
        }
    }

    /// Partitions whose time range overlaps [start_ns, end_ns].
    pub fn overlapping_partitions(&self, table: &str, start_ns: u64, end_ns: u64) -> Vec<u64> {
        let inner = self.inner.read().unwrap();
        inner.tables.get(table)
            .map(|desc| {
                desc.stats.iter()
                    .filter(|s| s.min_timestamp_ns <= end_ns && s.max_timestamp_ns >= start_ns)
                    .map(|s| s.partition_key)
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        let mut tables = HashMap::new();
        tables.insert("logs".into(),    TableDesc::from_schema("logs",    log_schema()));
        tables.insert("metrics".into(), TableDesc::from_schema("metrics", metric_schema()));
        tables.insert("traces".into(),  TableDesc::from_schema("traces",  span_schema()));
        Self { inner: RwLock::new(CatalogInner { tables }) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_resolves_builtin_tables() {
        let cat = Catalog::new();
        let logs = cat.get_table("logs").unwrap();
        assert!(!logs.columns.is_empty());
        assert!(logs.column("timestamp_ns").is_some());
    }

    #[test]
    fn catalog_rejects_unknown_table() {
        let cat = Catalog::new();
        assert!(cat.get_table("nonexistent").is_err());
    }

    #[test]
    fn catalog_partition_pruning() {
        let cat = Catalog::new();
        cat.update_stats("logs", vec![
            (1, ZoneMap { min_timestamp_ns: 100, max_timestamp_ns: 200, row_count: 10, columns: Default::default() }),
            (2, ZoneMap { min_timestamp_ns: 300, max_timestamp_ns: 400, row_count: 5,  columns: Default::default() }),
        ]);
        let hit = cat.overlapping_partitions("logs", 150, 350);
        assert_eq!(hit.len(), 2);
        let miss = cat.overlapping_partitions("logs", 500, 600);
        assert_eq!(miss.len(), 0);
    }
}
