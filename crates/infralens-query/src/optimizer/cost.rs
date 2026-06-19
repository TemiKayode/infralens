//! Lightweight cost model for join strategy selection.

use crate::{
    catalog::{Catalog, TableDesc},
    planner::JoinStrategy,
};

/// Estimated cardinality of a table (sum of row counts across all partitions).
pub fn estimated_rows(desc: &TableDesc) -> u64 {
    desc.stats.iter().map(|s| s.row_count).sum()
}

/// Choose join strategy based on estimated cardinalities.
pub fn choose_join_strategy(left_rows: u64, right_rows: u64) -> JoinStrategy {
    let small = 10_000u64;
    let medium = 1_000_000u64;
    match (left_rows, right_rows) {
        (l, r) if l <= small || r <= small   => JoinStrategy::NestedLoop,
        (l, r) if l <= medium && r <= medium => JoinStrategy::Hash,
        _                                    => JoinStrategy::SortMerge,
    }
}
