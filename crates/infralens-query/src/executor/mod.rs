//! Vectorised pull-based executor.  Each operator produces Arrow RecordBatches.

pub mod aggregate;
pub mod filter;
pub mod limit;
pub mod scan;
pub mod sort;

use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use crate::error::Result;

// ── Operator trait ────────────────────────────────────────────────────────────

pub trait PhysicalOperator: Send {
    fn schema(&self) -> Arc<Schema>;
    /// Pull the next batch.  `None` = stream exhausted.
    fn poll_next(&mut self) -> Result<Option<RecordBatch>>;
}

// ── Build executor tree from PhysicalPlan ─────────────────────────────────────

use infralens_storage::engine::StorageEngine;

use crate::planner::{LogicalPlan, PhysicalPlan};

pub fn build(plan: PhysicalPlan, storage: Arc<StorageEngine>) -> Result<Box<dyn PhysicalOperator>> {
    build_logical(plan.logical, plan.batch_size, storage)
}

fn build_logical(
    plan:       LogicalPlan,
    batch_size: usize,
    storage:    Arc<StorageEngine>,
) -> Result<Box<dyn PhysicalOperator>> {
    use crate::planner::LogicalPlan::*;

    match plan {
        Scan(op) => Ok(Box::new(scan::ScanOperator::new(op, batch_size, storage)?)),

        Filter { predicate, input } => {
            let child = build_logical(*input, batch_size, storage)?;
            Ok(Box::new(filter::FilterOperator::new(child, predicate)))
        }

        Aggregate { keys, aggregates, input } => {
            let child = build_logical(*input, batch_size, storage)?;
            Ok(Box::new(aggregate::AggregateOperator::new(child, keys, aggregates)?))
        }

        Sort { keys, input } => {
            let child = build_logical(*input, batch_size, storage)?;
            Ok(Box::new(sort::SortOperator::new(child, keys)))
        }

        Limit { n, offset, input } => {
            let child = build_logical(*input, batch_size, storage)?;
            Ok(Box::new(limit::LimitOperator::new(child, n, offset)))
        }

        // Project is handled in scan/scan-pushdown; no separate operator needed.
        Project { input, .. } => build_logical(*input, batch_size, storage),

        // Distributed joins: not yet supported in local executor.
        Join { .. } => Err(crate::error::QueryError::Execution(
            "cross-table JOIN requires distributed executor (Phase 3 local scope: single-table)".into()
        )),
    }
}
