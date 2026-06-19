//! Table scan operator — reads Parquet SSTables from the storage engine.

use std::{
    collections::VecDeque,
    sync::Arc,
};

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use infralens_storage::{engine::StorageEngine, sstable};

use crate::{
    error::{QueryError, Result},
    executor::PhysicalOperator,
    planner::ScanOp,
};

pub struct ScanOperator {
    schema:     Arc<Schema>,
    batches:    VecDeque<RecordBatch>,
    batch_size: usize,
}

impl ScanOperator {
    pub fn new(op: ScanOp, batch_size: usize, storage: Arc<StorageEngine>) -> Result<Self> {
        let schema = infralens_storage_schema_for(&op.table)?;
        let batches = load_batches(&op, batch_size, &storage, &schema)?;
        Ok(Self { schema, batches, batch_size })
    }
}

impl PhysicalOperator for ScanOperator {
    fn schema(&self) -> Arc<Schema> { self.schema.clone() }

    fn poll_next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.pop_front())
    }
}

fn infralens_storage_schema_for(table: &str) -> Result<Arc<Schema>> {
    use infralens_common::schema::{log_schema, metric_schema, span_schema};
    match table {
        "logs"    => Ok(log_schema()),
        "metrics" => Ok(metric_schema()),
        "traces"  => Ok(span_schema()),
        other     => Err(QueryError::Bind(format!("unknown table '{other}'"))),
    }
}

fn load_batches(
    op:         &ScanOp,
    batch_size: usize,
    storage:    &StorageEngine,
    _schema:    &Arc<Schema>,
) -> Result<VecDeque<RecordBatch>> {
    let signal = signal_for_table(&op.table)?;
    let parquet_paths = storage.sstable_paths_for_signal(signal, &op.partition_filter);

    let mut all: VecDeque<RecordBatch> = VecDeque::new();
    for path in parquet_paths {
        let reader = sstable::read_parquet(&path, batch_size)
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| QueryError::Execution(e.to_string()))?;
            // Apply predicate pushdown if present
            if let Some(pred) = &op.predicate {
                let filtered = apply_predicate(batch, pred)?;
                if filtered.num_rows() > 0 { all.push_back(filtered); }
            } else {
                all.push_back(batch);
            }
        }
    }

    // Apply limit at scan level to avoid reading more than needed
    if let Some(limit) = op.limit {
        let mut total = 0usize;
        let limit_usize = limit as usize;
        all.retain(|b| {
            if total >= limit_usize { return false; }
            total += b.num_rows();
            true
        });
    }

    Ok(all)
}

fn signal_for_table(table: &str) -> Result<u8> {
    match table {
        "logs"    => Ok(0),
        "metrics" => Ok(1),
        "traces"  => Ok(2),
        other     => Err(QueryError::Bind(format!("unknown table '{other}'"))),
    }
}

fn apply_predicate(batch: RecordBatch, pred: &crate::ast::Expr) -> Result<RecordBatch> {
    use crate::functions::eval::eval_filter;
    let mask = eval_filter(&batch, pred)?;
    arrow::compute::filter_record_batch(&batch, &mask)
        .map_err(|e| QueryError::Arrow(e))
}
