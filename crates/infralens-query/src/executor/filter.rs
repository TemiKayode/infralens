use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use crate::{
    ast::Expr,
    error::{QueryError, Result},
    executor::PhysicalOperator,
    functions::eval::eval_filter,
};

pub struct FilterOperator {
    child:     Box<dyn PhysicalOperator>,
    predicate: Expr,
}

impl FilterOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, predicate: Expr) -> Self {
        Self { child, predicate }
    }
}

impl PhysicalOperator for FilterOperator {
    fn schema(&self) -> Arc<Schema> { self.child.schema() }

    fn poll_next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            match self.child.poll_next()? {
                None        => return Ok(None),
                Some(batch) => {
                    let mask = eval_filter(&batch, &self.predicate)?;
                    let out  = arrow::compute::filter_record_batch(&batch, &mask)
                        .map_err(QueryError::Arrow)?;
                    if out.num_rows() > 0 { return Ok(Some(out)); }
                    // else: filtered to zero rows, pull next batch
                }
            }
        }
    }
}
