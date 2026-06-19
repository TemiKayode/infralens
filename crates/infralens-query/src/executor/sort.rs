use std::sync::Arc;

use arrow::{
    compute::{lexsort_to_indices, SortColumn, SortOptions},
    datatypes::Schema,
    record_batch::RecordBatch,
};

use crate::{
    ast::Expr,
    error::{QueryError, Result},
    executor::PhysicalOperator,
};

pub struct SortOperator {
    child:  Option<Box<dyn PhysicalOperator>>,
    keys:   Vec<(Expr, bool)>,
    result: Option<Vec<RecordBatch>>,
}

impl SortOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, keys: Vec<(Expr, bool)>) -> Self {
        Self { child: Some(child), keys, result: None }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut op = self.child.take().unwrap();
        let mut batches = Vec::new();
        while let Some(b) = op.poll_next()? { batches.push(b); }

        if batches.is_empty() { self.result = Some(vec![]); return Ok(()); }

        // Concatenate all batches, sort, re-split
        let schema  = batches[0].schema();
        let concat  = arrow::compute::concat_batches(&schema, &batches)
            .map_err(QueryError::Arrow)?;

        let sort_cols: Vec<SortColumn> = self.keys.iter().filter_map(|(expr, asc)| {
            let col_name = match expr {
                Expr::Column { name, .. } => name.as_str(),
                _ => return None,
            };
            let col_idx = concat.schema().index_of(col_name).ok()?;
            Some(SortColumn {
                values:  concat.column(col_idx).clone(),
                options: Some(SortOptions { descending: !asc, nulls_first: false }),
            })
        }).collect();

        if sort_cols.is_empty() {
            self.result = Some(vec![concat]);
            return Ok(());
        }

        let indices = lexsort_to_indices(&sort_cols, None).map_err(QueryError::Arrow)?;
        let sorted  = arrow::compute::take_record_batch(&concat, &indices)
            .map_err(QueryError::Arrow)?;

        self.result = Some(vec![sorted]);
        Ok(())
    }
}

impl PhysicalOperator for SortOperator {
    fn schema(&self) -> Arc<Schema> {
        self.child.as_ref().map(|c| c.schema())
            .or_else(|| self.result.as_ref().and_then(|v| v.first()).map(|b| b.schema()))
            .unwrap_or_else(|| Arc::new(Schema::empty()))
    }

    fn poll_next(&mut self) -> Result<Option<RecordBatch>> {
        if self.child.is_some() { self.materialize()?; }
        Ok(self.result.as_mut().and_then(|v| if v.is_empty() { None } else { Some(v.remove(0)) }))
    }
}
