use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use crate::{error::Result, executor::PhysicalOperator};

pub struct LimitOperator {
    child:      Box<dyn PhysicalOperator>,
    remaining:  u64,
    skip:       u64,
}

impl LimitOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, limit: u64, offset: u64) -> Self {
        Self { child, remaining: limit, skip: offset }
    }
}

impl PhysicalOperator for LimitOperator {
    fn schema(&self) -> Arc<Schema> { self.child.schema() }

    fn poll_next(&mut self) -> Result<Option<RecordBatch>> {
        if self.remaining == 0 { return Ok(None); }

        loop {
            match self.child.poll_next()? {
                None        => return Ok(None),
                Some(batch) => {
                    // Handle offset (skip rows)
                    let nrows = batch.num_rows() as u64;
                    if self.skip >= nrows {
                        self.skip -= nrows;
                        continue;
                    }
                    let batch = if self.skip > 0 {
                        let start = self.skip as usize;
                        self.skip = 0;
                        batch.slice(start, batch.num_rows() - start)
                    } else { batch };

                    // Trim to remaining limit
                    let take = (batch.num_rows() as u64).min(self.remaining);
                    self.remaining -= take;
                    let out = batch.slice(0, take as usize);
                    return Ok(Some(out));
                }
            }
        }
    }
}
