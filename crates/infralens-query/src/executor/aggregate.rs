//! Simple hash-based aggregate operator.  Consumes all input then emits one batch.

use std::{
    collections::HashMap,
    sync::Arc,
};

use arrow::{
    array::{
        ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder,
        StringBuilder, UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::{
    ast::Expr,
    error::{QueryError, Result},
    executor::PhysicalOperator,
    planner::AggExpr,
};

pub struct AggregateOperator {
    child:         Option<Box<dyn PhysicalOperator>>,
    group_by_keys: Vec<Expr>,
    aggregates:    Vec<AggExpr>,
    out_schema:    Arc<Schema>,
    result:        Option<RecordBatch>,
}

impl AggregateOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, keys: Vec<Expr>, aggregates: Vec<AggExpr>) -> Result<Self> {
        // Build output schema: key columns + aggregate columns
        let mut fields: Vec<Field> = keys.iter().map(|k| {
            let name = expr_name(k);
            Field::new(name, DataType::Int64, true)
        }).collect();
        for agg in &aggregates {
            let dt = if agg.func == "count" { DataType::Int64 } else { DataType::Float64 };
            fields.push(Field::new(&agg.alias, dt, false));
        }
        let out_schema = Arc::new(Schema::new(fields));
        Ok(Self { child: Some(child), group_by_keys: keys, aggregates, out_schema, result: None })
    }

    fn compute(&mut self) -> Result<()> {
        let child = self.child.take().unwrap();
        let mut all_batches = Vec::new();
        let mut op = child;
        while let Some(b) = op.poll_next()? { all_batches.push(b); }

        if all_batches.is_empty() {
            self.result = Some(RecordBatch::new_empty(self.out_schema.clone()));
            return Ok(());
        }

        // For no-group-by aggregation, produce a single row
        if self.group_by_keys.is_empty() {
            let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
            let mut cols: Vec<ArrayRef> = Vec::new();
            for agg in &self.aggregates {
                match agg.func.as_str() {
                    "count" => {
                        cols.push(Arc::new(Int64Array::from(vec![total_rows as i64])));
                    }
                    "sum" | "avg" | "min" | "max" => {
                        let val = compute_scalar_agg(&all_batches, &agg.func, agg.arg.as_ref())?;
                        cols.push(Arc::new(Float64Array::from(vec![val])));
                    }
                    _ => cols.push(Arc::new(Float64Array::from(vec![0.0f64]))),
                }
            }
            self.result = Some(RecordBatch::try_new(self.out_schema.clone(), cols)
                .map_err(QueryError::Arrow)?);
            return Ok(());
        }

        // GROUP BY: naive approach — collect all rows, group by string key
        // In production this would be a proper hash table
        let mut groups: HashMap<String, GroupState> = HashMap::new();
        for batch in &all_batches {
            for row in 0..batch.num_rows() {
                let key = extract_group_key(batch, row, &self.group_by_keys);
                let state = groups.entry(key).or_insert_with(GroupState::default);
                for agg in &self.aggregates {
                    let val = extract_f64(batch, row, agg.arg.as_ref());
                    state.accumulate(&agg.func, val);
                }
                // Track count separately
                groups.entry(extract_group_key(batch, row, &self.group_by_keys))
                    .and_modify(|s| s.count += 1);
            }
        }

        // Emit one output row per group
        let nrows = groups.len();
        let mut key_builders: Vec<StringBuilder> =
            self.group_by_keys.iter().map(|_| StringBuilder::new()).collect();
        let mut agg_arrays: Vec<Vec<f64>> = self.aggregates.iter().map(|_| Vec::new()).collect();

        for (key_str, state) in &groups {
            // Single-key assumption; multi-key would need splitting
            if let Some(b) = key_builders.first_mut() { b.append_value(key_str); }
            for (i, agg) in self.aggregates.iter().enumerate() {
                agg_arrays[i].push(state.finalize(&agg.func));
            }
        }

        let mut cols: Vec<ArrayRef> = Vec::new();
        for mut b in key_builders { cols.push(Arc::new(b.finish())); }
        for vals in agg_arrays { cols.push(Arc::new(Float64Array::from(vals))); }

        self.result = Some(RecordBatch::try_new(self.out_schema.clone(), cols)
            .map_err(QueryError::Arrow)?);
        Ok(())
    }
}

impl PhysicalOperator for AggregateOperator {
    fn schema(&self) -> Arc<Schema> { self.out_schema.clone() }

    fn poll_next(&mut self) -> Result<Option<RecordBatch>> {
        if self.child.is_some() { self.compute()?; }
        Ok(self.result.take())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

#[derive(Default)]
struct GroupState {
    count: usize,
    sum:   f64,
    min:   f64,
    max:   f64,
}

impl GroupState {
    fn accumulate(&mut self, func: &str, val: Option<f64>) {
        match (func, val) {
            ("sum" | "avg", Some(v)) => self.sum += v,
            ("min", Some(v)) => { if v < self.min || self.count == 0 { self.min = v; } }
            ("max", Some(v)) => { if v > self.max || self.count == 0 { self.max = v; } }
            _ => {}
        }
    }
    fn finalize(&self, func: &str) -> f64 {
        match func {
            "count" => self.count as f64,
            "sum"   => self.sum,
            "avg"   => if self.count > 0 { self.sum / self.count as f64 } else { 0.0 },
            "min"   => self.min,
            "max"   => self.max,
            _       => 0.0,
        }
    }
}

fn expr_name(e: &Expr) -> String {
    match e {
        Expr::Column { name, .. } => name.clone(),
        _                         => "key".to_string(),
    }
}

fn extract_group_key(batch: &RecordBatch, row: usize, _keys: &[Expr]) -> String {
    // Simplified: use first column value as group key
    batch.column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .map(|a| a.value(row).to_string())
        .unwrap_or_else(|| "?".to_string())
}

fn extract_f64(batch: &RecordBatch, row: usize, col_expr: Option<&Expr>) -> Option<f64> {
    let col_name = match col_expr? {
        Expr::Column { name, .. } => name,
        _ => return None,
    };
    let col_idx = batch.schema().index_of(col_name).ok()?;
    let col     = batch.column(col_idx);
    col.as_any().downcast_ref::<Float64Array>()
        .map(|a| a.value(row))
        .or_else(|| col.as_any().downcast_ref::<Int64Array>()
            .map(|a| a.value(row) as f64))
        .or_else(|| col.as_any().downcast_ref::<UInt64Array>()
            .map(|a| a.value(row) as f64))
}

fn compute_scalar_agg(
    batches: &[RecordBatch],
    func:    &str,
    arg:     Option<&Expr>,
) -> Result<f64> {
    let col_name = match arg {
        Some(Expr::Column { name, .. }) => name.as_str(),
        _ => return Ok(0.0),
    };

    let mut acc = match func { "min" => f64::INFINITY, "max" => f64::NEG_INFINITY, _ => 0.0 };
    let mut cnt = 0usize;

    for batch in batches {
        let col_idx = batch.schema().index_of(col_name)
            .map_err(|_| QueryError::Execution(format!("column '{col_name}' not found")))?;
        let col = batch.column(col_idx);
        let vals: Vec<f64> = if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
            (0..a.len()).map(|i| a.value(i)).collect()
        } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            (0..a.len()).map(|i| a.value(i) as f64).collect()
        } else { vec![] };

        for v in vals {
            match func {
                "sum" | "avg" => acc += v,
                "min" => { if v < acc { acc = v; } }
                "max" => { if v > acc { acc = v; } }
                _ => {}
            }
            cnt += 1;
        }
    }

    Ok(match func {
        "avg" => if cnt > 0 { acc / cnt as f64 } else { 0.0 },
        _ => acc,
    })
}
