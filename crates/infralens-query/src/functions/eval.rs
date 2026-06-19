//! Expression evaluator — maps AST Expr onto Arrow arrays for filtering.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
    },
    compute,
    record_batch::RecordBatch,
};
use arrow_ord::cmp as arrow_cmp;
use arrow_arith::numeric as arrow_num;

use crate::{
    ast::{BinOp, Expr, Literal, UnaryOp},
    error::{QueryError, Result},
    functions::temporal,
};

/// Evaluate a boolean expression to a BooleanArray (for WHERE / HAVING).
pub fn eval_filter(batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
    let arr = eval_expr(batch, expr)?;
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| QueryError::Execution("filter expression must be boolean".into()))
}

/// Evaluate an expression to an ArrayRef.
pub fn eval_expr(batch: &RecordBatch, expr: &Expr) -> Result<ArrayRef> {
    match expr {
        Expr::Literal(lit) => eval_literal(lit, batch.num_rows()),

        Expr::Column { name, table: _ } => {
            let idx = batch.schema().index_of(name)
                .map_err(|_| QueryError::Execution(format!("column '{name}' not found")))?;
            Ok(batch.column(idx).clone())
        }

        Expr::Star => Err(QueryError::Execution("cannot evaluate * as a value".into())),

        Expr::BinOp { op, left, right } => {
            eval_binop(batch, op, left, right)
        }

        Expr::UnaryOp { op: UnaryOp::Neg, operand } => {
            let arr = eval_expr(batch, operand)?;
            negate_numeric(arr)
        }
        Expr::UnaryOp { op: UnaryOp::Not, operand } => {
            let arr = eval_expr(batch, operand)?;
            let b = arr.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Execution("NOT requires boolean".into()))?;
            Ok(Arc::new(compute::not(b).map_err(QueryError::Arrow)?))
        }

        Expr::IsNull(e) => {
            let arr = eval_expr(batch, e)?;
            let nulls = (0..arr.len()).map(|i| Some(arr.is_null(i))).collect::<BooleanArray>();
            Ok(Arc::new(nulls))
        }
        Expr::IsNotNull(e) => {
            let arr = eval_expr(batch, e)?;
            let non_nulls = (0..arr.len()).map(|i| Some(arr.is_valid(i))).collect::<BooleanArray>();
            Ok(Arc::new(non_nulls))
        }

        Expr::Between { expr, low, high } => {
            let val  = eval_expr(batch, expr)?;
            let lo   = eval_expr(batch, low)?;
            let hi   = eval_expr(batch, high)?;
            let ge   = compare_arrays(&val, &lo, BinOp::GtEq)?;
            let le   = compare_arrays(&val, &hi, BinOp::LtEq)?;
            Ok(Arc::new(compute::and(&ge, &le).map_err(QueryError::Arrow)?))
        }

        Expr::In { expr, list } => {
            let val = eval_expr(batch, expr)?;
            let mut mask = BooleanArray::from(vec![false; val.len()]);
            for item in list {
                let cmp_arr = eval_expr(batch, item)?;
                let eq = compare_arrays(&val, &cmp_arr, BinOp::Eq)?;
                mask = compute::or(&mask, &eq).map_err(QueryError::Arrow)?;
            }
            Ok(Arc::new(mask))
        }

        Expr::Function { name, args } => eval_function(batch, name, args),

        // Pre-evaluated temporal nodes (after binder pass)
        Expr::TimeBucket { width_ns, ts_expr } => {
            let ts = eval_expr(batch, ts_expr)?;
            temporal::time_bucket(*width_ns, &ts)
        }
        Expr::Rate { value_expr, window_ns } => {
            let vals = eval_expr(batch, value_expr)?;
            temporal::rate(&vals, *window_ns)
        }
        Expr::Delta { value_expr, window_ns } => {
            let vals = eval_expr(batch, value_expr)?;
            temporal::delta(&vals, *window_ns)
        }
        Expr::HistogramQuantile { q, value_expr, buckets } => {
            let vals = eval_expr(batch, value_expr)?;
            temporal::histogram_quantile(*q, &vals, *buckets)
        }
        Expr::Now => {
            let ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64;
            eval_literal(&Literal::Int(ns), batch.num_rows())
        }
        Expr::Interval(ns) => eval_literal(&Literal::Int(*ns), batch.num_rows()),

        Expr::Like { expr, pattern, negated } => {
            let arr  = eval_expr(batch, expr)?;
            let strs = arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Execution("LIKE requires text column".into()))?;
            let pat  = glob_to_regex(pattern);
            let mask: BooleanArray = (0..strs.len())
                .map(|i| {
                    let v = strs.value(i);
                    let m = simple_like_match(v, &pat);
                    Some(if *negated { !m } else { m })
                })
                .collect();
            Ok(Arc::new(mask))
        }
    }
}

fn eval_literal(lit: &Literal, nrows: usize) -> Result<ArrayRef> {
    Ok(match lit {
        Literal::Int(n)        => Arc::new(Int64Array::from(vec![*n; nrows])),
        Literal::Float(f)      => Arc::new(Float64Array::from(vec![*f; nrows])),
        Literal::Str(s)        => Arc::new(StringArray::from(vec![s.as_str(); nrows])),
        Literal::Bool(b)       => Arc::new(BooleanArray::from(vec![*b; nrows])),
        Literal::Null          => Arc::new(BooleanArray::from(vec![Option::<bool>::None; nrows])),
        Literal::IntervalNs(n) => Arc::new(Int64Array::from(vec![*n; nrows])),
    })
}

fn eval_binop(batch: &RecordBatch, op: &BinOp, left: &Expr, right: &Expr) -> Result<ArrayRef> {
    match op {
        BinOp::And => {
            let l = eval_expr(batch, left)?;
            let r = eval_expr(batch, right)?;
            let lb = l.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Execution("AND: left not boolean".into()))?;
            let rb = r.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Execution("AND: right not boolean".into()))?;
            Ok(Arc::new(compute::and(lb, rb).map_err(QueryError::Arrow)?))
        }
        BinOp::Or => {
            let l = eval_expr(batch, left)?;
            let r = eval_expr(batch, right)?;
            let lb = l.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Execution("OR: left not boolean".into()))?;
            let rb = r.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Execution("OR: right not boolean".into()))?;
            Ok(Arc::new(compute::or(lb, rb).map_err(QueryError::Arrow)?))
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let l = eval_expr(batch, left)?;
            let r = eval_expr(batch, right)?;
            arithmetic_arrays(&l, &r, op)
        }
        cmp => {
            let l = eval_expr(batch, left)?;
            let r = eval_expr(batch, right)?;
            Ok(Arc::new(compare_arrays(&l, &r, *cmp)?))
        }
    }
}

fn compare_arrays(l: &ArrayRef, r: &ArrayRef, op: BinOp) -> Result<BooleanArray> {
    macro_rules! cmp {
        ($fn:ident) => {{
            if let (Some(la), Some(ra)) = (l.as_any().downcast_ref::<UInt64Array>(),
                                           r.as_any().downcast_ref::<UInt64Array>()) {
                return arrow_cmp::$fn(la, ra).map_err(QueryError::Arrow);
            }
            if let (Some(la), Some(ra)) = (l.as_any().downcast_ref::<Int64Array>(),
                                           r.as_any().downcast_ref::<Int64Array>()) {
                return arrow_cmp::$fn(la, ra).map_err(QueryError::Arrow);
            }
            if let (Some(la), Some(ra)) = (l.as_any().downcast_ref::<Float64Array>(),
                                           r.as_any().downcast_ref::<Float64Array>()) {
                return arrow_cmp::$fn(la, ra).map_err(QueryError::Arrow);
            }
            if let (Some(la), Some(ra)) = (l.as_any().downcast_ref::<StringArray>(),
                                           r.as_any().downcast_ref::<StringArray>()) {
                return arrow_cmp::$fn(la, ra).map_err(QueryError::Arrow);
            }
        }};
    }
    match op {
        BinOp::Eq    => { cmp!(eq);  }
        BinOp::NotEq => { cmp!(neq); }
        BinOp::Lt    => { cmp!(lt);  }
        BinOp::LtEq  => { cmp!(lt_eq);  }
        BinOp::Gt    => { cmp!(gt);  }
        BinOp::GtEq  => { cmp!(gt_eq); }
        _ => {}
    }
    Err(QueryError::Execution(format!("unsupported comparison between {:?} and {:?}", l.data_type(), r.data_type())))
}

fn arithmetic_arrays(l: &ArrayRef, r: &ArrayRef, op: &BinOp) -> Result<ArrayRef> {
    macro_rules! arith {
        ($fn:ident, $ty:ty) => {
            if let (Some(la), Some(ra)) = (l.as_any().downcast_ref::<$ty>(),
                                           r.as_any().downcast_ref::<$ty>()) {
                return arrow_num::$fn(la, ra).map_err(QueryError::Arrow);
            }
        };
    }
    match op {
        BinOp::Add => { arith!(add, Int64Array); arith!(add, Float64Array); }
        BinOp::Sub => { arith!(sub, Int64Array); arith!(sub, Float64Array); }
        BinOp::Mul => { arith!(mul, Int64Array); arith!(mul, Float64Array); }
        BinOp::Div => { arith!(div, Int64Array); arith!(div, Float64Array); }
        _ => {}
    }
    Err(QueryError::Execution("unsupported arithmetic types".into()))
}

fn negate_numeric(arr: ArrayRef) -> Result<ArrayRef> {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        let neg: Int64Array = (0..a.len()).map(|i| -a.value(i)).collect();
        return Ok(Arc::new(neg));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        let neg: Float64Array = (0..a.len()).map(|i| -a.value(i)).collect();
        return Ok(Arc::new(neg));
    }
    Err(QueryError::Execution("unary minus on non-numeric type".into()))
}

fn eval_function(batch: &RecordBatch, name: &str, args: &[Expr]) -> Result<ArrayRef> {
    match name {
        "now" => {
            let ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;
            Ok(Arc::new(Int64Array::from(vec![ns; batch.num_rows()])))
        }
        "time_bucket" => {
            if args.len() < 2 {
                return Err(QueryError::Execution("time_bucket(width, ts)".into()));
            }
            let width_arr = eval_expr(batch, &args[0])?;
            let width_ns  = scalar_i64(&width_arr)?;
            let ts        = eval_expr(batch, &args[1])?;
            temporal::time_bucket(width_ns, &ts)
        }
        "rate" => {
            if args.len() < 2 {
                return Err(QueryError::Execution("rate(value, window)".into()));
            }
            let vals      = eval_expr(batch, &args[0])?;
            let window    = eval_expr(batch, &args[1])?;
            let window_ns = scalar_i64(&window)?;
            temporal::rate(&vals, window_ns)
        }
        "delta" => {
            if args.len() < 2 {
                return Err(QueryError::Execution("delta(value, window)".into()));
            }
            let vals      = eval_expr(batch, &args[0])?;
            let window    = eval_expr(batch, &args[1])?;
            let window_ns = scalar_i64(&window)?;
            temporal::delta(&vals, window_ns)
        }
        "histogram_quantile" => {
            if args.len() < 2 {
                return Err(QueryError::Execution("histogram_quantile(q, col[, buckets])".into()));
            }
            let q_arr  = eval_expr(batch, &args[0])?;
            let q      = scalar_f64(&q_arr)?;
            let vals   = eval_expr(batch, &args[1])?;
            let buckets = if args.len() >= 3 {
                scalar_i64(&eval_expr(batch, &args[2])?)? as u32
            } else { 50 };
            temporal::histogram_quantile(q, &vals, buckets)
        }
        other => Err(QueryError::Execution(format!("unknown function '{other}'"))),
    }
}

fn scalar_i64(arr: &ArrayRef) -> Result<i64> {
    arr.as_any().downcast_ref::<Int64Array>()
        .map(|a| a.value(0))
        .or_else(|| arr.as_any().downcast_ref::<UInt64Array>().map(|a| a.value(0) as i64))
        .ok_or_else(|| QueryError::Execution("expected integer scalar".into()))
}

fn scalar_f64(arr: &ArrayRef) -> Result<f64> {
    arr.as_any().downcast_ref::<Float64Array>()
        .map(|a| a.value(0))
        .or_else(|| arr.as_any().downcast_ref::<Int64Array>().map(|a| a.value(0) as f64))
        .ok_or_else(|| QueryError::Execution("expected float scalar".into()))
}

fn glob_to_regex(pat: &str) -> String {
    pat.replace('%', ".*").replace('_', ".")
}

fn simple_like_match(s: &str, pattern: &str) -> bool {
    // Minimal LIKE: only supports % wildcard; proper impl would use a regex
    if pattern.contains(".*") {
        let parts: Vec<&str> = pattern.split(".*").collect();
        let mut remaining = s;
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() { continue; }
            match remaining.find(part) {
                Some(pos) => {
                    if i == 0 && !pattern.starts_with(".*") && pos != 0 { return false; }
                    remaining = &remaining[pos + part.len()..];
                }
                None => return false,
            }
        }
        if !pattern.ends_with(".*") && !remaining.is_empty() { return false; }
        true
    } else {
        s == pattern
    }
}
