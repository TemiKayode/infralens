//! Temporal built-in functions executed on Arrow arrays.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Array, UInt64Array},
    datatypes::{DataType, Field},
};

use crate::error::{QueryError, Result};

/// `time_bucket(width_ns, ts_array)` → UInt64Array of bucket boundaries.
pub fn time_bucket(width_ns: i64, ts: &ArrayRef) -> Result<ArrayRef> {
    if width_ns <= 0 {
        return Err(QueryError::Execution("time_bucket width must be > 0".into()));
    }
    let arr = ts.as_any().downcast_ref::<UInt64Array>()
        .ok_or_else(|| QueryError::Execution("time_bucket: expected UInt64 timestamp".into()))?;
    let w = width_ns as u64;
    let buckets: Vec<u64> = (0..arr.len()).map(|i| (arr.value(i) / w) * w).collect();
    Ok(Arc::new(UInt64Array::from(buckets)))
}

/// `rate(values, window_ns)` → Float64Array: events-per-second rate over the window.
pub fn rate(values: &ArrayRef, window_ns: i64) -> Result<ArrayRef> {
    let window_s = window_ns as f64 / 1e9;
    if window_s <= 0.0 {
        return Err(QueryError::Execution("rate: window must be > 0".into()));
    }
    let count = values.len() as f64;
    let r = count / window_s;
    let out: Vec<f64> = vec![r; values.len()];
    Ok(Arc::new(Float64Array::from(out)))
}

/// `delta(values, _window_ns)` → Float64Array: last - first over the input slice.
pub fn delta(values: &ArrayRef, _window_ns: i64) -> Result<ArrayRef> {
    let arr = to_f64_slice(values)?;
    if arr.is_empty() { return Ok(Arc::new(Float64Array::from(vec![0.0f64]))); }
    let d = arr[arr.len() - 1] - arr[0];
    Ok(Arc::new(Float64Array::from(vec![d; values.len()])))
}

/// `histogram_quantile(q, values, _buckets)` → approximate quantile using sort.
pub fn histogram_quantile(q: f64, values: &ArrayRef, _buckets: u32) -> Result<ArrayRef> {
    if !(0.0..=1.0).contains(&q) {
        return Err(QueryError::Execution("histogram_quantile: q must be in [0, 1]".into()));
    }
    let mut vals = to_f64_slice(values)?;
    if vals.is_empty() { return Ok(Arc::new(Float64Array::from(vec![0.0f64]))); }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((q * (vals.len() as f64 - 1.0)).round() as usize).min(vals.len() - 1);
    let qval = vals[idx];
    Ok(Arc::new(Float64Array::from(vec![qval; values.len()])))
}

fn to_f64_slice(arr: &ArrayRef) -> Result<Vec<f64>> {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Ok((0..a.len()).map(|i| a.value(i)).collect());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Ok((0..a.len()).map(|i| a.value(i) as f64).collect());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Ok((0..a.len()).map(|i| a.value(i) as f64).collect());
    }
    Err(QueryError::Execution("temporal function: unsupported numeric type".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_bucket() {
        let ts: ArrayRef = Arc::new(UInt64Array::from(vec![
            0u64, 60_000_000_000, 90_000_000_000, 120_000_000_000,
        ]));
        let buckets = time_bucket(60_000_000_000, &ts).unwrap();
        let b = buckets.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(b.value(0), 0);
        assert_eq!(b.value(1), 60_000_000_000);
        assert_eq!(b.value(2), 60_000_000_000);
        assert_eq!(b.value(3), 120_000_000_000);
    }

    #[test]
    fn test_histogram_quantile() {
        let vals: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        let p50 = histogram_quantile(0.5, &vals, 50).unwrap();
        let p50_arr = p50.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((p50_arr.value(0) - 3.0).abs() < 1e-9);
    }
}
