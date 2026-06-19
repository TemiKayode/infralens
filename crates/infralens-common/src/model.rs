//! Internal normalised data model for all three signal types.
//!
//! These types are the single source of truth between the ingest and storage layers.
//! They are deliberately free of protocol-buffer or Arrow concerns; those conversions
//! live in the respective crates.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Signal-type discriminant ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum SignalType {
    Log    = 0,
    Metric = 1,
    Span   = 2,
}

impl TryFrom<u8> for SignalType {
    type Error = crate::CommonError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(SignalType::Log),
            1 => Ok(SignalType::Metric),
            2 => Ok(SignalType::Span),
            other => Err(crate::CommonError::InvalidSignalType(other)),
        }
    }
}

// ── Polymorphic attribute value ───────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnyValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Array(Vec<AnyValue>),
    Map(HashMap<String, AnyValue>),
}

impl From<String> for AnyValue {
    fn from(s: String) -> Self { AnyValue::String(s) }
}
impl From<&str> for AnyValue {
    fn from(s: &str) -> Self { AnyValue::String(s.to_owned()) }
}
impl From<i64> for AnyValue {
    fn from(i: i64) -> Self { AnyValue::Int(i) }
}
impl From<f64> for AnyValue {
    fn from(f: f64) -> Self { AnyValue::Double(f) }
}
impl From<bool> for AnyValue {
    fn from(b: bool) -> Self { AnyValue::Bool(b) }
}

// ── Instrumentation scope ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstrumentationScope {
    pub name:       String,
    pub version:    String,
    pub attributes: HashMap<String, AnyValue>,
}

// ── Log record ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    pub timestamp_ns:          u64,
    pub observed_timestamp_ns: u64,
    pub trace_id:              Option<[u8; 16]>,
    pub span_id:               Option<[u8; 8]>,
    pub severity_number:       i32,
    pub severity_text:         String,
    pub body:                  Option<AnyValue>,
    pub attributes:            HashMap<String, AnyValue>,
    pub resource_attributes:   HashMap<String, AnyValue>,
    pub scope:                 InstrumentationScope,
    pub schema_url:            String,
}

// ── Metric record ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricType {
    Gauge,
    Sum { is_monotonic: bool, temporality: i32 },
    Histogram { temporality: i32 },
    ExponentialHistogram { temporality: i32 },
    Summary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramData {
    pub count:           u64,
    pub sum:             Option<f64>,
    pub min:             Option<f64>,
    pub max:             Option<f64>,
    pub explicit_bounds: Vec<f64>,
    pub bucket_counts:   Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    pub timestamp_ns:          u64,
    pub start_timestamp_ns:    u64,
    pub name:                  String,
    pub description:           String,
    pub unit:                  String,
    pub metric_type:           MetricType,
    pub value_double:          Option<f64>,
    pub value_int:             Option<i64>,
    pub histogram:             Option<HistogramData>,
    pub attributes:            HashMap<String, AnyValue>,
    pub resource_attributes:   HashMap<String, AnyValue>,
    pub scope:                 InstrumentationScope,
    pub schema_url:            String,
}

// ── Span record ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub timestamp_ns: u64,
    pub name:         String,
    pub attributes:   HashMap<String, AnyValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLink {
    pub trace_id:    [u8; 16],
    pub span_id:     [u8; 8],
    pub trace_state: String,
    pub attributes:  HashMap<String, AnyValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanRecord {
    pub trace_id:            [u8; 16],
    pub span_id:             [u8; 8],
    pub parent_span_id:      Option<[u8; 8]>,
    pub name:                String,
    pub kind:                i32,
    pub start_time_ns:       u64,
    pub end_time_ns:         u64,
    pub duration_ns:         u64,
    pub status_code:         i32,
    pub status_message:      String,
    pub attributes:          HashMap<String, AnyValue>,
    pub resource_attributes: HashMap<String, AnyValue>,
    pub scope:               InstrumentationScope,
    pub events:              Vec<SpanEvent>,
    pub links:               Vec<SpanLink>,
    pub schema_url:          String,
}

// ── Batch envelope ────────────────────────────────────────────────────────────

/// The unit of work passed through the ingest channel to the storage writer.
#[derive(Debug)]
pub enum IngestBatch {
    Logs(Vec<LogRecord>),
    Metrics(Vec<MetricPoint>),
    Spans(Vec<SpanRecord>),
}

impl IngestBatch {
    pub fn signal_type(&self) -> SignalType {
        match self {
            IngestBatch::Logs(_)    => SignalType::Log,
            IngestBatch::Metrics(_) => SignalType::Metric,
            IngestBatch::Spans(_)   => SignalType::Span,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            IngestBatch::Logs(v)    => v.len(),
            IngestBatch::Metrics(v) => v.len(),
            IngestBatch::Spans(v)   => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
}
