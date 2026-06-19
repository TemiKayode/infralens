//! Arrow schemas for each signal type.
//!
//! Phase 1 stores attribute maps as JSON-encoded Utf8 columns.
//! Phase 2 will migrate to Arrow Map<Utf8, DenseUnion> for projection pushdown.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub fn log_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_ns",          DataType::UInt64, false),
        Field::new("observed_timestamp_ns", DataType::UInt64, false),
        Field::new("trace_id",              DataType::Binary, true),
        Field::new("span_id",               DataType::Binary, true),
        Field::new("severity_number",       DataType::Int32,  false),
        Field::new("severity_text",         DataType::Utf8,   true),
        Field::new("body",                  DataType::Utf8,   true),
        Field::new("attributes",            DataType::Utf8,   true),
        Field::new("resource_attributes",   DataType::Utf8,   true),
        Field::new("scope_name",            DataType::Utf8,   true),
        Field::new("scope_version",         DataType::Utf8,   true),
        Field::new("schema_url",            DataType::Utf8,   true),
    ]))
}

pub fn metric_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_ns",          DataType::UInt64,  false),
        Field::new("start_timestamp_ns",    DataType::UInt64,  true),
        Field::new("name",                  DataType::Utf8,    false),
        Field::new("description",           DataType::Utf8,    true),
        Field::new("unit",                  DataType::Utf8,    true),
        Field::new("metric_type",           DataType::Int8,    false),
        Field::new("value_double",          DataType::Float64, true),
        Field::new("value_int",             DataType::Int64,   true),
        // Histogram sub-fields (null for non-histogram metrics)
        Field::new("histogram_count",       DataType::UInt64,  true),
        Field::new("histogram_sum",         DataType::Float64, true),
        Field::new("histogram_bounds",      DataType::Binary,  true),
        Field::new("histogram_counts",      DataType::Binary,  true),
        Field::new("attributes",            DataType::Utf8,    true),
        Field::new("resource_attributes",   DataType::Utf8,    true),
        Field::new("scope_name",            DataType::Utf8,    true),
        Field::new("scope_version",         DataType::Utf8,    true),
        Field::new("schema_url",            DataType::Utf8,    true),
    ]))
}

pub fn span_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("trace_id",            DataType::Binary, false),
        Field::new("span_id",             DataType::Binary, false),
        Field::new("parent_span_id",      DataType::Binary, true),
        Field::new("name",                DataType::Utf8,   false),
        Field::new("kind",                DataType::Int32,  false),
        Field::new("start_time_ns",       DataType::UInt64, false),
        Field::new("end_time_ns",         DataType::UInt64, false),
        Field::new("duration_ns",         DataType::UInt64, false),
        Field::new("status_code",         DataType::Int32,  false),
        Field::new("status_message",      DataType::Utf8,   true),
        Field::new("attributes",          DataType::Utf8,   true),
        Field::new("resource_attributes", DataType::Utf8,   true),
        Field::new("scope_name",          DataType::Utf8,   true),
        Field::new("scope_version",       DataType::Utf8,   true),
        Field::new("events",              DataType::Utf8,   true),
        Field::new("links",               DataType::Utf8,   true),
        Field::new("schema_url",          DataType::Utf8,   true),
    ]))
}
