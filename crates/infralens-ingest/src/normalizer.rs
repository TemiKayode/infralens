//! Converts raw OTLP protobuf types into the internal data model.
//!
//! All field-level transformations live here so that the gRPC/HTTP handlers
//! are thin and the conversion logic is unit-testable.

use infralens_common::model::{
    AnyValue, HistogramData, InstrumentationScope, LogRecord, MetricPoint, MetricType, SpanEvent,
    SpanLink, SpanRecord,
};
use infralens_proto::{
    common::v1::{any_value, AnyValue as ProtoAnyValue, InstrumentationScope as ProtoScope},
    logs::v1::{LogRecord as ProtoLog, ResourceLogs},
    metrics::v1::{metric::Data, Metric as ProtoMetric, ResourceMetrics},
    trace::v1::{ResourceSpans, Span as ProtoSpan},
};
use std::collections::HashMap;

// ── AnyValue conversion ───────────────────────────────────────────────────────

pub fn convert_any_value(proto: &ProtoAnyValue) -> AnyValue {
    match &proto.value {
        Some(any_value::Value::StringValue(s))  => AnyValue::String(s.clone()),
        Some(any_value::Value::BoolValue(b))    => AnyValue::Bool(*b),
        Some(any_value::Value::IntValue(i))     => AnyValue::Int(*i),
        Some(any_value::Value::DoubleValue(d))  => AnyValue::Double(*d),
        Some(any_value::Value::BytesValue(b))   => AnyValue::Bytes(b.clone()),
        Some(any_value::Value::ArrayValue(a))   => {
            AnyValue::Array(a.values.iter().map(convert_any_value).collect())
        }
        Some(any_value::Value::KvlistValue(kv)) => {
            AnyValue::Map(
                kv.values.iter().map(|kv| {
                    let v = kv.value.as_ref().map(convert_any_value).unwrap_or(AnyValue::String(String::new()));
                    (kv.key.clone(), v)
                }).collect()
            )
        }
        None => AnyValue::String(String::new()),
    }
}

fn convert_attrs(attrs: &[infralens_proto::common::v1::KeyValue]) -> HashMap<String, AnyValue> {
    attrs.iter().filter_map(|kv| {
        kv.value.as_ref().map(|v| (kv.key.clone(), convert_any_value(v)))
    }).collect()
}

fn convert_scope(scope: &Option<ProtoScope>) -> InstrumentationScope {
    scope.as_ref().map(|s| InstrumentationScope {
        name:       s.name.clone(),
        version:    s.version.clone(),
        attributes: convert_attrs(&s.attributes),
    }).unwrap_or_default()
}

fn bytes_to_array16(b: &[u8]) -> Option<[u8; 16]> {
    if b.len() == 16 { let mut a = [0u8; 16]; a.copy_from_slice(b); Some(a) } else { None }
}
fn bytes_to_array8(b: &[u8]) -> Option<[u8; 8]> {
    if b.len() == 8  { let mut a = [0u8; 8];  a.copy_from_slice(b); Some(a) } else { None }
}

// ── Log normalisation ─────────────────────────────────────────────────────────

pub fn normalise_logs(resource_logs: &[ResourceLogs]) -> Vec<LogRecord> {
    let mut out = Vec::new();
    for rl in resource_logs {
        let resource_attrs = rl.resource.as_ref()
            .map(|r| convert_attrs(&r.attributes))
            .unwrap_or_default();

        for sl in &rl.scope_logs {
            let scope = convert_scope(&sl.scope);
            for proto_rec in &sl.log_records {
                out.push(LogRecord {
                    timestamp_ns:          proto_rec.time_unix_nano,
                    observed_timestamp_ns: proto_rec.observed_time_unix_nano,
                    trace_id:              bytes_to_array16(&proto_rec.trace_id),
                    span_id:               bytes_to_array8(&proto_rec.span_id),
                    severity_number:       proto_rec.severity_number,
                    severity_text:         proto_rec.severity_text.clone(),
                    body:                  proto_rec.body.as_ref().map(convert_any_value),
                    attributes:            convert_attrs(&proto_rec.attributes),
                    resource_attributes:   resource_attrs.clone(),
                    scope:                 scope.clone(),
                    schema_url:            sl.schema_url.clone(),
                });
            }
        }
    }
    out
}

// ── Metric normalisation ──────────────────────────────────────────────────────

pub fn normalise_metrics(resource_metrics: &[ResourceMetrics]) -> Vec<MetricPoint> {
    let mut out = Vec::new();
    for rm in resource_metrics {
        let resource_attrs = rm.resource.as_ref()
            .map(|r| convert_attrs(&r.attributes))
            .unwrap_or_default();

        for sm in &rm.scope_metrics {
            let scope = convert_scope(&sm.scope);
            for metric in &sm.metrics {
                out.extend(normalise_metric(metric, &resource_attrs, &scope, &sm.schema_url));
            }
        }
    }
    out
}

fn normalise_metric(
    m:               &ProtoMetric,
    resource_attrs:  &HashMap<String, AnyValue>,
    scope:           &InstrumentationScope,
    schema_url:      &str,
) -> Vec<MetricPoint> {
    let mut out = Vec::new();

    match &m.data {
        Some(Data::Gauge(g)) => {
            for dp in &g.data_points {
                out.push(MetricPoint {
                    timestamp_ns:        dp.time_unix_nano,
                    start_timestamp_ns:  dp.start_time_unix_nano,
                    name:                m.name.clone(),
                    description:         m.description.clone(),
                    unit:                m.unit.clone(),
                    metric_type:         MetricType::Gauge,
                    value_double:        match &dp.value { Some(infralens_proto::metrics::v1::number_data_point::Value::AsDouble(d)) => Some(*d), _ => None },
                    value_int:           match &dp.value { Some(infralens_proto::metrics::v1::number_data_point::Value::AsInt(i)) => Some(*i), _ => None },
                    histogram:           None,
                    attributes:          convert_attrs(&dp.attributes),
                    resource_attributes: resource_attrs.clone(),
                    scope:               scope.clone(),
                    schema_url:          schema_url.to_string(),
                });
            }
        }
        Some(Data::Sum(s)) => {
            for dp in &s.data_points {
                out.push(MetricPoint {
                    timestamp_ns:        dp.time_unix_nano,
                    start_timestamp_ns:  dp.start_time_unix_nano,
                    name:                m.name.clone(),
                    description:         m.description.clone(),
                    unit:                m.unit.clone(),
                    metric_type:         MetricType::Sum {
                        is_monotonic: s.is_monotonic,
                        temporality:  s.aggregation_temporality,
                    },
                    value_double:        match &dp.value { Some(infralens_proto::metrics::v1::number_data_point::Value::AsDouble(d)) => Some(*d), _ => None },
                    value_int:           match &dp.value { Some(infralens_proto::metrics::v1::number_data_point::Value::AsInt(i)) => Some(*i), _ => None },
                    histogram:           None,
                    attributes:          convert_attrs(&dp.attributes),
                    resource_attributes: resource_attrs.clone(),
                    scope:               scope.clone(),
                    schema_url:          schema_url.to_string(),
                });
            }
        }
        Some(Data::Histogram(h)) => {
            for dp in &h.data_points {
                out.push(MetricPoint {
                    timestamp_ns:        dp.time_unix_nano,
                    start_timestamp_ns:  dp.start_time_unix_nano,
                    name:                m.name.clone(),
                    description:         m.description.clone(),
                    unit:                m.unit.clone(),
                    metric_type:         MetricType::Histogram { temporality: h.aggregation_temporality },
                    value_double:        None,
                    value_int:           None,
                    histogram:           Some(HistogramData {
                        count:           dp.count,
                        sum:             dp.sum,
                        min:             dp.min,
                        max:             dp.max,
                        explicit_bounds: dp.explicit_bounds.clone(),
                        bucket_counts:   dp.bucket_counts.clone(),
                    }),
                    attributes:          convert_attrs(&dp.attributes),
                    resource_attributes: resource_attrs.clone(),
                    scope:               scope.clone(),
                    schema_url:          schema_url.to_string(),
                });
            }
        }
        // ExponentialHistogram and Summary: map to generic data points for now.
        _ => {}
    }

    out
}

// ── Trace normalisation ───────────────────────────────────────────────────────

pub fn normalise_spans(resource_spans: &[ResourceSpans]) -> Vec<SpanRecord> {
    let mut out = Vec::new();
    for rs in resource_spans {
        let resource_attrs = rs.resource.as_ref()
            .map(|r| convert_attrs(&r.attributes))
            .unwrap_or_default();

        for ss in &rs.scope_spans {
            let scope = convert_scope(&ss.scope);
            for proto_span in &ss.spans {
                let Some(trace_id) = bytes_to_array16(&proto_span.trace_id) else { continue; };
                let Some(span_id)  = bytes_to_array8(&proto_span.span_id)   else { continue; };

                let duration_ns = proto_span.end_time_unix_nano
                    .saturating_sub(proto_span.start_time_unix_nano);

                let events: Vec<SpanEvent> = proto_span.events.iter().map(|e| SpanEvent {
                    timestamp_ns: e.time_unix_nano,
                    name:         e.name.clone(),
                    attributes:   convert_attrs(&e.attributes),
                }).collect();

                let links: Vec<SpanLink> = proto_span.links.iter().filter_map(|l| {
                    Some(SpanLink {
                        trace_id:    bytes_to_array16(&l.trace_id)?,
                        span_id:     bytes_to_array8(&l.span_id)?,
                        trace_state: l.trace_state.clone(),
                        attributes:  convert_attrs(&l.attributes),
                    })
                }).collect();

                out.push(SpanRecord {
                    trace_id,
                    span_id,
                    parent_span_id:      bytes_to_array8(&proto_span.parent_span_id),
                    name:                proto_span.name.clone(),
                    kind:                proto_span.kind,
                    start_time_ns:       proto_span.start_time_unix_nano,
                    end_time_ns:         proto_span.end_time_unix_nano,
                    duration_ns,
                    status_code:         proto_span.status.as_ref().map(|s| s.code).unwrap_or(0),
                    status_message:      proto_span.status.as_ref().map(|s| s.message.clone()).unwrap_or_default(),
                    attributes:          convert_attrs(&proto_span.attributes),
                    resource_attributes: resource_attrs.clone(),
                    scope:               scope.clone(),
                    events,
                    links,
                    schema_url:          ss.schema_url.clone(),
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use infralens_proto::{
        common::v1::{AnyValue as ProtoAV, KeyValue as ProtoKV, any_value::Value},
        logs::v1::{ResourceLogs, ScopeLogs},
    };

    fn make_proto_log(ts: u64, body: &str) -> infralens_proto::logs::v1::LogRecord {
        infralens_proto::logs::v1::LogRecord {
            time_unix_nano:          ts,
            observed_time_unix_nano: ts,
            severity_number:         9,
            severity_text:           "INFO".to_string(),
            body:                    Some(ProtoAV { value: Some(Value::StringValue(body.to_string())) }),
            attributes:              vec![ProtoKV {
                key:   "env".to_string(),
                value: Some(ProtoAV { value: Some(Value::StringValue("prod".to_string())) }),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn log_normalisation_preserves_fields() {
        let resource_logs = vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![make_proto_log(1_000_000, "test message")],
                schema_url: "https://example.com/schema".to_string(),
            }],
            schema_url: String::new(),
        }];

        let records = normalise_logs(&resource_logs);
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.timestamp_ns, 1_000_000);
        assert_eq!(rec.severity_number, 9);
        assert!(matches!(&rec.body, Some(AnyValue::String(s)) if s == "test message"));
        assert!(rec.attributes.contains_key("env"));
    }

    #[test]
    fn span_normalisation_computes_duration() {
        let resource_spans = vec![ResourceSpans {
            resource: None,
            scope_spans: vec![infralens_proto::trace::v1::ScopeSpans {
                scope: None,
                spans: vec![infralens_proto::trace::v1::Span {
                    trace_id:              vec![1u8; 16],
                    span_id:               vec![2u8; 8],
                    name:                  "my-span".to_string(),
                    start_time_unix_nano:  1_000_000_000,
                    end_time_unix_nano:    1_500_000_000,
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }];

        let spans = normalise_spans(&resource_spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].duration_ns, 500_000_000);
        assert_eq!(spans[0].name, "my-span");
    }
}
