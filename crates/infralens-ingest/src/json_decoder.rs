//! OTLP/JSON decoder — maps the OTLP JSON wire format directly to the internal model.
//!
//! The OTLP JSON encoding (https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding)
//! uses camelCase field names and represents numeric timestamps as decimal strings.
//! Trace IDs (16 bytes) and span IDs (8 bytes) are hex-encoded strings.

use infralens_common::model::{
    AnyValue, InstrumentationScope, LogRecord, MetricPoint, MetricType,
    SpanEvent, SpanLink, SpanRecord,
};
use serde_json::Value;
use std::collections::HashMap;

// ── AnyValue ──────────────────────────────────────────────────────────────────

fn decode_any_value(v: &Value) -> AnyValue {
    if let Some(s) = v.get("stringValue").and_then(Value::as_str) {
        return AnyValue::String(s.to_string());
    }
    if let Some(b) = v.get("boolValue").and_then(Value::as_bool) {
        return AnyValue::Bool(b);
    }
    if let Some(i) = v.get("intValue") {
        let n = i.as_i64()
            .or_else(|| i.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0);
        return AnyValue::Int(n);
    }
    if let Some(d) = v.get("doubleValue").and_then(Value::as_f64) {
        return AnyValue::Double(d);
    }
    if let Some(arr) = v.get("arrayValue")
        .and_then(|av| av.get("values"))
        .and_then(Value::as_array)
    {
        return AnyValue::Array(arr.iter().map(decode_any_value).collect());
    }
    AnyValue::String(String::new())
}

fn decode_attrs(attrs: &Value) -> HashMap<String, AnyValue> {
    attrs.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|kv| {
                    let key = kv.get("key")?.as_str()?.to_string();
                    let val = kv.get("value")
                        .map(decode_any_value)
                        .unwrap_or(AnyValue::String(String::new()));
                    Some((key, val))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn decode_scope(scope: Option<&Value>) -> InstrumentationScope {
    scope.map(|s| InstrumentationScope {
        name:       s.get("name").and_then(Value::as_str).unwrap_or("").to_string(),
        version:    s.get("version").and_then(Value::as_str).unwrap_or("").to_string(),
        attributes: s.get("attributes").map(|a| decode_attrs(a)).unwrap_or_default(),
    }).unwrap_or_default()
}

fn decode_u64(v: &Value) -> u64 {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0)
}

fn hex_to_16(s: &str) -> Option<[u8; 16]> {
    let s = s.trim();
    if s.len() != 32 { return None; }
    let bytes: Option<Vec<u8>> = (0..16)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect();
    let bytes = bytes?;
    let mut a = [0u8; 16];
    a.copy_from_slice(&bytes);
    Some(a)
}

fn hex_to_8(s: &str) -> Option<[u8; 8]> {
    let s = s.trim();
    if s.len() != 16 { return None; }
    let bytes: Option<Vec<u8>> = (0..8)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect();
    let bytes = bytes?;
    let mut a = [0u8; 8];
    a.copy_from_slice(&bytes);
    Some(a)
}

// ── Logs ──────────────────────────────────────────────────────────────────────

pub fn decode_logs(json: &Value) -> Vec<LogRecord> {
    let mut out = Vec::new();
    let Some(resource_logs) = json.get("resourceLogs").and_then(Value::as_array) else {
        return out;
    };
    for rl in resource_logs {
        let resource_attrs = rl.get("resource")
            .and_then(|r| r.get("attributes"))
            .map(|a| decode_attrs(a))
            .unwrap_or_default();

        let scope_logs = rl.get("scopeLogs").and_then(Value::as_array);
        for sl in scope_logs.into_iter().flatten() {
            let scope     = decode_scope(sl.get("scope"));
            let schema    = sl.get("schemaUrl").and_then(Value::as_str).unwrap_or("").to_string();
            let log_recs  = sl.get("logRecords").and_then(Value::as_array);
            for rec in log_recs.into_iter().flatten() {
                let ts = rec.get("timeUnixNano").map(decode_u64).unwrap_or(0);
                out.push(LogRecord {
                    timestamp_ns:          ts,
                    observed_timestamp_ns: rec.get("observedTimeUnixNano").map(decode_u64).unwrap_or(ts),
                    trace_id:              rec.get("traceId").and_then(Value::as_str).and_then(hex_to_16),
                    span_id:               rec.get("spanId").and_then(Value::as_str).and_then(hex_to_8),
                    severity_number:       rec.get("severityNumber").and_then(Value::as_i64).unwrap_or(0) as i32,
                    severity_text:         rec.get("severityText").and_then(Value::as_str).unwrap_or("").to_string(),
                    body:                  rec.get("body").map(decode_any_value),
                    attributes:            rec.get("attributes").map(|a| decode_attrs(a)).unwrap_or_default(),
                    resource_attributes:   resource_attrs.clone(),
                    scope:                 scope.clone(),
                    schema_url:            schema.clone(),
                });
            }
        }
    }
    out
}

// ── Metrics ───────────────────────────────────────────────────────────────────

pub fn decode_metrics(json: &Value) -> Vec<MetricPoint> {
    let mut out = Vec::new();
    let Some(resource_metrics) = json.get("resourceMetrics").and_then(Value::as_array) else {
        return out;
    };
    for rm in resource_metrics {
        let resource_attrs = rm.get("resource")
            .and_then(|r| r.get("attributes"))
            .map(|a| decode_attrs(a))
            .unwrap_or_default();

        let scope_metrics = rm.get("scopeMetrics").and_then(Value::as_array);
        for sm in scope_metrics.into_iter().flatten() {
            let scope  = decode_scope(sm.get("scope"));
            let schema = sm.get("schemaUrl").and_then(Value::as_str).unwrap_or("").to_string();
            let metrics = sm.get("metrics").and_then(Value::as_array);
            for m in metrics.into_iter().flatten() {
                let name        = m.get("name").and_then(Value::as_str).unwrap_or("").to_string();
                let description = m.get("description").and_then(Value::as_str).unwrap_or("").to_string();
                let unit        = m.get("unit").and_then(Value::as_str).unwrap_or("").to_string();

                if let Some(gauge) = m.get("gauge") {
                    for dp in gauge.get("dataPoints").and_then(Value::as_array).into_iter().flatten() {
                        out.push(MetricPoint {
                            timestamp_ns:        dp.get("timeUnixNano").map(decode_u64).unwrap_or(0),
                            start_timestamp_ns:  dp.get("startTimeUnixNano").map(decode_u64).unwrap_or(0),
                            name:                name.clone(),
                            description:         description.clone(),
                            unit:                unit.clone(),
                            metric_type:         MetricType::Gauge,
                            value_double:        dp.get("asDouble").and_then(Value::as_f64),
                            value_int:           dp.get("asInt").and_then(Value::as_i64),
                            histogram:           None,
                            attributes:          dp.get("attributes").map(|a| decode_attrs(a)).unwrap_or_default(),
                            resource_attributes: resource_attrs.clone(),
                            scope:               scope.clone(),
                            schema_url:          schema.clone(),
                        });
                    }
                }

                if let Some(sum) = m.get("sum") {
                    let is_monotonic = sum.get("isMonotonic").and_then(Value::as_bool).unwrap_or(false);
                    let temporality  = sum.get("aggregationTemporality").and_then(Value::as_i64).unwrap_or(0) as i32;
                    for dp in sum.get("dataPoints").and_then(Value::as_array).into_iter().flatten() {
                        out.push(MetricPoint {
                            timestamp_ns:        dp.get("timeUnixNano").map(decode_u64).unwrap_or(0),
                            start_timestamp_ns:  dp.get("startTimeUnixNano").map(decode_u64).unwrap_or(0),
                            name:                name.clone(),
                            description:         description.clone(),
                            unit:                unit.clone(),
                            metric_type:         MetricType::Sum { is_monotonic, temporality },
                            value_double:        dp.get("asDouble").and_then(Value::as_f64),
                            value_int:           dp.get("asInt").and_then(Value::as_i64),
                            histogram:           None,
                            attributes:          dp.get("attributes").map(|a| decode_attrs(a)).unwrap_or_default(),
                            resource_attributes: resource_attrs.clone(),
                            scope:               scope.clone(),
                            schema_url:          schema.clone(),
                        });
                    }
                }
            }
        }
    }
    out
}

// ── Spans ─────────────────────────────────────────────────────────────────────

pub fn decode_spans(json: &Value) -> Vec<SpanRecord> {
    let mut out = Vec::new();
    let Some(resource_spans) = json.get("resourceSpans").and_then(Value::as_array) else {
        return out;
    };
    for rs in resource_spans {
        let resource_attrs = rs.get("resource")
            .and_then(|r| r.get("attributes"))
            .map(|a| decode_attrs(a))
            .unwrap_or_default();

        let scope_spans = rs.get("scopeSpans").and_then(Value::as_array);
        for ss in scope_spans.into_iter().flatten() {
            let scope  = decode_scope(ss.get("scope"));
            let schema = ss.get("schemaUrl").and_then(Value::as_str).unwrap_or("").to_string();
            let spans  = ss.get("spans").and_then(Value::as_array);
            for span in spans.into_iter().flatten() {
                let Some(trace_id) = span.get("traceId").and_then(Value::as_str).and_then(hex_to_16) else {
                    continue;
                };
                let Some(span_id) = span.get("spanId").and_then(Value::as_str).and_then(hex_to_8) else {
                    continue;
                };
                let start_ns = span.get("startTimeUnixNano").map(decode_u64).unwrap_or(0);
                let end_ns   = span.get("endTimeUnixNano").map(decode_u64).unwrap_or(0);
                out.push(SpanRecord {
                    trace_id,
                    span_id,
                    parent_span_id: span.get("parentSpanId").and_then(Value::as_str).and_then(hex_to_8),
                    name:           span.get("name").and_then(Value::as_str).unwrap_or("").to_string(),
                    kind:           span.get("kind").and_then(Value::as_i64).unwrap_or(0) as i32,
                    start_time_ns:  start_ns,
                    end_time_ns:    end_ns,
                    duration_ns:    end_ns.saturating_sub(start_ns),
                    status_code:    span.get("status").and_then(|s| s.get("code")).and_then(Value::as_i64).unwrap_or(0) as i32,
                    status_message: span.get("status").and_then(|s| s.get("message")).and_then(Value::as_str).unwrap_or("").to_string(),
                    attributes:          span.get("attributes").map(|a| decode_attrs(a)).unwrap_or_default(),
                    resource_attributes: resource_attrs.clone(),
                    scope:               scope.clone(),
                    events: span.get("events").and_then(Value::as_array)
                        .map(|arr| arr.iter().map(|e| SpanEvent {
                            timestamp_ns: e.get("timeUnixNano").map(decode_u64).unwrap_or(0),
                            name:         e.get("name").and_then(Value::as_str).unwrap_or("").to_string(),
                            attributes:   e.get("attributes").map(|a| decode_attrs(a)).unwrap_or_default(),
                        }).collect())
                        .unwrap_or_default(),
                    links: span.get("links").and_then(Value::as_array)
                        .map(|arr| arr.iter().filter_map(|l| {
                            Some(SpanLink {
                                trace_id:    hex_to_16(l.get("traceId")?.as_str()?)?,
                                span_id:     hex_to_8(l.get("spanId")?.as_str()?)?,
                                trace_state: l.get("traceState").and_then(Value::as_str).unwrap_or("").to_string(),
                                attributes:  l.get("attributes").map(|a| decode_attrs(a)).unwrap_or_default(),
                            })
                        }).collect())
                        .unwrap_or_default(),
                    schema_url: schema.clone(),
                });
            }
        }
    }
    out
}
