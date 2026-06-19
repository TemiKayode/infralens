//! OTLP/HTTP ingest handlers for axum.
//!
//! Endpoints:
//!   POST /v1/logs    — accepts application/x-protobuf or application/json
//!   POST /v1/metrics — same
//!   POST /v1/traces  — same

use crate::{json_decoder, normalizer, processor::IngestPipeline};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use infralens_common::model::IngestBatch;
use infralens_proto::{
    collector::{
        logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
        metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
        trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
    },
};
use prost::Message;
use tracing::{debug, warn};

const CONTENT_TYPE_PROTO: &str = "application/x-protobuf";
const CONTENT_TYPE_JSON:  &str = "application/json";

fn content_type(headers: &HeaderMap) -> &str {
    headers.get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn router(pipeline: IngestPipeline) -> Router {
    Router::new()
        .route("/v1/logs",    post(handle_logs))
        .route("/v1/metrics", post(handle_metrics))
        .route("/v1/traces",  post(handle_traces))
        .with_state(pipeline)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn handle_logs(
    State(pipeline): State<IngestPipeline>,
    headers:         HeaderMap,
    body:            Bytes,
) -> impl IntoResponse {
    let ct = content_type(&headers);
    let records = if ct.starts_with(CONTENT_TYPE_JSON) {
        match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(v)  => json_decoder::decode_logs(&v),
            Err(e) => {
                warn!(error = %e, "Failed to decode OTLP/JSON logs");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        }
    } else if ct.starts_with(CONTENT_TYPE_PROTO) {
        match ExportLogsServiceRequest::decode(body) {
            Ok(r)  => normalizer::normalise_logs(&r.resource_logs),
            Err(e) => {
                warn!(error = %e, "Failed to decode OTLP/protobuf logs");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        }
    } else {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json or application/x-protobuf").into_response();
    };

    debug!(count = records.len(), "OTLP/HTTP logs export");
    match pipeline.submit(IngestBatch::Logs(records)).await {
        Ok(_) => {
            if ct.starts_with(CONTENT_TYPE_JSON) {
                (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)],
                 r#"{"partialSuccess":{}}"#).into_response()
            } else {
                let mut buf = Vec::new();
                ExportLogsServiceResponse::default().encode(&mut buf).unwrap();
                (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_PROTO)], buf).into_response()
            }
        }
        Err(e) if e.is_retriable() => (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn handle_metrics(
    State(pipeline): State<IngestPipeline>,
    headers:         HeaderMap,
    body:            Bytes,
) -> impl IntoResponse {
    let ct = content_type(&headers);
    let records = if ct.starts_with(CONTENT_TYPE_JSON) {
        match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(v)  => json_decoder::decode_metrics(&v),
            Err(e) => {
                warn!(error = %e, "Failed to decode OTLP/JSON metrics");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        }
    } else if ct.starts_with(CONTENT_TYPE_PROTO) {
        match ExportMetricsServiceRequest::decode(body) {
            Ok(r)  => normalizer::normalise_metrics(&r.resource_metrics),
            Err(e) => {
                warn!(error = %e, "Failed to decode OTLP/protobuf metrics");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        }
    } else {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json or application/x-protobuf").into_response();
    };

    debug!(count = records.len(), "OTLP/HTTP metrics export");
    match pipeline.submit(IngestBatch::Metrics(records)).await {
        Ok(_) => {
            if ct.starts_with(CONTENT_TYPE_JSON) {
                (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)],
                 r#"{"partialSuccess":{}}"#).into_response()
            } else {
                let mut buf = Vec::new();
                ExportMetricsServiceResponse::default().encode(&mut buf).unwrap();
                (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_PROTO)], buf).into_response()
            }
        }
        Err(e) if e.is_retriable() => (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn handle_traces(
    State(pipeline): State<IngestPipeline>,
    headers:         HeaderMap,
    body:            Bytes,
) -> impl IntoResponse {
    let ct = content_type(&headers);
    let spans = if ct.starts_with(CONTENT_TYPE_JSON) {
        match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(v)  => json_decoder::decode_spans(&v),
            Err(e) => {
                warn!(error = %e, "Failed to decode OTLP/JSON traces");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        }
    } else if ct.starts_with(CONTENT_TYPE_PROTO) {
        match ExportTraceServiceRequest::decode(body) {
            Ok(r)  => normalizer::normalise_spans(&r.resource_spans),
            Err(e) => {
                warn!(error = %e, "Failed to decode OTLP/protobuf traces");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        }
    } else {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json or application/x-protobuf").into_response();
    };

    debug!(count = spans.len(), "OTLP/HTTP trace export");
    match pipeline.submit(IngestBatch::Spans(spans)).await {
        Ok(_) => {
            if ct.starts_with(CONTENT_TYPE_JSON) {
                (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)],
                 r#"{"partialSuccess":{}}"#).into_response()
            } else {
                let mut buf = Vec::new();
                ExportTraceServiceResponse::default().encode(&mut buf).unwrap();
                (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_PROTO)], buf).into_response()
            }
        }
        Err(e) if e.is_retriable() => (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
