//! OTLP/HTTP ingest handlers for axum.
//!
//! Endpoints:
//!   POST /v1/logs    — accepts application/x-protobuf or application/json
//!   POST /v1/metrics — same
//!   POST /v1/traces  — same
//!
//! This implementation accepts only Protobuf bodies (Content-Type: application/x-protobuf).
//! JSON support can be added in Phase 2 using the pbjson crate.

use crate::{normalizer, processor::IngestPipeline};
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
    if !is_proto_content_type(&headers) {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "Use application/x-protobuf").into_response();
    }

    let req = match ExportLogsServiceRequest::decode(body) {
        Ok(r)  => r,
        Err(e) => {
            warn!(error = %e, "Failed to decode OTLP/HTTP logs request");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    let records = normalizer::normalise_logs(&req.resource_logs);
    debug!(count = records.len(), "OTLP/HTTP logs export");

    match pipeline.submit(IngestBatch::Logs(records)).await {
        Ok(_) => {
            let resp = ExportLogsServiceResponse::default();
            let mut buf = Vec::new();
            resp.encode(&mut buf).unwrap();
            (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_PROTO)], buf).into_response()
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
    if !is_proto_content_type(&headers) {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "Use application/x-protobuf").into_response();
    }

    let req = match ExportMetricsServiceRequest::decode(body) {
        Ok(r)  => r,
        Err(e) => {
            warn!(error = %e, "Failed to decode OTLP/HTTP metrics request");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    let records = normalizer::normalise_metrics(&req.resource_metrics);
    debug!(count = records.len(), "OTLP/HTTP metrics export");

    match pipeline.submit(IngestBatch::Metrics(records)).await {
        Ok(_) => {
            let resp = ExportMetricsServiceResponse::default();
            let mut buf = Vec::new();
            resp.encode(&mut buf).unwrap();
            (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_PROTO)], buf).into_response()
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
    if !is_proto_content_type(&headers) {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "Use application/x-protobuf").into_response();
    }

    let req = match ExportTraceServiceRequest::decode(body) {
        Ok(r)  => r,
        Err(e) => {
            warn!(error = %e, "Failed to decode OTLP/HTTP trace request");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    let spans = normalizer::normalise_spans(&req.resource_spans);
    debug!(count = spans.len(), "OTLP/HTTP trace export");

    match pipeline.submit(IngestBatch::Spans(spans)).await {
        Ok(_) => {
            let resp = ExportTraceServiceResponse::default();
            let mut buf = Vec::new();
            resp.encode(&mut buf).unwrap();
            (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE_PROTO)], buf).into_response()
        }
        Err(e) if e.is_retriable() => (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn is_proto_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.starts_with(CONTENT_TYPE_PROTO))
        .unwrap_or(false)
}
