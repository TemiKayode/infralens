//! tonic gRPC service implementations for all three OTLP collector services.

use crate::{normalizer, processor::IngestPipeline};
use infralens_common::model::IngestBatch;
use infralens_proto::collector::{
    logs::v1::{
        logs_service_server::LogsService, ExportLogsPartialSuccess,
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    metrics::v1::{
        metrics_service_server::MetricsService, ExportMetricsPartialSuccess,
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    trace::v1::{
        trace_service_server::TraceService, ExportTracePartialSuccess,
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
};
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

// ── Logs service ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct OtlpLogsService {
    pipeline: IngestPipeline,
}

impl OtlpLogsService {
    pub fn new(pipeline: IngestPipeline) -> Self { Self { pipeline } }
}

#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();
        let records = normalizer::normalise_logs(&req.resource_logs);
        let count   = records.len() as i64;
        debug!(count, "OTLP/gRPC logs export");

        match self.pipeline.submit(IngestBatch::Logs(records)).await {
            Ok(_) => Ok(Response::new(ExportLogsServiceResponse {
                partial_success: Some(ExportLogsPartialSuccess {
                    rejected_log_records: 0,
                    error_message:        String::new(),
                }),
            })),
            Err(e) if e.is_retriable() => {
                warn!(error = %e, "Back-pressure on log ingest");
                Err(Status::resource_exhausted(e.to_string()))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

// ── Metrics service ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct OtlpMetricsService {
    pipeline: IngestPipeline,
}

impl OtlpMetricsService {
    pub fn new(pipeline: IngestPipeline) -> Self { Self { pipeline } }
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req     = request.into_inner();
        let records = normalizer::normalise_metrics(&req.resource_metrics);
        let count   = records.len() as i64;
        debug!(count, "OTLP/gRPC metrics export");

        match self.pipeline.submit(IngestBatch::Metrics(records)).await {
            Ok(_) => Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: Some(ExportMetricsPartialSuccess {
                    rejected_data_points: 0,
                    error_message:        String::new(),
                }),
            })),
            Err(e) if e.is_retriable() => Err(Status::resource_exhausted(e.to_string())),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

// ── Trace service ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct OtlpTraceService {
    pipeline: IngestPipeline,
}

impl OtlpTraceService {
    pub fn new(pipeline: IngestPipeline) -> Self { Self { pipeline } }
}

#[tonic::async_trait]
impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req   = request.into_inner();
        let spans = normalizer::normalise_spans(&req.resource_spans);
        let count = spans.len() as i64;
        debug!(count, "OTLP/gRPC trace export");

        match self.pipeline.submit(IngestBatch::Spans(spans)).await {
            Ok(_) => Ok(Response::new(ExportTraceServiceResponse {
                partial_success: Some(ExportTracePartialSuccess {
                    rejected_spans: 0,
                    error_message:  String::new(),
                }),
            })),
            Err(e) if e.is_retriable() => Err(Status::resource_exhausted(e.to_string())),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
