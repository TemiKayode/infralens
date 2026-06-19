//! InfraLens server entry point.
//!
//! Starts:
//!   1. Prometheus metrics exporter
//!   2. Structured JSON logger
//!   3. StorageEngine (LSM + WAL)
//!   4. IngestPipeline (bounded channel → storage)
//!   5. OTLP/gRPC server  (tonic, :4317)
//!   6. OTLP/HTTP server  (axum, :4318, also serves /healthz and /readyz)

mod telemetry;

use infralens_common::config::InfraLensConfig;
use infralens_ingest::{
    grpc::{OtlpLogsService, OtlpMetricsService, OtlpTraceService},
    processor::IngestPipeline,
};
use infralens_proto::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use infralens_storage::StorageEngine;
use std::{net::SocketAddr, sync::Arc};
use tokio::{signal, task::JoinSet};
use tonic::transport::Server as TonicServer;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Configuration ────────────────────────────────────────────────────────
    let env = std::env::var("INFRALENS_ENV").unwrap_or_else(|_| "default".to_string());
    let cfg = InfraLensConfig::load(&env).unwrap_or_else(|e| {
        eprintln!("Config load error ({e}), using defaults");
        InfraLensConfig::default()
    });

    // ── Telemetry ────────────────────────────────────────────────────────────
    telemetry::init_logging(&cfg.telemetry);
    if let Err(e) = telemetry::init_metrics(&cfg.server.metrics_addr) {
        error!(error = %e, "Failed to start Prometheus exporter");
    }

    info!(
        grpc_addr    = %cfg.server.grpc_addr,
        http_addr    = %cfg.server.http_addr,
        metrics_addr = %cfg.server.metrics_addr,
        data_dir     = %cfg.storage.data_dir,
        "InfraLens starting"
    );

    // ── Storage engine ───────────────────────────────────────────────────────
    let engine: Arc<StorageEngine> = StorageEngine::open(cfg.storage.clone()).await?;

    // ── Ingest pipeline ──────────────────────────────────────────────────────
    let pipeline = IngestPipeline::new(cfg.ingest.clone(), Arc::clone(&engine));

    // ── Spawn servers ────────────────────────────────────────────────────────
    let mut join_set = JoinSet::new();

    // gRPC server (OTLP/gRPC)
    {
        let grpc_addr: SocketAddr = cfg.server.grpc_addr.parse()?;
        let logs_svc    = LogsServiceServer::new(OtlpLogsService::new(pipeline.clone()));
        let metrics_svc = MetricsServiceServer::new(OtlpMetricsService::new(pipeline.clone()));
        let trace_svc   = TraceServiceServer::new(OtlpTraceService::new(pipeline.clone()));

        join_set.spawn(async move {
            info!(%grpc_addr, "gRPC server listening");
            TonicServer::builder()
                .add_service(logs_svc)
                .add_service(metrics_svc)
                .add_service(trace_svc)
                .serve(grpc_addr)
                .await
                .map_err(|e| anyhow::anyhow!("gRPC server error: {e}"))
        });
    }

    // HTTP server (OTLP/HTTP + management endpoints)
    {
        let http_addr: SocketAddr = cfg.server.http_addr.parse()?;
        let otlp_router = infralens_ingest::http::router(pipeline.clone());

        let app = axum::Router::new()
            .route("/healthz", axum::routing::get(|| async { "ok" }))
            .route("/readyz",  axum::routing::get(|| async { "ok" }))
            .merge(otlp_router)
            .layer(tower_http::trace::TraceLayer::new_for_http());

        join_set.spawn(async move {
            info!(%http_addr, "HTTP server listening");
            let listener = tokio::net::TcpListener::bind(http_addr).await?;
            axum::serve(listener, app)
                .await
                .map_err(|e| anyhow::anyhow!("HTTP server error: {e}"))
        });
    }

    // ── Shutdown signal ───────────────────────────────────────────────────────
    let engine_shutdown = Arc::clone(&engine);
    tokio::select! {
        res = join_set.join_next() => {
            if let Some(Err(e)) = res {
                error!(error = %e, "Server task failed");
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received Ctrl-C, shutting down");
        }
    }

    info!("Flushing storage engine…");
    if let Err(e) = engine_shutdown.close().await {
        error!(error = %e, "Storage engine close error");
    }

    info!("InfraLens shutdown complete");
    Ok(())
}
