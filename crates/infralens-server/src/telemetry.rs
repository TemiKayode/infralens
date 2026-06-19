//! Configures structured logging and Prometheus metrics for the server process.

use infralens_common::config::TelemetryConfig;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{
    filter::EnvFilter,
    fmt,
    prelude::*,
};

pub fn init_logging(cfg: &TelemetryConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.log_level));

    if cfg.json_logs {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer())
            .init();
    }
}

pub fn init_metrics(addr: &str) -> anyhow::Result<()> {
    let socket: std::net::SocketAddr = addr.parse()?;
    PrometheusBuilder::new()
        .with_http_listener(socket)
        .install()
        .map_err(|e| anyhow::anyhow!("Prometheus install error: {e}"))?;
    Ok(())
}
