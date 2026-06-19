use serde::{Deserialize, Serialize};

/// Top-level configuration, loaded from TOML files and environment variables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraLensConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub ingest: IngestConfig,
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// gRPC listen address (OTLP/gRPC).
    pub grpc_addr: String,
    /// HTTP listen address (OTLP/HTTP + management).
    pub http_addr: String,
    /// Prometheus metrics scrape endpoint.
    pub metrics_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base directory for all partition data.
    pub data_dir: String,
    /// Maximum size of the active MemTable before it is frozen (bytes).
    pub memtable_size_bytes: usize,
    /// How many L0 SSTables trigger a compaction run.
    pub l0_compaction_trigger: usize,
    /// How often (seconds) the compaction worker polls for work.
    pub compaction_interval_secs: u64,
    /// Partition granularity in hours (default: 1).
    pub partition_hours: u64,
    /// Parquet row-group size in number of rows.
    pub parquet_row_group_size: usize,
    /// fsync interval for WAL writes in milliseconds (0 = sync every write).
    pub wal_sync_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Bounded channel depth between ingest handlers and storage writer.
    pub buffer_depth: usize,
    /// Max records per write batch assembled by the processor.
    pub max_batch_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Log level filter (e.g. "info", "debug", "infralens=trace").
    pub log_level: String,
    /// Emit logs as JSON (for production log ingestion).
    pub json_logs: bool,
}

impl Default for InfraLensConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                grpc_addr:    "0.0.0.0:4317".to_string(),
                http_addr:    "0.0.0.0:4318".to_string(),
                metrics_addr: "0.0.0.0:9090".to_string(),
            },
            storage: StorageConfig {
                data_dir:                "./data".to_string(),
                memtable_size_bytes:     64 * 1024 * 1024, // 64 MiB
                l0_compaction_trigger:   4,
                compaction_interval_secs: 30,
                partition_hours:         1,
                parquet_row_group_size:  1_000_000,
                wal_sync_interval_ms:    100,
            },
            ingest: IngestConfig {
                buffer_depth:       4096,
                max_batch_records:  1000,
            },
            telemetry: TelemetryConfig {
                log_level: "info".to_string(),
                json_logs: true,
            },
        }
    }
}

impl InfraLensConfig {
    /// Load configuration from layered TOML files and INFRALENS_ env vars.
    pub fn load(env: &str) -> anyhow::Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name(&format!("config/{env}")).required(false))
            .add_source(config::Environment::with_prefix("INFRALENS").separator("__"))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}
