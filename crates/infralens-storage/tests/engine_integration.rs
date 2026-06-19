//! Integration tests for the full storage engine write path.

use infralens_common::{
    config::StorageConfig,
    model::{AnyValue, IngestBatch, InstrumentationScope, LogRecord, MetricPoint, MetricType, SpanRecord},
};
use infralens_storage::StorageEngine;
use std::collections::HashMap;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

fn make_config(dir: &TempDir) -> StorageConfig {
    StorageConfig {
        data_dir:                 dir.path().to_string_lossy().to_string(),
        memtable_size_bytes:      1024 * 1024, // 1 MiB — small to trigger flush easily
        l0_compaction_trigger:    4,
        compaction_interval_secs: 3600,        // don't compact in tests
        partition_hours:          1,
        parquet_row_group_size:   1_000,
        wal_sync_interval_ms:     0,
    }
}

fn make_log(ts: u64) -> LogRecord {
    LogRecord {
        timestamp_ns:          ts,
        observed_timestamp_ns: ts,
        trace_id:              None,
        span_id:               None,
        severity_number:       9,
        severity_text:         "INFO".to_string(),
        body:                  Some(AnyValue::String(format!("message at {ts}"))),
        attributes:            [("env".to_string(), AnyValue::String("test".to_string()))].into(),
        resource_attributes:   [("service.name".to_string(), AnyValue::String("my-svc".to_string()))].into(),
        scope:                 InstrumentationScope::default(),
        schema_url:            String::new(),
    }
}

fn make_metric(ts: u64, name: &str, value: f64) -> MetricPoint {
    MetricPoint {
        timestamp_ns:        ts,
        start_timestamp_ns:  ts - 60_000_000_000,
        name:                name.to_string(),
        description:         String::new(),
        unit:                "1".to_string(),
        metric_type:         MetricType::Gauge,
        value_double:        Some(value),
        value_int:           None,
        histogram:           None,
        attributes:          HashMap::new(),
        resource_attributes: HashMap::new(),
        scope:               InstrumentationScope::default(),
        schema_url:          String::new(),
    }
}

fn make_span(trace_id: [u8; 16], span_id: [u8; 8], start_ns: u64) -> SpanRecord {
    SpanRecord {
        trace_id,
        span_id,
        parent_span_id:      None,
        name:                "test-span".to_string(),
        kind:                2, // SERVER
        start_time_ns:       start_ns,
        end_time_ns:         start_ns + 100_000_000,
        duration_ns:         100_000_000,
        status_code:         1, // OK
        status_message:      String::new(),
        attributes:          [("http.method".to_string(), AnyValue::String("GET".to_string()))].into(),
        resource_attributes: HashMap::new(),
        scope:               InstrumentationScope::default(),
        events:              vec![],
        links:               vec![],
        schema_url:          String::new(),
    }
}

#[tokio::test]
async fn write_and_close_logs() {
    let dir    = TempDir::new().unwrap();
    let engine = StorageEngine::open(make_config(&dir)).await.unwrap();

    let logs: Vec<LogRecord> = (0..50).map(|i| make_log(i * 1_000_000_000)).collect();
    engine.write_batch(IngestBatch::Logs(logs)).await.unwrap();

    engine.close().await.unwrap();

    // After closing, parquet files should exist under the data dir.
    let parquet_count = count_files(dir.path(), "parquet");
    // Even without a flush trigger, flush_all is called on close.
    assert!(parquet_count >= 1, "expected at least one parquet file, got {parquet_count}");
}

#[tokio::test]
async fn write_multiple_signals() {
    let dir    = TempDir::new().unwrap();
    let engine = StorageEngine::open(make_config(&dir)).await.unwrap();

    let ts_base = 1_700_000_000_000_000_000u64; // 2023-ish

    engine.write_batch(IngestBatch::Logs(vec![make_log(ts_base)])).await.unwrap();
    engine.write_batch(IngestBatch::Metrics(vec![make_metric(ts_base, "cpu.utilization", 0.72)])).await.unwrap();
    engine.write_batch(IngestBatch::Spans(vec![make_span([1u8; 16], [2u8; 8], ts_base)])).await.unwrap();

    engine.close().await.unwrap();

    // Should have data files for all three signals.
    let parquet_count = count_files(dir.path(), "parquet");
    assert!(parquet_count >= 3, "expected ≥3 parquet files, got {parquet_count}");
}

#[tokio::test]
async fn memtable_flush_on_size_limit() {
    let dir    = TempDir::new().unwrap();
    // Very small memtable so writes trigger a flush immediately.
    let mut config = make_config(&dir);
    config.memtable_size_bytes = 1; // 1 byte — every write triggers a freeze

    let engine = StorageEngine::open(config).await.unwrap();

    for i in 0..10u64 {
        engine.write_batch(IngestBatch::Logs(vec![make_log(i * 1_000_000_000)])).await.unwrap();
    }

    // Give the flush worker time to drain.
    sleep(Duration::from_millis(200)).await;
    engine.close().await.unwrap();

    let parquet_count = count_files(dir.path(), "parquet");
    assert!(parquet_count >= 1, "flush should have produced parquet files");
}

#[tokio::test]
async fn wal_files_created() {
    let dir    = TempDir::new().unwrap();
    let engine = StorageEngine::open(make_config(&dir)).await.unwrap();

    engine.write_batch(IngestBatch::Logs(vec![make_log(1_000_000_000)])).await.unwrap();
    engine.close().await.unwrap();

    let wal_count = count_files(dir.path(), "log");
    assert!(wal_count >= 1, "WAL file should exist");
}

fn count_files(dir: &std::path::Path, ext: &str) -> usize {
    walkdir(dir)
        .into_iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some(ext))
        .count()
}

fn walkdir(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut result = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                result.extend(walkdir(&path));
            } else {
                result.push(path);
            }
        }
    }
    result
}

// ── Property-based tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        #[ignore = "proptest — run with --include-ignored"]
        fn prop_logs_survive_roundtrip(
            timestamps in prop::collection::vec(0u64..u64::MAX / 2, 1..=200),
            bodies in prop::collection::vec("[a-z ]{0,64}", 1..=200),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all().build().unwrap();
            rt.block_on(async {
                let dir = TempDir::new().unwrap();
                let engine = StorageEngine::open(make_config(&dir)).await.unwrap();

                let logs: Vec<LogRecord> = timestamps.iter().zip(bodies.iter()).map(|(&ts, body)| {
                    let mut l = make_log(ts);
                    l.body = Some(AnyValue::String(body.clone()));
                    l
                }).collect();
                let count = logs.len();

                engine.write_batch(IngestBatch::Logs(logs)).await.unwrap();
                engine.close().await.unwrap();

                let parquet_count = count_files(dir.path(), "parquet");
                prop_assert!(parquet_count >= 1, "should have at least one parquet file");

                // Read back and verify row count.
                let mut total_rows = 0usize;
                for path in walkdir(dir.path()).into_iter().filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet")) {
                    let file = std::fs::File::open(&path).unwrap();
                    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                        .unwrap().build().unwrap();
                    for batch in reader {
                        total_rows += batch.unwrap().num_rows();
                    }
                }
                prop_assert_eq!(total_rows, count);
            });
        }
    }
}
