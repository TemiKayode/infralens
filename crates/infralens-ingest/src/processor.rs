//! The `IngestPipeline` owns the bounded channel between protocol handlers and
//! the storage engine.  It provides the single write point for all signal types.

use crate::error::{IngestError, Result};
use infralens_common::{config::IngestConfig, model::IngestBatch};
use infralens_storage::StorageEngine;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, warn};

// ── Public pipeline handle ────────────────────────────────────────────────────

#[derive(Clone)]
pub struct IngestPipeline {
    tx:     mpsc::Sender<IngestBatch>,
    config: IngestConfig,
}

impl IngestPipeline {
    /// Construct and start the background writer task.
    pub fn new(config: IngestConfig, engine: Arc<StorageEngine>) -> Self {
        let (tx, rx) = mpsc::channel::<IngestBatch>(config.buffer_depth);

        tokio::spawn(writer_task(rx, engine, config.max_batch_records));

        Self { tx, config }
    }

    /// Submit a batch for storage.  Returns `Err(BufferFull)` if the channel
    /// is saturated — callers must propagate this as a back-pressure signal.
    pub async fn submit(&self, batch: IngestBatch) -> Result<()> {
        match self.tx.try_send(batch) {
            Ok(_)  => {
                debug!("Batch enqueued");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("infralens_ingest_backpressure_total").increment(1);
                warn!("Ingest buffer full — dropping batch (back-pressure)");
                Err(IngestError::BufferFull)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(IngestError::BufferFull) // engine shut down
            }
        }
    }
}

// ── Background writer ─────────────────────────────────────────────────────────

async fn writer_task(
    mut rx:           mpsc::Receiver<IngestBatch>,
    engine:           Arc<StorageEngine>,
    _max_batch_size:  usize, // future: batch assembly
) {
    while let Some(batch) = rx.recv().await {
        let signal = batch.signal_type();
        let count  = batch.len();

        match engine.write_batch(batch).await {
            Ok(_)  => {
                metrics::counter!(
                    "infralens_storage_writes_total",
                    "signal" => format!("{signal:?}"),
                    "status" => "ok"
                ).increment(count as u64);
            }
            Err(e) => {
                warn!(error = %e, ?signal, count, "Storage write failed");
                metrics::counter!(
                    "infralens_storage_writes_total",
                    "signal" => format!("{signal:?}"),
                    "status" => "error"
                ).increment(count as u64);
            }
        }
    }
    tracing::info!("Ingest writer task exiting");
}
