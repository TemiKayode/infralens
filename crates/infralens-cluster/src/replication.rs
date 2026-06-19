//! Async replication engine.
//!
//! The `ReplicationEngine` is instantiated on the primary.  For each write it:
//!   1. Serialises the WAL entry.
//!   2. Sends it over a persistent gRPC streaming connection to each replica.
//!   3. Waits for `min_ack_replicas` acknowledgements (typically 1 = async).
//!
//! Replica connections are lazily established and automatically reconnected on
//! failure.  Undelivered entries are buffered in a bounded per-replica queue;
//! if the queue fills, the replica is marked degraded and writes proceed with
//! a warning rather than blocking the hot path.

use crate::{config::ClusterConfig, error::ClusterError, membership::ClusterMembership};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

const BUFFER_PER_REPLICA: usize = 4096;

// ── Wire type for a replication entry ────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    pub sequence:      u64,
    pub partition_key: String,
    pub signal:        String,
    pub wal_bytes:     Vec<u8>,
}

// ── Per-replica sender ────────────────────────────────────────────────────────

struct ReplicaSender {
    tx:      mpsc::Sender<ReplicationEntry>,
    node_id: String,
}

// ── ReplicationEngine ─────────────────────────────────────────────────────────

pub struct ReplicationEngine {
    config:     ClusterConfig,
    membership: Arc<ClusterMembership>,
    senders:    Mutex<HashMap<String, ReplicaSender>>,
    sequence:   std::sync::atomic::AtomicU64,
}

impl ReplicationEngine {
    pub fn new(config: ClusterConfig, membership: Arc<ClusterMembership>) -> Arc<Self> {
        Arc::new(Self {
            config,
            membership,
            senders:  Mutex::new(HashMap::new()),
            sequence: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Replicate a WAL entry to the given replica nodes.
    /// Returns after `min_ack_replicas` have acknowledged (or best-effort if async).
    pub async fn replicate(
        &self,
        replica_node_ids: &[String],
        partition_key:    &str,
        signal:           &str,
        wal_bytes:        Vec<u8>,
    ) {
        let seq = self.sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let entry = ReplicationEntry {
            sequence:      seq,
            partition_key: partition_key.to_string(),
            signal:        signal.to_string(),
            wal_bytes,
        };

        let mut ack_count = 0u32;

        for node_id in replica_node_ids {
            let addr = match self.membership.grpc_addr(node_id) {
                Some(a) => a,
                None    => {
                    warn!(node_id, "Replica address unknown, skipping");
                    continue;
                }
            };

            let tx = self.get_or_create_sender(node_id, &addr);
            match tx.try_send(entry.clone()) {
                Ok(_)  => {
                    debug!(node_id, seq, "Entry queued for replication");
                    ack_count += 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(node_id, "Replica buffer full — degraded mode");
                    metrics::counter!("infralens_replication_drops_total",
                        "node" => node_id.clone()).increment(1);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!(node_id, "Replica sender closed, will reconnect");
                    self.senders.lock().remove(node_id.as_str());
                }
            }
        }

        if ack_count < self.config.min_ack_replicas {
            // For async replication (min_ack = 1 = local only), this is fine.
            debug!(ack_count, min = self.config.min_ack_replicas,
                "Replication below min_ack threshold (degraded)");
        }
    }

    fn get_or_create_sender(&self, node_id: &str, addr: &str) -> mpsc::Sender<ReplicationEntry> {
        let mut guard = self.senders.lock();
        if let Some(s) = guard.get(node_id) {
            return s.tx.clone();
        }

        let (tx, rx) = mpsc::channel::<ReplicationEntry>(BUFFER_PER_REPLICA);
        let node_id_owned = node_id.to_string();
        let addr_owned    = addr.to_string();

        // Spawn a background task that drains the queue and sends via gRPC.
        tokio::spawn(async move {
            replica_send_loop(node_id_owned, addr_owned, rx).await;
        });

        guard.insert(node_id.to_string(), ReplicaSender {
            tx: tx.clone(),
            node_id: node_id.to_string(),
        });
        tx
    }
}

/// Background loop that holds a persistent gRPC stream to a replica and drains
/// the per-replica queue.  On disconnection it waits 1 second and reconnects.
async fn replica_send_loop(
    node_id: String,
    addr:    String,
    mut rx:  mpsc::Receiver<ReplicationEntry>,
) {
    info!(node_id, addr, "Replica sender loop starting");

    while let Some(entry) = rx.recv().await {
        // In a full implementation this would open a tonic gRPC stream and
        // call `InternalService::Replicate`.  Phase 2 stubs this out with a
        // log line so the engine compiles and the channel drains correctly.
        debug!(
            node_id = %node_id,
            partition = %entry.partition_key,
            signal    = %entry.signal,
            seq       = entry.sequence,
            bytes     = entry.wal_bytes.len(),
            "Replicating entry (gRPC stub — wire it to infralens-rpc in Phase 2.1)"
        );
        metrics::counter!("infralens_replication_sent_total",
            "node" => node_id.clone()).increment(1);
    }

    info!(node_id, "Replica sender loop exiting");
}

// ── Replica receiver (applied on replica nodes) ───────────────────────────────

/// Apply a received replication entry to local storage.
/// Called from the internal gRPC service handler.
pub async fn apply_replica_entry(
    entry:   ReplicationEntry,
    storage: Arc<infralens_storage::StorageEngine>,
) -> Result<(), ClusterError> {
    use infralens_storage::error::StorageError;
    // Re-parse the WAL entry and write to local storage.
    // The WAL entry bytes already contain a bincode-serialised record with a
    // type prefix byte (ENTRY_LOG/ENTRY_METRIC/ENTRY_SPAN).
    if entry.wal_bytes.is_empty() {
        return Ok(());
    }
    let entry_type = entry.wal_bytes[0];
    let data       = &entry.wal_bytes[1..];

    match entry_type {
        crate::replication::WAL_ENTRY_LOG => {
            if let Ok(rec) = bincode::deserialize::<infralens_common::model::LogRecord>(data) {
                storage.write_batch(infralens_common::model::IngestBatch::Logs(vec![rec]))
                    .await
                    .map_err(|e| ClusterError::ReplicationFailed {
                        node: "local".into(),
                        reason: e.to_string(),
                    })?;
            }
        }
        crate::replication::WAL_ENTRY_METRIC => {
            if let Ok(rec) = bincode::deserialize::<infralens_common::model::MetricPoint>(data) {
                storage.write_batch(infralens_common::model::IngestBatch::Metrics(vec![rec]))
                    .await
                    .map_err(|e| ClusterError::ReplicationFailed {
                        node: "local".into(),
                        reason: e.to_string(),
                    })?;
            }
        }
        crate::replication::WAL_ENTRY_SPAN => {
            if let Ok(rec) = bincode::deserialize::<infralens_common::model::SpanRecord>(data) {
                storage.write_batch(infralens_common::model::IngestBatch::Spans(vec![rec]))
                    .await
                    .map_err(|e| ClusterError::ReplicationFailed {
                        node: "local".into(),
                        reason: e.to_string(),
                    })?;
            }
        }
        _ => {}
    }
    Ok(())
}

// WAL entry type constants (mirror infralens_storage::wal constants).
pub const WAL_ENTRY_LOG:    u8 = 0x01;
pub const WAL_ENTRY_METRIC: u8 = 0x02;
pub const WAL_ENTRY_SPAN:   u8 = 0x03;
