//! Internal gRPC server — implements `InternalService` on each storage node.

use crate::internal::v1::{
    internal_service_server::{InternalService, InternalServiceServer},
    GetNodeInfoRequest, GetNodeInfoResponse, NodeStats, ReplicateAck, ReplicateRequest,
    ShardQueryRequest, ShardQueryResponse, SyncShardChunk, SyncShardRequest,
};
use infralens_storage::StorageEngine;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

pub struct InternalServiceImpl {
    storage: Arc<StorageEngine>,
    node_id: String,
}

impl InternalServiceImpl {
    pub fn new(storage: Arc<StorageEngine>, node_id: String) -> Self {
        Self { storage, node_id }
    }

    pub fn into_server(self) -> InternalServiceServer<Self> {
        InternalServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl InternalService for InternalServiceImpl {
    type QueryShardStream = ReceiverStream<Result<ShardQueryResponse, Status>>;
    type ReplicateStream  = ReceiverStream<Result<ReplicateAck, Status>>;
    type SyncShardStream  = ReceiverStream<Result<SyncShardChunk, Status>>;

    async fn query_shard(
        &self,
        request: Request<ShardQueryRequest>,
    ) -> Result<Response<Self::QueryShardStream>, Status> {
        let req = request.into_inner();
        debug!(query_id = %req.query_id, signal = %req.signal, "ShardQuery received");

        let (tx, rx) = tokio::sync::mpsc::channel(64);

        // Phase 2 stub: signal that we have no data (empty last response).
        // Phase 3 wires this to the query executor.
        tokio::spawn(async move {
            let _ = tx.send(Ok(ShardQueryResponse {
                record_batch_ipc: vec![],
                is_last:          true,
                rows_returned:    0,
                error:            String::new(),
            })).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn replicate(
        &self,
        request: Request<Streaming<ReplicateRequest>>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        let mut stream = request.into_inner();
        let storage    = Arc::clone(&self.storage);
        let (tx, rx)   = tokio::sync::mpsc::channel(256);

        tokio::spawn(async move {
            while let Ok(Some(entry)) = stream.message().await {
                let seq = entry.sequence;
                // Apply entry to local storage.
                let replica_entry = infralens_cluster::replication::ReplicationEntry {
                    sequence:      entry.sequence,
                    partition_key: entry.partition_key,
                    signal:        entry.signal,
                    wal_bytes:     entry.wal_entry,
                };
                let accepted = infralens_cluster::replication::apply_replica_entry(
                    replica_entry,
                    Arc::clone(&storage),
                ).await.is_ok();

                let _ = tx.send(Ok(ReplicateAck {
                    sequence: seq,
                    accepted,
                    error: String::new(),
                })).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn sync_shard(
        &self,
        _request: Request<SyncShardRequest>,
    ) -> Result<Response<Self::SyncShardStream>, Status> {
        // Full shard transfer for new nodes — Phase 2.1 work item.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Ok(SyncShardChunk {
                filename:       String::new(),
                data:           vec![],
                is_last_chunk:  true,
                is_last_file:   true,
            })).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_node_info(
        &self,
        _request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        Ok(Response::new(GetNodeInfoResponse {
            node_id:        self.node_id.clone(),
            version:        env!("CARGO_PKG_VERSION").to_string(),
            primary_shards: vec![],
            replica_shards: vec![],
            ring_version:   0,
            ready:          true,
            stats:          Some(NodeStats {
                memtable_bytes:    0,
                sstable_count:     0,
                ingested_total:    0,
                ingest_rate_per_s: 0.0,
            }),
        }))
    }
}
