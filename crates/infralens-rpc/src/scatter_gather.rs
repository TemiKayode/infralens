//! Scatter/gather query coordinator.
//!
//! Given a list of shard nodes and a serialised `PhysicalPlan`, this module
//! fans the query out in parallel, collects Arrow IPC `RecordBatch` streams
//! from each shard, and merges them into a single output stream.

use crate::internal::v1::{
    internal_service_client::InternalServiceClient, ShardQueryRequest,
};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use infralens_cluster::membership::ClusterMembership;
use std::io::Cursor;
use std::sync::Arc;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tracing::{debug, error, warn};

pub struct ScatterGather {
    membership: Arc<ClusterMembership>,
}

/// The result of a scatter/gather execution.
pub struct ScatterResult {
    pub batches:     Vec<RecordBatch>,
    pub error_nodes: Vec<String>,
}

impl ScatterGather {
    pub fn new(membership: Arc<ClusterMembership>) -> Self {
        Self { membership }
    }

    /// Fan out `plan_bytes` to all `target_nodes`, collect and return merged batches.
    pub async fn execute(
        &self,
        query_id:     String,
        plan_bytes:   Vec<u8>,
        start_ns:     u64,
        end_ns:       u64,
        signal:       String,
        target_nodes: Vec<String>,
    ) -> ScatterResult {
        let view = self.membership.view();
        let mut join_set: JoinSet<(String, Result<Vec<RecordBatch>, String>)> =
            JoinSet::new();

        for node_id in &target_nodes {
            let addr = match view.nodes.iter().find(|n| &n.node_id == node_id) {
                Some(n) => n.internal_grpc_addr.clone(),
                None    => {
                    warn!(node_id, "Node not in view, skipping");
                    continue;
                }
            };

            let req = ShardQueryRequest {
                query_id:   query_id.clone(),
                plan_bytes: plan_bytes.clone(),
                start_ns,
                end_ns,
                signal:     signal.clone(),
                max_rows:   0,
            };

            let nid = node_id.clone();
            join_set.spawn(async move {
                let result = query_single_shard(addr, req).await;
                (nid, result)
            });
        }

        let mut all_batches = Vec::new();
        let mut error_nodes = Vec::new();

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok((node_id, Ok(batches))) => {
                    debug!(node_id, count = batches.len(), "Shard returned batches");
                    all_batches.extend(batches);
                }
                Ok((node_id, Err(e))) => {
                    error!(node_id, error = %e, "Shard query failed");
                    error_nodes.push(node_id);
                }
                Err(e) => {
                    error!(error = %e, "Join error in scatter/gather");
                }
            }
        }

        ScatterResult { batches: all_batches, error_nodes }
    }
}

async fn query_single_shard(
    addr: String,
    req:  ShardQueryRequest,
) -> Result<Vec<RecordBatch>, String> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| e.to_string())?;

    let mut client = InternalServiceClient::new(channel);
    let mut stream = client.query_shard(req).await
        .map_err(|e| e.to_string())?
        .into_inner();

    let mut batches = Vec::new();
    while let Some(resp) = stream.message().await.map_err(|e| e.to_string())? {
        if !resp.error.is_empty() {
            return Err(resp.error);
        }
        if !resp.record_batch_ipc.is_empty() {
            let cursor  = Cursor::new(resp.record_batch_ipc);
            let mut rdr = StreamReader::try_new(cursor, None)
                .map_err(|e| e.to_string())?;
            while let Some(batch) = rdr.next() {
                batches.push(batch.map_err(|e| e.to_string())?);
            }
        }
        if resp.is_last { break; }
    }
    Ok(batches)
}
