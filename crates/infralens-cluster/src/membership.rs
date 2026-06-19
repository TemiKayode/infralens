//! Cluster membership via etcd.
//!
//! Each node registers itself under `/infralens/nodes/{node_id}` with a TTL lease
//! and refreshes it every `lease_ttl / 3` seconds.  A watcher goroutine keeps
//! the local `ClusterView` consistent with the authoritative etcd state.

use crate::{
    config::ClusterConfig,
    error::{ClusterError, Result},
    ring::ConsistentHashRing,
};
use etcd_client::{Client, GetOptions, PutOptions, WatchOptions};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

const LEASE_TTL_SECS: i64 = 15;
const NODES_PREFIX: &str  = "/infralens/nodes/";
const RING_KEY: &str       = "/infralens/ring/current";

// ── NodeMeta ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMeta {
    pub node_id:           String,
    pub internal_grpc_addr: String,
    pub version:           String,
    pub started_at_ns:     u64,
}

// ── Live cluster view ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ClusterView {
    pub nodes: Vec<NodeMeta>,
    pub ring:  ConsistentHashRing,
}

// ── ClusterMembership ─────────────────────────────────────────────────────────

pub struct ClusterMembership {
    config: ClusterConfig,
    view:   Arc<RwLock<ClusterView>>,
}

impl ClusterMembership {
    /// Connect to etcd, register this node, and spawn the lease-refresh and
    /// watch tasks.  Returns once the initial view has been loaded.
    pub async fn join(config: ClusterConfig) -> Result<Arc<Self>> {
        let mut client = Client::connect(&config.etcd_endpoints, None).await?;

        // Acquire a lease for our node registration.
        let lease     = client.lease_grant(LEASE_TTL_SECS, None).await?;
        let lease_id  = lease.id();

        // Register this node.
        let meta = NodeMeta {
            node_id:           config.node_id.clone(),
            internal_grpc_addr: config.internal_grpc_addr.clone(),
            version:           env!("CARGO_PKG_VERSION").to_string(),
            started_at_ns:     std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        };
        let meta_json = serde_json::to_string(&meta)
            .map_err(|e| ClusterError::Serialisation(e.to_string()))?;

        let node_key = format!("{NODES_PREFIX}{}", config.node_id);
        client
            .put(
                node_key.as_bytes(),
                meta_json.as_bytes(),
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await?;

        info!(node_id = %config.node_id, "Registered with etcd");

        // Load initial cluster view.
        let initial_view = load_view(&mut client, config.virtual_nodes).await?;
        let view         = Arc::new(RwLock::new(initial_view));

        let membership = Arc::new(Self { config: config.clone(), view: Arc::clone(&view) });

        // Spawn lease-refresh task.
        let mut refresh_client = Client::connect(&config.etcd_endpoints, None).await?;
        let refresh_interval   = Duration::from_secs((LEASE_TTL_SECS / 3) as u64);
        tokio::spawn(async move {
            let mut ticker = time::interval(refresh_interval);
            loop {
                ticker.tick().await;
                if let Err(e) = refresh_client.lease_keep_alive(lease_id).await {
                    error!(error = %e, "etcd lease keep-alive failed");
                }
            }
        });

        // Spawn watch task that keeps `view` up-to-date.
        let mut watch_client = Client::connect(&config.etcd_endpoints, None).await?;
        let watch_view       = Arc::clone(&view);
        let vn               = config.virtual_nodes;
        tokio::spawn(async move {
            watch_membership(&mut watch_client, watch_view, vn).await;
        });

        Ok(membership)
    }

    /// Current cluster view snapshot.
    pub fn view(&self) -> ClusterView {
        self.view.read().clone()
    }

    /// Is the given node_id reachable (present in the view)?
    pub fn is_alive(&self, node_id: &str) -> bool {
        self.view.read().nodes.iter().any(|n| n.node_id == node_id)
    }

    /// Internal gRPC address for a node.
    pub fn grpc_addr(&self, node_id: &str) -> Option<String> {
        self.view
            .read()
            .nodes
            .iter()
            .find(|n| n.node_id == node_id)
            .map(|n| n.internal_grpc_addr.clone())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn load_view(client: &mut Client, virtual_nodes: u32) -> Result<ClusterView> {
    let resp  = client
        .get(NODES_PREFIX, Some(GetOptions::new().with_prefix()))
        .await?;
    let mut nodes = Vec::new();
    let mut ring  = ConsistentHashRing::new(virtual_nodes);

    for kv in resp.kvs() {
        if let Ok(json) = std::str::from_utf8(kv.value()) {
            if let Ok(meta) = serde_json::from_str::<NodeMeta>(json) {
                ring.add_node(&meta.node_id);
                nodes.push(meta);
            }
        }
    }
    debug!(node_count = nodes.len(), "Loaded cluster view from etcd");
    Ok(ClusterView { nodes, ring })
}

async fn watch_membership(
    client:        &mut Client,
    view:          Arc<RwLock<ClusterView>>,
    virtual_nodes: u32,
) {
    let (mut _watcher, mut stream) = match client
        .watch(NODES_PREFIX, Some(WatchOptions::new().with_prefix()))
        .await
    {
        Ok(w)  => w,
        Err(e) => { error!(error = %e, "etcd watch failed"); return; }
    };

    info!("etcd membership watch active");

    while let Some(resp) = stream.message().await.unwrap_or(None) {
        for event in resp.events() {
            use etcd_client::EventType;
            match event.event_type() {
                EventType::Put => {
                    if let Some(kv) = event.kv() {
                        if let Ok(json) = std::str::from_utf8(kv.value()) {
                            if let Ok(meta) = serde_json::from_str::<NodeMeta>(json) {
                                let mut guard = view.write();
                                guard.nodes.retain(|n| n.node_id != meta.node_id);
                                guard.ring.add_node(&meta.node_id);
                                info!(node_id = %meta.node_id, "Node joined cluster");
                                guard.nodes.push(meta);
                            }
                        }
                    }
                }
                EventType::Delete => {
                    if let Some(kv) = event.kv() {
                        let key = String::from_utf8_lossy(kv.key());
                        let node_id = key.trim_start_matches(NODES_PREFIX);
                        let mut guard = view.write();
                        guard.nodes.retain(|n| n.node_id != node_id);
                        guard.ring.remove_node(node_id);
                        warn!(node_id = %node_id, "Node left cluster");
                    }
                }
            }
        }
    }
}
