//! Shard router — decides whether a write belongs to this node (primary)
//! or should be forwarded, and which nodes need replica copies.

use crate::{
    membership::ClusterMembership,
    ring::ShardId,
};
use std::sync::Arc;

/// The routing decision for a single write.
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// Node that is the primary for this shard.
    pub primary:  String,
    /// Additional nodes that need a replica copy.
    pub replicas: Vec<String>,
    /// True if this node is the primary.
    pub is_local: bool,
}

pub struct ShardRouter {
    membership: Arc<ClusterMembership>,
    local_id:   String,
    rep_factor: u32,
}

impl ShardRouter {
    pub fn new(
        membership: Arc<ClusterMembership>,
        local_id:   String,
        rep_factor: u32,
    ) -> Self {
        Self { membership, local_id, rep_factor }
    }

    /// Compute the routing decision for a given series key.
    pub fn route(&self, series_key: &str) -> RoutingDecision {
        let shard = ShardId::for_key(series_key);
        let view  = self.membership.view();

        let primary = view
            .ring
            .primary_for(shard)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| self.local_id.clone());

        let replicas = view
            .ring
            .replica_nodes(shard, self.rep_factor as usize)
            .unwrap_or_default()
            .into_iter()
            .filter(|n| *n != primary)
            .take((self.rep_factor - 1) as usize)
            .map(|n| n.to_string())
            .collect();

        let is_local = primary == self.local_id;
        RoutingDecision { primary, replicas, is_local }
    }

    /// Derive the series key for a log record.
    pub fn log_series_key(partition: &str, severity: i32) -> String {
        format!("logs|{partition}|sev={severity}")
    }

    /// Derive the series key for a metric point.
    pub fn metric_series_key(partition: &str, name: &str) -> String {
        format!("metrics|{partition}|{name}")
    }

    /// Derive the series key for a span.
    pub fn span_series_key(partition: &str, service: &str) -> String {
        format!("traces|{partition}|{service}")
    }
}
