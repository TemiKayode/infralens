//! Consistent hash ring with configurable virtual-node count.
//!
//! Virtual nodes are placed at `sha256(node_id + "#" + i)[..8]` positions
//! on a 64-bit ring.  Lookups walk the ring clockwise to find the first node
//! at or after the key hash.

use crate::error::{ClusterError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

/// Opaque shard identifier (a u64 ring position).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u64);

impl ShardId {
    /// Derive the shard ID for a given series key.
    pub fn for_key(key: &str) -> Self {
        ShardId(hash64(key.as_bytes()))
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Consistent hash ring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistentHashRing {
    /// BTreeMap from virtual-node position → physical node_id.
    ring:          BTreeMap<u64, String>,
    virtual_nodes: u32,
    /// Ring schema version, incremented on every topology change.
    pub version:   u64,
}

impl ConsistentHashRing {
    pub fn new(virtual_nodes: u32) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
            version: 0,
        }
    }

    /// Add a node and its virtual positions.
    pub fn add_node(&mut self, node_id: &str) {
        for i in 0..self.virtual_nodes {
            let key = format!("{node_id}#{i}");
            let pos = hash64(key.as_bytes());
            self.ring.insert(pos, node_id.to_string());
        }
        self.version += 1;
    }

    /// Remove a node and all its virtual positions.
    pub fn remove_node(&mut self, node_id: &str) {
        for i in 0..self.virtual_nodes {
            let key = format!("{node_id}#{i}");
            let pos = hash64(key.as_bytes());
            self.ring.remove(&pos);
        }
        self.version += 1;
    }

    /// Return the primary node responsible for the given key.
    pub fn primary_for(&self, shard: ShardId) -> Result<&str> {
        if self.ring.is_empty() {
            return Err(ClusterError::EmptyRing);
        }
        // Walk clockwise: find first position >= key, wrapping around.
        let node = self.ring
            .range(shard.0..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, n)| n.as_str())
            .ok_or(ClusterError::EmptyRing)?;
        Ok(node)
    }

    /// Return `count` distinct nodes starting at `shard` (for replication).
    pub fn replica_nodes(&self, shard: ShardId, count: usize) -> Result<Vec<&str>> {
        if self.ring.is_empty() {
            return Err(ClusterError::EmptyRing);
        }
        let mut result: Vec<&str> = Vec::new();
        let mut seen:  std::collections::HashSet<&str> = std::collections::HashSet::new();

        // Two passes: from shard.0 to MAX, then from 0.
        for (_, node) in self.ring.range(shard.0..).chain(self.ring.iter()) {
            if seen.insert(node.as_str()) {
                result.push(node.as_str());
            }
            if result.len() >= count { break; }
        }
        Ok(result)
    }

    pub fn is_empty(&self) -> bool { self.ring.is_empty() }
    pub fn node_count(&self) -> usize {
        self.ring.values().collect::<std::collections::HashSet<_>>().len()
    }
}

fn hash64(data: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    data.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_always_returns_itself() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node-a");
        let shard = ShardId::for_key("logs|2024010100|{env=prod}");
        assert_eq!(ring.primary_for(shard).unwrap(), "node-a");
    }

    #[test]
    fn uniform_distribution() {
        let mut ring = ConsistentHashRing::new(150);
        for i in 0..3 { ring.add_node(&format!("node-{i}")); }

        let mut counts = std::collections::HashMap::<String, u32>::new();
        for i in 0..10_000u64 {
            let shard = ShardId::for_key(&format!("key-{i}"));
            let node  = ring.primary_for(shard).unwrap().to_string();
            *counts.entry(node).or_insert(0) += 1;
        }
        // Each node should handle roughly 33 % ± 10 %.
        for (node, count) in &counts {
            let pct = *count as f64 / 10_000.0;
            assert!(pct > 0.23 && pct < 0.43, "node {node} got {pct:.1%}");
        }
    }

    #[test]
    fn replica_nodes_distinct() {
        let mut ring = ConsistentHashRing::new(150);
        for i in 0..5 { ring.add_node(&format!("node-{i}")); }
        let shard   = ShardId::for_key("test-key");
        let replicas = ring.replica_nodes(shard, 3).unwrap();
        assert_eq!(replicas.len(), 3);
        let unique: std::collections::HashSet<_> = replicas.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn remove_node_excludes_it() {
        let mut ring = ConsistentHashRing::new(150);
        ring.add_node("node-a");
        ring.add_node("node-b");
        ring.remove_node("node-a");

        for i in 0..1000u64 {
            let shard = ShardId::for_key(&format!("k{i}"));
            assert_eq!(ring.primary_for(shard).unwrap(), "node-b");
        }
    }
}
