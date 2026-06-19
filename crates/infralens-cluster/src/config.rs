use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub enabled:              bool,
    pub node_id:              String,   // UUID; auto-generated if empty
    pub internal_grpc_addr:   String,
    pub etcd_endpoints:       Vec<String>,
    pub replication_factor:   u32,
    pub min_ack_replicas:     u32,
    pub virtual_nodes:        u32,
    pub replica_buffer_bytes: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled:              false,
            node_id:              String::new(),
            internal_grpc_addr:   "0.0.0.0:4319".to_string(),
            etcd_endpoints:       vec!["http://localhost:2379".to_string()],
            replication_factor:   3,
            min_ack_replicas:     1,
            virtual_nodes:        150,
            replica_buffer_bytes: 64 * 1024 * 1024,
        }
    }
}
