use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("etcd error: {0}")]
    Etcd(#[from] etcd_client::Error),

    #[error("node {0} not found in ring")]
    NodeNotFound(String),

    #[error("ring is empty — no nodes registered")]
    EmptyRing,

    #[error("replication failed to {node}: {reason}")]
    ReplicationFailed { node: String, reason: String },

    #[error("shard {0} has no primary")]
    NoPrimary(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialisation error: {0}")]
    Serialisation(String),
}

pub type Result<T> = std::result::Result<T, ClusterError>;
