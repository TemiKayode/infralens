//! CRD type definitions for `InfraLensCluster`.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Spec ──────────────────────────────────────────────────────────────────────

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group   = "infralens.io",
    version = "v1alpha1",
    kind    = "InfraLensCluster",
    namespaced,
    status  = "InfraLensClusterStatus",
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
)]
pub struct InfraLensClusterSpec {
    /// Number of InfraLens server pods.
    pub replicas: u32,

    /// StorageClass for PVCs.
    #[serde(default = "default_storage_class")]
    pub storage_class: String,

    /// Storage size per pod (e.g. "100Gi").
    #[serde(default = "default_storage_size")]
    pub storage_size: String,

    /// Container image to run.
    #[serde(default = "default_image")]
    pub image: String,

    /// etcd endpoints for cluster membership.
    pub etcd_endpoints: Vec<String>,

    /// InfraLens configuration overrides.
    #[serde(default)]
    pub config: ClusterConfig,

    /// Pod resource requests/limits.
    #[serde(default)]
    pub resources: ResourceSpec,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
pub struct ClusterConfig {
    #[serde(default = "default_memtable_size")]
    pub memtable_size_bytes: u64,
    #[serde(default = "default_partition_hours")]
    pub partition_hours: u64,
    #[serde(default = "default_l0_trigger")]
    pub l0_compaction_trigger: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
pub struct ResourceSpec {
    #[serde(default)]
    pub requests: ResourceQuantity,
    #[serde(default)]
    pub limits:   ResourceQuantity,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ResourceQuantity {
    pub cpu:    String,
    pub memory: String,
}

impl Default for ResourceQuantity {
    fn default() -> Self {
        Self { cpu: "1".into(), memory: "2Gi".into() }
    }
}

// ── Status ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct InfraLensClusterStatus {
    pub phase:            ClusterPhase,
    pub ready_replicas:   u32,
    pub observed_generation: i64,
    pub message:          Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum ClusterPhase {
    Pending,
    Provisioning,
    Running,
    Degraded,
    Failed,
}

// ── Defaults ──────────────────────────────────────────────────────────────────

fn default_storage_class() -> String { "standard".into() }
fn default_storage_size()  -> String { "50Gi".into() }
fn default_image()         -> String { "ghcr.io/infralens/infralens-server:latest".into() }
fn default_memtable_size() -> u64    { 64 * 1024 * 1024 }
fn default_partition_hours() -> u64  { 1 }
fn default_l0_trigger()    -> usize  { 4 }
