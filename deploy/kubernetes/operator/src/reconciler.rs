//! Reconciliation logic for `InfraLensCluster`.

use std::sync::Arc;

use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        core::v1::{
            ConfigMap, Container, EnvVar, PersistentVolumeClaim, PersistentVolumeClaimSpec,
            PodSpec, PodTemplateSpec, ResourceRequirements, Service, ServicePort, ServiceSpec,
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::LabelSelector,
    },
};
use kube::{
    api::{ObjectMeta, Patch, PatchParams},
    runtime::controller::Action,
    Api, Client, Resource, ResourceExt,
};
use serde_json::json;
use std::collections::BTreeMap;
use tokio::time::Duration;
use tracing::{info, warn};

use crate::crd::{ClusterPhase, InfraLensCluster, InfraLensClusterStatus};

pub struct Context {
    pub client: Client,
}

pub async fn reconcile(
    cluster: Arc<InfraLensCluster>,
    ctx:     Arc<Context>,
) -> Result<Action, kube::Error> {
    let ns   = cluster.namespace().unwrap_or_default();
    let name = cluster.name_any();

    info!(name = %name, ns = %ns, "Reconciling InfraLensCluster");

    // 1. Reconcile ConfigMap
    reconcile_configmap(&cluster, &ctx.client, &ns).await?;

    // 2. Reconcile Service (headless for StatefulSet DNS)
    reconcile_service(&cluster, &ctx.client, &ns).await?;

    // 3. Reconcile StatefulSet
    reconcile_statefulset(&cluster, &ctx.client, &ns).await?;

    // 4. Update status
    update_status(&cluster, &ctx.client, &ns).await?;

    // Requeue every 30 s for health monitoring
    Ok(Action::requeue(Duration::from_secs(30)))
}

pub fn error_policy(
    _cluster: Arc<InfraLensCluster>,
    err:      &kube::Error,
    _ctx:     Arc<Context>,
) -> Action {
    warn!(error = %err, "Reconcile failed; retrying in 10s");
    Action::requeue(Duration::from_secs(10))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn labels(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".into(),     "infralens".into()),
        ("app.kubernetes.io/instance".into(), name.into()),
        ("app.kubernetes.io/component".into(), "server".into()),
    ])
}

async fn reconcile_configmap(
    cluster: &InfraLensCluster,
    client:  &Client,
    ns:      &str,
) -> Result<(), kube::Error> {
    let name = format!("{}-config", cluster.name_any());
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);

    let etcd_csv = cluster.spec.etcd_endpoints.join(",");
    let data = BTreeMap::from([
        ("INFRALENS__STORAGE__MEMTABLE_SIZE_BYTES".into(),
            cluster.spec.config.memtable_size_bytes.to_string()),
        ("INFRALENS__STORAGE__PARTITION_HOURS".into(),
            cluster.spec.config.partition_hours.to_string()),
        ("INFRALENS__CLUSTER__ETCD_ENDPOINTS".into(), etcd_csv),
    ]);

    let cm = ConfigMap {
        metadata: ObjectMeta {
            name:      Some(name.clone()),
            namespace: Some(ns.into()),
            labels:    Some(labels(&cluster.name_any())),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    };

    api.patch(
        &name,
        &PatchParams::apply("infralens-operator"),
        &Patch::Apply(&cm),
    ).await?;
    Ok(())
}

async fn reconcile_service(
    cluster: &InfraLensCluster,
    client:  &Client,
    ns:      &str,
) -> Result<(), kube::Error> {
    let name = cluster.name_any();
    let api: Api<Service> = Api::namespaced(client.clone(), ns);

    let svc = Service {
        metadata: ObjectMeta {
            name:      Some(name.clone()),
            namespace: Some(ns.into()),
            labels:    Some(labels(&name)),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip:   Some("None".into()), // headless
            selector:     Some(labels(&name)),
            ports: Some(vec![
                ServicePort { name: Some("otlp-grpc".into()),  port: 4317, ..Default::default() },
                ServicePort { name: Some("otlp-http".into()),  port: 4318, ..Default::default() },
                ServicePort { name: Some("metrics".into()),    port: 9090, ..Default::default() },
                ServicePort { name: Some("internal".into()),   port: 5317, ..Default::default() },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };

    api.patch(
        &name,
        &PatchParams::apply("infralens-operator"),
        &Patch::Apply(&svc),
    ).await?;
    Ok(())
}

async fn reconcile_statefulset(
    cluster: &InfraLensCluster,
    client:  &Client,
    ns:      &str,
) -> Result<(), kube::Error> {
    let name    = cluster.name_any();
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
    let lbls    = labels(&name);
    let replicas = cluster.spec.replicas as i32;

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name:      Some(name.clone()),
            namespace: Some(ns.into()),
            labels:    Some(lbls.clone()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas:    Some(replicas),
            selector:    LabelSelector { match_labels: Some(lbls.clone()), ..Default::default() },
            service_name: name.clone(),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(lbls.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name:  "infralens".into(),
                        image: Some(cluster.spec.image.clone()),
                        env_from: Some(vec![
                            k8s_openapi::api::core::v1::EnvFromSource {
                                config_map_ref: Some(
                                    k8s_openapi::api::core::v1::ConfigMapEnvSource {
                                        name: Some(format!("{name}-config")),
                                        ..Default::default()
                                    }
                                ),
                                ..Default::default()
                            }
                        ]),
                        env: Some(vec![
                            EnvVar {
                                name:  "POD_NAME".into(),
                                value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                    field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                                        field_path: "metadata.name".into(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                        ]),
                        ports: Some(vec![
                            k8s_openapi::api::core::v1::ContainerPort {
                                name: Some("otlp-grpc".into()), container_port: 4317, ..Default::default()
                            },
                            k8s_openapi::api::core::v1::ContainerPort {
                                name: Some("otlp-http".into()), container_port: 4318, ..Default::default()
                            },
                            k8s_openapi::api::core::v1::ContainerPort {
                                name: Some("metrics".into()), container_port: 9090, ..Default::default()
                            },
                        ]),
                        volume_mounts: Some(vec![
                            k8s_openapi::api::core::v1::VolumeMount {
                                name:       "data".into(),
                                mount_path: "/data".into(),
                                ..Default::default()
                            }
                        ]),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta { name: Some("data".into()), ..Default::default() },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes:       Some(vec!["ReadWriteOnce".into()]),
                    storage_class_name: Some(cluster.spec.storage_class.clone()),
                    resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                        requests: Some(BTreeMap::from([
                            ("storage".into(), Quantity(cluster.spec.storage_size.clone())),
                        ])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    api.patch(
        &name,
        &PatchParams::apply("infralens-operator"),
        &Patch::Apply(&sts),
    ).await?;
    Ok(())
}

async fn update_status(
    cluster: &InfraLensCluster,
    client:  &Client,
    ns:      &str,
) -> Result<(), kube::Error> {
    let name   = cluster.name_any();
    let api: Api<InfraLensCluster> = Api::namespaced(client.clone(), ns);

    let status = InfraLensClusterStatus {
        phase:               ClusterPhase::Running,
        ready_replicas:      cluster.spec.replicas,
        observed_generation: cluster.metadata.generation.unwrap_or(0),
        message:             None,
    };

    api.patch_status(
        &name,
        &PatchParams::apply("infralens-operator"),
        &Patch::Merge(json!({ "status": status })),
    ).await?;
    Ok(())
}
