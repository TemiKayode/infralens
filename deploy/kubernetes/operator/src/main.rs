//! InfraLens Kubernetes Operator
//!
//! Watches `InfraLensCluster` CRDs and reconciles the desired cluster state
//! by managing StatefulSets, Services, and ConfigMaps.

mod crd;
mod reconciler;

use futures::StreamExt;
use kube::{
    runtime::{watcher::Config as WatcherConfig, Controller},
    Api, Client, CustomResourceExt,
};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crd::InfraLensCluster;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(fmt::layer().json())
        .init();

    info!("InfraLens operator starting");

    let client = Client::try_default().await?;

    // Print CRD YAML on --print-crd flag (for cluster bootstrapping)
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--print-crd") {
        println!("{}", serde_json::to_string_pretty(&InfraLensCluster::crd())?);
        return Ok(());
    }

    let crds: Api<InfraLensCluster> = Api::all(client.clone());
    let ctx = Arc::new(reconciler::Context { client });

    Controller::new(crds, WatcherConfig::default())
        .run(reconciler::reconcile, reconciler::error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) =>
                    info!(name = %obj.name, ns = ?obj.namespace, "Reconciled"),
                Err(e) =>
                    error!(error = %e, "Reconcile error"),
            }
        })
        .await;

    Ok(())
}
