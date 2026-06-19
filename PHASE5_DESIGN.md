# InfraLens Phase 5 — Observability & Operations

## 1. Overview

Phase 5 delivers the production deployment stack:

| Component | Purpose |
|-----------|---------|
| Kubernetes operator (`kube-rs`) | Manages `InfraLensCluster` CRD; handles scaling, rolling updates, and self-healing |
| Helm chart | Templated Kubernetes manifests for one-command installs |
| Tiltfile | Live-reload development environment; ties Rust build + Docker + k8s in one loop |
| docker-compose update | Full local stack including LLM copilot |

---

## 2. Custom Resource: `InfraLensCluster`

```yaml
apiVersion: infralens.io/v1alpha1
kind: InfraLensCluster
metadata:
  name: prod
spec:
  replicas:    3
  storageClass: fast-nvme
  storageSize:  100Gi
  image:        ghcr.io/infralens/infralens-server:latest
  etcdEndpoints:
    - http://etcd-0.etcd:2379
    - http://etcd-1.etcd:2379
    - http://etcd-2.etcd:2379
  config:
    memtableSizeBytes:   67108864  # 64 MiB
    partitionHours:      1
    l0CompactionTrigger: 4
  resources:
    requests: { cpu: "1", memory: "2Gi" }
    limits:   { cpu: "4", memory: "8Gi" }
```

### Operator reconciliation loop

```
Watch InfraLensCluster → diff desired vs actual
  ├── Scale up:   create StatefulSet pods + register in etcd
  ├── Scale down: drain pod, deregister from ring, delete pod
  ├── Update:     rolling restart (one pod at a time)
  └── Health:     requeue every 30s; patch .status.phase
```

---

## 3. Helm Chart Structure

```
deploy/helm/infralens/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── statefulset.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   ├── serviceaccount.yaml
│   ├── rbac.yaml
│   ├── hpa.yaml
│   ├── pdb.yaml
│   └── NOTES.txt
└── crds/
    └── infralenscluster.yaml
```

---

## 4. Tiltfile

Tilt provides a live-reload dev loop:
1. Watch Rust source → `cargo build --release` → rebuild Docker image layer
2. Watch Go source → `go build` → rebuild api-gateway image
3. Watch Python source → sync files into running container (no rebuild)
4. `kubectl apply` updated manifests → rolling pod restart
5. Forward ports: `:4317` (OTLP gRPC), `:4318` (OTLP HTTP), `:8080` (API gateway), `:8081` (copilot)

---

## 5. Self-observability (dogfooding)

InfraLens observes itself:
- All crates emit OTLP metrics to `localhost:4317` via `opentelemetry-otlp`
- Spans from the query execution path are exported to the local InfraLens server
- A Grafana dashboard (`deploy/grafana/infralens-overview.json`) shows:
  - Ingest throughput (logs/metrics/spans per second)
  - Storage flush latency (p50/p95/p99)
  - Query latency histogram
  - Cluster ring membership
  - SSTable count per signal
