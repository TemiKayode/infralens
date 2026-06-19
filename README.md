# InfraLens

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.79%2B-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#building)

A production-grade, horizontally scalable observability platform — ingest logs, metrics, and traces via OpenTelemetry, store them in a custom columnar LSM engine, query them with a SQL-like language, and get AI-powered root cause analysis from an LLM copilot.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [What Is Built](#what-is-built)
3. [Prerequisites](#prerequisites)
4. [Directory Layout](#directory-layout)
5. [Building](#building)
6. [Running Locally (single node)](#running-locally-single-node)
7. [Running with Docker Compose (full stack)](#running-with-docker-compose-full-stack)
8. [Running on Kubernetes (production)](#running-on-kubernetes-production)
9. [Development Loop with Tilt](#development-loop-with-tilt)
10. [Sending Telemetry Data](#sending-telemetry-data)
11. [Querying with IQL](#querying-with-iql)
12. [LLM Copilot](#llm-copilot)
13. [Configuration Reference](#configuration-reference)
14. [Running Tests](#running-tests)
15. [Port Reference](#port-reference)
16. [Phase-by-Phase Summary](#phase-by-phase-summary)
17. [Troubleshooting](#troubleshooting)
18. [Architecture Deep Dives](#architecture-deep-dives)
19. [Contributing](#contributing)
20. [License](#license)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Clients / SDKs                                  │
│          (OpenTelemetry SDKs, curl, otel-collector, your services)           │
└──────────┬─────────────────────────────────────────────────┬─────────────────┘
           │ OTLP/gRPC :4317                                 │ OTLP/HTTP :4318
           ▼                                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         InfraLens Server  (Rust)                             │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │  Ingest Pipeline                                                    │     │
│  │  OTLP gRPC receiver ──►  Normaliser ──►  bounded channel           │     │
│  │  OTLP HTTP receiver ──/                         │                  │     │
│  └──────────────────────────────────────────────────┼──────────────────┘     │
│                                                     ▼                        │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Storage Engine  (LSM — columnar Parquet)                            │    │
│  │                                                                      │    │
│  │  WAL (per-partition) ──► MemTable (BTreeMap) ──► FlushWorker        │    │
│  │                                                        │             │    │
│  │                                    ┌───────────────────┘             │    │
│  │                                    ▼                                 │    │
│  │              SSTable  (.parquet + .bloom + .zonemap)                 │    │
│  │                         CompactionWorker (merge L0→L1)              │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Query Engine  (IQL)                                                 │    │
│  │  Lexer ──► Parser ──► AST ──► Planner ──► Optimizer ──► Executor    │    │
│  │  (predicate pushdown, partition pruning, zone-map skipping)          │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Cluster Layer (distributed mode)                                    │    │
│  │  Consistent-hash ring ──► etcd membership ──► scatter/gather RPC    │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────┬────────────────────────────────────────── ┘
                                   │ gRPC :5317  (internal)
                     ┌─────────────▼──────────────┐
                     │   API Gateway  (Go / chi)   │
                     │   JWT + API-key auth        │
                     │   NDJSON streaming          │
                     │   :8080                     │
                     └─────────────┬───────────────┘
                                   │ HTTP
                     ┌─────────────▼───────────────┐
                     │   LLM Copilot  (Python)      │
                     │   GBNF constrained decoding  │
                     │   RCA pipeline               │
                     │   Feedback loop (SQLite)     │
                     │   :8081                      │
                     └─────────────────────────────-┘
```

### Storage on disk

```
{data_dir}/
└── partitions/
    └── 2024120114/          ← YYYYMMDDHH  (1-hour bucket, configurable)
        ├── logs/
        │   ├── wal.log
        │   ├── 0000001.parquet
        │   ├── 0000001.bloom
        │   └── 0000001.zonemap
        ├── metrics/
        └── spans/
```

---

## What Is Built

| Phase | Crate / Service | Description |
|-------|----------------|-------------|
| 1 | `infralens-common` | Shared types, Arrow schemas, config |
| 1 | `infralens-proto` | Generated OTLP protobuf + tonic stubs (pure-Rust, no `protoc`) |
| 1 | `infralens-storage` | WAL, MemTable, SSTable (Parquet), bloom filter, zone maps, compaction |
| 1 | `infralens-ingest` | OTLP/gRPC + HTTP receivers, normaliser, bounded ingest pipeline |
| 1 | `infralens-server` | Binary entry point, Prometheus metrics, structured logging |
| 2 | `infralens-cluster` | Consistent-hash ring, etcd membership, WAL replication |
| 2 | `infralens-rpc` | Scatter/gather gRPC, internal query server |
| 3 | `infralens-query` | IQL lexer/parser/AST, logical planner, optimizer (predicate pushdown, partition pruning), vectorised Arrow executor, temporal functions |
| 3 | `services/api-gateway` | Go HTTP gateway, JWT + API-key auth, NDJSON streaming |
| 4 | `services/llm-copilot` | Python FastAPI, llama-cpp GBNF constrained decoding, RCA, feedback SQLite loop |
| 5 | `deploy/kubernetes/operator` | kube-rs Kubernetes operator, `InfraLensCluster` CRD |
| 5 | `deploy/helm/infralens` | Helm chart for production deployment |
| 5 | `Tiltfile` | Live-reload dev environment on Kubernetes |
| 5 | `docker-compose.yml` | Full local stack (etcd, MinIO, Prometheus, Grafana) |

---

## Prerequisites

| Tool | Min Version | Purpose |
|------|-------------|---------|
| Rust | 1.79 (stable) | Build the Rust workspace |
| Docker | 24 | Run the full stack locally |
| Docker Compose | v2 (`docker compose`) | Orchestrate local services |
| Go | 1.23 | Build the API gateway (`services/api-gateway`) |
| Python | 3.11 | Run the LLM copilot (`services/llm-copilot`) |
| kubectl | 1.29 | Kubernetes deployments |
| helm | 3.14 | Install the Helm chart |
| tilt | 0.33 | Live-reload dev environment (optional) |

> **No external `protoc` needed.** Proto compilation uses the pure-Rust `protox` crate — `cargo build` just works.

---

## Directory Layout

```
infralens/
├── Cargo.toml                  Workspace manifest (8 crates)
├── Cargo.lock
├── rust-toolchain.toml         Pins Rust stable
├── Dockerfile                  Multi-stage image for infralens-server
├── docker-compose.yml          Full local stack
├── Tiltfile                    Tilt live-reload config
│
├── config/
│   ├── default.toml            Base configuration
│   └── development.toml        Dev overrides (smaller buffers, debug logs)
│
├── proto/
│   ├── opentelemetry/          OTLP proto definitions
│   └── infralens/internal/     Internal RPC proto definitions
│
├── crates/
│   ├── infralens-common/       Shared types, Arrow schemas
│   ├── infralens-proto/        Generated protobuf stubs
│   ├── infralens-storage/      LSM storage engine
│   ├── infralens-ingest/       OTLP receivers + pipeline
│   ├── infralens-server/       Main binary
│   ├── infralens-cluster/      Distributed coordination
│   ├── infralens-rpc/          Internal scatter/gather gRPC
│   └── infralens-query/        IQL query engine
│
├── services/
│   ├── api-gateway/            Go HTTP gateway
│   └── llm-copilot/            Python FastAPI copilot
│
├── deploy/
│   ├── helm/infralens/         Helm chart
│   ├── kubernetes/operator/    kube-rs CRD operator
│   └── prometheus.yml          Prometheus scrape config
│
└── vendor/
    └── etcd-client/            Vendored with protox build (no protoc required)
```

---

## Building

### Rust workspace (all 8 crates)

```bash
# Debug build (fast)
cargo build --workspace

# Release build (optimised — use this for production)
cargo build --release --workspace
```

### API Gateway (Go)

```bash
cd services/api-gateway
go mod tidy          # populate go.sum on first run
go build -o ../../target/api-gateway .
```

### LLM Copilot (Python)

```powershell
# PowerShell (Windows)
cd services/llm-copilot
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

```bash
# bash (macOS / Linux)
cd services/llm-copilot
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> `llama-cpp-python` compiles a small C extension. On Windows this requires the MSVC build tools or mingw. If you hit a compile error, install the pre-built wheel:
> ```bash
> pip install llama-cpp-python --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cpu
> ```

### Docker images

```bash
# InfraLens server
docker build -t infralens-server .

# API Gateway
docker build -t infralens-gateway services/api-gateway

# LLM Copilot
docker build -t infralens-copilot services/llm-copilot
```

---

## Running Locally (single node)

### 1. Start supporting services

```bash
docker compose up -d etcd minio prometheus grafana
```

### 2. Run the server

```powershell
# PowerShell — add Cargo to PATH first if needed
$env:PATH += ";$env:USERPROFILE\.cargo\bin"

# Development mode (verbose logs, smaller buffers)
$env:INFRALENS_ENV = "development"; cargo run --bin infralens-server

# Or build a release binary and run it directly
cargo build --release --bin infralens-server
.\target\release\infralens-server.exe
```

```bash
# bash (macOS / Linux)
INFRALENS_ENV=development cargo run --bin infralens-server
```

### 3. Verify it started

```powershell
# Health check
Invoke-RestMethod http://localhost:4318/healthz
# → ok

# Prometheus metrics (filter to infralens lines)
(Invoke-WebRequest http://localhost:9090/metrics).Content -split "`n" | Select-String "infralens"
```

### 4. Run the API Gateway (second terminal)

```powershell
cd services/api-gateway
$env:QUERY_BACKEND = "localhost:5317"; go run .
# → listening on :8080
```

```bash
# bash
cd services/api-gateway
QUERY_BACKEND=localhost:5317 go run .
```

### 5. Verify the gateway

```powershell
Invoke-RestMethod http://localhost:8080/healthz
# → ok
```

---

## Running with Docker Compose (full stack)

Docker Compose starts the complete production-like environment: etcd, MinIO, InfraLens server, API Gateway, Prometheus, and Grafana.

```bash
# Start everything (except LLM copilot)
docker compose up -d

# Check all services are healthy
docker compose ps

# Watch logs
docker compose logs -f infralens api-gateway
```

### Enable the LLM Copilot

The copilot requires a GGUF model file (~2 GB). Download it first:

```bash
# Download Llama 3.2 3B Instruct (Q4_K_M quantisation, ~2 GB)
mkdir -p models
curl -L -o models/llama-3.2-3b-instruct.Q4_K_M.gguf \
  https://huggingface.co/bartowski/Llama-3.2-3B-Instruct-GGUF/resolve/main/Llama-3.2-3B-Instruct-Q4_K_M.gguf
```

Then start the copilot profile:

```bash
docker compose --profile copilot up -d llm-copilot
```

### Service endpoints after `docker compose up`

| Service | URL |
|---------|-----|
| OTLP gRPC | `localhost:4317` |
| OTLP HTTP | `http://localhost:4318` |
| API Gateway | `http://localhost:8080` |
| LLM Copilot | `http://localhost:8081` |
| MinIO Console | `http://localhost:9001` (minioadmin / minioadmin123) |
| Prometheus | `http://localhost:9091` |
| Grafana | `http://localhost:3000` (admin / admin) |
| etcd | `localhost:2379` |

### Tear down

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop containers, delete volumes (wipes data)
```

---

## Running on Kubernetes (production)

### Option A — Helm chart

```bash
# Create namespace
kubectl create namespace infralens

# Install with defaults (3-node cluster, 50 Gi storage each)
helm install infralens deploy/helm/infralens \
  --namespace infralens \
  --values deploy/helm/infralens/values.yaml

# Watch rollout
kubectl -n infralens rollout status statefulset/infralens

# Get the gateway address
kubectl -n infralens get svc infralens-gateway
```

**Override values for production:**

```bash
helm upgrade infralens deploy/helm/infralens \
  --namespace infralens \
  --set replicaCount=5 \
  --set storage.class=fast-ssd \
  --set storage.size=200Gi \
  --set gateway.env.jwtSecret=my-secret \
  --set copilot.enabled=true \
  --set image.tag=v1.2.3
```

### Option B — Kubernetes Operator

The operator automates cluster lifecycle management via the `InfraLensCluster` CRD.

```bash
# Build and push the operator image
cd deploy/kubernetes/operator
cargo build --release
docker build -t ghcr.io/your-org/infralens-operator:latest .
docker push  ghcr.io/your-org/infralens-operator:latest

# Apply the CRD and operator
kubectl apply -f deploy/helm/infralens/crds/

# Create a cluster
cat <<EOF | kubectl apply -f -
apiVersion: infralens.io/v1alpha1
kind: InfraLensCluster
metadata:
  name: prod
  namespace: infralens
spec:
  replicas: 3
  storageClass: fast-ssd
  image: ghcr.io/your-org/infralens-server:latest
  resources:
    requests:
      cpu: "2"
      memory: "4Gi"
EOF

# Watch the operator reconcile
kubectl -n infralens get infralenscluster prod -w
```

---

## Development Loop with Tilt

Tilt gives you hot-reload on both Kubernetes and Docker, rebuilding only what changed.

```bash
# Requirements: Docker Desktop with Kubernetes, tilt, helm, kubectl

# Start everything
tilt up

# Tilt opens a browser UI at http://localhost:10350
# Any change to crates/ triggers cargo build
# Any change to services/api-gateway/ triggers go build
# Python files in services/llm-copilot/ are synced live (no rebuild)
```

Tilt runs a single etcd + single InfraLens node for dev — no copilot by default (skips the 2 GB model download).

---

## Sending Telemetry Data

### OTLP/HTTP (JSON)

```bash
# Send a log record
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{"key":"service.name","value":{"stringValue":"my-service"}}]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "'$(date +%s%N)'",
          "severityNumber": 9,
          "severityText": "INFO",
          "body": {"stringValue": "hello from infralens"}
        }]
      }]
    }]
  }'

# Send a metric
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "resourceMetrics": [{
      "resource": {
        "attributes": [{"key":"service.name","value":{"stringValue":"my-service"}}]
      },
      "scopeMetrics": [{
        "metrics": [{
          "name": "http.request.duration",
          "gauge": {
            "dataPoints": [{
              "timeUnixNano": "'$(date +%s%N)'",
              "asDouble": 123.4,
              "attributes": [{"key":"http.method","value":{"stringValue":"GET"}}]
            }]
          }
        }]
      }]
    }]
  }'
```

### OTLP/gRPC

Point any OpenTelemetry SDK at `localhost:4317` (or your cluster's service address) without TLS:

```python
# Python example (opentelemetry-sdk)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor

exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
```

```go
// Go example
exp, _ := otlptracehttp.New(ctx,
    otlptracehttp.WithEndpoint("localhost:4318"),
    otlptracehttp.WithInsecure(),
)
```

### otel-collector relay

```yaml
# otel-collector-config.yaml
exporters:
  otlp:
    endpoint: "localhost:4317"
    tls:
      insecure: true

service:
  pipelines:
    logs:
      exporters: [otlp]
    metrics:
      exporters: [otlp]
    traces:
      exporters: [otlp]
```

---

## Querying with IQL

IQL (InfraLens Query Language) is a SQL dialect with built-in temporal functions. Queries go through the API Gateway at `http://localhost:8080/v1/query`.

### Basic syntax

```sql
-- Logs from the last 15 minutes
SELECT timestamp_ns, severity_text, body, service_name
FROM logs
WHERE timestamp_ns >= now() - INTERVAL '15 minutes'
  AND severity_number >= 9
ORDER BY timestamp_ns DESC
LIMIT 100;

-- Metric aggregation by service (last hour)
SELECT
  service_name,
  time_bucket('5 minutes', timestamp_ns) AS bucket,
  avg(value) AS avg_latency_ms
FROM metrics
WHERE metric_name = 'http.request.duration'
  AND timestamp_ns >= now() - INTERVAL '1 hour'
GROUP BY service_name, bucket
ORDER BY bucket DESC;

-- Error rate as a ratio
SELECT
  service_name,
  count(*) FILTER (WHERE severity_number >= 17) * 1.0 / count(*) AS error_rate
FROM logs
WHERE timestamp_ns >= now() - INTERVAL '10 minutes'
GROUP BY service_name;

-- Trace spans slower than 1 second
SELECT trace_id, name, service_name, duration_ns / 1e6 AS duration_ms
FROM traces
WHERE duration_ns > 1000000000
  AND timestamp_ns >= now() - INTERVAL '30 minutes'
ORDER BY duration_ns DESC
LIMIT 50;
```

### Temporal functions

| Function | Description |
|----------|-------------|
| `now()` | Current time as Unix nanoseconds |
| `time_bucket(width, ts)` | Floor timestamp to a fixed bucket width |
| `rate(value, window)` | Per-second rate of change over a window |
| `delta(value, window)` | Absolute change over a window |
| `histogram_quantile(q, col)` | Estimated quantile from histogram data |

### Interval literals

```sql
INTERVAL '5 minutes'
INTERVAL '1 hour'
INTERVAL '30 seconds'
INTERVAL '1 day'
INTERVAL '500ms'   -- or just: 500000000  (nanoseconds)
```

### Sending a query via the API Gateway

```bash
# POST to /v1/query — returns NDJSON rows
curl -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT body, service_name FROM logs WHERE timestamp_ns >= now() - INTERVAL '\''5 minutes'\'' LIMIT 10"}'
```

**Response format (NDJSON):**

```json
{"timestamp_ns":1733000000000000000,"body":"hello from infralens","service_name":"my-service"}
{"timestamp_ns":1733000001000000000,"body":"another log line","service_name":"my-service"}
```

### Authentication

The gateway supports two auth methods (disabled in development):

```bash
# JWT bearer token
curl -H "Authorization: Bearer <your-jwt-token>" ...

# API key
curl -H "X-API-Key: <your-api-key>" ...
```

Set `JWT_SECRET` env var on the gateway to enable JWT validation.

---

## LLM Copilot

The copilot converts natural language into IQL and performs automated root cause analysis.

### Translate natural language to IQL

```bash
curl -X POST http://localhost:8081/v1/nl2iql \
  -H "Content-Type: application/json" \
  -d '{"question": "Show me error logs from the payment service in the last hour"}'
```

Response:

```json
{
  "iql": "SELECT timestamp_ns, severity_text, body FROM logs WHERE service_name = 'payment' AND severity_number >= 17 AND timestamp_ns >= now() - INTERVAL '1 hour' ORDER BY timestamp_ns DESC LIMIT 100",
  "explanation": "Filtering logs for the payment service with ERROR severity (17+) over the last hour.",
  "confidence": 0.94
}
```

### Root cause analysis

```bash
curl -X POST http://localhost:8081/v1/rca \
  -H "Content-Type: application/json" \
  -d '{
    "service": "payment",
    "window_minutes": 30,
    "metric": "http.request.duration"
  }'
```

Response:

```json
{
  "anomalies": [
    {"timestamp_ns": 1733000500000000000, "z_score": 4.2, "value": 892.3}
  ],
  "correlations": [
    {"metric": "db.query.duration", "pearson_r": 0.91, "lag_seconds": 12}
  ],
  "narrative": "A 4.2σ latency spike in the payment service at 14:35 UTC correlates strongly (r=0.91) with increased database query latency with a 12-second lag, suggesting the database is the root cause. The spike resolved itself after 8 minutes."
}
```

### Submit feedback (improves future suggestions)

```bash
curl -X POST http://localhost:8081/v1/feedback \
  -H "Content-Type: application/json" \
  -d '{
    "question": "show payment errors",
    "generated_iql": "SELECT ...",
    "corrected_iql": "SELECT ... AND service_name = '\''payment'\''",
    "rating": 4
  }'
```

### Without a real GGUF model

If no model file is present, the copilot runs in **stub mode** and returns syntactically valid but template-based IQL. All API endpoints still work — useful for integration testing.

---

## Configuration Reference

Configuration is layered (highest priority wins):

1. Environment variables: `INFRALENS__<SECTION>__<KEY>` (double underscore separators)
2. `config/{INFRALENS_ENV}.toml`
3. `config/default.toml`

Set `INFRALENS_ENV=development` for development overrides.

### `config/default.toml`

```toml
[server]
grpc_addr    = "0.0.0.0:4317"   # OTLP gRPC
http_addr    = "0.0.0.0:4318"   # OTLP HTTP + /healthz
metrics_addr = "0.0.0.0:9090"   # Prometheus /metrics

[storage]
data_dir                 = "./data"
memtable_size_bytes      = 67108864   # 64 MiB — flush threshold
l0_compaction_trigger    = 4          # compact when 4 L0 files accumulate
compaction_interval_secs = 30
partition_hours          = 1          # time-bucket width
parquet_row_group_size   = 1000000
wal_sync_interval_ms     = 100        # fsync interval

[ingest]
buffer_depth      = 4096    # bounded channel depth
max_batch_records = 1000    # records per write batch

[telemetry]
log_level = "info"    # trace | debug | info | warn | error
json_logs = true      # false for human-readable dev logs
```

### Key environment variables

```bash
# Override any config key
INFRALENS__STORAGE__DATA_DIR=/mnt/nvme/data
INFRALENS__STORAGE__MEMTABLE_SIZE_BYTES=134217728  # 128 MiB
INFRALENS__TELEMETRY__LOG_LEVEL=debug

# Cluster mode
INFRALENS__CLUSTER__ETCD_ENDPOINTS=http://etcd:2379
INFRALENS__CLUSTER__NODE_ID=node-1

# Gateway
GATEWAY_ADDR=:8080
QUERY_BACKEND=infralens:5317
JWT_SECRET=your-secret-here

# Copilot
COPILOT_MODEL_PATH=/models/llama-3.2-3b-instruct.Q4_K_M.gguf
COPILOT_N_GPU_LAYERS=35      # 0 = CPU only; 35+ = GPU (requires CUDA)
COPILOT_GATEWAY_URL=http://api-gateway:8080
COPILOT_FEEDBACK_DB_PATH=/data/feedback.db
```

---

## Running Tests

```bash
# All unit + integration tests
cargo test --workspace

# Storage engine integration tests specifically
cargo test -p infralens-storage

# Query engine tests
cargo test -p infralens-query

# With output shown (useful for debugging)
cargo test --workspace -- --nocapture

# Release mode (faster for large integration tests)
cargo test --workspace --release
```

### Python copilot tests

```bash
cd services/llm-copilot
python -m pytest tests/ -v
```

---

## Port Reference

| Port | Protocol | Service | Purpose |
|------|----------|---------|---------|
| 2379 | TCP | etcd | Cluster membership, leader election |
| 4317 | gRPC | infralens-server | OTLP gRPC ingestion |
| 4318 | HTTP | infralens-server | OTLP HTTP ingestion + `/healthz` |
| 5317 | gRPC | infralens-server | Internal scatter/gather RPC |
| 8080 | HTTP | api-gateway | Query API, auth, NDJSON streaming |
| 8081 | HTTP | llm-copilot | NL→IQL, RCA, feedback endpoints |
| 9000 | HTTP | MinIO | S3-compatible object storage API |
| 9001 | HTTP | MinIO | MinIO web console |
| 9090 | HTTP | infralens-server | Prometheus metrics (`/metrics`) |
| 9091 | HTTP | Prometheus | Prometheus UI (host-mapped) |
| 3000 | HTTP | Grafana | Dashboards |

---

## Phase-by-Phase Summary

### Phase 1 — Ingest & Storage Core

- OTLP/gRPC and OTLP/HTTP receivers with protobuf parsing (no `protoc` required)
- Custom LSM storage engine: WAL → MemTable → flush to Parquet SSTables
- Bloom filters for key existence checks
- Zone maps for time-range pruning (skip entire files without reading them)
- Background compaction worker (merge L0 files to reduce read amplification)
- Time-based partitioning (1-hour buckets by default)
- Prometheus metrics exported at `:9090/metrics`

### Phase 2 — Distributed Coordination

- Consistent-hash ring for stable shard assignment across nodes
- etcd-backed cluster membership: nodes register, watch for joins/leaves
- WAL replication: the ring leader pushes WAL segments to replica nodes
- Scatter/gather RPC: fan out queries to all shards, merge results
- Pure-Rust etcd client (vendored with protox build — no `protoc` needed)

### Phase 3 — Query Engine & API Gateway

- **IQL lexer**: hand-written, zero-allocation tokeniser
- **IQL parser**: recursive-descent with full precedence chain (AND/OR/NOT, BETWEEN, IN, LIKE, IS NULL)
- **Logical planner**: builds `Scan → Filter → Project → Aggregate → Sort → Limit` trees
- **Optimizer** with four rules:
  - Constant folding (`now()` evaluated once at plan time)
  - Predicate pushdown into scan
  - Projection pushdown (read only needed columns from Parquet)
  - Partition pruning via zone maps (skip entire hour buckets)
- **Vectorised Arrow executor**: pull-based, 8192-row batches
- **Temporal functions**: `time_bucket`, `rate`, `delta`, `histogram_quantile`
- **Go API Gateway**: chi router, JWT + API-key middleware, NDJSON streaming

### Phase 4 — LLM Copilot

- **GBNF constrained decoding**: grammar generated from the live catalog — the LLM literally cannot output invalid column names or syntax
- **llama.cpp backend** via `llama-cpp-python` — runs on CPU or GPU
- **RCA pipeline**: z-score anomaly detection (numpy) + Pearson correlation (scipy) across metric pairs, LLM-generated narrative summary
- **Feedback loop**: corrections stored in SQLite, used to refine future prompts via few-shot examples

### Phase 5 — Kubernetes & Operations

- **kube-rs operator**: watches `InfraLensCluster` CRDs, reconciles StatefulSet + headless Service + ConfigMap using server-side apply
- **Helm chart**: production-ready, supports HPA autoscaling, PodDisruptionBudget, Ingress, ServiceMonitor
- **Tiltfile**: live-reload on Kubernetes — only rebuilds the changed service
- **docker-compose.yml**: complete local stack with health checks and startup ordering

---

## Troubleshooting

### `cargo build` fails with proto errors

The workspace uses a vendored `etcd-client` with a pure-Rust build (no `protoc`). If you see proto errors:

```bash
# Ensure the vendor path is present
ls vendor/etcd-client/build.rs   # should exist

# Force a clean rebuild of build scripts
cargo clean -p etcd-client
cargo build --workspace
```

### Server won't start — "address already in use"

```bash
# Find what's on port 4317
netstat -ano | findstr 4317   # Windows
lsof -i :4317                 # Linux/macOS

# Or change the port via env
INFRALENS__SERVER__GRPC_ADDR=0.0.0.0:14317 cargo run --bin infralens-server
```

### etcd connection refused

```bash
# Check etcd is running
docker compose ps etcd

# Start it if not
docker compose up -d etcd

# Test connectivity
curl http://localhost:2379/health
```

### LLM copilot returns stub responses

The copilot is in stub mode (no model loaded). Download the model:

```bash
mkdir -p models
curl -L -o models/llama-3.2-3b-instruct.Q4_K_M.gguf \
  https://huggingface.co/bartowski/Llama-3.2-3B-Instruct-GGUF/resolve/main/Llama-3.2-3B-Instruct-Q4_K_M.gguf

# Then set the path
COPILOT_MODEL_PATH=./models/llama-3.2-3b-instruct.Q4_K_M.gguf python services/llm-copilot/main.py
```

### Gateway returns 401 Unauthorized

JWT auth is enabled but no secret is configured, or your token is wrong:

```bash
# Disable auth for development (empty JWT_SECRET)
JWT_SECRET= go run services/api-gateway/main.go
```

### High memory usage

The MemTable grows until it hits `memtable_size_bytes` (default 64 MiB per partition). Reduce it for dev:

```bash
INFRALENS__STORAGE__MEMTABLE_SIZE_BYTES=4194304  # 4 MiB
```

### Parquet files not being compacted

Compaction triggers when `l0_compaction_trigger` L0 files accumulate (default 4). Force faster compaction:

```bash
INFRALENS__STORAGE__L0_COMPACTION_TRIGGER=2
INFRALENS__STORAGE__COMPACTION_INTERVAL_SECS=5
```

### Query returns no results

1. Verify data was ingested: `curl http://localhost:4318/healthz` returns `ok`
2. Check the time range — default queries use `now()` so clock skew matters
3. Check logs for flush errors: `docker compose logs infralens | Select-String flush` (PowerShell) or `| grep flush` (bash)
4. Widen the time window: `WHERE timestamp_ns >= now() - INTERVAL '1 day'`

---

## Quick Reference Card

```powershell
# ── Build ─────────────────────────────────────────────────────────────────────
$env:PATH += ";$env:USERPROFILE\.cargo\bin"       # add cargo to session PATH
cargo build --workspace                            # debug
cargo build --release --bin infralens-server       # release binary

# ── Run locally ───────────────────────────────────────────────────────────────
docker compose up -d etcd minio prometheus grafana
$env:INFRALENS_ENV = "development"; cargo run --bin infralens-server

# ── Full stack ────────────────────────────────────────────────────────────────
docker compose up -d
docker compose --profile copilot up -d llm-copilot   # optional

# ── Send data ─────────────────────────────────────────────────────────────────
$body = Get-Content demos\payloads\sample-logs.json -Raw -Encoding UTF8
Invoke-RestMethod -Uri http://localhost:4318/v1/logs -Method POST -ContentType "application/json" -Body $body

# ── Query ─────────────────────────────────────────────────────────────────────
$q = @{ query = "SELECT body FROM logs WHERE timestamp_ns >= now() - INTERVAL '5 minutes' LIMIT 5;" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8080/api/v1/query -Method POST -ContentType "application/json" -Body $q

# ── Copilot ───────────────────────────────────────────────────────────────────
$q = @{ question = "show me errors from the last 30 minutes" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8081/v1/nl2iql -Method POST -ContentType "application/json" -Body $q

# ── Tests ─────────────────────────────────────────────────────────────────────
cargo test --workspace
cargo test -p infralens-storage
cargo test -p infralens-query
```

---

## Architecture Deep Dives

Detailed technical documentation lives in [`docs/architecture/`](docs/architecture/):

| Document | Contents |
|----------|---------|
| [overview.md](docs/architecture/overview.md) | Component map, write/read paths, technology choices |
| [storage-engine.md](docs/architecture/storage-engine.md) | WAL, MemTable, SSTable (Parquet), bloom filters, zone maps, compaction |
| [query-engine.md](docs/architecture/query-engine.md) | IQL lexer/parser, logical planner, optimizer rules, Arrow executor |
| [cluster.md](docs/architecture/cluster.md) | Consistent-hash ring, etcd membership, WAL replication, scatter/gather |
| [llm-copilot.md](docs/architecture/llm-copilot.md) | GBNF grammar generation, RCA pipeline, feedback loop |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide, including:
- Development environment setup
- Crate dependency rules
- Formatting and linting requirements per language
- Commit message conventions
- Pull request process

Quick checks to run before opening a PR:

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cd services/api-gateway && go fmt ./... && go vet ./... && go test ./...
```

---

## License

Copyright 2026 TemiKayode

Licensed under the [Apache License, Version 2.0](LICENSE).
