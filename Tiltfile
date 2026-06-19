# InfraLens Tiltfile — live-reload development environment
#
# Prerequisites:
#   - Docker Desktop with Kubernetes enabled (or kind/minikube)
#   - tilt (https://tilt.dev)
#   - kubectl
#   - helm

# ── Settings ───────────────────────────────────────────────────────────────────

allow_k8s_contexts('docker-desktop', 'kind-infralens', 'minikube')

# ── Rust — infralens-server ────────────────────────────────────────────────────

local_resource(
    'cargo-build',
    cmd='cargo build --release -p infralens-server',
    deps=['crates/', 'Cargo.toml', 'Cargo.lock'],
    labels=['build'],
)

docker_build(
    'ghcr.io/infralens/infralens-server',
    '.',
    dockerfile='Dockerfile',
    only=['target/release/infralens-server', 'config/'],
    # Sync the binary without full rebuild when only config changes
    live_update=[
        sync('config/', '/app/config/'),
    ],
)

# ── Go — api-gateway ──────────────────────────────────────────────────────────

local_resource(
    'go-build',
    cmd='cd services/api-gateway && go build -o ../../target/api-gateway .',
    deps=['services/api-gateway/'],
    labels=['build'],
)

docker_build(
    'ghcr.io/infralens/api-gateway',
    'services/api-gateway',
    dockerfile='services/api-gateway/Dockerfile',
    live_update=[
        sync('services/api-gateway/', '/app/'),
        run('go build -o /app/api-gateway /app/*.go', trigger=['services/api-gateway/*.go']),
        restart_container(),
    ],
)

# ── Python — llm-copilot ──────────────────────────────────────────────────────

docker_build(
    'ghcr.io/infralens/llm-copilot',
    'services/llm-copilot',
    dockerfile='services/llm-copilot/Dockerfile',
    live_update=[
        # Sync Python files directly without Docker rebuild
        sync('services/llm-copilot/', '/app/'),
        run('pip install -q -r /app/requirements.txt',
            trigger=['services/llm-copilot/requirements.txt']),
        restart_container(),
    ],
)

# ── Helm chart ────────────────────────────────────────────────────────────────

k8s_yaml(helm(
    'deploy/helm/infralens',
    name='infralens',
    namespace='infralens',
    values=['deploy/helm/infralens/values.yaml'],
    set=[
        'image.pullPolicy=Never',       # Use locally built images
        'gateway.image.pullPolicy=Never',
        'copilot.enabled=false',        # Disable in dev (large model)
        'etcd.replicaCount=1',          # Single etcd in dev
        'replicaCount=1',               # Single server in dev
        'storage.size=5Gi',
    ],
))

# ── Port forwards ──────────────────────────────────────────────────────────────

k8s_resource(
    'infralens',
    port_forwards=[
        '4317:4317',   # OTLP gRPC
        '4318:4318',   # OTLP HTTP
        '9090:9090',   # Prometheus
    ],
    labels=['infralens'],
)

# ── Local dependencies (etcd for dev) ────────────────────────────────────────

local_resource(
    'etcd-dev',
    serve_cmd='docker run --rm -p 2379:2379 bitnami/etcd:3.5 '
              '--advertise-client-urls http://0.0.0.0:2379 '
              '--listen-client-urls http://0.0.0.0:2379 '
              '--allow-none-authentication',
    labels=['deps'],
)
