#!/usr/bin/env bash
# InfraLens quickstart demo — Linux / macOS
#
# Starts the full stack with Docker Compose, waits for health, injects
# sample telemetry, runs queries, and tears down cleanly.
#
# Usage:
#   bash demos/quickstart.sh          # interactive, prompts before teardown
#   SKIP_TEARDOWN=1 bash demos/quickstart.sh   # keep the stack running

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

GATEWAY="http://localhost:8080"
INGEST_HTTP="http://localhost:4318"

GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
NC="\033[0m"

info()    { echo -e "${GREEN}[infralens]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC} $*"; }
die()     { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

wait_for() {
    local url="$1" name="$2" retries="${3:-30}"
    info "Waiting for $name at $url ..."
    for i in $(seq 1 "$retries"); do
        if curl -sf "$url" >/dev/null 2>&1; then
            info "$name is ready."
            return 0
        fi
        sleep 2
    done
    die "$name did not become healthy after $((retries * 2)) seconds."
}

query() {
    local label="$1" iql="$2"
    info "Query: $label"
    echo "  IQL: $iql"
    result=$(curl -sf -X POST "$GATEWAY/v1/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"$iql\"}" || echo '{"error":"query failed"}')
    echo "  Result (first 3 rows):"
    echo "$result" | head -3 | sed 's/^/    /'
    echo ""
}

# ── Preflight ──────────────────────────────────────────────────────────────────

command -v docker  >/dev/null || die "Docker is required: https://docs.docker.com/get-docker/"
command -v curl    >/dev/null || die "curl is required"

cd "$REPO_ROOT"

# ── Start stack ────────────────────────────────────────────────────────────────

info "Starting InfraLens stack (docker compose up -d) ..."
docker compose up -d --build

wait_for "$INGEST_HTTP/healthz"    "infralens-server"  40
wait_for "$GATEWAY/healthz"        "api-gateway"       20

# ── Ingest sample data ─────────────────────────────────────────────────────────

NOW_NS=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time_ns()))")

info "Sending sample log records ..."
curl -sf -X POST "$INGEST_HTTP/v1/logs" \
    -H "Content-Type: application/json" \
    -d @"$SCRIPT_DIR/payloads/sample-logs.json" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print('  Accepted:', d.get('partialSuccess', 'ok'))" 2>/dev/null || \
    info "  Logs sent."

info "Sending sample metric data points ..."
curl -sf -X POST "$INGEST_HTTP/v1/metrics" \
    -H "Content-Type: application/json" \
    -d @"$SCRIPT_DIR/payloads/sample-metrics.json" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print('  Accepted:', d.get('partialSuccess', 'ok'))" 2>/dev/null || \
    info "  Metrics sent."

info "Sending sample trace spans ..."
curl -sf -X POST "$INGEST_HTTP/v1/traces" \
    -H "Content-Type: application/json" \
    -d @"$SCRIPT_DIR/payloads/sample-traces.json" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print('  Accepted:', d.get('partialSuccess', 'ok'))" 2>/dev/null || \
    info "  Traces sent."

# Allow the ingest pipeline to flush
info "Waiting 3 s for flush ..."
sleep 3

# ── Run queries ────────────────────────────────────────────────────────────────

echo ""
info "Running example IQL queries ..."
echo ""

query "Recent logs" \
    "SELECT timestamp_ns, severity_text, body, service_name FROM logs WHERE timestamp_ns >= now() - INTERVAL '5 minutes' ORDER BY timestamp_ns DESC LIMIT 5"

query "Error logs" \
    "SELECT timestamp_ns, body, service_name FROM logs WHERE severity_number >= 17 AND timestamp_ns >= now() - INTERVAL '5 minutes' ORDER BY timestamp_ns DESC LIMIT 5"

query "Metric avg by service" \
    "SELECT service_name, avg(value) AS avg_latency FROM metrics WHERE metric_name = 'http.request.duration' AND timestamp_ns >= now() - INTERVAL '5 minutes' GROUP BY service_name"

query "Slow traces" \
    "SELECT trace_id, body AS span_name, service_name, duration_ns FROM traces WHERE timestamp_ns >= now() - INTERVAL '5 minutes' ORDER BY duration_ns DESC LIMIT 5"

# ── Summary ────────────────────────────────────────────────────────────────────

echo ""
info "Demo complete! Service endpoints:"
echo "  OTLP gRPC        localhost:4317"
echo "  OTLP HTTP        $INGEST_HTTP"
echo "  API Gateway      $GATEWAY"
echo "  Prometheus       http://localhost:9091"
echo "  Grafana          http://localhost:3000  (admin / admin)"
echo "  MinIO console    http://localhost:9001  (minioadmin / minioadmin123)"
echo ""

# ── Optional teardown ──────────────────────────────────────────────────────────

if [[ "${SKIP_TEARDOWN:-0}" == "1" ]]; then
    info "SKIP_TEARDOWN=1 — stack left running."
    exit 0
fi

read -r -p "Tear down the stack? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    info "Tearing down ..."
    docker compose down
    info "Done."
else
    info "Stack left running. Stop it later with: docker compose down"
fi
