# Demos

End-to-end runnable demos for InfraLens.

---

## Quickstart (full stack)

Starts the complete Docker Compose stack, injects sample telemetry, runs IQL queries,
and prints results.

**Linux / macOS:**
```bash
bash demos/quickstart.sh
```

**Windows (PowerShell):**
```powershell
.\demos\quickstart.ps1
```

**Keep the stack running after the demo:**
```bash
SKIP_TEARDOWN=1 bash demos/quickstart.sh   # Linux/macOS
.\demos\quickstart.ps1 -SkipTeardown       # Windows
```

Prerequisites: Docker with Compose v2, `curl` (Linux/macOS).

---

## Sample Payloads

Pre-built OTLP JSON payloads you can send with a single `curl` command:

### Logs

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @demos/payloads/sample-logs.json
```

Contains 7 log records from `payment-service` and `api-gateway` at various severity
levels (INFO, WARN, ERROR, FATAL).

### Metrics

```bash
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -d @demos/payloads/sample-metrics.json
```

Contains `http.request.duration`, `db.query.duration`, and `payment.success_rate`
gauges for `payment-service` and `api-gateway`, including a simulated latency spike.

### Traces

```bash
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d @demos/payloads/sample-traces.json
```

Contains 5 spans across 3 traces: one successful payment, one failed payment (Stripe
gateway timeout), and one retry success.

---

## Example Queries

After ingesting the sample data, try these IQL queries via the API Gateway:

```bash
GATEWAY=http://localhost:8080

# Recent logs (last 5 minutes)
curl -s -X POST $GATEWAY/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT timestamp_ns, severity_text, body FROM logs WHERE timestamp_ns >= now() - INTERVAL '\''5 minutes'\'' ORDER BY timestamp_ns DESC LIMIT 10"}'

# Errors only
curl -s -X POST $GATEWAY/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT timestamp_ns, service_name, body FROM logs WHERE severity_number >= 17 AND timestamp_ns >= now() - INTERVAL '\''5 minutes'\''"}'

# Average latency by service
curl -s -X POST $GATEWAY/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT service_name, avg(value) AS avg_ms FROM metrics WHERE metric_name = '\''http.request.duration'\'' AND timestamp_ns >= now() - INTERVAL '\''5 minutes'\'' GROUP BY service_name"}'

# Slowest spans
curl -s -X POST $GATEWAY/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT trace_id, body AS name, service_name, duration_ns / 1000000 AS duration_ms FROM traces WHERE timestamp_ns >= now() - INTERVAL '\''5 minutes'\'' ORDER BY duration_ns DESC LIMIT 5"}'
```

---

## LLM Copilot Demo (requires model)

If you have the LLM copilot running (`docker compose --profile copilot up -d`):

```bash
# Natural language to IQL
curl -s -X POST http://localhost:8081/v1/nl2iql \
  -H "Content-Type: application/json" \
  -d '{"question": "show me errors from the payment service in the last 30 minutes"}'

# Root cause analysis
curl -s -X POST http://localhost:8081/v1/rca \
  -H "Content-Type: application/json" \
  -d '{"service": "payment-service", "window_minutes": 30, "metric": "http.request.duration"}'
```
