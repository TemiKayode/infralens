"""
InfraLens Load Test — Locust

Usage:
    locust -f locustfile.py --headless -u 200 -r 50 --run-time 60s \
           --host http://localhost:4318

Targets:
  POST /v1/logs    — OTLP JSON ingest (primary write path)
  POST /v1/metrics — OTLP JSON metrics ingest
  GET  /healthz    — health endpoint baseline
"""

import json
import time
from locust import HttpUser, task, between, constant_throughput

# ── Payloads ──────────────────────────────────────────────────────────────────

def _log_payload(i: int) -> str:
    return json.dumps({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "load-test"}},
                {"key": "host.name",    "value": {"stringValue": f"node-{i % 10}"}},
            ]},
            "scopeLogs": [{"scope": {"name": "load.test"}, "logRecords": [{
                "timeUnixNano": str(time.time_ns()),
                "severityNumber": 9,
                "severityText": "INFO",
                "body": {"stringValue": f"load test record {i}"},
                "attributes": [
                    {"key": "request_id", "value": {"stringValue": f"req-{i}"}},
                    {"key": "latency_ms", "value": {"doubleValue": float(i % 500)}},
                ],
            }]}],
        }]
    })

def _metric_payload(i: int) -> str:
    return json.dumps({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "load-test"}},
            ]},
            "scopeMetrics": [{"scope": {"name": "load.test"}, "metrics": [{
                "name": "http.request.duration",
                "unit": "ms",
                "gauge": {"dataPoints": [{
                    "timeUnixNano": str(time.time_ns()),
                    "asDouble": float(i % 1000),
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/payments"}},
                    ],
                }]},
            }]}],
        }]
    })

# ── Ingest user (write path) ───────────────────────────────────────────────────

class IngestUser(HttpUser):
    """Simulates an application shipping OTLP telemetry to InfraLens."""

    # Each virtual user fires ~5 req/s; with 200 users → ~1000 RPS baseline
    wait_time = between(0.1, 0.3)
    _counter = 0

    @task(8)
    def ingest_log(self):
        IngestUser._counter += 1
        self.client.post(
            "/v1/logs",
            data=_log_payload(IngestUser._counter),
            headers={"Content-Type": "application/json"},
            name="POST /v1/logs",
        )

    @task(2)
    def ingest_metric(self):
        IngestUser._counter += 1
        self.client.post(
            "/v1/metrics",
            data=_metric_payload(IngestUser._counter),
            headers={"Content-Type": "application/json"},
            name="POST /v1/metrics",
        )


# ── Health-check user (baseline) ──────────────────────────────────────────────

class HealthUser(HttpUser):
    """Baseline health-check traffic — should be near 0ms p99."""
    wait_time = constant_throughput(10)  # 10 req/s per user

    @task
    def health(self):
        self.client.get("/healthz", name="GET /healthz")
