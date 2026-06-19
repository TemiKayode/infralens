# InfraLens quickstart demo — Windows (PowerShell 5.1+)
#
# Starts the full stack with Docker Compose, waits for health, injects
# sample telemetry, runs queries, and optionally tears down.
#
# Usage:
#   .\demos\quickstart.ps1
#   .\demos\quickstart.ps1 -SkipTeardown   # keep the stack running

param(
    [switch]$SkipTeardown
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir

$Gateway    = "http://localhost:8080"
$IngestHttp = "http://localhost:4318"

function Info  { param($msg) Write-Host "[infralens] $msg" -ForegroundColor Green }
function Warn  { param($msg) Write-Host "[warn] $msg"      -ForegroundColor Yellow }
function Die   { param($msg) Write-Host "[error] $msg"     -ForegroundColor Red; exit 1 }

function WaitFor {
    param($Url, $Name, $Retries = 30)
    Info "Waiting for $Name at $Url ..."
    for ($i = 0; $i -lt $Retries; $i++) {
        try {
            $null = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
            Info "$Name is ready."
            return
        } catch { Start-Sleep -Seconds 2 }
    }
    Die "$Name did not become healthy after $($Retries * 2) seconds."
}

function RunQuery {
    param($Label, $Iql)
    Info "Query: $Label"
    Write-Host "  IQL: $Iql"
    try {
        $body   = @{ query = $Iql } | ConvertTo-Json -Compress
        $resp   = Invoke-RestMethod -Method Post -Uri "$Gateway/v1/query" `
                      -ContentType "application/json" -Body $body -ErrorAction Stop
        $lines  = ($resp -split "`n") | Select-Object -First 3
        Write-Host "  Result (first 3 rows):"
        $lines | ForEach-Object { Write-Host "    $_" }
    } catch {
        Warn "  Query failed: $_"
    }
    Write-Host ""
}

# ── Preflight ──────────────────────────────────────────────────────────────────

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Die "Docker is required: https://docs.docker.com/get-docker/"
}

Set-Location $RepoRoot

# ── Start stack ────────────────────────────────────────────────────────────────

Info "Starting InfraLens stack (docker compose up -d) ..."
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { Die "docker compose up failed" }

WaitFor "$IngestHttp/healthz" "infralens-server" 40
WaitFor "$Gateway/healthz"    "api-gateway"       20

# ── Ingest sample data ─────────────────────────────────────────────────────────

Info "Sending sample log records ..."
try {
    $logsPayload = Get-Content "$ScriptDir\payloads\sample-logs.json" -Raw
    Invoke-RestMethod -Method Post -Uri "$IngestHttp/v1/logs" `
        -ContentType "application/json" -Body $logsPayload | Out-Null
    Info "  Logs sent."
} catch { Warn "  Could not send logs: $_" }

Info "Sending sample metric data points ..."
try {
    $metricsPayload = Get-Content "$ScriptDir\payloads\sample-metrics.json" -Raw
    Invoke-RestMethod -Method Post -Uri "$IngestHttp/v1/metrics" `
        -ContentType "application/json" -Body $metricsPayload | Out-Null
    Info "  Metrics sent."
} catch { Warn "  Could not send metrics: $_" }

Info "Sending sample trace spans ..."
try {
    $tracesPayload = Get-Content "$ScriptDir\payloads\sample-traces.json" -Raw
    Invoke-RestMethod -Method Post -Uri "$IngestHttp/v1/traces" `
        -ContentType "application/json" -Body $tracesPayload | Out-Null
    Info "  Traces sent."
} catch { Warn "  Could not send traces: $_" }

Info "Waiting 3 s for flush ..."
Start-Sleep -Seconds 3

# ── Run queries ────────────────────────────────────────────────────────────────

Write-Host ""
Info "Running example IQL queries ..."
Write-Host ""

RunQuery "Recent logs" `
    "SELECT timestamp_ns, severity_text, body, service_name FROM logs WHERE timestamp_ns >= now() - INTERVAL '5 minutes' ORDER BY timestamp_ns DESC LIMIT 5"

RunQuery "Error logs" `
    "SELECT timestamp_ns, body, service_name FROM logs WHERE severity_number >= 17 AND timestamp_ns >= now() - INTERVAL '5 minutes' ORDER BY timestamp_ns DESC LIMIT 5"

RunQuery "Metric avg by service" `
    "SELECT service_name, avg(value) AS avg_latency FROM metrics WHERE metric_name = 'http.request.duration' AND timestamp_ns >= now() - INTERVAL '5 minutes' GROUP BY service_name"

RunQuery "Slow traces" `
    "SELECT trace_id, body AS span_name, service_name, duration_ns FROM traces WHERE timestamp_ns >= now() - INTERVAL '5 minutes' ORDER BY duration_ns DESC LIMIT 5"

# ── Summary ────────────────────────────────────────────────────────────────────

Write-Host ""
Info "Demo complete! Service endpoints:"
Write-Host "  OTLP gRPC        localhost:4317"
Write-Host "  OTLP HTTP        $IngestHttp"
Write-Host "  API Gateway      $Gateway"
Write-Host "  Prometheus       http://localhost:9091"
Write-Host "  Grafana          http://localhost:3000  (admin / admin)"
Write-Host "  MinIO console    http://localhost:9001  (minioadmin / minioadmin123)"
Write-Host ""

# ── Optional teardown ──────────────────────────────────────────────────────────

if ($SkipTeardown) {
    Info "SkipTeardown specified — stack left running."
    exit 0
}

$ans = Read-Host "Tear down the stack? [y/N]"
if ($ans -match "^[Yy]$") {
    Info "Tearing down ..."
    docker compose down
    Info "Done."
} else {
    Info "Stack left running. Stop it later with: docker compose down"
}
