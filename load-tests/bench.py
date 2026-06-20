"""
InfraLens throughput benchmark — no external dependencies beyond aiohttp.

Usage:
    python bench.py [--url http://localhost:4318] [--workers 50] [--duration 30] [--ramp 10000]

Outputs a Markdown-formatted results table to stdout.
"""

import asyncio
import argparse
import json
import time
import statistics
from dataclasses import dataclass, field
from typing import List

import aiohttp

# ── Payload builders ──────────────────────────────────────────────────────────

def log_payload(i: int) -> bytes:
    return json.dumps({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "bench"}},
                {"key": "host.name",    "value": {"stringValue": f"node-{i % 8}"}},
            ]},
            "scopeLogs": [{"scope": {"name": "bench"}, "logRecords": [{
                "timeUnixNano": str(time.time_ns()),
                "severityNumber": 9,
                "severityText": "INFO",
                "body": {"stringValue": f"bench record {i}"},
                "attributes": [
                    {"key": "idx",        "value": {"intValue": str(i)}},
                    {"key": "latency_ms", "value": {"doubleValue": float(i % 300)}},
                ],
            }]}],
        }]
    }).encode()


# ── Result accumulator ────────────────────────────────────────────────────────

@dataclass
class Results:
    latencies_ms: List[float] = field(default_factory=list)
    successes:    int = 0
    failures:     int = 0
    start:        float = 0.0
    end:          float = 0.0

    def elapsed(self) -> float:
        return self.end - self.start

    def rps(self) -> float:
        t = self.elapsed()
        return (self.successes + self.failures) / t if t > 0 else 0

    def p(self, pct: float) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        idx = max(0, int(len(s) * pct / 100) - 1)
        return s[idx]


# ── Worker ────────────────────────────────────────────────────────────────────

async def worker(
    session:   aiohttp.ClientSession,
    url:       str,
    results:   Results,
    stop_event: asyncio.Event,
    counter:   list,
):
    while not stop_event.is_set():
        i = counter[0]
        counter[0] += 1
        payload = log_payload(i)
        t0 = time.perf_counter()
        try:
            async with session.post(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                await resp.read()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                if resp.status == 200:
                    results.successes += 1
                    results.latencies_ms.append(elapsed_ms)
                else:
                    results.failures += 1
        except Exception:
            results.failures += 1


# ── Ramp ──────────────────────────────────────────────────────────────────────

async def run_stage(label: str, url: str, workers: int, duration: int) -> Results:
    results = Results()
    stop    = asyncio.Event()
    counter = [0]

    connector = aiohttp.TCPConnector(limit=workers + 20, force_close=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        results.start = time.perf_counter()
        tasks = [
            asyncio.create_task(worker(session, url, results, stop, counter))
            for _ in range(workers)
        ]
        await asyncio.sleep(duration)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        results.end = time.perf_counter()

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url",      default="http://localhost:4318/v1/logs")
    parser.add_argument("--duration", type=int, default=20, help="seconds per stage")
    args = parser.parse_args()

    stages = [
        ("Warm-up   (  10 workers)",  10),
        ("Low       (  25 workers)",  25),
        ("Medium    (  50 workers)",  50),
        ("High      ( 100 workers)", 100),
        ("Peak      ( 200 workers)", 200),
        ("Stress    ( 400 workers)", 400),
    ]

    print(f"\n{'InfraLens OTLP/HTTP Ingest Benchmark':^90}")
    print(f"{'Target: ' + args.url:^90}")
    print(f"{'Duration per stage: ' + str(args.duration) + 's':^90}\n")
    print(f"{'Stage':<30} {'Workers':>8} {'RPS':>8} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8} {'Errors':>8}")
    print("-" * 90)

    all_results = []
    for label, w in stages:
        r = await run_stage(label, args.url, w, args.duration)
        all_results.append((label, w, r))
        errors = f"{r.failures:,}" if r.failures else "0"
        print(
            f"{label:<30} {w:>8,} {r.rps():>8.0f} "
            f"{r.p(50):>8.1f} {r.p(95):>8.1f} {r.p(99):>8.1f} {errors:>8}"
        )

    # Summary
    peak = max(all_results, key=lambda x: x[2].rps())
    print("-" * 90)
    print(f"\nPeak throughput : {peak[2].rps():,.0f} RPS  ({peak[0].strip()})")
    if peak[2].latencies_ms:
        print(f"Best p50 latency: {peak[2].p(50):.1f} ms")
        print(f"Best p99 latency: {peak[2].p(99):.1f} ms")
    print(f"Total requests  : {sum(r.successes + r.failures for _, _, r in all_results):,}")
    print(f"Total errors    : {sum(r.failures for _, _, r in all_results):,}")
    print()

if __name__ == "__main__":
    asyncio.run(main())
