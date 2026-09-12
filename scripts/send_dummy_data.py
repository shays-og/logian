#!/usr/bin/env python3
"""
Logian Ingestion Server - Dummy Telemetry Generator
Generates realistic logs, traces, metrics, AI spans, incidents, and alert triggers
and sends them to the Go ingestion server over HTTP.

Requirements: Python 3.8+ (Zero external dependencies - uses standard library)
"""

import argparse
import datetime
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Dict, List, Optional, Tuple

SERVICES = [
    "checkout-api",
    "auth-service",
    "payment-gateway",
    "inventory-service",
    "ai-copilot",
    "recommendation-engine",
]

ENVIRONMENTS = ["prod", "staging", "dev"]
K8S_CLUSTERS = ["us-east-1-prod", "eu-central-1-prod", "us-west-2-staging"]
SEVERITIES = ["INFO", "INFO", "INFO", "WARN", "ERROR", "DEBUG"]
LLM_MODELS = ["gpt-4o", "claude-3-5-sonnet", "gemini-1.5-pro", "llama-3.1-70b"]
AGENTS = ["support-agent", "refund-assistant", "code-reviewer", "sql-generator"]


def now_iso() -> str:
    """Returns current UTC timestamp in ISO 8601 format."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def gen_hex(length: int) -> str:
    """Generates a random hex string of given length."""
    return "".join(random.choices("0123456789abcdef", k=length))


def generate_trace_and_logs(
    service: str, env: str, cluster: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Generates a correlated trace tree with parent/child spans, logs, and optional AI span."""
    trace_id = gen_hex(32)
    root_span_id = gen_hex(16)
    child_span_id = gen_hex(16)
    ts = now_iso()

    has_error = random.random() < 0.15
    root_status = "ERROR" if has_error else "OK"
    duration_ns = random.randint(15_000_000, 350_000_000)

    # 1. Root Span
    root_span = {
        "trace_id": trace_id,
        "span_id": root_span_id,
        "parent_span_id": "",
        "timestamp": ts,
        "service_name": service,
        "operation_name": f"HTTP POST /{service.split('-')[0]}/process",
        "span_kind": "SERVER",
        "duration_ns": duration_ns,
        "status_code": root_status,
        "attributes": {
            "http.method": "POST",
            "http.status_code": "500" if has_error else "200",
            "environment": env,
        },
    }

    # 2. Child Span
    child_span = {
        "trace_id": trace_id,
        "span_id": child_span_id,
        "parent_span_id": root_span_id,
        "timestamp": ts,
        "service_name": service,
        "operation_name": "db.query",
        "span_kind": "INTERNAL",
        "duration_ns": int(duration_ns * random.uniform(0.2, 0.7)),
        "status_code": root_status,
        "attributes": {
            "db.system": "postgresql",
            "db.statement": "SELECT * FROM orders WHERE id = $1",
        },
    }

    traces = [root_span, child_span]

    # 3. Correlated Logs
    logs = [
        {
            "timestamp": ts,
            "trace_id": trace_id,
            "span_id": root_span_id,
            "service_name": service,
            "severity": "INFO",
            "env": env,
            "k8s_cluster": cluster,
            "k8s_namespace_name": "core",
            "k8s_resource_name": f"{service}-{gen_hex(5)}",
            "instrumentation_library_name": "otel-go",
            "instrumentation_library_version": "1.30.0",
            "body": f"Incoming request to /{service.split('-')[0]}/process",
            "attributes": {"user.id": f"usr_{random.randint(1000, 9999)}"},
        }
    ]

    if has_error:
        logs.append(
            {
                "timestamp": ts,
                "trace_id": trace_id,
                "span_id": child_span_id,
                "service_name": service,
                "severity": "ERROR",
                "env": env,
                "k8s_cluster": cluster,
                "k8s_namespace_name": "core",
                "k8s_resource_name": f"{service}-{gen_hex(5)}",
                "instrumentation_library_name": "otel-go",
                "instrumentation_library_version": "1.30.0",
                "body": "Database query timed out after 5000ms",
                "attributes": {"error.type": "TimeoutError"},
            }
        )

    # 4. Correlated AI Span
    ai_span = None
    if service == "ai-copilot" or random.random() < 0.35:
        prompt_tokens = random.randint(150, 2000)
        completion_tokens = random.randint(50, 800)
        total_tokens = prompt_tokens + completion_tokens
        cost = round((prompt_tokens * 0.000005) + (completion_tokens * 0.000015), 5)
        model = random.choice(LLM_MODELS)
        agent = random.choice(AGENTS)

        ai_span = {
            "trace_id": trace_id,
            "span_id": gen_hex(16),
            "timestamp": ts,
            "service_name": service,
            "agent_name": agent,
            "span_kind": "LLM",
            "llm_model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cost_usd": cost,
            "duration_ns": random.randint(500_000_000, 3_000_000_000),
            "status_code": "OK" if not has_error else "ERROR",
            "attributes": {
                "temperature": "0.7",
                "prompt.type": "user_query",
            },
        }

    return traces, logs, ai_span


def generate_metrics(service: str) -> List[Dict[str, Any]]:
    """Generates synthetic counter, gauge, and histogram metrics."""
    ts = now_iso()
    metrics = []

    # Counter
    metrics.append(
        {
            "timestamp": ts,
            "metric_name": "http_requests_total",
            "service_name": service,
            "metric_type": "counter",
            "value": float(random.randint(1, 20)),
            "bucket_bounds": [],
            "bucket_counts": [],
            "quantiles": [],
            "quantile_values": [],
            "sum": 0.0,
            "count": 0,
            "labels": {
                "handler": "/process",
                "status": "200" if random.random() > 0.1 else "500",
            },
        }
    )

    # Gauge
    metrics.append(
        {
            "timestamp": ts,
            "metric_name": "process_cpu_usage_percent",
            "service_name": service,
            "metric_type": "gauge",
            "value": round(random.uniform(5.0, 85.0), 2),
            "bucket_bounds": [],
            "bucket_counts": [],
            "quantiles": [],
            "quantile_values": [],
            "sum": 0.0,
            "count": 0,
            "labels": {"instance": f"{service}-pod-1"},
        }
    )

    # Histogram
    bounds = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    counts = sorted([random.randint(5, 50) for _ in bounds])
    metrics.append(
        {
            "timestamp": ts,
            "metric_name": "http_request_duration_seconds",
            "service_name": service,
            "metric_type": "histogram",
            "value": 0.0,
            "bucket_bounds": bounds,
            "bucket_counts": counts,
            "quantiles": [0.5, 0.9, 0.99],
            "quantile_values": [0.045, 0.12, 0.45],
            "sum": round(random.uniform(10.0, 100.0), 2),
            "count": counts[-1],
            "labels": {"handler": "/process"},
        }
    )

    return metrics


def generate_incident_and_trigger(
    service: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Generates an incident and correlated alert trigger."""
    ts = now_iso()
    incident_id = str(uuid.uuid4())
    severity = random.choice(["high", "medium", "critical"])
    alert_rule_id = f"rule-{service}-high-error-rate"

    incident = {
        "incident_id": incident_id,
        "timestamp": ts,
        "service_name": service,
        "title": f"Elevated error rate on {service}",
        "severity": severity,
        "status": "open",
        "alert_rule_id": alert_rule_id,
        "resolved_at": None,
        "labels": {"team": service.split("-")[0], "environment": "prod"},
    }

    trigger = {
        "trigger_id": str(uuid.uuid4()),
        "incident_id": incident_id,
        "timestamp": ts,
        "status": "firing",
        "message": f"Error rate reached {random.randint(5, 25)}% in last 5m",
        "service_name": service,
        "trigger_count": random.randint(1, 10),
    }

    return incident, trigger


def send_batch(base_url: str, endpoint: str, payload: List[Dict[str, Any]]) -> Tuple[bool, int, str]:
    """Sends a JSON batch via HTTP POST using standard urllib."""
    url = f"{base_url.rstrip('/')}{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            return True, resp.status, body
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8") if e.fp else str(e)
        return False, e.code, err_body
    except urllib.error.URLError as e:
        return False, 0, str(e.reason)
    except Exception as e:
        return False, 0, str(e)


def check_health(base_url: str) -> bool:
    """Checks if the ingestion server is healthy before sending data."""
    url = f"{base_url.rstrip('/')}/healthz"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                print(f"[HEALTH] Ingestion server reachable at {base_url} (status: {data.get('status')})")
                return True
    except Exception as e:
        print(f"[WARNING] /healthz unreachable at {url}: {e}")
        return False
    return False


def run_generator(
    base_url: str,
    batch_size: int,
    total_batches: Optional[int],
    interval: float,
    continuous: bool,
    emit_incidents: bool,
):
    print("=" * 65)
    print(f" Logian Dummy Telemetry Generator")
    print(f" Target Server : {base_url}")
    print(f" Batch Size    : {batch_size} items per signal")
    print(f" Interval      : {interval}s")
    print(f" Mode          : {'Continuous (Ctrl+C to stop)' if continuous else f'{total_batches} batch(es)'}")
    print("=" * 65)

    check_health(base_url)

    batch_idx = 0
    while continuous or (total_batches is not None and batch_idx < total_batches):
        batch_idx += 1
        t_start = time.time()

        all_traces: List[Dict[str, Any]] = []
        all_logs: List[Dict[str, Any]] = []
        all_ai_spans: List[Dict[str, Any]] = []
        all_metrics: List[Dict[str, Any]] = []
        all_incidents: List[Dict[str, Any]] = []
        all_triggers: List[Dict[str, Any]] = []

        for _ in range(batch_size):
            service = random.choice(SERVICES)
            env = random.choice(ENVIRONMENTS)
            cluster = random.choice(K8S_CLUSTERS)

            traces, logs, ai_span = generate_trace_and_logs(service, env, cluster)
            all_traces.extend(traces)
            all_logs.extend(logs)
            if ai_span:
                all_ai_spans.append(ai_span)

            all_metrics.extend(generate_metrics(service))

            if emit_incidents and random.random() < 0.25:
                inc, trig = generate_incident_and_trigger(service)
                all_incidents.append(inc)
                all_triggers.append(trig)

        endpoints = [
            ("/v1/traces", all_traces, "traces"),
            ("/v1/logs", all_logs, "logs"),
            ("/v1/metrics", all_metrics, "metrics"),
        ]
        if all_ai_spans:
            endpoints.append(("/v1/ai-spans", all_ai_spans, "ai_spans"))
        if all_incidents:
            endpoints.append(("/v1/incidents", all_incidents, "incidents"))
        if all_triggers:
            endpoints.append(("/v1/alert-triggers", all_triggers, "alert_triggers"))

        results = []
        for ep, data, name in endpoints:
            ok, status, _ = send_batch(base_url, ep, data)
            tag = f"{status}" if ok else f"ERR:{status}"
            results.append(f"{name}:{len(data)}[{tag}]")

        elapsed_ms = (time.time() - t_start) * 1000
        now_str = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{now_str}] Batch #{batch_idx:04d} -> " + " | ".join(results) + f" ({elapsed_ms:.1f}ms)")

        if continuous or (total_batches is not None and batch_idx < total_batches):
            time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(
        description="Emit dummy telemetry data (logs, metrics, traces, AI spans, incidents) to Logian ingest server."
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8080",
        help="Base URL of the Logian ingestion server (default: http://localhost:8080)",
    )
    parser.add_argument(
        "-n",
        "--batches",
        type=int,
        default=5,
        help="Number of batches to send when not in continuous mode (default: 5)",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=10,
        help="Number of items per signal per batch (default: 10)",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=1.0,
        help="Interval in seconds between batches (default: 1.0)",
    )
    parser.add_argument(
        "-c",
        "--continuous",
        action="store_true",
        help="Run continuously until stopped with Ctrl+C",
    )
    parser.add_argument(
        "--no-incidents",
        action="store_true",
        help="Do not emit incidents and alert triggers",
    )

    args = parser.parse_args()

    try:
        run_generator(
            base_url=args.url,
            batch_size=args.batch_size,
            total_batches=None if args.continuous else args.batches,
            interval=args.interval,
            continuous=args.continuous,
            emit_incidents=not args.no_incidents,
        )
    except KeyboardInterrupt:
        print("\n[STOPPED] Dummy data generator stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()