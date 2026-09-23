# Logian Ingestion Server

Go HTTP service that accepts logs, metrics, traces, and AI spans and writes
them into the ClickHouse schema in `schema.sql`.

## Architecture

```
producers --HTTP--> [Server: 4x Buffer[T]] --batch insert--> ClickHouse
                                                                  ^
                                                                  |
                                                     Grafana (ClickHouse datasource, pull-based)
```

One important correction on the spec: **Grafana doesn't get data
"broadcast" to it.** Grafana has no ingestion path — it's a query/render
layer that polls a datasource on a refresh interval. So the pipeline is
"push into ClickHouse, Grafana pulls from ClickHouse," not "push into
both." `docker-compose.grafana.yml` + the provisioning file under
`grafana/provisioning/datasources/` wire Grafana's official ClickHouse
plugin to point at your existing ClickHouse container, pre-configured
so you don't have to click through the datasource UI.

## Running it

First bring up ClickHouse and Grafana together:

```bash
docker compose up -d
# ClickHouse: localhost:9000 (native) / localhost:8123 (HTTP)
# Grafana:    http://localhost:3000
```

`schema.sql` is mounted into ClickHouse's `docker-entrypoint-initdb.d/`,
so it runs automatically on first container start — no manual
`clickhouse-client < schema.sql` step needed. Grafana's datasource is
pre-provisioned to point at the `clickhouse` service by name, so
there's no manual network-connect step either (this replaces the
earlier two-compose-file setup, now that ClickHouse's version is
pinned and part of this repo rather than assumed to be running
elsewhere).

```bash
go mod tidy   # resolves clickhouse-go/v2 and generates go.sum
go build -o bin/ingest ./cmd/server
CLICKHOUSE_PASSWORD=devpassword CLICKHOUSE_ADDR=localhost:9000 ./bin/ingest
```

Config is env-driven (see `internal/config/config.go` for the full list
and defaults): `INGEST_HTTP_ADDR`, `CLICKHOUSE_ADDR`, `CLICKHOUSE_DATABASE`,
`CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_MAX_OPEN_CONNS`,
`CLICKHOUSE_MAX_IDLE_CONNS`, `CLICKHOUSE_ASYNC_INSERT`,
`INGEST_MAX_BATCH_SIZE`, `INGEST_FLUSH_INTERVAL`, `INGEST_CHANNEL_BUFFER`.


### Sending data

Each endpoint takes either a single JSON object or a JSON array (send a
batch in one request if your producer already buffers client-side):

```bash
curl -X POST localhost:8080/v1/logs -d '{
  "timestamp": "2026-08-26T10:00:00Z",
  "trace_id": "abc123", "span_id": "s1",
  "service_name": "checkout-api", "severity": "ERROR", "env": "prod",
  "k8s_cluster": "prod-use1", "k8s_namespace_name": "checkout",
  "k8s_resource_name": "checkout-api-7d9f", 
  "instrumentation_library_name": "otel-go", "instrumentation_library_version": "1.30.0",
  "body": "payment gateway timeout",
  "attributes": {"http.status_code": "504"}
}'

curl -X POST localhost:8080/v1/ai-spans -d '{
  "trace_id": "abc123", "span_id": "s2", "timestamp": "2026-08-26T10:00:00Z",
  "service_name": "checkout-api", "agent_name": "refund-agent",
  "span_kind": "LLM", "llm_model": "claude-sonnet-4-6",
  "prompt_tokens": 512, "completion_tokens": 128, "total_tokens": 640,
  "cost_usd": 0.004, "duration_ns": 850000000, "status_code": "OK",
  "attributes": {}
}'
```

`/v1/incidents` and `/v1/alert-triggers` follow the same shape.
`incident_id`/`trigger_id` in schema.sql have `DEFAULT
generateUUIDv4()`, but that default only fires on plain `INSERT`
statements — the native batch protocol this server uses bypasses it,
so the server generates the UUID itself if you omit the field
(`internal/ingest/handlers.go`'s `normalizeIncident`/`normalizeAlertTrigger`).
`alert_triggers.incident_id` is a foreign key you set to correlate a
trigger to an incident — the server leaves it alone (including
all-zero) rather than guessing:

```bash
curl -X POST localhost:8080/v1/incidents -d '{
  "timestamp": "2026-08-26T10:00:00Z", "service_name": "checkout-api",
  "title": "Elevated 5xx rate", "severity": "high", "status": "open",
  "alert_rule_id": "rule-503-rate", "resolved_at": null,
  "labels": {"team": "payments"}
}'

curl -X POST localhost:8080/v1/alert-triggers -d '{
  "incident_id": "<uuid returned above, or omit to leave uncorrelated>",
  "timestamp": "2026-08-26T10:00:05Z", "status": "firing",
  "message": "5xx rate above 5% for 2m", "service_name": "checkout-api",
  "trigger_count": 3
}'
```

`GET /healthz` returns `{"status":"ok","dropped":{...}}` — the
`dropped` counters per table tell you if a buffer is overflowing (i.e.
ClickHouse can't keep up with intake, and you should raise
`INGEST_CHANNEL_BUFFER`/batch size or shard the writer).


### Why buffered batching, not insert-per-request

ClickHouse is a columnar OLAP store — it's built for large, infrequent
inserts, not one-row-per-HTTP-request. Row-at-a-time inserts against
`MergeTree` create a new part per insert, and ClickHouse background-merges
those parts constantly; enough small inserts and merge pressure becomes
the bottleneck, not the disk or network. So each of the four signal
types gets its own `Buffer[T]` (`internal/ingest/buffer.go`):

- HTTP handlers push into a per-table **channel** and return `202` immediately
  — the request never waits on a ClickHouse round trip.
- A single background goroutine per table drains that channel into a
  slice and flushes via `PrepareBatch`/`Append`/`Send` — ClickHouse's
  native batch insert path — whichever comes first:
  - the batch hits `INGEST_MAX_BATCH_SIZE` (default 5000 rows), or
  - `INGEST_FLUSH_INTERVAL` elapses (default 2s).
- If the channel is full (default buffer 50,000/table), `Push` returns
  `false` instead of blocking; the handler reports which rows were
  rejected as `503` so a well-behaved producer can back off, instead of
  silently losing data or piling up goroutines waiting on a full channel.

This is the standard shape for high-throughput ClickHouse ingestion
(it's what OTel collector exporters and Vector's ClickHouse sink do
internally) — decouple accept-rate from write-rate with a bounded queue.

### Where the PoC cuts corners

You said no restrictions needed yet, so the following are deliberately
absent — flag them if this moves past capstone/PoC:

- **No auth** on the ingest endpoints.
- **No schema validation** beyond "does this JSON deserialize" — a
  garbage `service_name` will happily get written.
- **No retry/dead-letter path** on flush failure — a failed batch is
  logged and dropped. Fine for a demo; not fine once something depends
  on the data landing.
- **No compression tuning / TLS** — `useTLS` is wired but off by default
  since you said ClickHouse is in a local container.

## Next steps worth doing before this is more than a PoC

- Add auth (even a static bearer token) before this touches anything
  real.
- Add a Prometheus `/metrics` endpoint (batch flush latency, dropped
  counts, ClickHouse insert errors) — you already have the counters in
  `Buffer`, just need to export them instead of only exposing them via
  `/healthz`.
