-- ============================================================
-- Logian schema for ClickHouse
-- Run with: clickhouse-client --multiquery < schema.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS logian;

-- ------------------------------------------------------------
-- 1. Traces / Spans (the join spine for Logs, AI_Spans, Metrics)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logian.traces
(
    trace_id        String,
    span_id         String,
    parent_span_id  String,
    timestamp       DateTime64(3) CODEC(Delta, ZSTD),
    service_name    LowCardinality(String),
    operation_name  LowCardinality(String),
    span_kind       LowCardinality(String),
    duration_ns     UInt64,
    status_code     LowCardinality(String),
    attributes      Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, timestamp, trace_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

-- ------------------------------------------------------------
-- 2. Raw Logs
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logian.logs
(
    log_id                           UUID DEFAULT generateUUIDv4(),
    timestamp                        DateTime64(3) CODEC(Delta, ZSTD),
    trace_id                         String,
    span_id                          String,
    service_name                     LowCardinality(String),
    severity                         LowCardinality(String),
    env                              LowCardinality(String),
    k8s_cluster                      LowCardinality(String),
    k8s_namespace_name                LowCardinality(String),
    k8s_resource_name                String,
    instrumentation_library_name      LowCardinality(String),
    instrumentation_library_version   LowCardinality(String),
    body                             String,
    attributes                       Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

-- ------------------------------------------------------------
-- 3. Metrics (counter / gauge / histogram / summary)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logian.metrics
(
    timestamp         DateTime64(3) CODEC(Delta, ZSTD),
    metric_name        LowCardinality(String),
    service_name       LowCardinality(String),
    metric_type        LowCardinality(String),
    value              Float64,
    bucket_bounds      Array(Float64),
    bucket_counts      Array(UInt64),
    quantiles          Array(Float64),
    quantile_values     Array(Float64),
    sum                Float64,
    count              UInt64,
    labels             Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, metric_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

-- ------------------------------------------------------------
-- 4. AI Spans (LLM / agent telemetry, joins back to traces)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logian.ai_spans
(
    trace_id           String,
    span_id            String,
    timestamp          DateTime64(3) CODEC(Delta, ZSTD),
    service_name       LowCardinality(String),
    agent_name         LowCardinality(String),
    span_kind          LowCardinality(String),
    llm_model          LowCardinality(String),
    prompt_tokens      UInt32,
    completion_tokens  UInt32,
    total_tokens       UInt32,
    cost_usd           Float64,
    duration_ns        UInt64,
    status_code        LowCardinality(String),
    attributes         Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, agent_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

-- ------------------------------------------------------------
-- 5. Incidents (Active Incidents panel)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logian.incidents
(
    incident_id     UUID DEFAULT generateUUIDv4(),
    timestamp       DateTime64(3) CODEC(Delta, ZSTD),
    service_name    LowCardinality(String),
    title           String,
    severity        LowCardinality(String),
    status          LowCardinality(String),
    alert_rule_id   String,
    resolved_at     Nullable(DateTime64(3)),
    labels          Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (status, service_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY;

-- ------------------------------------------------------------
-- 6. Alert Triggers (Recent Events panel)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logian.alert_triggers
(
    trigger_id      UUID DEFAULT generateUUIDv4(),
    incident_id     UUID,
    timestamp       DateTime64(3) CODEC(Delta, ZSTD),
    status          LowCardinality(String),
    message         String,
    service_name    LowCardinality(String),
    trigger_count   UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (status, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
