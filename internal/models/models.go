// Package models defines the JSON wire format accepted by the ingestion
// server. Field names/types map 1:1 onto schema.sql so batch inserts can
// pass slices straight through to the ClickHouse driver.
package models

import (
	"time"

	"github.com/google/uuid"
)

// LogEntry mirrors logian.logs.
type LogEntry struct {
	Timestamp                      time.Time         `json:"timestamp"`
	TraceID                        string            `json:"trace_id"`
	SpanID                         string            `json:"span_id"`
	ServiceName                    string            `json:"service_name"`
	Severity                       string            `json:"severity"`
	Env                            string            `json:"env"`
	K8sCluster                     string            `json:"k8s_cluster"`
	K8sNamespaceName                string            `json:"k8s_namespace_name"`
	K8sResourceName                 string            `json:"k8s_resource_name"`
	InstrumentationLibraryName      string            `json:"instrumentation_library_name"`
	InstrumentationLibraryVersion   string            `json:"instrumentation_library_version"`
	Body                            string            `json:"body"`
	Attributes                      map[string]string `json:"attributes"`
}

// Trace mirrors logian.traces (the join spine).
type Trace struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id"`
	Timestamp     time.Time         `json:"timestamp"`
	ServiceName   string            `json:"service_name"`
	OperationName string            `json:"operation_name"`
	SpanKind      string            `json:"span_kind"`
	DurationNs    uint64            `json:"duration_ns"`
	StatusCode    string            `json:"status_code"`
	Attributes    map[string]string `json:"attributes"`
}

// Metric mirrors logian.metrics. BucketBounds/BucketCounts/Quantiles are
// only populated for histogram/summary metric_type rows; leave them nil
// (they'll be sent as empty arrays) for counter/gauge.
type Metric struct {
	Timestamp      time.Time         `json:"timestamp"`
	MetricName     string            `json:"metric_name"`
	ServiceName    string            `json:"service_name"`
	MetricType     string            `json:"metric_type"`
	Value          float64           `json:"value"`
	BucketBounds   []float64         `json:"bucket_bounds"`
	BucketCounts   []uint64          `json:"bucket_counts"`
	Quantiles      []float64         `json:"quantiles"`
	QuantileValues []float64         `json:"quantile_values"`
	Sum            float64           `json:"sum"`
	Count          uint64            `json:"count"`
	Labels         map[string]string `json:"labels"`
}

// AISpan mirrors logian.ai_spans (LLM/agent telemetry).
type AISpan struct {
	TraceID          string            `json:"trace_id"`
	SpanID           string            `json:"span_id"`
	Timestamp        time.Time         `json:"timestamp"`
	ServiceName      string            `json:"service_name"`
	AgentName        string            `json:"agent_name"`
	SpanKind         string            `json:"span_kind"`
	LLMModel         string            `json:"llm_model"`
	PromptTokens     uint32            `json:"prompt_tokens"`
	CompletionTokens uint32            `json:"completion_tokens"`
	TotalTokens      uint32            `json:"total_tokens"`
	CostUSD          float64           `json:"cost_usd"`
	DurationNs       uint64            `json:"duration_ns"`
	StatusCode       string            `json:"status_code"`
	Attributes       map[string]string `json:"attributes"`
}

// Incident mirrors logian.incidents. IncidentID has DEFAULT
// generateUUIDv4() in schema.sql, but batch inserts bypass column
// defaults, so the server fills it in if the producer leaves it zero
// (see ingest.handleIncidents). ResolvedAt is nullable — leave it nil
// for open incidents.
type Incident struct {
	IncidentID  uuid.UUID         `json:"incident_id"`
	Timestamp   time.Time         `json:"timestamp"`
	ServiceName string            `json:"service_name"`
	Title       string            `json:"title"`
	Severity    string            `json:"severity"`
	Status      string            `json:"status"`
	AlertRuleID string            `json:"alert_rule_id"`
	ResolvedAt  *time.Time        `json:"resolved_at"`
	Labels      map[string]string `json:"labels"`
}

// AlertTrigger mirrors logian.alert_triggers. TriggerID is filled in
// server-side like IncidentID above if omitted. IncidentID here is a
// foreign key to Incident.IncidentID — the producer should set it to
// correlate a trigger to an incident; left as uuid.Nil if the trigger
// hasn't been correlated to one yet.
type AlertTrigger struct {
	TriggerID    uuid.UUID `json:"trigger_id"`
	IncidentID   uuid.UUID `json:"incident_id"`
	Timestamp    time.Time `json:"timestamp"`
	Status       string    `json:"status"`
	Message      string    `json:"message"`
	ServiceName  string    `json:"service_name"`
	TriggerCount uint32    `json:"trigger_count"`
}
