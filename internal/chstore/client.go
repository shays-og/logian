// Package chstore wraps the ClickHouse native driver with one batch
// insert method per Logian table. Column order in each Append call
// must match schema.sql exactly — the driver appends positionally.
package chstore

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/logian/ingest/internal/config"
	"github.com/logian/ingest/internal/models"
)

type Client struct {
	conn driver.Conn
}

// NewClient opens a pooled native-protocol connection to ClickHouse.
// async_insert is opt-in (see config.ClickHouseConfig.AsyncInsert) —
// off by default because this server's own Buffer already batches
// client-side; enabling both just adds a redundant second queue.
func NewClient(ctx context.Context, cfg config.ClickHouseConfig, useTLS bool) (*Client, error) {
	settings := clickhouse.Settings{}
	if cfg.AsyncInsert() {
		settings["async_insert"] = 1
		settings["wait_for_async_insert"] = 0
	}

	opts := &clickhouse.Options{
		Addr: []string{cfg.Addr()},
		Auth: clickhouse.Auth{
			Database: cfg.Database(),
			Username: cfg.Username(),
			Password: cfg.Password().Reveal(),
		},
		Settings:             settings,
		DialTimeout:          5 * time.Second,
		MaxOpenConns:         cfg.MaxOpenConns(),
		MaxIdleConns:         cfg.MaxIdleConns(),
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	}
	if useTLS {
		opts.TLS = &tls.Config{}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) InsertLogs(ctx context.Context, rows []models.LogEntry) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO logian.logs (
			timestamp, trace_id, span_id, service_name, severity, env,
			k8s_cluster, k8s_namespace_name, k8s_resource_name,
			instrumentation_library_name, instrumentation_library_version,
			body, attributes
		)`)
	if err != nil {
		return fmt.Errorf("prepare logs batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.Timestamp, r.TraceID, r.SpanID, r.ServiceName, r.Severity, r.Env,
			r.K8sCluster, r.K8sNamespaceName, r.K8sResourceName,
			r.InstrumentationLibraryName, r.InstrumentationLibraryVersion,
			r.Body, r.Attributes,
		); err != nil {
			return fmt.Errorf("append log row: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) InsertTraces(ctx context.Context, rows []models.Trace) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO logian.traces (
			trace_id, span_id, parent_span_id, timestamp, service_name,
			operation_name, span_kind, duration_ns, status_code, attributes
		)`)
	if err != nil {
		return fmt.Errorf("prepare traces batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.TraceID, r.SpanID, r.ParentSpanID, r.Timestamp, r.ServiceName,
			r.OperationName, r.SpanKind, r.DurationNs, r.StatusCode, r.Attributes,
		); err != nil {
			return fmt.Errorf("append trace row: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) InsertMetrics(ctx context.Context, rows []models.Metric) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO logian.metrics (
			timestamp, metric_name, service_name, metric_type, value,
			bucket_bounds, bucket_counts, quantiles, quantile_values,
			sum, count, labels
		)`)
	if err != nil {
		return fmt.Errorf("prepare metrics batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.Timestamp, r.MetricName, r.ServiceName, r.MetricType, r.Value,
			r.BucketBounds, r.BucketCounts, r.Quantiles, r.QuantileValues,
			r.Sum, r.Count, r.Labels,
		); err != nil {
			return fmt.Errorf("append metric row: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) InsertIncidents(ctx context.Context, rows []models.Incident) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO logian.incidents (
			incident_id, timestamp, service_name, title, severity, status,
			alert_rule_id, resolved_at, labels
		)`)
	if err != nil {
		return fmt.Errorf("prepare incidents batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.IncidentID, r.Timestamp, r.ServiceName, r.Title, r.Severity, r.Status,
			r.AlertRuleID, r.ResolvedAt, r.Labels,
		); err != nil {
			return fmt.Errorf("append incident row: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) InsertAlertTriggers(ctx context.Context, rows []models.AlertTrigger) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO logian.alert_triggers (
			trigger_id, incident_id, timestamp, status, message, service_name, trigger_count
		)`)
	if err != nil {
		return fmt.Errorf("prepare alert_triggers batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.TriggerID, r.IncidentID, r.Timestamp, r.Status, r.Message, r.ServiceName, r.TriggerCount,
		); err != nil {
			return fmt.Errorf("append alert_trigger row: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) InsertAISpans(ctx context.Context, rows []models.AISpan) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO logian.ai_spans (
			trace_id, span_id, timestamp, service_name, agent_name, span_kind,
			llm_model, prompt_tokens, completion_tokens, total_tokens,
			cost_usd, duration_ns, status_code, attributes
		)`)
	if err != nil {
		return fmt.Errorf("prepare ai_spans batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.TraceID, r.SpanID, r.Timestamp, r.ServiceName, r.AgentName, r.SpanKind,
			r.LLMModel, r.PromptTokens, r.CompletionTokens, r.TotalTokens,
			r.CostUSD, r.DurationNs, r.StatusCode, r.Attributes,
		); err != nil {
			return fmt.Errorf("append ai_span row: %w", err)
		}
	}
	return batch.Send()
}
