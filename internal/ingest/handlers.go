package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/logian/ingest/internal/chstore"
	"github.com/logian/ingest/internal/models"
)

// Server holds the four table buffers and exposes their HTTP handlers.
// Each buffer runs its own background flush goroutine (started in Start).
type Server struct {
	logs          *Buffer[models.LogEntry]
	traces        *Buffer[models.Trace]
	metrics       *Buffer[models.Metric]
	aiSpans       *Buffer[models.AISpan]
	incidents     *Buffer[models.Incident]
	alertTriggers *Buffer[models.AlertTrigger]
	logger        *slog.Logger
}

func NewServer(ch *chstore.Client, cfg BufferConfig, logger *slog.Logger) *Server {
	return &Server{
		logs: NewBuffer("logs", cfg, func(ctx context.Context, rows []models.LogEntry) error {
			return ch.InsertLogs(ctx, rows)
		}, logger),
		traces: NewBuffer("traces", cfg, func(ctx context.Context, rows []models.Trace) error {
			return ch.InsertTraces(ctx, rows)
		}, logger),
		metrics: NewBuffer("metrics", cfg, func(ctx context.Context, rows []models.Metric) error {
			return ch.InsertMetrics(ctx, rows)
		}, logger),
		aiSpans: NewBuffer("ai_spans", cfg, func(ctx context.Context, rows []models.AISpan) error {
			return ch.InsertAISpans(ctx, rows)
		}, logger),
		incidents: NewBuffer("incidents", cfg, func(ctx context.Context, rows []models.Incident) error {
			return ch.InsertIncidents(ctx, rows)
		}, logger),
		alertTriggers: NewBuffer("alert_triggers", cfg, func(ctx context.Context, rows []models.AlertTrigger) error {
			return ch.InsertAlertTriggers(ctx, rows)
		}, logger),
		logger: logger,
	}
}

// Start launches all four buffer flush loops. Blocks until ctx is
// cancelled (call in a goroutine), draining in-flight items on exit.
func (s *Server) Start(ctx context.Context) {
	go s.logs.Run(ctx)
	go s.traces.Run(ctx)
	go s.metrics.Run(ctx)
	go s.aiSpans.Run(ctx)
	go s.incidents.Run(ctx)
	go s.alertTriggers.Run(ctx)
}

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", handleBatch(s.logs, nil))
	mux.HandleFunc("POST /v1/traces", handleBatch(s.traces, nil))
	mux.HandleFunc("POST /v1/metrics", handleBatch(s.metrics, nil))
	mux.HandleFunc("POST /v1/ai-spans", handleBatch(s.aiSpans, nil))
	mux.HandleFunc("POST /v1/incidents", handleBatch(s.incidents, normalizeIncident))
	mux.HandleFunc("POST /v1/alert-triggers", handleBatch(s.alertTriggers, normalizeAlertTrigger))
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

// normalizeIncident fills in IncidentID server-side when the producer
// leaves it zero. schema.sql's DEFAULT generateUUIDv4() only applies
// to plain INSERTs, not the native batch protocol we use, so we
// replicate it here instead of writing a zero UUID to the table.
func normalizeIncident(i *models.Incident) {
	if i.IncidentID == uuid.Nil {
		i.IncidentID = uuid.New()
	}
}

// normalizeAlertTrigger fills in TriggerID the same way. IncidentID is
// left as-is (including uuid.Nil) — it's a foreign key the producer
// sets to correlate a trigger to an incident, not something we should
// invent on its behalf.
func normalizeAlertTrigger(t *models.AlertTrigger) {
	if t.TriggerID == uuid.Nil {
		t.TriggerID = uuid.New()
	}
}

// handleBatch accepts either a single JSON object or a JSON array of
// objects, so producers can send one span or a whole batch in one
// request. It pushes each item into the buffer non-blockingly; if the
// buffer's channel is full, it returns 503 so the client backs off
// instead of the request silently succeeding with data dropped.
// normalize, if non-nil, runs on each decoded item before it's pushed
// (used to fill in server-generated UUIDs for incidents/alert triggers).
func handleBatch[T any](buf *Buffer[T], normalize func(*T)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items, err := decodeOneOrMany[T](r)
		if err != nil {
			http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if len(items) == 0 {
			http.Error(w, "empty payload", http.StatusBadRequest)
			return
		}
		if normalize != nil {
			for i := range items {
				normalize(&items[i])
			}
		}

		accepted := 0
		for _, item := range items {
			if buf.Push(item) {
				accepted++
			}
		}

		if accepted < len(items) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accepted": accepted,
				"rejected": len(items) - accepted,
				"reason":   "ingest buffer full, retry with backoff",
			})
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": accepted})
	}
}

func decodeOneOrMany[T any](r *http.Request) ([]T, error) {
	defer r.Body.Close()

	raw := json.RawMessage{}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, err
	}

	// Peek at the first non-whitespace byte to tell array from object.
	trimmed := trimLeadingSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var many []T
		if err := json.Unmarshal(raw, &many); err != nil {
			return nil, err
		}
		return many, nil
	}

	var one T
	if err := json.Unmarshal(raw, &one); err != nil {
		return nil, err
	}
	return []T{one}, nil
}

func trimLeadingSpace(b []byte) []byte {
	i := 0
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return b[i:]
		}
	}
	return b[i:]
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"dropped": map[string]uint64{
			"logs":           s.logs.Dropped(),
			"traces":         s.traces.Dropped(),
			"metrics":        s.metrics.Dropped(),
			"ai_spans":       s.aiSpans.Dropped(),
			"incidents":      s.incidents.Dropped(),
			"alert_triggers": s.alertTriggers.Dropped(),
		},
	})
}
