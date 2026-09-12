// Package ingest wires HTTP intake to buffered, batched ClickHouse writes.
package ingest

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// FlushFunc writes a batch of T to storage. It owns its own context
// timeout and should not retry internally — Buffer logs and drops the
// batch on failure rather than blocking the pipeline (PoC tradeoff: see
// README for what a production version should do instead — a retry
// queue or dead-letter table).
type FlushFunc[T any] func(ctx context.Context, items []T) error

// Buffer decouples HTTP handlers from ClickHouse writes: handlers push
// into a channel and return immediately; a single background goroutine
// accumulates items and flushes on whichever comes first — batch size
// or flush interval. This is the throughput lever: bigger batches mean
// fewer, cheaper inserts, at the cost of higher worst-case latency
// before a row is queryable.
type Buffer[T any] struct {
	name          string
	ch            chan T
	maxBatchSize  int
	flushInterval time.Duration
	flush         FlushFunc[T]

	dropped atomic.Uint64
	logger  *slog.Logger
}

type BufferConfig struct {
	MaxBatchSize      int
	FlushInterval     time.Duration
	ChannelBufferSize int
}

// NewBuffer creates and starts a buffer. Call Push to enqueue items;
// call Close (via context cancellation) to stop it during shutdown.
func NewBuffer[T any](name string, cfg BufferConfig, flush FlushFunc[T], logger *slog.Logger) *Buffer[T] {
	return &Buffer[T]{
		name:          name,
		ch:            make(chan T, cfg.ChannelBufferSize),
		maxBatchSize:  cfg.MaxBatchSize,
		flushInterval: cfg.FlushInterval,
		flush:         flush,
		logger:        logger,
	}
}

// Push enqueues an item without blocking. Returns false if the channel
// is full — the caller (HTTP handler) should surface this as a 503 so
// producers can back off, rather than silently dropping data.
func (b *Buffer[T]) Push(item T) bool {
	select {
	case b.ch <- item:
		return true
	default:
		b.dropped.Add(1)
		return false
	}
}

// Dropped returns the number of items rejected because the channel was
// full, for exposure on /metrics or /healthz.
func (b *Buffer[T]) Dropped() uint64 { return b.dropped.Load() }

// Run drains the channel, batching until maxBatchSize or flushInterval,
// whichever comes first. Blocks until ctx is cancelled, then flushes
// whatever remains before returning — call this in its own goroutine.
func (b *Buffer[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	buf := make([]T, 0, b.maxBatchSize)

	doFlush := func() {
		if len(buf) == 0 {
			return
		}
		items := make([]T, len(buf))
		copy(items, buf)
		buf = buf[:0]

		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := b.flush(flushCtx, items); err != nil {
			b.logger.Error("batch flush failed", "table", b.name, "rows", len(items), "err", err)
			return
		}
		b.logger.Debug("batch flushed", "table", b.name, "rows", len(items))
	}

	for {
		select {
		case item := <-b.ch:
			buf = append(buf, item)
			if len(buf) >= b.maxBatchSize {
				doFlush()
			}
		case <-ticker.C:
			doFlush()
		case <-ctx.Done():
			// Drain whatever's already queued before exiting so a
			// graceful shutdown doesn't lose in-flight rows.
			for {
				select {
				case item := <-b.ch:
					buf = append(buf, item)
				default:
					doFlush()
					return
				}
			}
		}
	}
}
