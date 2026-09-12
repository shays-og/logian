// Command server is the Logian ingestion server: accepts logs, metrics,
// traces, and AI spans over HTTP, batches them, and writes to ClickHouse.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/logian/ingest/internal/chstore"
	"github.com/logian/ingest/internal/config"
	"github.com/logian/ingest/internal/ingest"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	// Cancelling this ctx stops the buffer flush loops (they drain
	// in-flight items first) — tied to SIGINT/SIGTERM below.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	chClient, err := chstore.NewClient(ctx, cfg.ClickHouse(), false)
	if err != nil {
		logger.Error("failed to connect to clickhouse", "err", err)
		os.Exit(1)
	}
	defer chClient.Close()
	logger.Info("connected to clickhouse", "addr", cfg.ClickHouse().Addr(), "database", cfg.ClickHouse().Database())

	bufCfg := ingest.BufferConfig{
		MaxBatchSize:      cfg.Batch().MaxBatchSize(),
		FlushInterval:     cfg.Batch().FlushInterval(),
		ChannelBufferSize: cfg.Batch().ChannelBufferSize(),
	}
	ingestSrv := ingest.NewServer(chClient, bufCfg, logger)
	ingestSrv.Start(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr(),
		Handler:           ingestSrv.Routes(),
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("ingestion server listening", "addr", cfg.HTTPAddr())
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", "err", err)
	}

	// Give the buffer Run() loops (already unblocked by ctx.Done above)
	// a moment to flush their final batch before the process exits.
	time.Sleep(500 * time.Millisecond)
	logger.Info("shutdown complete")
}
