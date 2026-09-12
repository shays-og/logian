// Package config holds runtime configuration for the ingestion server.
package config

import (
	"os"
	"strconv"
	"time"
)

// Secret wraps sensitive config values (passwords, tokens) so they never
// leak into logs or String() output by accident. Call Reveal() explicitly
// at the point of use (e.g. building a driver connection).
type Secret string

func (s Secret) String() string   { return "***REDACTED***" }
func (s Secret) GoString() string { return "***REDACTED***" }
func (s Secret) Reveal() string   { return string(s) }

// ClickHouseConfig holds connection settings for the storage layer.
type ClickHouseConfig struct {
	addr         string
	database     string
	username     string
	password     Secret
	maxOpenConns int
	maxIdleConns int
	asyncInsert  bool
}

func (c ClickHouseConfig) Addr() string      { return c.addr }
func (c ClickHouseConfig) Database() string  { return c.database }
func (c ClickHouseConfig) Username() string  { return c.username }
func (c ClickHouseConfig) Password() Secret  { return c.password }
func (c ClickHouseConfig) MaxOpenConns() int { return c.maxOpenConns }
func (c ClickHouseConfig) MaxIdleConns() int { return c.maxIdleConns }
func (c ClickHouseConfig) AsyncInsert() bool { return c.asyncInsert }

// BatchConfig controls how aggressively each table buffer flushes to
// ClickHouse. Tune MaxBatchSize/FlushInterval per table if one signal
// (e.g. logs) runs much hotter than others.
type BatchConfig struct {
	maxBatchSize      int
	flushInterval     time.Duration
	channelBufferSize int
}

func (b BatchConfig) MaxBatchSize() int            { return b.maxBatchSize }
func (b BatchConfig) FlushInterval() time.Duration { return b.flushInterval }
func (b BatchConfig) ChannelBufferSize() int       { return b.channelBufferSize }

// Config is the top-level, immutable server configuration. Fields are
// unexported and accessed via methods so callers can't mutate config
// after startup and so we can add validation later without breaking
// call sites.
type Config struct {
	httpAddr   string
	clickhouse ClickHouseConfig
	batch      BatchConfig
}

func (c Config) HTTPAddr() string             { return c.httpAddr }
func (c Config) ClickHouse() ClickHouseConfig { return c.clickhouse }
func (c Config) Batch() BatchConfig           { return c.batch }

// Load builds config from environment variables, falling back to
// sane local-dev defaults (matching the docker-compose in this repo).
func Load() Config {
	return Config{
		httpAddr: getEnv("INGEST_HTTP_ADDR", ":8080"),
		clickhouse: ClickHouseConfig{
			addr:     getEnv("CLICKHOUSE_ADDR", "localhost:9000"),
			database: getEnv("CLICKHOUSE_DATABASE", "logian"),
			username: getEnv("CLICKHOUSE_USERNAME", "default"),
			password: Secret(getEnv("CLICKHOUSE_PASSWORD", "")),
			// Defaults tuned down from a typical 20/10 for constrained
			// hardware: each open connection costs ClickHouse server-side
			// memory and thread-pool slots, and on a weak box that
			// competes directly with query/merge memory. Raise these if
			// you move to real hardware and see connection contention.
			maxOpenConns: getEnvInt("CLICKHOUSE_MAX_OPEN_CONNS", 16),
			maxIdleConns: getEnvInt("CLICKHOUSE_MAX_IDLE_CONNS", 8),
			// Our own Buffer already accumulates up to MaxBatchSize rows
			// client-side before sending, so server-side async_insert
			// (which exists to coalesce many *small* inserts) is mostly
			// redundant overhead here — it adds another queue/flush cycle
			// on top of ours. Off by default; flip it on only if you
			// lower INGEST_MAX_BATCH_SIZE enough that individual batches
			// get small again.
			asyncInsert: getEnvBool("CLICKHOUSE_ASYNC_INSERT", false),
		},
		batch: BatchConfig{
			maxBatchSize:      getEnvInt("INGEST_MAX_BATCH_SIZE", 5000),
			flushInterval:     getEnvDuration("INGEST_FLUSH_INTERVAL", 2*time.Second),
			channelBufferSize: getEnvInt("INGEST_CHANNEL_BUFFER", 50000),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
