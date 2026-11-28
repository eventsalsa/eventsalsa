package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

var (
	// ErrBlankDatabaseURL indicates DatabaseURL was empty or whitespace.
	ErrBlankDatabaseURL = errors.New("config: database URL must not be blank")
	// ErrBlankStreamType indicates Snapshot.StreamType was empty or whitespace.
	ErrBlankStreamType = errors.New("config: snapshot stream type must not be blank")
	// ErrInvalidSchemaVersion indicates Snapshot.SchemaVersion was less than 1.
	ErrInvalidSchemaVersion = errors.New("config: snapshot schema version must be >= 1")
	// ErrInvalidEveryNEvents indicates Snapshot.EveryNEvents was less than 1.
	ErrInvalidEveryNEvents = errors.New("config: snapshot every-N-events must be >= 1")
	// ErrInvalidMaxAttempts indicates Retry.MaxAttempts was less than 1.
	ErrInvalidMaxAttempts = errors.New("config: retry max attempts must be >= 1")
)

// #nosec G101 -- local development example default DSN
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" //nolint:gosec // local DSN

// Config holds runtime configuration for Event-Sourced Iteration 4.
type Config struct {
	DatabaseURL string
	Snapshot    SnapshotConfig
	Retry       RetryConfig
}

// SnapshotConfig tunes eventsalsa/snapshot behavior for Order streams.
type SnapshotConfig struct {
	StreamType    string
	SchemaVersion int
	EveryNEvents  int64
	Compress      bool
}

// RetryConfig configures optimistic concurrency retries on the command bus.
type RetryConfig struct {
	MaxAttempts int
}

// Default returns sensible defaults for local execution.
func Default() Config {
	return Config{
		DatabaseURL: defaultDatabaseURL,
		Snapshot: SnapshotConfig{
			StreamType:    "Order",
			SchemaVersion: 1,
			EveryNEvents:  3,
			Compress:      true,
		},
		Retry: RetryConfig{
			MaxAttempts: 3,
		},
	}
}

// LoadEnv loads configuration from process environment variables via os.Getenv.
func LoadEnv() (Config, error) {
	return FromEnv(os.Getenv)
}

// FromEnv builds and validates Config by applying environment overrides on top of Default().
func FromEnv(getenv func(string) string) (Config, error) {
	cfg := Default()
	if getenv == nil {
		return cfg, cfg.Validate()
	}

	if v := strings.TrimSpace(getenv("DATABASE_URL")); v != "" {
		cfg.DatabaseURL = v
	}
	if v := strings.TrimSpace(getenv("ORDER_STREAM_TYPE")); v != "" {
		cfg.Snapshot.StreamType = v
	}
	if v := strings.TrimSpace(getenv("SNAPSHOT_SCHEMA_VERSION")); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, err
		}
		cfg.Snapshot.SchemaVersion = parsed
	}
	if v := strings.TrimSpace(getenv("SNAPSHOT_EVERY_N_EVENTS")); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Config{}, err
		}
		cfg.Snapshot.EveryNEvents = parsed
	}
	if v := strings.TrimSpace(getenv("SNAPSHOT_COMPRESS")); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, err
		}
		cfg.Snapshot.Compress = parsed
	}
	if v := strings.TrimSpace(getenv("COMMAND_RETRY_MAX_ATTEMPTS")); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, err
		}
		cfg.Retry.MaxAttempts = parsed
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks that every required configuration field is valid.
func (c Config) Validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return ErrBlankDatabaseURL
	}
	if strings.TrimSpace(c.Snapshot.StreamType) == "" {
		return ErrBlankStreamType
	}
	if c.Snapshot.SchemaVersion < 1 {
		return ErrInvalidSchemaVersion
	}
	if c.Snapshot.EveryNEvents < 1 {
		return ErrInvalidEveryNEvents
	}
	if c.Retry.MaxAttempts < 1 {
		return ErrInvalidMaxAttempts
	}
	return nil
}
