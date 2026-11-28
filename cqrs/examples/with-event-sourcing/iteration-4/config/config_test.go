package config_test

import (
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/config"
)

func TestFromEnvAndValidation(t *testing.T) {
	t.Parallel()

	def := config.Default()
	if err := def.Validate(); err != nil {
		t.Fatalf("Default().Validate() = %v", err)
	}
	nilEnvCfg, err := config.FromEnv(nil)
	if err != nil || nilEnvCfg != def {
		t.Fatalf("FromEnv(nil) = (%+v, %v), want (%+v, nil)", nilEnvCfg, err, def)
	}

	cfg, err := config.FromEnv(func(key string) string {
		return map[string]string{
			"DATABASE_URL":               "postgres://app:pw@db:5432/prod",
			"ORDER_STREAM_TYPE":          "CustomerOrder",
			"SNAPSHOT_SCHEMA_VERSION":    "2",
			"SNAPSHOT_EVERY_N_EVENTS":    "10",
			"SNAPSHOT_COMPRESS":          "false",
			"COMMAND_RETRY_MAX_ATTEMPTS": "5",
		}[key]
	})
	if err != nil {
		t.Fatalf("FromEnv valid err = %v", err)
	}
	if cfg.DatabaseURL != "postgres://app:pw@db:5432/prod" ||
		cfg.Snapshot.StreamType != "CustomerOrder" ||
		cfg.Snapshot.SchemaVersion != 2 ||
		cfg.Snapshot.EveryNEvents != 10 ||
		cfg.Snapshot.Compress ||
		cfg.Retry.MaxAttempts != 5 {
		t.Fatalf("unexpected parsed config: %+v", cfg)
	}

	for _, envKey := range []string{
		"SNAPSHOT_SCHEMA_VERSION",
		"SNAPSHOT_EVERY_N_EVENTS",
		"SNAPSHOT_COMPRESS",
		"COMMAND_RETRY_MAX_ATTEMPTS",
	} {
		k := envKey
		if _, parseErr := config.FromEnv(func(name string) string {
			if name == k {
				return "not-a-valid-value"
			}
			return ""
		}); parseErr == nil {
			t.Fatalf("expected parse error for %s", k)
		}
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*config.Config)
		wantErr error
	}{
		{name: "blank dsn", mutate: func(c *config.Config) { c.DatabaseURL = "  " }, wantErr: config.ErrBlankDatabaseURL},
		{name: "blank stream type", mutate: func(c *config.Config) { c.Snapshot.StreamType = "" }, wantErr: config.ErrBlankStreamType},
		{name: "zero schema version", mutate: func(c *config.Config) { c.Snapshot.SchemaVersion = 0 }, wantErr: config.ErrInvalidSchemaVersion},
		{name: "zero interval", mutate: func(c *config.Config) { c.Snapshot.EveryNEvents = 0 }, wantErr: config.ErrInvalidEveryNEvents},
		{name: "zero retries", mutate: func(c *config.Config) { c.Retry.MaxAttempts = 0 }, wantErr: config.ErrInvalidMaxAttempts},
	} {
		c := config.Default()
		tc.mutate(&c)
		if err := c.Validate(); !errors.Is(err, tc.wantErr) {
			t.Fatalf("%s Validate() = %v, want %v", tc.name, err, tc.wantErr)
		}
	}

	if _, err := config.FromEnv(func(k string) string {
		if k == "SNAPSHOT_EVERY_N_EVENTS" {
			return "0"
		}
		return ""
	}); !errors.Is(err, config.ErrInvalidEveryNEvents) {
		t.Fatalf("FromEnv zero interval err = %v, want %v", err, config.ErrInvalidEveryNEvents)
	}
}
