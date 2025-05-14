package helpers

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ----- helpers -----

// envOr returns env[key] if set, otherwise def.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envOrInt converts env[key] to int or falls back to def.
func EnvOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envOrDur parses env[key] as time.Duration or falls back to def.
func envOrDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// ----- flags & config -----

type ChConfig struct {
	addr            string
	database        string
	username        string
	password        string
	dialTimeout     time.Duration
	connMaxLifetime time.Duration
	maxOpenConns    int
	maxIdleConns    int
}

func LoadClickhouseConfig() *ChConfig {
	cfg := &ChConfig{}

	flag.StringVar(&cfg.addr,
		"ch.addr",
		envOr("CH_ADDR", "127.0.0.1:9001"),
		"ClickHouse host:port (env CH_ADDR)",
	)
	flag.StringVar(&cfg.database,
		"ch.db",
		envOr("CH_DATABASE", "default"),
		"ClickHouse database (env CH_DATABASE)",
	)
	flag.StringVar(&cfg.username,
		"ch.user",
		envOr("CH_USER", "default"),
		"ClickHouse username (env CH_USER)",
	)
	flag.StringVar(&cfg.password,
		"ch.pass",
		envOr("CH_PASS", ""),
		"ClickHouse password (env CH_PASS)",
	)
	flag.DurationVar(&cfg.dialTimeout,
		"ch.dial-timeout",
		envOrDur("CH_DIAL_TIMEOUT", 5*time.Second),
		"Dial timeout (env CH_DIAL_TIMEOUT, e.g. \"10s\")",
	)
	flag.DurationVar(&cfg.connMaxLifetime,
		"ch.conn-max-lifetime",
		envOrDur("CH_CONN_MAX_LIFETIME", 10*time.Minute),
		"Max connection lifetime (env CH_CONN_MAX_LIFETIME, e.g. \"30m\")",
	)
	flag.IntVar(&cfg.maxOpenConns,
		"ch.max-open-conns",
		EnvOrInt("CH_MAX_OPEN_CONNS", 5),
		"Max open conns (env CH_MAX_OPEN_CONNS)",
	)
	flag.IntVar(&cfg.maxIdleConns,
		"ch.max-idle-conns",
		EnvOrInt("CH_MAX_IDLE_CONNS", 2),
		"Max idle conns (env CH_MAX_IDLE_CONNS)",
	)

	flag.Parse()
	return cfg
}

// ----- use the config to build clickhouse.Options -----

func NewClickHouseOptions(cfg *ChConfig) *clickhouse.Options {
	return &clickhouse.Options{
		Addr:            []string{cfg.addr},
		Auth:            clickhouse.Auth{Database: cfg.database, Username: cfg.username, Password: cfg.password},
		DialTimeout:     cfg.dialTimeout,
		ConnMaxLifetime: cfg.connMaxLifetime,
		MaxOpenConns:    cfg.maxOpenConns,
		MaxIdleConns:    cfg.maxIdleConns,
	}
}

const (
	maxRetries     = 5
	initialBackoff = 500 * time.Millisecond
)

func OverlayFromEnv(defaults map[string]string, envVar string) map[string]string {
	merged := make(map[string]string, len(defaults))
	for k, v := range defaults {
		merged[k] = v
	}
	raw, ok := os.LookupEnv(envVar)
	if !ok || strings.TrimSpace(raw) == "" {
		return merged // nothing to overlay
	}
	pairs := strings.Split(raw, ",")
	for _, p := range pairs {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue // ignore malformed
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if key != "" && val != "" {
			merged[key] = val
		}
	}
	return merged
}
