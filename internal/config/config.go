package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds application configuration loaded from environment variables.
type Config struct {
	PostgresURL          string
	RedisURL             string
	KafkaBrokers         []string
	ElasticsearchURL     string
	HTTPPort             string
	LogLevel             string
	LogRetentionDays     int
	LogRetentionInterval time.Duration
	RetentionMetricsPort string
}

// Load reads and validates configuration from environment variables (and .env file if present).
func Load() (*Config, error) {
	// Attempt to load .env file if present without overriding existing env vars
	loadEnvFile(".env")

	cfg := &Config{
		PostgresURL:          getEnv("POSTGRES_URL"),
		RedisURL:             getEnvWithFallback("REDIS_URL", "REDIS_ADDR"),
		ElasticsearchURL:     getEnvWithFallback("ELASTICSEARCH_URL", "ES_URL"),
		HTTPPort:             getEnv("HTTP_PORT"),
		LogLevel:             strings.ToLower(getEnv("LOG_LEVEL")),
		RetentionMetricsPort: getEnv("RETENTION_METRICS_PORT"),
	}

	// Parse Kafka brokers
	kafkaBrokersRaw := getEnv("KAFKA_BROKERS")
	if kafkaBrokersRaw != "" {
		for _, broker := range strings.Split(kafkaBrokersRaw, ",") {
			broker = strings.TrimSpace(broker)
			if broker != "" {
				cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
			}
		}
	}

	// Parse LogRetentionDays (default: 30)
	retentionDaysStr := getEnv("LOG_RETENTION_DAYS")
	if retentionDaysStr == "" {
		cfg.LogRetentionDays = 30
	} else {
		days, err := strconv.Atoi(retentionDaysStr)
		if err != nil || days <= 0 {
			return nil, fmt.Errorf("invalid LOG_RETENTION_DAYS %q (must be a positive integer)", retentionDaysStr)
		}
		cfg.LogRetentionDays = days
	}

	// Parse LogRetentionInterval (default: 1 hour)
	retentionIntervalStr := getEnv("LOG_RETENTION_INTERVAL")
	if retentionIntervalStr == "" {
		cfg.LogRetentionInterval = 1 * time.Hour
	} else {
		dur, err := time.ParseDuration(retentionIntervalStr)
		if err != nil || dur <= 0 {
			return nil, fmt.Errorf("invalid LOG_RETENTION_INTERVAL %q (must be a valid positive duration, e.g. 1h, 30m)", retentionIntervalStr)
		}
		cfg.LogRetentionInterval = dur
	}

	// Apply defaults
	if cfg.HTTPPort == "" {
		cfg.HTTPPort = "8081"
	} else {
		cfg.HTTPPort = strings.TrimPrefix(cfg.HTTPPort, ":")
	}

	if cfg.RetentionMetricsPort == "" {
		cfg.RetentionMetricsPort = "8084"
	} else {
		cfg.RetentionMetricsPort = strings.TrimPrefix(cfg.RetentionMetricsPort, ":")
	}

	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	// Validate required fields
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation error: %w", err)
	}

	return cfg, nil
}

func (c *Config) validate() error {
	var missing []string

	if c.PostgresURL == "" {
		missing = append(missing, "POSTGRES_URL")
	}
	if c.RedisURL == "" {
		missing = append(missing, "REDIS_URL")
	}
	if len(c.KafkaBrokers) == 0 {
		missing = append(missing, "KAFKA_BROKERS")
	}
	if c.ElasticsearchURL == "" {
		missing = append(missing, "ELASTICSEARCH_URL")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variable(s): %s", strings.Join(missing, ", "))
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
		// valid
	default:
		return fmt.Errorf("invalid LOG_LEVEL %q (allowed: debug, info, warn, error)", c.LogLevel)
	}

	return nil
}

func getEnv(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

func getEnvWithFallback(primary, fallback string) string {
	if val := getEnv(primary); val != "" {
		return val
	}
	return getEnv(fallback)
}

func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = strings.Trim(val, `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
}
