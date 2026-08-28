package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds application configuration loaded from environment variables.
type Config struct {
	PostgresURL              string
	PostgresHost             string
	PostgresPort             string
	PostgresDB               string
	PostgresUser             string
	PostgresPassword         string
	RedisURL                 string
	KafkaBrokers             []string
	ElasticsearchURL         string
	HTTPPort                 string
	LogLevel                 string
	LogRetentionDays         int
	LogRetentionInterval     time.Duration
	RetentionMetricsPort     string
	ElasticBulkSize          int
	ElasticBulkFlushInterval time.Duration
	ProcessorWorkers         int
}

// Load reads and validates configuration from environment variables (and .env file if present).
func Load() (*Config, error) {
	// Attempt to load .env file if present without overriding existing env vars
	loadEnvFile(".env")

	cfg := &Config{
		PostgresURL:          getEnv("POSTGRES_URL"),
		PostgresHost:         getEnv("POSTGRES_HOST"),
		PostgresPort:         getEnv("POSTGRES_PORT"),
		PostgresDB:           getEnv("POSTGRES_DB"),
		PostgresUser:         getEnv("POSTGRES_USER"),
		PostgresPassword:     getEnv("POSTGRES_PASSWORD"),
		RedisURL:             getEnvWithFallback("REDIS_URL", "REDIS_ADDR"),
		ElasticsearchURL:     getEnvWithFallback("ELASTICSEARCH_URL", "ES_URL"),
		HTTPPort:             getEnv("HTTP_PORT"),
		LogLevel:             strings.ToLower(getEnv("LOG_LEVEL")),
		RetentionMetricsPort: getEnv("RETENTION_METRICS_PORT"),
	}

	// Construct PostgresURL from discrete components if POSTGRES_URL is empty
	if cfg.PostgresURL == "" && cfg.PostgresHost != "" {
		port := cfg.PostgresPort
		if port == "" {
			port = "5432"
		}
		db := cfg.PostgresDB
		if db == "" {
			db = "log_analytics"
		}
		user := cfg.PostgresUser
		if user == "" {
			user = "postgres"
		}
		userInfo := url.UserPassword(user, cfg.PostgresPassword)
		cfg.PostgresURL = fmt.Sprintf("postgresql://%s@%s:%s/%s?sslmode=disable", userInfo.String(), cfg.PostgresHost, port, db)
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

	// Parse ElasticBulkSize (default: 200)
	bulkSizeStr := getEnv("ELASTIC_BULK_SIZE")
	if bulkSizeStr == "" {
		cfg.ElasticBulkSize = 200
	} else {
		size, err := strconv.Atoi(bulkSizeStr)
		if err != nil || size <= 0 {
			return nil, fmt.Errorf("invalid ELASTIC_BULK_SIZE %q (must be a positive integer)", bulkSizeStr)
		}
		cfg.ElasticBulkSize = size
	}

	// Parse ElasticBulkFlushInterval (default: 100ms)
	flushIntervalStr := getEnv("ELASTIC_BULK_FLUSH_INTERVAL")
	if flushIntervalStr == "" {
		cfg.ElasticBulkFlushInterval = 100 * time.Millisecond
	} else {
		dur, err := time.ParseDuration(flushIntervalStr)
		if err != nil || dur <= 0 {
			return nil, fmt.Errorf("invalid ELASTIC_BULK_FLUSH_INTERVAL %q (must be a positive duration, e.g. 100ms)", flushIntervalStr)
		}
		cfg.ElasticBulkFlushInterval = dur
	}

	// Parse ProcessorWorkers (default: 1)
	workersStr := getEnv("PROCESSOR_WORKERS")
	if workersStr == "" {
		cfg.ProcessorWorkers = 1
	} else {
		w, err := strconv.Atoi(workersStr)
		if err != nil || w <= 0 {
			return nil, fmt.Errorf("invalid PROCESSOR_WORKERS %q (must be a positive integer)", workersStr)
		}
		cfg.ProcessorWorkers = w
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
		return fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
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
	paths := []string{path, "../" + path, "../../" + path}
	var data []byte
	var err error
	for _, p := range paths {
		if data, err = os.ReadFile(p); err == nil {
			break
		}
	}
	if len(data) == 0 {
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
