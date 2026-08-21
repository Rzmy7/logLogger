package config

import (
	"os"
	"reflect"
	"testing"
)

func TestLoad_Success(t *testing.T) {
	// Set all required variables
	os.Setenv("POSTGRES_URL", "postgresql://localhost:5432/test")
	os.Setenv("REDIS_URL", "redis://localhost:6379")
	os.Setenv("KAFKA_BROKERS", "localhost:9092, localhost:9093")
	os.Setenv("ELASTICSEARCH_URL", "http://localhost:9200")
	os.Setenv("HTTP_PORT", "8081")
	os.Setenv("LOG_LEVEL", "DEBUG")
	defer func() {
		os.Unsetenv("POSTGRES_URL")
		os.Unsetenv("REDIS_URL")
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("ELASTICSEARCH_URL")
		os.Unsetenv("HTTP_PORT")
		os.Unsetenv("LOG_LEVEL")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.PostgresURL != "postgresql://localhost:5432/test" {
		t.Errorf("expected PostgresURL, got %s", cfg.PostgresURL)
	}
	if cfg.RedisURL != "redis://localhost:6379" {
		t.Errorf("expected RedisURL, got %s", cfg.RedisURL)
	}
	expectedBrokers := []string{"localhost:9092", "localhost:9093"}
	if !reflect.DeepEqual(cfg.KafkaBrokers, expectedBrokers) {
		t.Errorf("expected KafkaBrokers %v, got %v", expectedBrokers, cfg.KafkaBrokers)
	}
	if cfg.ElasticsearchURL != "http://localhost:9200" {
		t.Errorf("expected ElasticsearchURL, got %s", cfg.ElasticsearchURL)
	}
	if cfg.HTTPPort != "8081" {
		t.Errorf("expected HTTPPort 8081, got %s", cfg.HTTPPort)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel debug, got %s", cfg.LogLevel)
	}
}

func TestLoad_ValidationFailure(t *testing.T) {
	// Clear env
	os.Unsetenv("POSTGRES_URL")
	os.Unsetenv("REDIS_URL")
	os.Unsetenv("REDIS_ADDR")
	os.Unsetenv("KAFKA_BROKERS")
	os.Unsetenv("ELASTICSEARCH_URL")
	os.Unsetenv("ES_URL")

	// Missing envs will fail if no .env supplies them or if required are empty
	// We test validation directly
	c := &Config{
		LogLevel: "invalid_level",
	}
	if err := c.validate(); err == nil {
		t.Error("expected validation error, got nil")
	}
}
