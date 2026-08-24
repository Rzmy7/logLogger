package main

import (
	"testing"
	"time"
)

func TestConfig_Validation(t *testing.T) {
	cfg := &Config{
		IngestorURL: "http://localhost:8081/api/v1/logs",
		Workers:     5,
		Level:       "MIXED",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	badCfg := &Config{
		IngestorURL: "",
		Workers:     5,
		Level:       "MIXED",
	}
	if err := badCfg.Validate(); err == nil {
		t.Error("expected error for empty IngestorURL, got nil")
	}

	badLevelCfg := &Config{
		IngestorURL: "http://localhost:8081/api/v1/logs",
		Workers:     5,
		Level:       "INVALID_LEVEL",
	}
	if err := badLevelCfg.Validate(); err == nil {
		t.Error("expected error for invalid level, got nil")
	}
}

func TestGenerateLogPayload_Structure(t *testing.T) {
	cfg := &Config{
		Services:       []string{"auth-service", "payment-api"},
		Level:          "MIXED",
		IncludeTraceID: true,
		IncludeIP:      true,
		MessageLen:     50,
		ErrorRate:      -1,
	}

	payload := GenerateLogPayload(cfg)
	if payload == nil {
		t.Fatal("expected non-nil log payload")
	}

	if payload.Timestamp == "" {
		t.Error("expected non-empty timestamp")
	}
	if _, err := time.Parse(time.RFC3339, payload.Timestamp); err != nil {
		if _, errNano := time.Parse(time.RFC3339Nano, payload.Timestamp); errNano != nil {
			t.Errorf("timestamp is not valid RFC3339: %s", payload.Timestamp)
		}
	}

	if payload.Service != "auth-service" && payload.Service != "payment-api" {
		t.Errorf("unexpected service name: %s", payload.Service)
	}

	if payload.Message == "" {
		t.Error("expected non-empty message")
	}

	if payload.TraceID == "" {
		t.Error("expected non-empty trace_id")
	}

	if payload.IP == "" {
		t.Error("expected non-empty ip")
	}
}

func TestSelectLevel_ErrorRateOverride(t *testing.T) {
	cfg := &Config{
		ErrorRate: 1.0, // 100% error/fatal
	}

	for i := 0; i < 50; i++ {
		lvl := SelectLevel(cfg)
		if lvl != "ERROR" && lvl != "FATAL" {
			t.Errorf("expected ERROR or FATAL when ErrorRate=1.0, got %s", lvl)
		}
	}
}
