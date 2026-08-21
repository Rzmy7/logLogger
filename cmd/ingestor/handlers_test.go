package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rzmy7/logLogger/internal/kafka"
)

func TestHealthCheck(t *testing.T) {
	mockProducer := kafka.NewMockProducer()
	h := NewHandler(mockProducer)
	router := NewRouter(h)

	req, err := http.NewRequest(http.MethodGet, "/health", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	expectedBody := `{"status":"ok"}`
	if rec.Body.String() != expectedBody {
		t.Errorf("expected body %q, got %q", expectedBody, rec.Body.String())
	}
}

func TestIngestLogs_Success(t *testing.T) {
	mockProducer := kafka.NewMockProducer()
	h := NewHandler(mockProducer)
	router := NewRouter(h)

	payload := `{
		"timestamp": "2026-08-06T10:00:00Z",
		"level": "ERROR",
		"service": "payment-api",
		"message": "DB connection timeout after 30s",
		"trace_id": "abc-123-def-456",
		"ip": "192.168.1.5"
	}`

	req, err := http.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", "req_test_123")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp IngestSuccessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Data.Status != "queued" {
		t.Errorf("expected status 'queued', got %q", resp.Data.Status)
	}
	if resp.Data.TraceID != "abc-123-def-456" {
		t.Errorf("expected trace_id 'abc-123-def-456', got %q", resp.Data.TraceID)
	}

	if len(mockProducer.Messages) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(mockProducer.Messages))
	}
	if mockProducer.Messages[0].Topic != kafka.TopicAppLogs {
		t.Errorf("expected topic %q, got %q", kafka.TopicAppLogs, mockProducer.Messages[0].Topic)
	}
	if mockProducer.Messages[0].Key != "abc-123-def-456" {
		t.Errorf("expected key 'abc-123-def-456', got %q", mockProducer.Messages[0].Key)
	}
}

func TestIngestLogs_ValidationFailure(t *testing.T) {
	mockProducer := kafka.NewMockProducer()
	h := NewHandler(mockProducer)
	router := NewRouter(h)

	invalidPayload := `{
		"timestamp": "not-a-timestamp",
		"level": "CRITICAL",
		"service": "",
		"message": "",
		"ip": "invalid-ip"
	}`

	req, err := http.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(invalidPayload))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
	}

	var errResp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if errResp.Error.Code != "VALIDATION_ERROR" {
		t.Errorf("expected code VALIDATION_ERROR, got %q", errResp.Error.Code)
	}
	if len(errResp.Error.Details) == 0 {
		t.Error("expected validation details, got empty")
	}
}

func TestIngestLogs_KafkaError(t *testing.T) {
	mockProducer := kafka.NewMockProducer()
	mockProducer.Err = errors.New("kafka connection down")
	h := NewHandler(mockProducer)
	router := NewRouter(h)

	payload := `{
		"timestamp": "2026-08-06T10:00:00Z",
		"level": "INFO",
		"service": "auth-service",
		"message": "User logged in"
	}`

	req, err := http.NewRequest(http.MethodPost, "/api/v1/logs", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d: %s", rec.Code, rec.Body.String())
	}
}
