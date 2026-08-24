package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/models"
	"github.com/Rzmy7/logLogger/internal/redis"
)

func setupTestRouter() (*redis.MockMetricsRecorder, *elastic.MockIndexer, http.Handler) {
	mockRedis := redis.NewMockMetricsRecorder()
	mockES := elastic.NewMockIndexer()
	h := NewHandler(mockRedis, mockES)
	router := NewRouter(h)
	return mockRedis, mockES, router
}

func TestHealthCheck_Healthy(t *testing.T) {
	_, _, router := setupTestRouter()

	req, _ := http.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	data := resp["data"].(map[string]any)
	if data["status"] != "healthy" {
		t.Errorf("expected healthy status, got %v", data["status"])
	}
}

func TestHealthCheck_Degraded(t *testing.T) {
	_, mockES, router := setupTestRouter()
	mockES.Err = errors.New("es down")

	req, _ := http.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	data := resp["data"].(map[string]any)
	if data["status"] != "degraded" {
		t.Errorf("expected degraded status, got %v", data["status"])
	}

	services := data["services"].(map[string]any)
	if services["elasticsearch"] != "down" {
		t.Errorf("expected elasticsearch down, got %v", services["elasticsearch"])
	}
	if services["redis"] != "up" {
		t.Errorf("expected redis up, got %v", services["redis"])
	}
}

func TestLiveMetrics(t *testing.T) {
	mockRedis, _, router := setupTestRouter()

	ctx := context.Background()
	logMsg := &models.LogMessage{
		Timestamp: "2026-08-21T10:00:00Z",
		Level:     "ERROR",
		Service:   "payment-api",
		Message:   "Gateway timeout",
	}
	_ = mockRedis.RecordLog(ctx, logMsg, []byte(`{}`))

	req, _ := http.NewRequest(http.MethodGet, "/metrics/live", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	data := resp["data"].(map[string]any)
	if data["total_logs"].(float64) != 1 {
		t.Errorf("expected total_logs 1, got %v", data["total_logs"])
	}
}

func TestTopErrors(t *testing.T) {
	mockRedis, _, router := setupTestRouter()

	ctx := context.Background()
	_ = mockRedis.RecordLog(ctx, &models.LogMessage{
		Timestamp: "2026-08-21T10:00:00Z",
		Level:     "ERROR",
		Service:   "payment-api",
		Message:   "Gateway timeout",
	}, nil)

	req, _ := http.NewRequest(http.MethodGet, "/metrics/top-errors?n=5", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	data := resp["data"].(map[string]any)
	topErrors := data["top_errors"].([]any)
	if len(topErrors) != 1 {
		t.Fatalf("expected 1 top error, got %d", len(topErrors))
	}
}

func TestTopServices(t *testing.T) {
	mockRedis, _, router := setupTestRouter()

	ctx := context.Background()
	_ = mockRedis.RecordLog(ctx, &models.LogMessage{
		Timestamp: "2026-08-21T10:00:00Z",
		Level:     "INFO",
		Service:   "auth-service",
		Message:   "User login",
	}, nil)

	req, _ := http.NewRequest(http.MethodGet, "/metrics/top-services?n=5", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	data := resp["data"].(map[string]any)
	topServices := data["top_services"].([]any)
	if len(topServices) != 1 {
		t.Fatalf("expected 1 top service, got %d", len(topServices))
	}
}

func TestSearch(t *testing.T) {
	_, mockES, router := setupTestRouter()

	ctx := context.Background()
	_ = mockES.IndexLog(ctx, &models.LogMessage{
		Timestamp: "2026-08-21T10:00:00Z",
		Level:     "ERROR",
		Service:   "payment-api",
		Message:   "DB connection timeout",
		TraceID:   "trace-123",
	}, time.Now().UTC())

	req, _ := http.NewRequest(http.MethodGet, "/search?q=timeout&service=payment-api&level=ERROR", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	data := resp["data"].(map[string]any)
	if data["total"].(float64) != 1 {
		t.Errorf("expected total 1, got %v", data["total"])
	}
}

func TestSearch_InvalidDateFormat(t *testing.T) {
	_, _, router := setupTestRouter()

	req, _ := http.NewRequest(http.MethodGet, "/search?from=2026-08-21", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	var errResp APIErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if errResp.Error.Code != "VALIDATION_ERROR" {
		t.Errorf("expected VALIDATION_ERROR code, got %s", errResp.Error.Code)
	}
}
