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
	"github.com/Rzmy7/logLogger/internal/retention"
)

// MockSearcherAndLifecycle implements both elastic.Searcher and elastic.IndexLifecycleClient.
type MockSearcherAndLifecycle struct {
	*elastic.MockIndexer
	*elastic.MockIndexLifecycleClient
}

func setupTestRouter() (*redis.MockMetricsRecorder, *MockSearcherAndLifecycle, *retention.ElasticsearchRetentionManager, http.Handler) {
	mockRedis := redis.NewMockMetricsRecorder()
	mockES := &MockSearcherAndLifecycle{
		MockIndexer:              elastic.NewMockIndexer(),
		MockIndexLifecycleClient: elastic.NewMockIndexLifecycleClient(),
	}
	retentionMgr := retention.NewManager(mockES.MockIndexLifecycleClient)
	h := NewHandler(mockRedis, mockES.MockIndexer, retentionMgr)
	router := NewRouter(h)
	return mockRedis, mockES, retentionMgr, router
}

func TestHealthCheck_Healthy(t *testing.T) {
	_, _, _, router := setupTestRouter()

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

	if resp["status"] != "healthy" {
		t.Errorf("expected healthy status, got %v", resp["status"])
	}
}

func TestHealthCheck_Degraded(t *testing.T) {
	_, mockES, _, router := setupTestRouter()
	mockES.MockIndexer.Err = errors.New("es down")

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

	if resp["status"] != "degraded" {
		t.Errorf("expected degraded status, got %v", resp["status"])
	}
}

func TestLiveMetrics(t *testing.T) {
	mockRedis, _, _, router := setupTestRouter()
	ctx := context.Background()
	_ = mockRedis.RecordLog(ctx, &models.LogMessage{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     "INFO",
		Service:   "payment-api",
		Message:   "Payment ok",
	}, nil)

	req, _ := http.NewRequest(http.MethodGet, "/metrics/live", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestTopErrors(t *testing.T) {
	mockRedis, _, _, router := setupTestRouter()
	ctx := context.Background()
	_ = mockRedis.RecordLog(ctx, &models.LogMessage{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     "ERROR",
		Service:   "payment-api",
		Message:   "Timeout error",
	}, nil)

	req, _ := http.NewRequest(http.MethodGet, "/metrics/top-errors?n=5", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestTopServices(t *testing.T) {
	mockRedis, _, _, router := setupTestRouter()
	ctx := context.Background()
	_ = mockRedis.RecordLog(ctx, &models.LogMessage{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     "INFO",
		Service:   "payment-api",
		Message:   "Payment ok",
	}, nil)

	req, _ := http.NewRequest(http.MethodGet, "/metrics/top-services?n=5", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestSearch(t *testing.T) {
	_, mockES, _, router := setupTestRouter()
	ctx := context.Background()
	_ = mockES.MockIndexer.IndexLog(ctx, &models.LogMessage{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     "INFO",
		Service:   "payment-api",
		Message:   "Search test",
	}, time.Now().UTC())

	req, _ := http.NewRequest(http.MethodGet, "/search?q=Search&service=payment-api&level=INFO", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestSearch_InvalidDateFormat(t *testing.T) {
	_, _, _, router := setupTestRouter()

	req, _ := http.NewRequest(http.MethodGet, "/search?from=invalid-date", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestAdmin_GetLogStats(t *testing.T) {
	_, mockES, _, router := setupTestRouter()
	now := time.Now().UTC()
	idx1 := elastic.IndexNameForTime(now.AddDate(0, 0, -5))
	mockES.MockIndexLifecycleClient.Indices[idx1] = elastic.IndexInfo{Name: idx1, DocCount: 50, StoreSizeBytes: 1024}

	req, _ := http.NewRequest(http.MethodGet, "/admin/logs/stats", nil)
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
	if data["total_indices"].(float64) != 1 {
		t.Errorf("expected 1 index, got %v", data["total_indices"])
	}
}

func TestAdmin_RunRetention(t *testing.T) {
	_, mockES, _, router := setupTestRouter()
	now := time.Now().UTC()
	oldIdx := elastic.IndexNameForTime(now.AddDate(0, 0, -40))
	mockES.MockIndexLifecycleClient.Indices[oldIdx] = elastic.IndexInfo{Name: oldIdx, DocCount: 50}

	req, _ := http.NewRequest(http.MethodPost, "/admin/logs/retention/run?days=30", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if _, exists := mockES.MockIndexLifecycleClient.Indices[oldIdx]; exists {
		t.Errorf("index %s should have been deleted by retention run", oldIdx)
	}
}

func TestAdmin_DeleteIndexByName(t *testing.T) {
	_, mockES, _, router := setupTestRouter()
	now := time.Now().UTC()
	todayIdx := elastic.IndexNameForTime(now)
	historicalIdx := elastic.IndexNameForTime(now.AddDate(0, 0, -10))

	mockES.MockIndexLifecycleClient.Indices[todayIdx] = elastic.IndexInfo{Name: todayIdx, DocCount: 100}
	mockES.MockIndexLifecycleClient.Indices[historicalIdx] = elastic.IndexInfo{Name: historicalIdx, DocCount: 50}

	// 1. Delete historical index -> 200 OK
	req, _ := http.NewRequest(http.MethodDelete, "/admin/logs/indices/"+historicalIdx, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK deleting historical index, got %d", rec.Code)
	}

	// 2. Delete today's active write index -> 422 Unprocessable Entity
	reqProt, _ := http.NewRequest(http.MethodDelete, "/admin/logs/indices/"+todayIdx, nil)
	recProt := httptest.NewRecorder()
	router.ServeHTTP(recProt, reqProt)

	if recProt.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for protected active index, got %d", recProt.Code)
	}

	// 3. Delete invalid index name -> 400 Bad Request
	reqInv, _ := http.NewRequest(http.MethodDelete, "/admin/logs/indices/not-a-log-index", nil)
	recInv := httptest.NewRecorder()
	router.ServeHTTP(recInv, reqInv)

	if recInv.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid index name, got %d", recInv.Code)
	}
}

func TestAdmin_DeleteLogsBefore(t *testing.T) {
	_, mockES, _, router := setupTestRouter()
	now := time.Now().UTC()
	oldIdx := elastic.IndexNameForTime(now.AddDate(0, 0, -20))
	mockES.MockIndexLifecycleClient.Indices[oldIdx] = elastic.IndexInfo{Name: oldIdx, DocCount: 30}

	cutoffStr := now.AddDate(0, 0, -10).Format(time.RFC3339)
	req, _ := http.NewRequest(http.MethodDelete, "/admin/logs?before="+cutoffStr, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	if _, exists := mockES.MockIndexLifecycleClient.Indices[oldIdx]; exists {
		t.Errorf("index %s should have been deleted", oldIdx)
	}
}
