package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestNewClient_BaseURL(t *testing.T) {
	os.Unsetenv("LOGCTL_API_URL")
	c1 := NewClient()
	if c1.BaseURL() != DefaultAPIURL {
		t.Errorf("expected default URL %s, got %s", DefaultAPIURL, c1.BaseURL())
	}

	os.Setenv("LOGCTL_API_URL", "http://custom:9999/")
	defer os.Unsetenv("LOGCTL_API_URL")

	c2 := NewClient()
	if c2.BaseURL() != "http://custom:9999" {
		t.Errorf("expected trimmed custom URL, got %s", c2.BaseURL())
	}

	c3 := NewClient(WithBaseURL("http://option:8888/"))
	if c3.BaseURL() != "http://option:8888" {
		t.Errorf("expected option URL, got %s", c3.BaseURL())
	}
}

func TestClient_Health(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"status": "healthy",
			"dependencies": {"elasticsearch": "healthy", "redis": "healthy"},
			"meta": {"request_id": "req-1", "timestamp": "2026-08-29T12:00:00Z"}
		}`))
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))
	resp, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "healthy" {
		t.Errorf("expected status healthy, got %s", resp.Status)
	}
	if resp.Dependencies["elasticsearch"] != "healthy" {
		t.Errorf("expected healthy ES, got %s", resp.Dependencies["elasticsearch"])
	}
}

func TestClient_GetLogStats(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/logs/stats" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"data": {
				"total_logs": 100,
				"total_indices": 2,
				"total_size_bytes": 10240,
				"oldest_index": "logs-v1-2026.08.28",
				"newest_index": "logs-v1-2026.08.29",
				"indices": [
					{"name": "logs-v1-2026.08.28", "doc_count": 50, "store_size_bytes": 5120, "creation_date": "2026-08-28T00:00:00Z", "status": "open"},
					{"name": "logs-v1-2026.08.29", "doc_count": 50, "store_size_bytes": 5120, "creation_date": "2026-08-29T00:00:00Z", "status": "open"}
				]
			},
			"meta": {"request_id": "req-2", "timestamp": "2026-08-29T12:00:00Z"}
		}`))
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))
	stats, err := c.GetLogStats(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Data.TotalLogs != 100 {
		t.Errorf("expected 100 logs, got %d", stats.Data.TotalLogs)
	}
	if len(stats.Data.Indices) != 2 {
		t.Errorf("expected 2 indices, got %d", len(stats.Data.Indices))
	}
}

func TestClient_SearchLogs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("service") != "payment-api" || q.Get("level") != "ERROR" || q.Get("tenant_id") != "t1" {
			t.Errorf("unexpected query params: %v", q)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"data": {
				"total": 1,
				"page": 1,
				"size": 20,
				"logs": [
					{
						"id": "doc-1",
						"timestamp": "2026-08-29T10:00:00Z",
						"level": "ERROR",
						"service": "payment-api",
						"message": "Payment failed",
						"trace_id": "tr-100",
						"tenant_id": "t1"
					}
				]
			},
			"meta": {"request_id": "req-3", "timestamp": "2026-08-29T12:00:00Z"}
		}`))
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))
	res, err := c.SearchLogs(context.Background(), SearchParams{
		Service:  "payment-api",
		Level:    "ERROR",
		TenantID: "t1",
		Page:     1,
		Size:     20,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Data.Total != 1 {
		t.Errorf("expected 1 result, got %d", res.Data.Total)
	}
	if len(res.Data.Logs) != 1 || res.Data.Logs[0].TraceID != "tr-100" {
		t.Errorf("unexpected log doc: %v", res.Data.Logs)
	}
}

func TestClient_DeleteIndex_SuccessAndErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/logs/indices/logs-v1-2020.01.01":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"index":"logs-v1-2020.01.01","status":"deleted"},"meta":{"request_id":"req-4"}}`))
		case "/admin/logs/indices/logs-v1-2026.08.29":
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":{"code":"PROTECTED_INDEX","message":"today's active index is protected"},"meta":{"request_id":"req-5"}}`))
		case "/admin/logs/indices/.kibana":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"INVALID_INDEX_NAME","message":"invalid index prefix"},"meta":{"request_id":"req-6"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"NOT_FOUND","message":"index not found"}}`))
		}
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))

	// 1. Success
	resp, err := c.DeleteIndex(context.Background(), "logs-v1-2020.01.01")
	if err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
	if resp.Data.Status != "deleted" {
		t.Errorf("expected status deleted, got %s", resp.Data.Status)
	}

	// 2. Protected Index (422)
	_, errProtected := c.DeleteIndex(context.Background(), "logs-v1-2026.08.29")
	if errProtected == nil {
		t.Fatal("expected error on protected index, got nil")
	}
	var apiErr *APIError
	if !isAPIErrorHelper(errProtected, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", errProtected, errProtected)
	}
	if apiErr.StatusCode != 422 || apiErr.Code != "PROTECTED_INDEX" {
		t.Errorf("expected 422 PROTECTED_INDEX, got %d %s", apiErr.StatusCode, apiErr.Code)
	}

	// 3. Invalid Index Name (400)
	_, errInvalid := c.DeleteIndex(context.Background(), ".kibana")
	if errInvalid == nil {
		t.Fatal("expected error on .kibana, got nil")
	}
	if isAPIErrorHelper(errInvalid, &apiErr) {
		if apiErr.StatusCode != 400 || apiErr.Code != "INVALID_INDEX_NAME" {
			t.Errorf("expected 400 INVALID_INDEX_NAME, got %d %s", apiErr.StatusCode, apiErr.Code)
		}
	}

	// 4. Empty Index
	_, errEmpty := c.DeleteIndex(context.Background(), "")
	if errEmpty == nil {
		t.Fatal("expected error on empty index name")
	}
}

func isAPIErrorHelper(err error, target **APIError) bool {
	if ae, ok := err.(*APIError); ok {
		*target = ae
		return true
	}
	return false
}

func TestClient_DeleteBefore(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("before") == "2026-08-01T00:00:00Z" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"data": {
					"cutoff_date": "2026-08-01T00:00:00Z",
					"evaluated_count": 3,
					"deleted_count": 2,
					"deleted_indices": ["logs-v1-2026.07.15", "logs-v1-2026.07.20"],
					"duration": 123456
				},
				"meta": {"request_id":"req-7"}
			}`))
		} else {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"VALIDATION_ERROR","message":"invalid timestamp"}}`))
		}
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))
	res, err := c.DeleteBefore(context.Background(), "2026-08-01T00:00:00Z")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Data.DeletedCount != 2 {
		t.Errorf("expected 2 deleted indices, got %d", res.Data.DeletedCount)
	}

	_, errInvalid := c.DeleteBefore(context.Background(), "invalid-date")
	if errInvalid == nil {
		t.Fatal("expected error on invalid date")
	}
}

func TestClient_RunRetention(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("days") != "15" {
			t.Errorf("expected days=15, got %s", r.URL.Query().Get("days"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"data": {
				"cutoff_date": "2026-08-14T00:00:00Z",
				"evaluated_count": 4,
				"deleted_count": 1,
				"deleted_indices": ["logs-v1-2026.08.01"],
				"duration": 54321
			},
			"meta": {"request_id":"req-8"}
		}`))
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))
	res, err := c.RunRetention(context.Background(), 15)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Data.DeletedCount != 1 {
		t.Errorf("expected 1 deleted, got %d", res.Data.DeletedCount)
	}
}

func TestClient_ContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Health(ctx)
	if err == nil {
		t.Fatal("expected context canceled error, got nil")
	}
}

func TestClient_MalformedJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{invalid-json`))
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL))
	_, err := c.Health(context.Background())
	if err == nil {
		t.Fatal("expected JSON decode error, got nil")
	}
}

func TestClient_UnreachableServer(t *testing.T) {
	c := NewClient(WithBaseURL("http://127.0.0.1:54321"))
	_, err := c.Health(context.Background())
	if err == nil {
		t.Fatal("expected connection failure error, got nil")
	}
}
