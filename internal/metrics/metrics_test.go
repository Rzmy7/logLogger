package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPMetricsMiddleware(t *testing.T) {
	middleware := HTTPMetricsMiddleware("test-service")

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/error" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	wrapped := middleware(nextHandler)

	// Test Success
	req := httptest.NewRequest(http.MethodGet, "/api/v1/test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}

	// Test Error
	reqErr := httptest.NewRequest(http.MethodPost, "/error", nil)
	recErr := httptest.NewRecorder()
	wrapped.ServeHTTP(recErr, reqErr)

	if recErr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", recErr.Code)
	}

	// Test Prometheus Handler Endpoint
	promReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promRec := httptest.NewRecorder()
	Handler().ServeHTTP(promRec, promReq)

	if promRec.Code != http.StatusOK {
		t.Errorf("expected 200 OK on /metrics, got %d", promRec.Code)
	}

	body := promRec.Body.String()
	if body == "" {
		t.Error("expected non-empty Prometheus metrics body")
	}
}
