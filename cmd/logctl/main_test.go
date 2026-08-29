package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestApp(mockServerURL string, stdinContent string) (*App, *bytes.Buffer, *bytes.Buffer) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	stdin := strings.NewReader(stdinContent)

	app := &App{
		stdout: stdout,
		stderr: stderr,
		stdin:  stdin,
	}
	return app, stdout, stderr
}

func setupMockAnalyticsServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"status": "healthy",
				"dependencies": {"elasticsearch": "healthy", "redis": "healthy"},
				"meta": {"request_id": "test-req-1", "timestamp": "2026-08-29T12:00:00Z"}
			}`))

		case r.URL.Path == "/admin/logs/stats":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"data": {
					"total_logs": 200,
					"total_indices": 1,
					"total_size_bytes": 20480,
					"oldest_index": "logs-v1-2026.08.29",
					"newest_index": "logs-v1-2026.08.29",
					"indices": [
						{"name": "logs-v1-2026.08.29", "doc_count": 200, "store_size_bytes": 20480, "creation_date": "2026-08-29T00:00:00Z", "status": "open"}
					]
				},
				"meta": {"request_id": "test-req-2"}
			}`))

		case r.URL.Path == "/search":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"data": {
					"total": 1,
					"page": 1,
					"size": 20,
					"logs": [
						{
							"id": "doc-99",
							"timestamp": "2026-08-29T10:00:00Z",
							"level": "INFO",
							"service": "auth-service",
							"message": "User logged in",
							"trace_id": "tr-99",
							"tenant_id": "default"
						}
					]
				},
				"meta": {"request_id": "test-req-3"}
			}`))

		case r.URL.Path == "/admin/logs/indices/logs-v1-2020.01.01" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"index":"logs-v1-2020.01.01","status":"deleted"},"meta":{"request_id":"test-req-4"}}`))

		case r.URL.Path == "/admin/logs/indices/logs-v1-2026.08.29" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":{"code":"PROTECTED_INDEX","message":"today's active index is protected"},"meta":{"request_id":"test-req-5"}}`))

		case r.URL.Path == "/admin/logs" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"data": {
					"cutoff_date": "2026-08-01T00:00:00Z",
					"evaluated_count": 5,
					"deleted_count": 2,
					"deleted_indices": ["logs-v1-2026.07.01", "logs-v1-2026.07.02"],
					"duration": 50000
				},
				"meta": {"request_id": "test-req-6"}
			}`))

		case r.URL.Path == "/admin/logs/retention/run" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"data": {
					"cutoff_date": "2026-07-30T00:00:00Z",
					"evaluated_count": 3,
					"deleted_count": 1,
					"deleted_indices": ["logs-v1-2026.07.10"],
					"duration": 60000
				},
				"meta": {"request_id": "test-req-7"}
			}`))

		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"NOT_FOUND","message":"not found"}}`))
		}
	}))
}

func TestCLI_Help(t *testing.T) {
	app, stdout, _ := newTestApp("", "")
	code := app.Run([]string{"--help"})
	if code != 0 {
		t.Errorf("expected exit 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "logctl - Administrative CLI") {
		t.Errorf("expected help text, got: %s", stdout.String())
	}
}

func TestCLI_Health_TabularAndJSON(t *testing.T) {
	ts := setupMockAnalyticsServer()
	defer ts.Close()

	// 1. Tabular
	app, stdout, _ := newTestApp(ts.URL, "")
	code := app.Run([]string{"--api-url", ts.URL, "health"})
	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "STATUS") || !strings.Contains(stdout.String(), "healthy") {
		t.Errorf("unexpected tabular output: %s", stdout.String())
	}

	// 2. JSON
	app2, stdout2, _ := newTestApp(ts.URL, "")
	code2 := app2.Run([]string{"--api-url", ts.URL, "health", "--json"})
	if code2 != 0 {
		t.Fatalf("expected exit 0, got %d", code2)
	}
	var raw map[string]any
	if err := json.Unmarshal(stdout2.Bytes(), &raw); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if raw["status"] != "healthy" {
		t.Errorf("expected status healthy, got %v", raw["status"])
	}
}

func TestCLI_Logs_Stats(t *testing.T) {
	ts := setupMockAnalyticsServer()
	defer ts.Close()

	app, stdout, _ := newTestApp(ts.URL, "")
	code := app.Run([]string{"--api-url", ts.URL, "logs", "stats"})
	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Total Logs:     200") {
		t.Errorf("unexpected stats output: %s", stdout.String())
	}
}

func TestCLI_Logs_Search(t *testing.T) {
	ts := setupMockAnalyticsServer()
	defer ts.Close()

	app, stdout, _ := newTestApp(ts.URL, "")
	code := app.Run([]string{"--api-url", ts.URL, "logs", "search", "--service", "auth-service", "--level", "INFO"})
	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "User logged in") || !strings.Contains(stdout.String(), "auth-service") {
		t.Errorf("unexpected search output: %s", stdout.String())
	}
}

func TestCLI_Logs_DeleteIndex_Confirmations(t *testing.T) {
	ts := setupMockAnalyticsServer()
	defer ts.Close()

	// 1. Interactive 'y' confirmation
	app1, stdout1, _ := newTestApp(ts.URL, "y\n")
	code1 := app1.Run([]string{"--api-url", ts.URL, "logs", "delete-index", "logs-v1-2020.01.01"})
	if code1 != 0 {
		t.Fatalf("expected exit 0 on 'y', got %d", code1)
	}
	if !strings.Contains(stdout1.String(), "Success: Index \"logs-v1-2020.01.01\" was deleted.") {
		t.Errorf("unexpected delete output: %s", stdout1.String())
	}

	// 2. Interactive 'n' rejection
	app2, _, stderr2 := newTestApp(ts.URL, "n\n")
	code2 := app2.Run([]string{"--api-url", ts.URL, "logs", "delete-index", "logs-v1-2020.01.01"})
	if code2 != 1 {
		t.Fatalf("expected exit 1 on 'n', got %d", code2)
	}
	if !strings.Contains(stderr2.String(), "Operation canceled by user.") {
		t.Errorf("unexpected cancel output: %s", stderr2.String())
	}

	// 3. Flag --yes bypass
	app3, stdout3, _ := newTestApp(ts.URL, "")
	code3 := app3.Run([]string{"--api-url", ts.URL, "logs", "delete-index", "logs-v1-2020.01.01", "--yes"})
	if code3 != 0 {
		t.Fatalf("expected exit 0 on --yes, got %d", code3)
	}
	if !strings.Contains(stdout3.String(), "Success: Index \"logs-v1-2020.01.01\" was deleted.") {
		t.Errorf("unexpected delete output: %s", stdout3.String())
	}

	// 4. Server-Side Protected Index Rejection (422)
	app4, _, stderr4 := newTestApp(ts.URL, "")
	code4 := app4.Run([]string{"--api-url", ts.URL, "logs", "delete-index", "logs-v1-2026.08.29", "--yes"})
	if code4 != 1 {
		t.Fatalf("expected exit 1 on protected index, got %d", code4)
	}
	if !strings.Contains(stderr4.String(), "PROTECTED_INDEX") {
		t.Errorf("expected PROTECTED_INDEX error in stderr, got: %s", stderr4.String())
	}
}

func TestCLI_Logs_DeleteBefore(t *testing.T) {
	ts := setupMockAnalyticsServer()
	defer ts.Close()

	// 1. With --yes
	app, stdout, _ := newTestApp(ts.URL, "")
	code := app.Run([]string{"--api-url", ts.URL, "logs", "delete-before", "2026-08-01T00:00:00Z", "--yes"})
	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	if !strings.Contains(stdout.String(), "Deleted Indices:   2") {
		t.Errorf("unexpected delete before output: %s", stdout.String())
	}

	// 2. Canceled by user
	app2, _, stderr2 := newTestApp(ts.URL, "no\n")
	code2 := app2.Run([]string{"--api-url", ts.URL, "logs", "delete-before", "2026-08-01T00:00:00Z"})
	if code2 != 1 {
		t.Fatalf("expected exit 1 on cancel, got %d", code2)
	}
	if !strings.Contains(stderr2.String(), "Operation canceled by user.") {
		t.Errorf("unexpected cancel output: %s", stderr2.String())
	}
}

func TestCLI_Retention_StatusAndRun(t *testing.T) {
	ts := setupMockAnalyticsServer()
	defer ts.Close()

	// Status
	app1, stdout1, _ := newTestApp(ts.URL, "")
	code1 := app1.Run([]string{"--api-url", ts.URL, "retention", "status"})
	if code1 != 0 {
		t.Fatalf("expected exit 0, got %d", code1)
	}
	if !strings.Contains(stdout1.String(), "Log Retention & Storage Status") {
		t.Errorf("unexpected retention status output: %s", stdout1.String())
	}

	// Run
	app2, stdout2, _ := newTestApp(ts.URL, "")
	code2 := app2.Run([]string{"--api-url", ts.URL, "retention", "run", "--days", "30"})
	if code2 != 0 {
		t.Fatalf("expected exit 0, got %d", code2)
	}
	if !strings.Contains(stdout2.String(), "Retention Execution Complete") {
		t.Errorf("unexpected retention run output: %s", stdout2.String())
	}
}

func TestCLI_UnknownCommandAndMissingArgs(t *testing.T) {
	app, _, stderr := newTestApp("", "")
	code := app.Run([]string{"nonexistent"})
	if code != 1 {
		t.Errorf("expected exit 1 for unknown command, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Error: unknown command") {
		t.Errorf("expected error message in stderr, got: %s", stderr.String())
	}

	app2, _, stderr2 := newTestApp("", "")
	code2 := app2.Run([]string{"logs", "delete-index"})
	if code2 != 1 {
		t.Errorf("expected exit 1 for missing arg, got %d", code2)
	}
	if !strings.Contains(stderr2.String(), "Error: missing required argument") {
		t.Errorf("expected missing arg error in stderr, got: %s", stderr2.String())
	}
}
