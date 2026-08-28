package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

func TestMockIndexer_IndexBatch(t *testing.T) {
	mock := NewMockIndexer()
	ctx := context.Background()

	// 1. Empty Batch
	res, err := mock.IndexBatch(ctx, nil)
	if err != nil || res.TotalDocs != 0 {
		t.Fatalf("expected empty batch to succeed, got %v (err: %v)", res, err)
	}

	// 2. Batch with mixed tenants and dates
	docs := []*models.LogDocument{
		{
			TenantID:   "tenant-a",
			Timestamp:  "2026-08-28T10:00:00Z",
			Level:      "INFO",
			Service:    "order-service",
			Message:    "Order created",
			TraceID:    "trace-1",
			IP:         "192.168.1.1",
			IngestedAt: time.Now().UTC().Format(time.RFC3339Nano),
		},
		{
			TenantID:   "tenant-b",
			Timestamp:  "2026-08-29T12:00:00Z",
			Level:      "ERROR",
			Service:    "payment-api",
			Message:    "Card declined",
			TraceID:    "trace-2",
			IP:         "192.168.1.2",
			IngestedAt: time.Now().UTC().Format(time.RFC3339Nano),
		},
	}

	res, err = mock.IndexBatch(ctx, docs)
	if err != nil {
		t.Fatalf("unexpected error indexing batch: %v", err)
	}

	if res.TotalDocs != 2 || res.SuccessDocs != 2 || res.FailedDocs != 0 {
		t.Errorf("expected 2 total and 2 successful docs, got %+v", res)
	}
	if len(mock.Documents) != 2 {
		t.Errorf("expected 2 stored documents, got %d", len(mock.Documents))
	}
	if mock.IndexNames[0] != "logs-v1-2026.08.28" {
		t.Errorf("expected index logs-v1-2026.08.28, got %s", mock.IndexNames[0])
	}
	if mock.IndexNames[1] != "logs-v1-2026.08.29" {
		t.Errorf("expected index logs-v1-2026.08.29, got %s", mock.IndexNames[1])
	}
}

func TestBulkIndexer_HTTPParsing(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		respBody      string
		expectErr     bool
		expectSuccess int
		expectFailed  int
	}{
		{
			name:       "All Success",
			statusCode: http.StatusOK,
			respBody: `{
				"took": 12,
				"errors": false,
				"items": [
					{"index": {"_index": "logs-v1-2026.08.28", "_id": "doc1", "status": 201}},
					{"index": {"_index": "logs-v1-2026.08.28", "_id": "doc2", "status": 200}}
				]
			}`,
			expectErr:     false,
			expectSuccess: 2,
			expectFailed:  0,
		},
		{
			name:       "Partial Failure (One invalid item)",
			statusCode: http.StatusOK,
			respBody: `{
				"took": 15,
				"errors": true,
				"items": [
					{"index": {"_index": "logs-v1-2026.08.28", "_id": "doc1", "status": 201}},
					{
						"index": {
							"_index": "logs-v1-2026.08.28",
							"_id": "doc2",
							"status": 400,
							"error": {
								"type": "mapper_parsing_exception",
								"reason": "failed to parse field [ip]"
							}
						}
					}
				]
			}`,
			expectErr:     false,
			expectSuccess: 1,
			expectFailed:  1,
		},
		{
			name:       "Transient Cluster 503",
			statusCode: http.StatusServiceUnavailable,
			respBody:   `{"error": "node not ready"}`,
			expectErr:  true,
		},
		{
			name:       "Malformed JSON Response",
			statusCode: http.StatusOK,
			respBody:   `{invalid-json`,
			expectErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.respBody))
			}))
			defer server.Close()

			client, err := NewClient(server.URL)
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}

			docs := []*models.LogDocument{
				{
					TenantID:  "tenant-a",
					Timestamp: "2026-08-28T10:00:00Z",
					Service:   "svc-1",
					Level:     "INFO",
					Message:   "msg 1",
				},
				{
					TenantID:  "tenant-a",
					Timestamp: "2026-08-28T10:00:01Z",
					Service:   "svc-2",
					Level:     "ERROR",
					Message:   "msg 2",
				},
			}

			res, err := client.IndexBatch(context.Background(), docs)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.SuccessDocs != tc.expectSuccess {
				t.Errorf("expected %d successes, got %d", tc.expectSuccess, res.SuccessDocs)
			}
			if res.FailedDocs != tc.expectFailed {
				t.Errorf("expected %d failures, got %d", tc.expectFailed, res.FailedDocs)
			}
		})
	}
}

func TestLogDocument_DeterministicID(t *testing.T) {
	doc1 := &models.LogDocument{
		TenantID:  "tenant-a",
		Timestamp: "2026-08-28T10:00:00Z",
		Level:     "INFO",
		Service:   "order-svc",
		Message:   "Order #123 created",
		TraceID:   "trace-xyz",
	}

	doc2 := &models.LogDocument{
		TenantID:  "tenant-a",
		Timestamp: "2026-08-28T10:00:00Z",
		Level:     "INFO",
		Service:   "order-svc",
		Message:   "Order #123 created",
		TraceID:   "trace-xyz",
	}

	doc3 := &models.LogDocument{
		TenantID:  "tenant-b",
		Timestamp: "2026-08-28T10:00:00Z",
		Level:     "INFO",
		Service:   "order-svc",
		Message:   "Order #123 created",
		TraceID:   "trace-xyz",
	}

	id1 := doc1.DeterministicID()
	id2 := doc2.DeterministicID()
	id3 := doc3.DeterministicID()

	if id1 == "" || len(id1) != 32 {
		t.Errorf("expected 32-char hex ID, got %q (len %d)", id1, len(id1))
	}
	if id1 != id2 {
		t.Errorf("expected identical IDs for same document identity, got %s vs %s", id1, id2)
	}
	if id1 == id3 {
		t.Errorf("expected different IDs for different tenants, got identical %s", id1)
	}
}

func TestBulkIndexer_200DocumentBatch(t *testing.T) {
	docs := make([]*models.LogDocument, 200)
	for i := 0; i < 200; i++ {
		docs[i] = &models.LogDocument{
			TenantID:   fmt.Sprintf("tenant-%d", i%5),
			Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
			Level:      "INFO",
			Service:    "load-test",
			Message:    fmt.Sprintf("Simulated log message %d", i),
			TraceID:    fmt.Sprintf("trace-%d", i),
			IP:         "10.0.0.1",
			IngestedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}

	// Mock server that returns 200 bulk response for all 200 items
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		items := make([]esBulkResponseItem, 200)
		for i := 0; i < 200; i++ {
			items[i].Index.Status = 201
			items[i].Index.Index = "logs-v1-2026.08.28"
			items[i].Index.ID = fmt.Sprintf("doc-%d", i)
		}
		resp := esBulkResponse{Took: 25, Errors: false, Items: items}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	res, err := client.IndexBatch(context.Background(), docs)
	if err != nil {
		t.Fatalf("failed to index 200-doc batch: %v", err)
	}

	if res.TotalDocs != 200 || res.SuccessDocs != 200 || res.FailedDocs != 0 {
		t.Fatalf("expected 200/200 successes, got %+v", res)
	}
}
