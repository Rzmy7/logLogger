package elastic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

func TestIndexNameForTime(t *testing.T) {
	sampleTime := time.Date(2026, 8, 21, 10, 30, 0, 0, time.UTC)
	expected := "logs-v1-2026.08.21"
	if got := IndexNameForTime(sampleTime); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestNewClient_EmptyURL(t *testing.T) {
	_, err := NewClient("")
	if err == nil {
		t.Fatal("expected error for empty URL, got nil")
	}
}

func TestMockIndexer(t *testing.T) {
	mock := NewMockIndexer()
	var _ Indexer = mock
	var _ Searcher = mock

	ctx := context.Background()
	if err := mock.EnsureTemplate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.TemplateCheck {
		t.Error("expected TemplateCheck to be true")
	}

	logMsg := &models.LogMessage{
		Timestamp: "2026-08-21T10:00:00Z",
		Level:     "ERROR",
		Service:   "auth-service",
		Message:   "Password mismatch",
		TraceID:   "trace-001",
		IP:        "192.168.1.1",
	}

	ingestedAt := time.Date(2026, 8, 21, 10, 0, 1, 0, time.UTC)
	if err := mock.IndexLog(ctx, logMsg, ingestedAt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.Documents) != 1 {
		t.Fatalf("expected 1 document, got %d", len(mock.Documents))
	}

	// Test Search
	result, err := mock.SearchLogs(ctx, SearchParams{
		Service: "auth-service",
		Level:   "ERROR",
		Query:   "mismatch",
	})
	if err != nil {
		t.Fatalf("unexpected search error: %v", err)
	}
	if result.Total != 1 || len(result.Logs) != 1 {
		t.Errorf("expected 1 result, got %d", result.Total)
	}

	// Test error propagation
	mock.Err = errors.New("es down")
	if err := mock.IndexLog(ctx, logMsg, ingestedAt); err == nil {
		t.Fatal("expected error, got nil")
	}
}
