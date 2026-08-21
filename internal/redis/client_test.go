package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/Rzmy7/logLogger/internal/models"
)

func TestNewClient_Validation(t *testing.T) {
	_, err := NewClient("")
	if err == nil {
		t.Fatal("expected error for empty redis URL, got nil")
	}
}

func TestMockMetricsRecorder_HappyPath(t *testing.T) {
	mock := NewMockMetricsRecorder()
	var _ MetricsRecorder = mock

	ctx := context.Background()

	logMsg := &models.LogMessage{
		Timestamp: "2026-08-21T10:00:00Z",
		Level:     "ERROR",
		Service:   "payment-api",
		Message:   "Gateway timeout",
		TraceID:   "trace-123",
		IP:        "192.168.1.10",
	}

	rawJSON := []byte(`{"message":"Gateway timeout"}`)
	if err := mock.RecordLog(ctx, logMsg, rawJSON); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify total logs
	if count := mock.Counters["stats:logs:total"]; count != 1 {
		t.Errorf("expected stats:logs:total to be 1, got %d", count)
	}

	// Verify service logs
	if count := mock.Counters["stats:logs:payment-api"]; count != 1 {
		t.Errorf("expected stats:logs:payment-api to be 1, got %d", count)
	}

	// Verify level logs
	if count := mock.Counters["stats:logs:level:error"]; count != 1 {
		t.Errorf("expected stats:logs:level:error to be 1, got %d", count)
	}

	// Verify service leaderboard
	if score := mock.Leaderboards["leaderboard:services"]["payment-api"]; score != 1 {
		t.Errorf("expected leaderboard:services payment-api score to be 1, got %f", score)
	}

	// Verify error counters
	if count := mock.Counters["stats:errors:payment-api"]; count != 1 {
		t.Errorf("expected stats:errors:payment-api to be 1, got %d", count)
	}

	// Verify error leaderboard
	if score := mock.Leaderboards["leaderboard:errors"]["Gateway timeout"]; score != 1 {
		t.Errorf("expected leaderboard:errors score 1, got %f", score)
	}

	// Verify recent errors list
	if len(mock.Lists["recent:errors:payment-api"]) != 1 {
		t.Fatalf("expected 1 recent error, got %d", len(mock.Lists["recent:errors:payment-api"]))
	}

	// Test error simulation
	mock.Err = errors.New("redis down")
	if err := mock.RecordLog(ctx, logMsg, rawJSON); err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !mock.Closed {
		t.Error("expected closed to be true")
	}
}
