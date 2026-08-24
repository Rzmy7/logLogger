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
	var _ MetricsReader = mock

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

	// Query live metrics
	total, services, err := mock.GetLiveMetrics(ctx, []string{"payment-api"})
	if err != nil {
		t.Fatalf("unexpected GetLiveMetrics error: %v", err)
	}
	if total != 1 {
		t.Errorf("expected total 1, got %d", total)
	}
	if services["payment-api"].TotalLogs != 1 {
		t.Errorf("expected payment-api total logs 1, got %d", services["payment-api"].TotalLogs)
	}

	// Query top errors
	topErrors, err := mock.GetTopErrors(ctx, 5)
	if err != nil {
		t.Fatalf("unexpected GetTopErrors error: %v", err)
	}
	if len(topErrors) != 1 || topErrors[0].Message != "Gateway timeout" {
		t.Errorf("expected 1 top error with message 'Gateway timeout', got %v", topErrors)
	}

	// Query top services
	topServices, err := mock.GetTopServices(ctx, 5)
	if err != nil {
		t.Fatalf("unexpected GetTopServices error: %v", err)
	}
	if len(topServices) != 1 || topServices[0].Service != "payment-api" {
		t.Errorf("expected 1 top service 'payment-api', got %v", topServices)
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
