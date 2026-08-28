package elastic

import (
	"context"
	"testing"
	"time"
)

func TestMockIndexLifecycleClient(t *testing.T) {
	mock := NewMockIndexLifecycleClient()
	now := time.Now().UTC()

	mock.Indices["logs-v1-2026.08.20"] = IndexInfo{
		Name:           "logs-v1-2026.08.20",
		DocCount:       150,
		StoreSizeBytes: 10240,
		CreationDate:   now.AddDate(0, 0, -8),
		Status:         "open",
	}
	mock.Indices["logs-v1-2026.08.28"] = IndexInfo{
		Name:           "logs-v1-2026.08.28",
		DocCount:       300,
		StoreSizeBytes: 20480,
		CreationDate:   now,
		Status:         "open",
	}

	ctx := context.Background()

	// Test ListIndices
	indices, err := mock.ListIndices(ctx, "logs-v1-*")
	if err != nil {
		t.Fatalf("unexpected list error: %v", err)
	}
	if len(indices) != 2 {
		t.Errorf("expected 2 indices, got %d", len(indices))
	}

	// Test GetIndexStats
	stats, err := mock.GetIndexStats(ctx, "logs-v1-*")
	if err != nil {
		t.Fatalf("unexpected stats error: %v", err)
	}
	if stats.TotalLogs != 450 {
		t.Errorf("expected 450 total logs, got %d", stats.TotalLogs)
	}
	if stats.TotalSizeBytes != 30720 {
		t.Errorf("expected 30720 bytes, got %d", stats.TotalSizeBytes)
	}

	// Test DeleteIndex
	if err := mock.DeleteIndex(ctx, "logs-v1-2026.08.20"); err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}

	// Test Delete Nonexistent
	if err := mock.DeleteIndex(ctx, "logs-v1-2026.08.20"); err == nil {
		t.Error("expected error deleting already deleted index")
	}
}
