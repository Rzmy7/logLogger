package retention

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
)

func TestParseIndexDate(t *testing.T) {
	tests := []struct {
		name      string
		indexName string
		wantDate  string
		wantErr   bool
	}{
		{
			name:      "Valid daily index",
			indexName: "logs-v1-2026.08.28",
			wantDate:  "2026-08-28",
			wantErr:   false,
		},
		{
			name:      "Valid past index",
			indexName: "logs-v1-2025.01.15",
			wantDate:  "2025-01-15",
			wantErr:   false,
		},
		{
			name:      "Invalid prefix",
			indexName: "other-index-2026.08.28",
			wantErr:   true,
		},
		{
			name:      "Malformed date separator",
			indexName: "logs-v1-2026-08-28",
			wantErr:   true,
		},
		{
			name:      "Arbitrary query string",
			indexName: "logs-v1-*",
			wantErr:   true,
		},
		{
			name:      "Kibana system index",
			indexName: ".kibana_8.11.4_001",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseIndexDate(tt.indexName)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseIndexDate(%q) error = %v, wantErr = %v", tt.indexName, err, tt.wantErr)
			}
			if !tt.wantErr && parsed.Format("2006-01-02") != tt.wantDate {
				t.Errorf("ParseIndexDate(%q) = %v, want %v", tt.indexName, parsed.Format("2006-01-02"), tt.wantDate)
			}
		})
	}
}

func TestRetentionManager_RunRetention(t *testing.T) {
	mockClient := elastic.NewMockIndexLifecycleClient()
	mgr := NewManager(mockClient)
	ctx := context.Background()

	now := time.Now().UTC()
	todayIndex := elastic.IndexNameForTime(now)
	index10DaysAgo := elastic.IndexNameForTime(now.AddDate(0, 0, -10))
	index45DaysAgo := elastic.IndexNameForTime(now.AddDate(0, 0, -45))
	index60DaysAgo := elastic.IndexNameForTime(now.AddDate(0, 0, -60))

	mockClient.Indices[todayIndex] = elastic.IndexInfo{Name: todayIndex, DocCount: 500}
	mockClient.Indices[index10DaysAgo] = elastic.IndexInfo{Name: index10DaysAgo, DocCount: 200}
	mockClient.Indices[index45DaysAgo] = elastic.IndexInfo{Name: index45DaysAgo, DocCount: 150}
	mockClient.Indices[index60DaysAgo] = elastic.IndexInfo{Name: index60DaysAgo, DocCount: 100}

	// 1. Run retention with 30 days retention
	res, err := mgr.RunRetention(ctx, 30)
	if err != nil {
		t.Fatalf("unexpected retention error: %v", err)
	}

	if res.EvaluatedCount != 4 {
		t.Errorf("expected 4 evaluated indices, got %d", res.EvaluatedCount)
	}
	if res.DeletedCount != 2 {
		t.Errorf("expected 2 deleted indices, got %d", res.DeletedCount)
	}

	// Verify that todayIndex and index10DaysAgo remain
	if _, exists := mockClient.Indices[todayIndex]; !exists {
		t.Errorf("active index %s was unexpectedly deleted", todayIndex)
	}
	if _, exists := mockClient.Indices[index10DaysAgo]; !exists {
		t.Errorf("recent index %s was unexpectedly deleted", index10DaysAgo)
	}

	// Verify that index45DaysAgo and index60DaysAgo were deleted
	if _, exists := mockClient.Indices[index45DaysAgo]; exists {
		t.Errorf("expired index %s was not deleted", index45DaysAgo)
	}
	if _, exists := mockClient.Indices[index60DaysAgo]; exists {
		t.Errorf("expired index %s was not deleted", index60DaysAgo)
	}

	// 2. Test Invalid retention days
	if _, err := mgr.RunRetention(ctx, 0); !errors.Is(err, ErrInvalidRetentionDays) {
		t.Errorf("expected ErrInvalidRetentionDays, got %v", err)
	}
	if _, err := mgr.RunRetention(ctx, -5); !errors.Is(err, ErrInvalidRetentionDays) {
		t.Errorf("expected ErrInvalidRetentionDays, got %v", err)
	}
}

func TestRetentionManager_DeleteIndexByName(t *testing.T) {
	mockClient := elastic.NewMockIndexLifecycleClient()
	mgr := NewManager(mockClient)
	ctx := context.Background()

	now := time.Now().UTC()
	todayIndex := elastic.IndexNameForTime(now)
	historicalIndex := elastic.IndexNameForTime(now.AddDate(0, 0, -15))

	mockClient.Indices[todayIndex] = elastic.IndexInfo{Name: todayIndex, DocCount: 100}
	mockClient.Indices[historicalIndex] = elastic.IndexInfo{Name: historicalIndex, DocCount: 50}

	// 1. Attempt to delete active write index -> must be rejected
	err := mgr.DeleteIndexByName(ctx, todayIndex)
	if !errors.Is(err, ErrProtectedIndex) {
		t.Fatalf("expected ErrProtectedIndex when deleting active index, got: %v", err)
	}
	if _, exists := mockClient.Indices[todayIndex]; !exists {
		t.Errorf("active index %s was deleted despite protection", todayIndex)
	}

	// 2. Attempt to delete invalid index name -> must be rejected
	if err := mgr.DeleteIndexByName(ctx, "invalid_index_name"); !errors.Is(err, ErrInvalidIndexName) {
		t.Errorf("expected ErrInvalidIndexName, got: %v", err)
	}
	if err := mgr.DeleteIndexByName(ctx, ".kibana"); !errors.Is(err, ErrInvalidIndexName) {
		t.Errorf("expected ErrInvalidIndexName for system index, got: %v", err)
	}

	// 3. Delete valid historical index -> success
	if err := mgr.DeleteIndexByName(ctx, historicalIndex); err != nil {
		t.Fatalf("unexpected error deleting historical index: %v", err)
	}
	if _, exists := mockClient.Indices[historicalIndex]; exists {
		t.Errorf("historical index %s still exists after deletion", historicalIndex)
	}
}

func TestRetentionManager_DeleteIndicesBefore(t *testing.T) {
	mockClient := elastic.NewMockIndexLifecycleClient()
	mgr := NewManager(mockClient)
	ctx := context.Background()

	now := time.Now().UTC()
	todayIndex := elastic.IndexNameForTime(now)
	index5DaysAgo := elastic.IndexNameForTime(now.AddDate(0, 0, -5))
	index20DaysAgo := elastic.IndexNameForTime(now.AddDate(0, 0, -20))

	mockClient.Indices[todayIndex] = elastic.IndexInfo{Name: todayIndex, DocCount: 100}
	mockClient.Indices[index5DaysAgo] = elastic.IndexInfo{Name: index5DaysAgo, DocCount: 80}
	mockClient.Indices[index20DaysAgo] = elastic.IndexInfo{Name: index20DaysAgo, DocCount: 60}

	// 1. Reject future timestamp
	future := now.Add(24 * time.Hour)
	if _, err := mgr.DeleteIndicesBefore(ctx, future); !errors.Is(err, ErrFutureCutoff) {
		t.Errorf("expected ErrFutureCutoff for future timestamp, got: %v", err)
	}

	// 2. Delete before 10 days ago
	cutoff := now.AddDate(0, 0, -10)
	res, err := mgr.DeleteIndicesBefore(ctx, cutoff)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.DeletedCount != 1 {
		t.Errorf("expected 1 deleted index, got %d", res.DeletedCount)
	}
	if _, exists := mockClient.Indices[index20DaysAgo]; exists {
		t.Errorf("index %s should have been deleted", index20DaysAgo)
	}
	if _, exists := mockClient.Indices[index5DaysAgo]; !exists {
		t.Errorf("index %s should have been preserved", index5DaysAgo)
	}
	if _, exists := mockClient.Indices[todayIndex]; !exists {
		t.Errorf("today index %s should have been preserved", todayIndex)
	}
}

func TestRetentionManager_GetStats(t *testing.T) {
	mockClient := elastic.NewMockIndexLifecycleClient()
	mgr := NewManager(mockClient)
	ctx := context.Background()

	now := time.Now().UTC()
	idx1 := elastic.IndexNameForTime(now.AddDate(0, 0, -10))
	idx2 := elastic.IndexNameForTime(now)

	mockClient.Indices[idx1] = elastic.IndexInfo{Name: idx1, DocCount: 200, StoreSizeBytes: 1000}
	mockClient.Indices[idx2] = elastic.IndexInfo{Name: idx2, DocCount: 300, StoreSizeBytes: 1500}

	stats, err := mgr.GetStats(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting stats: %v", err)
	}

	if stats.TotalLogs != 500 {
		t.Errorf("expected 500 total logs, got %d", stats.TotalLogs)
	}
	if stats.TotalIndices != 2 {
		t.Errorf("expected 2 total indices, got %d", stats.TotalIndices)
	}
	if stats.OldestIndex != idx1 {
		t.Errorf("expected oldest index %s, got %s", idx1, stats.OldestIndex)
	}
	if stats.NewestIndex != idx2 {
		t.Errorf("expected newest index %s, got %s", idx2, stats.NewestIndex)
	}
}

func TestRetentionRunner_RunOnce_NoOverlap(t *testing.T) {
	mockClient := elastic.NewMockIndexLifecycleClient()
	mgr := NewManager(mockClient)

	runner := NewRetentionRunner(mgr, 30, 100*time.Millisecond)

	ctx := context.Background()
	res, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result on initial run")
	}
}
