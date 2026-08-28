package retention

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/metrics"
)

var (
	// LogIndexRegex strictly enforces the logs-v1-YYYY.MM.DD index naming schema.
	LogIndexRegex = regexp.MustCompile(`^logs-v1-(\d{4})\.(\d{2})\.(\d{2})$`)

	// Sentinel errors for safety guarantees
	ErrProtectedIndex       = errors.New("cannot delete current active write index")
	ErrInvalidIndexName     = errors.New("invalid log index name format (must be logs-v1-YYYY.MM.DD)")
	ErrInvalidRetentionDays = errors.New("retention days must be a positive integer greater than zero")
	ErrFutureCutoff         = errors.New("cutoff timestamp cannot be in the future")
)

// RetentionResult summarizes the results of an index retention cycle.
type RetentionResult struct {
	EvaluatedCount int           `json:"evaluated_count"`
	DeletedCount   int           `json:"deleted_count"`
	DeletedIndices []string      `json:"deleted_indices"`
	CutoffDate     time.Time     `json:"cutoff_date"`
	Duration       time.Duration `json:"duration"`
}

// Manager defines the business contract for log lifecycle and retention management.
type Manager interface {
	RunRetention(ctx context.Context, retentionDays int) (*RetentionResult, error)
	DeleteIndexByName(ctx context.Context, indexName string) error
	DeleteIndicesBefore(ctx context.Context, before time.Time) (*RetentionResult, error)
	GetStats(ctx context.Context) (*elastic.LogStats, error)
}

// ElasticsearchRetentionManager implements Manager for Elasticsearch storage.
type ElasticsearchRetentionManager struct {
	client elastic.IndexLifecycleClient
}

// NewManager creates a new ElasticsearchRetentionManager.
func NewManager(client elastic.IndexLifecycleClient) *ElasticsearchRetentionManager {
	return &ElasticsearchRetentionManager{
		client: client,
	}
}

// ParseIndexDate extracts the date from a valid log index name (logs-v1-YYYY.MM.DD).
func ParseIndexDate(indexName string) (time.Time, error) {
	matches := LogIndexRegex.FindStringSubmatch(indexName)
	if len(matches) != 4 {
		return time.Time{}, fmt.Errorf("%w: %q", ErrInvalidIndexName, indexName)
	}

	dateStr := fmt.Sprintf("%s-%s-%s", matches[1], matches[2], matches[3])
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %q (%v)", ErrInvalidIndexName, indexName, err)
	}

	return t.UTC(), nil
}

// IsActiveIndex returns true if indexName corresponds to today's UTC active write index.
func IsActiveIndex(indexName string, now time.Time) bool {
	todayIndex := elastic.IndexNameForTime(now)
	return indexName == todayIndex
}

// RunRetention evaluates all logs-v1-* indices and deletes those older than retentionDays.
func (m *ElasticsearchRetentionManager) RunRetention(ctx context.Context, retentionDays int) (*RetentionResult, error) {
	if retentionDays <= 0 {
		return nil, ErrInvalidRetentionDays
	}

	start := time.Now()
	nowUTC := start.UTC()
	cutoffDate := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -retentionDays)

	defer func() {
		metrics.RetentionDuration.Observe(time.Since(start).Seconds())
	}()

	indices, err := m.client.ListIndices(ctx, elastic.IndexPattern)
	if err != nil {
		metrics.RetentionRunsTotal.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("failed to list indices for retention: %w", err)
	}

	result := &RetentionResult{
		EvaluatedCount: len(indices),
		DeletedIndices: make([]string, 0),
		CutoffDate:     cutoffDate,
	}
	metrics.RetentionIndicesEvaluatedTotal.Add(float64(len(indices)))

	for _, idx := range indices {
		// Strictly validate naming pattern
		idxDate, err := ParseIndexDate(idx.Name)
		if err != nil {
			log.Printf("[RETENTION] Skipping non-standard index %q: %v", idx.Name, err)
			continue
		}

		// Never delete active/today's write index
		if IsActiveIndex(idx.Name, nowUTC) {
			continue
		}

		// Check if expired: index date is strictly before cutoff date
		if idxDate.Before(cutoffDate) {
			log.Printf("[RETENTION] Expired index identified: %s (date=%s, cutoff=%s)", idx.Name, idxDate.Format("2006-01-02"), cutoffDate.Format("2006-01-02"))
			if delErr := m.client.DeleteIndex(ctx, idx.Name); delErr != nil {
				log.Printf("[RETENTION ERROR] Failed to delete expired index %q: %v", idx.Name, delErr)
				metrics.RetentionRunsTotal.WithLabelValues("error").Inc()
				return result, fmt.Errorf("failed to delete expired index %q: %w", idx.Name, delErr)
			}
			result.DeletedIndices = append(result.DeletedIndices, idx.Name)
			result.DeletedCount++
			metrics.RetentionIndicesDeletedTotal.Inc()
		}
	}

	result.Duration = time.Since(start)
	metrics.RetentionRunsTotal.WithLabelValues("success").Inc()
	log.Printf("[RETENTION] Cycle complete: evaluated=%d, deleted=%d, duration=%v", result.EvaluatedCount, result.DeletedCount, result.Duration)
	return result, nil
}

// DeleteIndexByName safely deletes a single log index after rigorous safety validation.
func (m *ElasticsearchRetentionManager) DeleteIndexByName(ctx context.Context, indexName string) error {
	// 1. Validate index name against logs-v1-YYYY.MM.DD regex
	if _, err := ParseIndexDate(indexName); err != nil {
		metrics.AdminDeletionsTotal.WithLabelValues("by_name", "invalid_input").Inc()
		return err
	}

	// 2. Reject deletion of today's active write index
	if IsActiveIndex(indexName, time.Now().UTC()) {
		metrics.AdminDeletionsTotal.WithLabelValues("by_name", "rejected_protected").Inc()
		return fmt.Errorf("%w: %q is the active write index", ErrProtectedIndex, indexName)
	}

	// 3. Execute deletion
	if err := m.client.DeleteIndex(ctx, indexName); err != nil {
		metrics.AdminDeletionsTotal.WithLabelValues("by_name", "error").Inc()
		return err
	}

	metrics.AdminDeletionsTotal.WithLabelValues("by_name", "success").Inc()
	log.Printf("[ADMIN LIFECYCLE] Index %q deleted successfully", indexName)
	return nil
}

// DeleteIndicesBefore deletes all log indices created before a specific timestamp.
func (m *ElasticsearchRetentionManager) DeleteIndicesBefore(ctx context.Context, before time.Time) (*RetentionResult, error) {
	nowUTC := time.Now().UTC()
	beforeUTC := before.UTC()

	if beforeUTC.After(nowUTC) {
		metrics.AdminDeletionsTotal.WithLabelValues("before_timestamp", "invalid_input").Inc()
		return nil, ErrFutureCutoff
	}

	start := time.Now()
	indices, err := m.client.ListIndices(ctx, elastic.IndexPattern)
	if err != nil {
		metrics.AdminDeletionsTotal.WithLabelValues("before_timestamp", "error").Inc()
		return nil, fmt.Errorf("failed to list indices: %w", err)
	}

	result := &RetentionResult{
		EvaluatedCount: len(indices),
		DeletedIndices: make([]string, 0),
		CutoffDate:     beforeUTC,
	}

	for _, idx := range indices {
		idxDate, err := ParseIndexDate(idx.Name)
		if err != nil {
			continue
		}

		if IsActiveIndex(idx.Name, nowUTC) {
			continue
		}

		if idxDate.Before(beforeUTC) {
			if delErr := m.client.DeleteIndex(ctx, idx.Name); delErr != nil {
				metrics.AdminDeletionsTotal.WithLabelValues("before_timestamp", "error").Inc()
				return result, fmt.Errorf("failed to delete index %q: %w", idx.Name, delErr)
			}
			result.DeletedIndices = append(result.DeletedIndices, idx.Name)
			result.DeletedCount++
		}
	}

	result.Duration = time.Since(start)
	metrics.AdminDeletionsTotal.WithLabelValues("before_timestamp", "success").Inc()
	log.Printf("[ADMIN LIFECYCLE] Delete before %s complete: evaluated=%d, deleted=%d", beforeUTC.Format(time.RFC3339), result.EvaluatedCount, result.DeletedCount)
	return result, nil
}

// GetStats returns comprehensive storage statistics with annotated oldest and newest log dates.
func (m *ElasticsearchRetentionManager) GetStats(ctx context.Context) (*elastic.LogStats, error) {
	stats, err := m.client.GetIndexStats(ctx, elastic.IndexPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage stats: %w", err)
	}

	// Filter and sort log indices by date
	type datedIndex struct {
		name string
		date time.Time
	}
	var logIndices []datedIndex

	for _, idx := range stats.Indices {
		if t, err := ParseIndexDate(idx.Name); err == nil {
			logIndices = append(logIndices, datedIndex{name: idx.Name, date: t})
		}
	}

	if len(logIndices) > 0 {
		sort.Slice(logIndices, func(i, j int) bool {
			return logIndices[i].date.Before(logIndices[j].date)
		})

		stats.OldestIndex = logIndices[0].name
		oldestDate := logIndices[0].date
		stats.OldestLogDate = &oldestDate

		stats.NewestIndex = logIndices[len(logIndices)-1].name
		newestDate := logIndices[len(logIndices)-1].date
		stats.NewestLogDate = &newestDate
	}

	return stats, nil
}
