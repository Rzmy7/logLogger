package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// IndexInfo describes metadata for an individual Elasticsearch log index.
type IndexInfo struct {
	Name           string    `json:"name"`
	DocCount       int64     `json:"doc_count"`
	StoreSizeBytes int64     `json:"store_size_bytes"`
	CreationDate   time.Time `json:"creation_date"`
	Status         string    `json:"status"` // open, close
}

// LogStats provides cluster-wide and index-level storage statistics for logs.
type LogStats struct {
	TotalLogs      int64       `json:"total_logs"`
	TotalIndices   int         `json:"total_indices"`
	TotalSizeBytes int64       `json:"total_size_bytes"`
	OldestIndex    string      `json:"oldest_index,omitempty"`
	OldestLogDate  *time.Time  `json:"oldest_log_date,omitempty"`
	NewestIndex    string      `json:"newest_index,omitempty"`
	NewestLogDate  *time.Time  `json:"newest_log_date,omitempty"`
	Indices        []IndexInfo `json:"indices"`
}

// IndexLifecycleClient defines the contract for managing Elasticsearch log index lifecycles.
type IndexLifecycleClient interface {
	ListIndices(ctx context.Context, pattern string) ([]IndexInfo, error)
	DeleteIndex(ctx context.Context, indexName string) error
	GetIndexStats(ctx context.Context, pattern string) (*LogStats, error)
	Ping(ctx context.Context) error
}

// ListIndices retrieves a list of indices matching pattern with their doc counts, storage sizes, and creation dates.
func (c *Client) ListIndices(ctx context.Context, pattern string) ([]IndexInfo, error) {
	if strings.TrimSpace(pattern) == "" {
		pattern = IndexPattern
	}

	res, err := c.es.Cat.Indices(
		c.es.Cat.Indices.WithContext(ctx),
		c.es.Cat.Indices.WithIndex(pattern),
		c.es.Cat.Indices.WithFormat("json"),
		c.es.Cat.Indices.WithH("index", "docs.count", "store.size", "creation.date", "status"),
		c.es.Cat.Indices.WithBytes("b"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query elasticsearch indices: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// 404 means no indices exist matching pattern
		if res.StatusCode == 404 {
			return []IndexInfo{}, nil
		}
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("elasticsearch cat indices error: %s (%s)", res.Status(), string(body))
	}

	var rawIndices []struct {
		Index        string `json:"index"`
		DocsCount    string `json:"docs.count"`
		StoreSize    string `json:"store.size"`
		CreationDate string `json:"creation.date"`
		Status       string `json:"status"`
	}

	if err := json.NewDecoder(res.Body).Decode(&rawIndices); err != nil {
		return nil, fmt.Errorf("failed to decode indices response: %w", err)
	}

	indices := make([]IndexInfo, 0, len(rawIndices))
	for _, raw := range rawIndices {
		docCount, _ := strconv.ParseInt(raw.DocsCount, 10, 64)
		storeSize, _ := strconv.ParseInt(raw.StoreSize, 10, 64)

		var creationTime time.Time
		if epochMillis, err := strconv.ParseInt(raw.CreationDate, 10, 64); err == nil && epochMillis > 0 {
			creationTime = time.UnixMilli(epochMillis).UTC()
		}

		indices = append(indices, IndexInfo{
			Name:           raw.Index,
			DocCount:       docCount,
			StoreSizeBytes: storeSize,
			CreationDate:   creationTime,
			Status:         raw.Status,
		})
	}

	return indices, nil
}

// DeleteIndex deletes a specific Elasticsearch index by name.
func (c *Client) DeleteIndex(ctx context.Context, indexName string) error {
	if strings.TrimSpace(indexName) == "" {
		return errors.New("index name cannot be empty")
	}

	res, err := c.es.Indices.Delete(
		[]string{indexName},
		c.es.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to delete elasticsearch index %q: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return fmt.Errorf("index %q not found: %w", indexName, errors.New("404 Not Found"))
		}
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("elasticsearch delete index error for %q: %s (%s)", indexName, res.Status(), string(body))
	}

	return nil
}

// GetIndexStats computes overall storage and log document statistics across all log indices.
func (c *Client) GetIndexStats(ctx context.Context, pattern string) (*LogStats, error) {
	indices, err := c.ListIndices(ctx, pattern)
	if err != nil {
		return nil, err
	}

	stats := &LogStats{
		TotalIndices: len(indices),
		Indices:      indices,
	}

	for _, idx := range indices {
		stats.TotalLogs += idx.DocCount
		stats.TotalSizeBytes += idx.StoreSizeBytes
	}

	return stats, nil
}
