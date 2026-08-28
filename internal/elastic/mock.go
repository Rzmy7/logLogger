package elastic

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

// MockIndexer is an in-memory implementation of Indexer and Searcher for tests.
type MockIndexer struct {
	mu            sync.Mutex
	Documents     []*models.LogDocument
	IndexNames    []string
	TemplateCheck bool
	Err           error
}

// NewMockIndexer creates a new MockIndexer.
func NewMockIndexer() *MockIndexer {
	return &MockIndexer{
		Documents:  make([]*models.LogDocument, 0),
		IndexNames: make([]string, 0),
	}
}

// EnsureTemplate records that the template was checked.
func (m *MockIndexer) EnsureTemplate(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.TemplateCheck = true
	return nil
}

// IndexLog records the indexed document in memory.
func (m *MockIndexer) IndexLog(ctx context.Context, logMsg *models.LogMessage, ingestedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}

	t, err := logMsg.ParsedTime()
	if err != nil {
		t = ingestedAt
	}
	indexName := IndexNameForTime(t)

	m.IndexNames = append(m.IndexNames, indexName)
	m.Documents = append(m.Documents, logMsg.ToDocument(ingestedAt))
	return nil
}

// Ping returns m.Err or nil.
func (m *MockIndexer) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Err
}

// SearchLogs filters the in-memory documents based on SearchParams.
func (m *MockIndexer) SearchLogs(ctx context.Context, params SearchParams) (*SearchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Err != nil {
		return nil, m.Err
	}

	var matched []*models.LogDocument
	for _, doc := range m.Documents {
		if params.TenantID != "" && doc.TenantID != params.TenantID {
			continue
		}
		if params.Service != "" && doc.Service != params.Service {
			continue
		}
		if params.Level != "" && strings.ToUpper(doc.Level) != strings.ToUpper(params.Level) {
			continue
		}
		if params.TraceID != "" && doc.TraceID != params.TraceID {
			continue
		}
		if params.Query != "" && params.Query != "*" && !strings.Contains(strings.ToLower(doc.Message), strings.ToLower(params.Query)) {
			continue
		}
		matched = append(matched, doc)
	}

	page := params.Page
	if page <= 0 {
		page = 1
	}
	size := params.Size
	if size <= 0 {
		size = 20
	}

	return &SearchResult{
		Total: int64(len(matched)),
		Page:  page,
		Size:  size,
		Pages: 1,
		Logs:  matched,
	}, nil
}

// MockIndexLifecycleClient provides an in-memory mock for IndexLifecycleClient.
type MockIndexLifecycleClient struct {
	mu          sync.RWMutex
	Indices     map[string]IndexInfo
	DeleteError error
	ListError   error
	StatsError  error
	PingError   error
}

// NewMockIndexLifecycleClient creates a new initialized mock client.
func NewMockIndexLifecycleClient() *MockIndexLifecycleClient {
	return &MockIndexLifecycleClient{
		Indices: make(map[string]IndexInfo),
	}
}

func (m *MockIndexLifecycleClient) ListIndices(ctx context.Context, pattern string) ([]IndexInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.ListError != nil {
		return nil, m.ListError
	}

	result := make([]IndexInfo, 0, len(m.Indices))
	for _, info := range m.Indices {
		result = append(result, info)
	}
	return result, nil
}

func (m *MockIndexLifecycleClient) DeleteIndex(ctx context.Context, indexName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.DeleteError != nil {
		return m.DeleteError
	}

	if _, exists := m.Indices[indexName]; !exists {
		return errors.New("index not found: 404 Not Found")
	}

	delete(m.Indices, indexName)
	return nil
}

func (m *MockIndexLifecycleClient) GetIndexStats(ctx context.Context, pattern string) (*LogStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.StatsError != nil {
		return nil, m.StatsError
	}

	stats := &LogStats{
		TotalIndices: len(m.Indices),
		Indices:      make([]IndexInfo, 0, len(m.Indices)),
	}

	for _, idx := range m.Indices {
		stats.TotalLogs += idx.DocCount
		stats.TotalSizeBytes += idx.StoreSizeBytes
		stats.Indices = append(stats.Indices, idx)
	}

	return stats, nil
}

func (m *MockIndexLifecycleClient) Ping(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.PingError
}
