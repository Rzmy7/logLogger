package elastic

import (
	"context"
	"sync"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

// MockIndexer is an in-memory implementation of Indexer for tests.
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
