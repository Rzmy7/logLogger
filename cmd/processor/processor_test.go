package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/kafka"
	"github.com/Rzmy7/logLogger/internal/models"
	"github.com/Rzmy7/logLogger/internal/redis"
	kafkaGo "github.com/segmentio/kafka-go"
)

// MockMessageConsumer simulates Kafka message consumption for batch unit tests.
type MockMessageConsumer struct {
	mu          sync.Mutex
	Messages    []kafkaGo.Message
	Committed   []kafkaGo.Message
	FetchErr    error
	CommitErr   error
	fetchIdx    int
	closed      bool
	fetchSignal chan struct{}
}

func NewMockMessageConsumer(msgs []kafkaGo.Message) *MockMessageConsumer {
	return &MockMessageConsumer{
		Messages:    msgs,
		Committed:   make([]kafkaGo.Message, 0),
		fetchSignal: make(chan struct{}, 1),
	}
}

func (m *MockMessageConsumer) FetchMessage(ctx context.Context) (kafkaGo.Message, error) {
	m.mu.Lock()
	if m.FetchErr != nil {
		err := m.FetchErr
		m.mu.Unlock()
		return kafkaGo.Message{}, err
	}
	if m.fetchIdx >= len(m.Messages) {
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return kafkaGo.Message{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			return kafkaGo.Message{}, context.Canceled
		}
	}
	msg := m.Messages[m.fetchIdx]
	m.fetchIdx++
	m.mu.Unlock()
	return msg, nil
}

func (m *MockMessageConsumer) CommitMessages(ctx context.Context, msgs ...kafkaGo.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.CommitErr != nil {
		return m.CommitErr
	}
	m.Committed = append(m.Committed, msgs...)
	return nil
}

func (m *MockMessageConsumer) Stats() kafkaGo.ReaderStats {
	return kafkaGo.ReaderStats{}
}

func (m *MockMessageConsumer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// PartialFailureBulkIndexer simulates Elasticsearch bulk with specific item rejections.
type PartialFailureBulkIndexer struct {
	mu           sync.Mutex
	FailDocIndex int // index to fail
	IndexedDocs  []*models.LogDocument
}

func (p *PartialFailureBulkIndexer) IndexBatch(ctx context.Context, docs []*models.LogDocument) (*elastic.BulkResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.IndexedDocs = append(p.IndexedDocs, docs...)
	res := &elastic.BulkResult{
		TotalDocs:   len(docs),
		ItemSuccess: make([]bool, len(docs)),
	}

	for i := range docs {
		if i == p.FailDocIndex {
			res.FailedDocs++
			res.ItemSuccess[i] = false
			res.Errors = append(res.Errors, elastic.BulkItemError{
				Index:  i,
				Status: 400,
				Type:   "mapper_parsing_exception",
				Reason: "invalid field type",
			})
		} else {
			res.SuccessDocs++
			res.ItemSuccess[i] = true
		}
	}
	return res, nil
}

func TestBatchProcessor_ProcessBatch_Success(t *testing.T) {
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()
	mockConsumer := NewMockMessageConsumer(nil)

	cfg := BatchConfig{
		BulkSize:      5,
		FlushInterval: 50 * time.Millisecond,
		ProcessorID:   "test-proc-1",
	}
	bp := NewBatchProcessor(cfg, mockConsumer, mockES, mockRedis, mockDLQ)

	ctx := context.Background()
	msgs := []kafkaGo.Message{
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-1"),
			Value:  []byte(`{"timestamp":"2026-08-28T10:00:00Z","level":"INFO","service":"svc-1","message":"Log 1"}`),
			Offset: 10,
		},
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-2"),
			Value:  []byte(`{"timestamp":"2026-08-28T10:00:01Z","level":"ERROR","service":"svc-2","message":"Log 2"}`),
			Offset: 11,
		},
	}

	if err := bp.ProcessBatch(ctx, msgs); err != nil {
		t.Fatalf("unexpected error processing batch: %v", err)
	}

	// Verify ES
	if len(mockES.Documents) != 2 {
		t.Fatalf("expected 2 documents in ES, got %d", len(mockES.Documents))
	}

	// Verify Redis
	if mockRedis.Counters["stats:logs:total"] != 2 {
		t.Errorf("expected Redis total_logs 2, got %d", mockRedis.Counters["stats:logs:total"])
	}
	if mockRedis.Counters["stats:errors:svc-2"] != 1 {
		t.Errorf("expected Redis svc-2 errors 1, got %d", mockRedis.Counters["stats:errors:svc-2"])
	}

	// Verify Commits
	if len(mockConsumer.Committed) != 2 {
		t.Fatalf("expected 2 committed offsets, got %d", len(mockConsumer.Committed))
	}
}

func TestBatchProcessor_PoisonAndPartialFailure(t *testing.T) {
	partialES := &PartialFailureBulkIndexer{FailDocIndex: 1} // 2nd valid doc fails in ES
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()
	mockConsumer := NewMockMessageConsumer(nil)

	cfg := BatchConfig{
		BulkSize:      5,
		FlushInterval: 50 * time.Millisecond,
		ProcessorID:   "test-proc-1",
	}
	bp := NewBatchProcessor(cfg, mockConsumer, partialES, mockRedis, mockDLQ)

	ctx := context.Background()
	msgs := []kafkaGo.Message{
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-1"),
			Value:  []byte(`{"timestamp":"2026-08-28T10:00:00Z","level":"INFO","service":"svc-1","message":"Log 1"}`),
			Offset: 10,
		},
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-2"),
			Value:  []byte(`{"timestamp":"2026-08-28T10:00:01Z","level":"ERROR","service":"svc-2","message":"Log 2 rejected by ES"}`),
			Offset: 11,
		},
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-3"),
			Value:  []byte(`{malformed-json-payload}`), // Poison JSON
			Offset: 12,
		},
	}

	if err := bp.ProcessBatch(ctx, msgs); err != nil {
		t.Fatalf("unexpected error processing batch: %v", err)
	}

	// 2 messages should be in DLQ: 1 malformed JSON, 1 rejected by ES mapper
	if len(mockDLQ.Messages) != 2 {
		t.Fatalf("expected 2 DLQ messages, got %d", len(mockDLQ.Messages))
	}

	// Exactly 1 message succeeded in Redis
	if mockRedis.Counters["stats:logs:total"] != 1 {
		t.Errorf("expected 1 successful log in Redis, got %d", mockRedis.Counters["stats:logs:total"])
	}

	// All 3 messages safely handled -> all 3 committed
	if len(mockConsumer.Committed) != 3 {
		t.Fatalf("expected 3 committed offsets, got %d", len(mockConsumer.Committed))
	}
}

func TestBatchProcessor_MultiTenantIsolation(t *testing.T) {
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()
	mockConsumer := NewMockMessageConsumer(nil)

	cfg := BatchConfig{
		BulkSize:      10,
		FlushInterval: 50 * time.Millisecond,
		ProcessorID:   "test-proc-1",
	}
	bp := NewBatchProcessor(cfg, mockConsumer, mockES, mockRedis, mockDLQ)

	ctx := context.Background()
	msgs := []kafkaGo.Message{
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-a"),
			Value:  []byte(`{"tenant_id":"tenant-alpha","timestamp":"2026-08-28T10:00:00Z","level":"INFO","service":"order-svc","message":"Alpha order"}`),
			Offset: 20,
		},
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-b"),
			Value:  []byte(`{"tenant_id":"tenant-beta","timestamp":"2026-08-28T10:00:01Z","level":"ERROR","service":"payment-svc","message":"Beta error"}`),
			Offset: 21,
		},
		{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte("key-c"),
			Value:  []byte(`{"timestamp":"2026-08-28T10:00:02Z","level":"WARN","service":"auth-svc","message":"Default tenant warning"}`),
			Offset: 22,
		},
	}

	if err := bp.ProcessBatch(ctx, msgs); err != nil {
		t.Fatalf("unexpected error processing multi-tenant batch: %v", err)
	}

	// Verify ES documents carry exact tenant_id
	if len(mockES.Documents) != 3 {
		t.Fatalf("expected 3 documents in ES, got %d", len(mockES.Documents))
	}
	if mockES.Documents[0].TenantID != "tenant-alpha" {
		t.Errorf("expected tenant-alpha, got %s", mockES.Documents[0].TenantID)
	}
	if mockES.Documents[1].TenantID != "tenant-beta" {
		t.Errorf("expected tenant-beta, got %s", mockES.Documents[1].TenantID)
	}
	if mockES.Documents[2].TenantID != models.DefaultTenantID {
		t.Errorf("expected default tenant, got %s", mockES.Documents[2].TenantID)
	}

	// Verify Redis isolated keys
	if mockRedis.Counters["tenant:tenant-alpha:stats:logs:total"] != 1 {
		t.Errorf("expected tenant-alpha total_logs 1, got %d", mockRedis.Counters["tenant:tenant-alpha:stats:logs:total"])
	}
	if mockRedis.Counters["tenant:tenant-beta:stats:errors:payment-svc"] != 1 {
		t.Errorf("expected tenant-beta payment errors 1, got %d", mockRedis.Counters["tenant:tenant-beta:stats:errors:payment-svc"])
	}
	if mockRedis.Counters["stats:logs:total"] != 1 {
		t.Errorf("expected default tenant stats:logs:total 1, got %d", mockRedis.Counters["stats:logs:total"])
	}
}

func TestBatchProcessor_Run_SizeTrigger(t *testing.T) {
	msgs := make([]kafkaGo.Message, 5)
	for i := 0; i < 5; i++ {
		msgs[i] = kafkaGo.Message{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte(fmt.Sprintf("key-%d", i)),
			Value:  []byte(fmt.Sprintf(`{"timestamp":"2026-08-28T10:00:0%dZ","level":"INFO","service":"svc","message":"msg %d"}`, i, i)),
			Offset: int64(i + 1),
		}
	}

	mockConsumer := NewMockMessageConsumer(msgs)
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()

	cfg := BatchConfig{
		BulkSize:      5,
		FlushInterval: 1 * time.Second, // Long interval to ensure size triggers it
		ProcessorID:   "proc-1",
	}
	bp := NewBatchProcessor(cfg, mockConsumer, mockES, mockRedis, mockDLQ)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = bp.Run(ctx)

	if len(mockES.Documents) != 5 {
		t.Errorf("expected 5 documents indexed on size trigger, got %d", len(mockES.Documents))
	}
	if len(mockConsumer.Committed) != 5 {
		t.Errorf("expected 5 committed messages, got %d", len(mockConsumer.Committed))
	}
}

func TestBatchProcessor_Run_TimerTrigger(t *testing.T) {
	msgs := make([]kafkaGo.Message, 2)
	for i := 0; i < 2; i++ {
		msgs[i] = kafkaGo.Message{
			Topic:  kafka.TopicAppLogs,
			Key:    []byte(fmt.Sprintf("key-%d", i)),
			Value:  []byte(fmt.Sprintf(`{"timestamp":"2026-08-28T10:00:0%dZ","level":"INFO","service":"svc","message":"msg %d"}`, i, i)),
			Offset: int64(i + 1),
		}
	}

	mockConsumer := NewMockMessageConsumer(msgs)
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()

	cfg := BatchConfig{
		BulkSize:      100,                   // Large bulk size
		FlushInterval: 30 * time.Millisecond, // Fast timer
		ProcessorID:   "proc-1",
	}
	bp := NewBatchProcessor(cfg, mockConsumer, mockES, mockRedis, mockDLQ)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_ = bp.Run(ctx)

	if len(mockES.Documents) != 2 {
		t.Errorf("expected 2 documents indexed on timer trigger, got %d", len(mockES.Documents))
	}
	if len(mockConsumer.Committed) != 2 {
		t.Errorf("expected 2 committed messages, got %d", len(mockConsumer.Committed))
	}
}
