package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/kafka"
	"github.com/Rzmy7/logLogger/internal/models"
	"github.com/Rzmy7/logLogger/internal/redis"
	kafkaGo "github.com/segmentio/kafka-go"
)

func TestProcessor_ValidMessage_SuccessPath(t *testing.T) {
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()

	ctx := context.Background()
	payload := []byte(`{
		"timestamp": "2026-08-21T10:00:00Z",
		"level": "INFO",
		"service": "order-service",
		"message": "Order created",
		"trace_id": "trace-101",
		"ip": "192.168.1.1"
	}`)

	msg := kafkaGo.Message{
		Topic:  kafka.TopicAppLogs,
		Key:    []byte("trace-101"),
		Value:  payload,
		Offset: 42,
	}

	err := ProcessMessage(ctx, msg, mockES, mockRedis, mockDLQ, "test-proc-1")
	if err != nil {
		t.Fatalf("unexpected error processing valid message: %v", err)
	}

	// Verify Elasticsearch document
	if len(mockES.Documents) != 1 {
		t.Fatalf("expected 1 document in ES, got %d", len(mockES.Documents))
	}
	if mockES.Documents[0].Service != "order-service" {
		t.Errorf("expected service 'order-service', got %s", mockES.Documents[0].Service)
	}

	// Verify Redis metrics
	if mockRedis.Counters["stats:logs:total"] != 1 {
		t.Errorf("expected Redis total_logs 1, got %d", mockRedis.Counters["stats:logs:total"])
	}
	if mockRedis.Counters["stats:logs:order-service"] != 1 {
		t.Errorf("expected Redis service counter 1, got %d", mockRedis.Counters["stats:logs:order-service"])
	}

	// Verify NO messages published to DLQ
	if len(mockDLQ.Messages) != 0 {
		t.Errorf("expected 0 DLQ messages, got %d", len(mockDLQ.Messages))
	}
}

func TestProcessor_MalformedMessage_RoutedToDLQ(t *testing.T) {
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()

	ctx := context.Background()
	malformedPayload := []byte(`{invalid-json: "bad_format"`)

	msg := kafkaGo.Message{
		Topic:  kafka.TopicAppLogs,
		Key:    []byte("bad-key"),
		Value:  malformedPayload,
		Offset: 99,
	}

	err := ProcessMessage(ctx, msg, mockES, mockRedis, mockDLQ, "proc-node-7")
	// Must return nil so Kafka offset is committed (poison message handled)
	if err != nil {
		t.Fatalf("expected nil error on successful DLQ route, got %v", err)
	}

	// Verify 0 writes to ES and Redis
	if len(mockES.Documents) != 0 {
		t.Errorf("expected 0 ES documents on malformed message, got %d", len(mockES.Documents))
	}
	if mockRedis.Counters["stats:logs:total"] != 0 {
		t.Errorf("expected 0 Redis total logs on malformed message, got %d", mockRedis.Counters["stats:logs:total"])
	}

	// Verify exactly 1 message in DLQ
	if len(mockDLQ.Messages) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(mockDLQ.Messages))
	}

	dlqRecord := mockDLQ.Messages[0]
	if dlqRecord.Topic != kafka.TopicAppLogsDLQ {
		t.Errorf("expected topic %q, got %q", kafka.TopicAppLogsDLQ, dlqRecord.Topic)
	}
	if dlqRecord.Key != "bad-key" {
		t.Errorf("expected key 'bad-key', got %q", dlqRecord.Key)
	}

	var dlqMsg models.DLQMessage
	if err := json.Unmarshal(dlqRecord.Value, &dlqMsg); err != nil {
		t.Fatalf("failed to decode DLQ message payload: %v", err)
	}

	if dlqMsg.OriginalMessage != string(malformedPayload) {
		t.Errorf("expected original_message %q, got %q", string(malformedPayload), dlqMsg.OriginalMessage)
	}
	if dlqMsg.ProcessorID != "proc-node-7" {
		t.Errorf("expected processor_id 'proc-node-7', got %q", dlqMsg.ProcessorID)
	}
	if dlqMsg.Error == "" {
		t.Error("expected non-empty error in DLQ message")
	}
	if dlqMsg.FailedAt == "" {
		t.Error("expected non-empty failed_at in DLQ message")
	}
}

func TestProcessor_SchemaInvalidMessage_RoutedToDLQ(t *testing.T) {
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()

	ctx := context.Background()
	// Missing required level, service, timestamp
	schemaInvalidPayload := []byte(`{"message": "missing headers"}`)

	msg := kafkaGo.Message{
		Topic:  kafka.TopicAppLogs,
		Key:    []byte("key-0"),
		Value:  schemaInvalidPayload,
		Offset: 105,
	}

	err := ProcessMessage(ctx, msg, mockES, mockRedis, mockDLQ, "proc-node-1")
	if err != nil {
		t.Fatalf("expected nil error on schema-invalid message routed to DLQ, got %v", err)
	}

	if len(mockDLQ.Messages) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(mockDLQ.Messages))
	}
}

func TestProcessor_DLQPublishFailure_ReturnsError(t *testing.T) {
	mockES := elastic.NewMockIndexer()
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()
	mockDLQ.Err = errors.New("kafka cluster unreachable for DLQ")

	ctx := context.Background()
	malformedPayload := []byte(`{broken-json}`)

	msg := kafkaGo.Message{
		Topic:  kafka.TopicAppLogs,
		Key:    []byte("key-failed-dlq"),
		Value:  malformedPayload,
		Offset: 120,
	}

	err := ProcessMessage(ctx, msg, mockES, mockRedis, mockDLQ, "proc-node-1")
	if err == nil {
		t.Fatal("expected error when DLQ publication fails, got nil")
	}
}

func TestProcessor_ESFailure_NotRoutedToDLQ(t *testing.T) {
	mockES := elastic.NewMockIndexer()
	mockES.Err = errors.New("elasticsearch network timeout")
	mockRedis := redis.NewMockMetricsRecorder()
	mockDLQ := kafka.NewMockProducer()

	ctx := context.Background()
	validPayload := []byte(`{
		"timestamp": "2026-08-21T10:00:00Z",
		"level": "INFO",
		"service": "order-service",
		"message": "Valid order"
	}`)

	msg := kafkaGo.Message{
		Topic:  kafka.TopicAppLogs,
		Key:    []byte("order-1"),
		Value:  validPayload,
		Offset: 150,
	}

	err := ProcessMessage(ctx, msg, mockES, mockRedis, mockDLQ, "proc-node-1")
	if err == nil {
		t.Fatal("expected error on transient ES failure, got nil")
	}

	// Transient failures must NOT be sent to DLQ
	if len(mockDLQ.Messages) != 0 {
		t.Errorf("transient downstream failure should not route to DLQ, got %d messages", len(mockDLQ.Messages))
	}
}
