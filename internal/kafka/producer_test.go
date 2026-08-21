package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestNewProducer_EmptyBrokers(t *testing.T) {
	_, err := NewProducer([]string{})
	if err == nil {
		t.Fatal("expected error when initializing producer with empty brokers, got nil")
	}
}

func TestNewProducer_Success(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil producer")
	}
	if p.defaultTopic != TopicAppLogs {
		t.Errorf("expected default topic %q, got %q", TopicAppLogs, p.defaultTopic)
	}
	_ = p.Close()
}

func TestMockProducer(t *testing.T) {
	mock := NewMockProducer()
	var _ Producer = mock

	ctx := context.Background()
	payload := []byte(`{"message":"test log"}`)
	err := mock.Publish(ctx, "trace-123", payload)
	if err != nil {
		t.Fatalf("unexpected mock publish error: %v", err)
	}

	if len(mock.Messages) != 1 {
		t.Fatalf("expected 1 recorded message, got %d", len(mock.Messages))
	}
	if mock.Messages[0].Topic != TopicAppLogs {
		t.Errorf("expected topic %q, got %q", TopicAppLogs, mock.Messages[0].Topic)
	}
	if mock.Messages[0].Key != "trace-123" {
		t.Errorf("expected key 'trace-123', got %q", mock.Messages[0].Key)
	}

	mock.Err = errors.New("simulated network failure")
	if err := mock.Publish(ctx, "trace-456", payload); err == nil {
		t.Fatal("expected error from simulated failure, got nil")
	}

	if err := mock.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !mock.Closed {
		t.Error("expected mock to be marked closed")
	}
}
