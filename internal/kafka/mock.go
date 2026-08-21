package kafka

import (
	"context"
	"sync"
)

// PublishedMessage represents a message recorded by MockProducer.
type PublishedMessage struct {
	Topic string
	Key   string
	Value []byte
}

// MockProducer is an in-memory mock implementation of Producer for testing.
type MockProducer struct {
	mu       sync.Mutex
	Messages []PublishedMessage
	Closed   bool
	Err      error
}

// NewMockProducer initializes an empty MockProducer.
func NewMockProducer() *MockProducer {
	return &MockProducer{
		Messages: make([]PublishedMessage, 0),
	}
}

// Publish records a published message to the default topic.
func (m *MockProducer) Publish(ctx context.Context, key string, value []byte) error {
	return m.PublishToTopic(ctx, TopicAppLogs, key, value)
}

// PublishToTopic records a published message to the specified topic.
func (m *MockProducer) PublishToTopic(ctx context.Context, topic, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Err != nil {
		return m.Err
	}

	m.Messages = append(m.Messages, PublishedMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	})
	return nil
}

// Close marks the mock producer as closed.
func (m *MockProducer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}
