package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Rzmy7/logLogger/internal/metrics"
	kafkaGo "github.com/segmentio/kafka-go"
)

// Documented Kafka topics from docs/04-data-model.md
const (
	TopicAppLogs    = "app-logs"
	TopicAppLogsDLQ = "app-logs-dlq"
)

// Producer defines the contract for producing log messages to Kafka.
type Producer interface {
	Publish(ctx context.Context, key string, value []byte) error
	PublishToTopic(ctx context.Context, topic, key string, value []byte) error
	Close() error
}

// WriterProducer is a Kafka producer implementation using segmentio/kafka-go Writer.
type WriterProducer struct {
	writer       *kafkaGo.Writer
	defaultTopic string
}

// NewProducer creates a new Kafka producer configured with the given broker addresses.
func NewProducer(brokers []string) (*WriterProducer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka brokers list cannot be empty")
	}

	writer := &kafkaGo.Writer{
		Addr:         kafkaGo.TCP(brokers...),
		Balancer:     &kafkaGo.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkaGo.RequireOne,
		Async:        false,
	}

	return &WriterProducer{
		writer:       writer,
		defaultTopic: TopicAppLogs,
	}, nil
}

// Publish publishes a message with the given key and value to the default TopicAppLogs.
func (p *WriterProducer) Publish(ctx context.Context, key string, value []byte) error {
	return p.PublishToTopic(ctx, p.defaultTopic, key, value)
}

// PublishToTopic publishes a message to a specific topic with a partitioning key and payload.
func (p *WriterProducer) PublishToTopic(ctx context.Context, topic, key string, value []byte) error {
	msg := kafkaGo.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		metrics.KafkaMessagesProducedTotal.WithLabelValues(topic, "error").Inc()
		log.Printf("[ERROR] Failed to publish message to Kafka topic %q: %v", topic, err)
		return fmt.Errorf("kafka publish error: %w", err)
	}

	metrics.KafkaMessagesProducedTotal.WithLabelValues(topic, "success").Inc()
	return nil
}

// Close closes the underlying Kafka writer and flushes any buffered messages.
func (p *WriterProducer) Close() error {
	if p.writer != nil {
		log.Println("[INFO] Closing Kafka producer connection")
		return p.writer.Close()
	}
	return nil
}
