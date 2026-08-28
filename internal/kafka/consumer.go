package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Rzmy7/logLogger/internal/metrics"
	"github.com/Rzmy7/logLogger/internal/models"
	kafkaGo "github.com/segmentio/kafka-go"
)

// Documented Consumer Group from docs/04-data-model.md
const (
	GroupIDLogProcessors = "log-processors"
)

// MessageHandler is a callback function invoked for each consumed Kafka message.
type MessageHandler func(ctx context.Context, msg kafkaGo.Message) error

// MessageConsumer defines the contract for fetching and committing Kafka messages.
type MessageConsumer interface {
	FetchMessage(ctx context.Context) (kafkaGo.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkaGo.Message) error
	Stats() kafkaGo.ReaderStats
	Close() error
}

// Consumer wraps a segmentio/kafka-go Reader to consume messages from Kafka topics.
type Consumer struct {
	reader *kafkaGo.Reader
}

// NewConsumer creates a new Kafka consumer for a topic and consumer group.
func NewConsumer(brokers []string, groupID, topic string) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafka brokers list cannot be empty")
	}
	if groupID == "" {
		return nil, errors.New("consumer groupID cannot be empty")
	}
	if topic == "" {
		return nil, errors.New("consumer topic cannot be empty")
	}

	reader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		MaxWait:        50 * time.Millisecond,
		CommitInterval: 0, // Explicit manual commit only
		StartOffset:    kafkaGo.FirstOffset,
	})

	return &Consumer{
		reader: reader,
	}, nil
}

// FetchMessage fetches a single message without committing.
func (c *Consumer) FetchMessage(ctx context.Context) (kafkaGo.Message, error) {
	if c.reader == nil {
		return kafkaGo.Message{}, errors.New("reader is not initialized")
	}
	return c.reader.FetchMessage(ctx)
}

// CommitMessages commits offsets for the given messages.
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafkaGo.Message) error {
	if c.reader == nil {
		return errors.New("reader is not initialized")
	}
	if len(msgs) == 0 {
		return nil
	}
	return c.reader.CommitMessages(ctx, msgs...)
}

// Stats returns reader statistics including consumer lag.
func (c *Consumer) Stats() kafkaGo.ReaderStats {
	if c.reader != nil {
		return c.reader.Stats()
	}
	return kafkaGo.ReaderStats{}
}

// Start begins the message consumption loop until ctx is canceled or a fatal error occurs.
func (c *Consumer) Start(ctx context.Context, handler MessageHandler) error {
	groupID := c.reader.Config().GroupID
	topic := c.reader.Config().Topic
	log.Printf("[INFO] Starting Kafka consumer for group %q on topic %q", groupID, topic)

	// Periodic lag updater
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := c.Stats()
				metrics.KafkaConsumerLag.WithLabelValues(topic, groupID, "total").Set(float64(stats.Lag))
			}
		}
	}()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				log.Println("[INFO] Consumer context canceled, stopping read loop")
				return nil
			}
			log.Printf("[ERROR] Error fetching message from Kafka: %v", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		metrics.KafkaMessagesConsumedTotal.WithLabelValues(msg.Topic, groupID).Inc()
		stats := c.Stats()
		metrics.KafkaConsumerLag.WithLabelValues(msg.Topic, groupID, strconv.Itoa(msg.Partition)).Set(float64(stats.Lag))

		if err := handler(ctx, msg); err != nil {
			log.Printf("[WARN] Handler error processing message (topic=%s, partition=%d, offset=%d): %v", msg.Topic, msg.Partition, msg.Offset, err)
			// Reset offset so the message is not skipped upon next fetch
			_ = c.reader.SetOffset(msg.Offset)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Second):
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			log.Printf("[ERROR] Failed to commit offset %d: %v", msg.Offset, err)
		}
	}
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	if c.reader != nil {
		log.Println("[INFO] Closing Kafka consumer")
		return c.reader.Close()
	}
	return nil
}

// ParseLogMessage deserializes raw message bytes into a LogMessage struct.
func ParseLogMessage(data []byte) (*models.LogMessage, error) {
	var msg models.LogMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal log message: %w", err)
	}
	if msg.Timestamp == "" || msg.Level == "" || msg.Service == "" || msg.Message == "" {
		return nil, errors.New("log message missing required fields")
	}
	return &msg, nil
}
