package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
	kafkaGo "github.com/segmentio/kafka-go"
)

// Documented Consumer Group from docs/04-data-model.md
const (
	GroupIDLogProcessors = "log-processors"
)

// MessageHandler is a callback function invoked for each consumed Kafka message.
type MessageHandler func(ctx context.Context, msg kafkaGo.Message) error

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
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafkaGo.FirstOffset,
	})

	return &Consumer{
		reader: reader,
	}, nil
}

// Start begins the message consumption loop until ctx is canceled or a fatal error occurs.
func (c *Consumer) Start(ctx context.Context, handler MessageHandler) error {
	log.Printf("[INFO] Starting Kafka consumer for group %q on topic %q", c.reader.Config().GroupID, c.reader.Config().Topic)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Println("[INFO] Consumer context canceled, stopping read loop")
				return nil
			}
			return fmt.Errorf("error fetching message: %w", err)
		}

		if err := handler(ctx, msg); err != nil {
			log.Printf("[WARN] Handler error processing message at offset %d: %v", msg.Offset, err)
			// Continue or DLQ in future steps; commit to avoid head-of-line blocking if required
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
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
