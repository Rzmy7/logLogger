package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Rzmy7/logLogger/internal/config"
	"github.com/Rzmy7/logLogger/internal/kafka"
	kafkaGo "github.com/segmentio/kafka-go"
)

func main() {
	log.Println("[INFO] Initializing Stream Processor...")

	// 1. Load Configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load configuration: %v", err)
	}

	// 2. Set up context that listens for termination signals
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 3. Initialize Kafka Consumer
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, kafka.GroupIDLogProcessors, kafka.TopicAppLogs)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("[ERROR] Error closing consumer: %v", err)
		}
	}()

	// 4. Define Message Handler
	handler := func(ctx context.Context, msg kafkaGo.Message) error {
		logMsg, err := kafka.ParseLogMessage(msg.Value)
		if err != nil {
			log.Printf("[WARN] Failed to deserialize log message (key=%s, offset=%d): %v", string(msg.Key), msg.Offset, err)
			return err
		}

		traceID := logMsg.TraceID
		if traceID == "" {
			traceID = "-"
		}
		ip := logMsg.IP
		if ip == "" {
			ip = "-"
		}

		log.Printf("[PROCESSED] service=%s level=%s trace_id=%s ip=%s time=%s message=%q",
			logMsg.Service,
			logMsg.Level,
			traceID,
			ip,
			logMsg.Timestamp,
			logMsg.Message,
		)

		return nil
	}

	// 5. Start Consumption Loop
	log.Println("[INFO] Stream Processor running and waiting for events...")
	if err := consumer.Start(ctx, handler); err != nil {
		log.Fatalf("[FATAL] Consumer loop terminated with error: %v", err)
	}

	log.Println("[INFO] Stream Processor stopped cleanly.")
}
