package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rzmy7/logLogger/internal/config"
	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/kafka"
	"github.com/Rzmy7/logLogger/internal/redis"
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

	// 3. Initialize Elasticsearch Client & Template
	esClient, err := elastic.NewClient(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Elasticsearch client: %v", err)
	}

	if err := esClient.EnsureTemplate(ctx); err != nil {
		log.Printf("[WARN] Failed to apply Elasticsearch index template on startup: %v", err)
	}

	// 4. Initialize Redis Client
	redisClient, err := redis.NewClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Redis client: %v", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("[ERROR] Error closing Redis client: %v", err)
		}
	}()

	// 5. Initialize Kafka Consumer
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, kafka.GroupIDLogProcessors, kafka.TopicAppLogs)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("[ERROR] Error closing consumer: %v", err)
		}
	}()

	// 6. Define Message Handler (ES Indexing + Redis Metrics)
	handler := func(ctx context.Context, msg kafkaGo.Message) error {
		logMsg, err := kafka.ParseLogMessage(msg.Value)
		if err != nil {
			log.Printf("[WARN] Failed to deserialize log message (key=%s, offset=%d): %v", string(msg.Key), msg.Offset, err)
			return err
		}

		ingestedAt := time.Now().UTC()

		// 6.1 Index document into Elasticsearch
		if err := esClient.IndexLog(ctx, logMsg, ingestedAt); err != nil {
			log.Printf("[ERROR] Failed to index document into Elasticsearch (key=%s, offset=%d): %v", string(msg.Key), msg.Offset, err)
			return fmt.Errorf("elasticsearch indexing failed: %w", err)
		}

		// 6.2 Record real-time metrics in Redis
		if err := redisClient.RecordLog(ctx, logMsg, msg.Value); err != nil {
			log.Printf("[ERROR] Failed to record Redis metrics (key=%s, offset=%d): %v", string(msg.Key), msg.Offset, err)
			return fmt.Errorf("redis metrics recording failed: %w", err)
		}

		traceID := logMsg.TraceID
		if traceID == "" {
			traceID = "-"
		}
		ip := logMsg.IP
		if ip == "" {
			ip = "-"
		}

		t, err := logMsg.ParsedTime()
		if err != nil {
			t = ingestedAt
		}
		indexName := elastic.IndexNameForTime(t)

		log.Printf("[PROCESSED, INDEXED & METRICS] index=%s service=%s level=%s trace_id=%s ip=%s time=%s message=%q",
			indexName,
			logMsg.Service,
			logMsg.Level,
			traceID,
			ip,
			logMsg.Timestamp,
			logMsg.Message,
		)

		return nil
	}

	// 7. Start Consumption Loop
	log.Println("[INFO] Stream Processor running and waiting for events...")
	if err := consumer.Start(ctx, handler); err != nil {
		log.Fatalf("[FATAL] Consumer loop terminated with error: %v", err)
	}

	log.Println("[INFO] Stream Processor stopped cleanly.")
}
