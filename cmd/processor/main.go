package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rzmy7/logLogger/internal/config"
	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/kafka"
	"github.com/Rzmy7/logLogger/internal/metrics"
	"github.com/Rzmy7/logLogger/internal/models"
	"github.com/Rzmy7/logLogger/internal/redis"
	"github.com/go-chi/chi/v5"
	kafkaGo "github.com/segmentio/kafka-go"
)

// ProcessMessage processes a single Kafka message: routes poison/malformed messages to DLQ,
// or indexes valid messages into Elasticsearch and records metrics in Redis.
func ProcessMessage(
	ctx context.Context,
	msg kafkaGo.Message,
	esClient elastic.Indexer,
	redisClient redis.MetricsRecorder,
	dlqProducer kafka.Producer,
	processorID string,
) error {
	start := time.Now()
	defer func() {
		metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
	}()

	logMsg, err := kafka.ParseLogMessage(msg.Value)
	if err != nil {
		log.Printf("[WARN] Malformed/invalid message received (offset=%d): %v", msg.Offset, err)
		metrics.KafkaProcessingFailuresTotal.WithLabelValues(kafka.TopicAppLogs, "parse_error").Inc()

		dlqPayload := models.NewDLQMessage(string(msg.Value), err.Error(), processorID)
		dlqBytes, marshalErr := json.Marshal(dlqPayload)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal DLQ message: %w", marshalErr)
		}

		// Publish to app-logs-dlq topic
		if pubErr := dlqProducer.PublishToTopic(ctx, kafka.TopicAppLogsDLQ, string(msg.Key), dlqBytes); pubErr != nil {
			log.Printf("[ERROR] Failed to publish message to DLQ topic %q: %v", kafka.TopicAppLogsDLQ, pubErr)
			return fmt.Errorf("dlq publish error: %w", pubErr)
		}

		metrics.KafkaDLQMessagesTotal.WithLabelValues(kafka.TopicAppLogsDLQ).Inc()
		log.Printf("[DLQ] Successfully routed invalid message (offset=%d) to %s", msg.Offset, kafka.TopicAppLogsDLQ)
		// Return nil so consumer commits the original offset and avoids poison pill blockage
		return nil
	}

	ingestedAt := time.Now().UTC()

	// 1. Index document into Elasticsearch
	if err := esClient.IndexLog(ctx, logMsg, ingestedAt); err != nil {
		metrics.KafkaProcessingFailuresTotal.WithLabelValues(kafka.TopicAppLogs, "es_error").Inc()
		log.Printf("[ERROR] Failed to index document into Elasticsearch (key=%s, offset=%d): %v", string(msg.Key), msg.Offset, err)
		return fmt.Errorf("elasticsearch indexing failed: %w", err)
	}

	// 2. Record real-time metrics in Redis
	if err := redisClient.RecordLog(ctx, logMsg, msg.Value); err != nil {
		metrics.KafkaProcessingFailuresTotal.WithLabelValues(kafka.TopicAppLogs, "redis_error").Inc()
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

func startMetricsServer(ctx context.Context, port string) *http.Server {
	r := chi.NewRouter()
	r.Handle("/metrics", metrics.Handler())
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"processor"}`))
	})

	srv := &http.Server{
		Addr:              port,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("[INFO] Processor metrics server listening on %s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[ERROR] Processor metrics server error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	return srv
}

func main() {
	log.Println("[INFO] Initializing Stream Processor...")

	// 1. Load Configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load configuration: %v", err)
	}

	processorID, _ := os.Hostname()
	if processorID == "" {
		processorID = "processor-1"
	}

	// 2. Set up context that listens for termination signals
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 3. Start Metrics HTTP Server for Prometheus scraping (:8083)
	metricsPort := os.Getenv("PROCESSOR_METRICS_PORT")
	if metricsPort == "" {
		metricsPort = ":8083"
	} else if metricsPort[0] != ':' {
		metricsPort = ":" + metricsPort
	}
	startMetricsServer(ctx, metricsPort)

	// 4. Initialize Elasticsearch Client & Template
	esClient, err := elastic.NewClient(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Elasticsearch client: %v", err)
	}

	if err := esClient.EnsureTemplate(ctx); err != nil {
		log.Printf("[WARN] Failed to apply Elasticsearch index template on startup: %v", err)
	}

	// 5. Initialize Redis Client
	redisClient, err := redis.NewClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Redis client: %v", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("[ERROR] Error closing Redis client: %v", err)
		}
	}()

	// 6. Initialize DLQ Producer
	dlqProducer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize DLQ producer: %v", err)
	}
	defer func() {
		if err := dlqProducer.Close(); err != nil {
			log.Printf("[ERROR] Error closing DLQ producer: %v", err)
		}
	}()

	// 7. Initialize Kafka Consumer
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, kafka.GroupIDLogProcessors, kafka.TopicAppLogs)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("[ERROR] Error closing consumer: %v", err)
		}
	}()

	// 8. Define Message Handler
	handler := func(ctx context.Context, msg kafkaGo.Message) error {
		return ProcessMessage(ctx, msg, esClient, redisClient, dlqProducer, processorID)
	}

	// 9. Start Consumption Loop
	log.Println("[INFO] Stream Processor running and waiting for events...")
	if err := consumer.Start(ctx, handler); err != nil {
		log.Fatalf("[FATAL] Consumer loop terminated with error: %v", err)
	}

	log.Println("[INFO] Stream Processor stopped cleanly.")
}
