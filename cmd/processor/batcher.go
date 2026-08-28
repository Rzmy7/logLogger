package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/kafka"
	"github.com/Rzmy7/logLogger/internal/metrics"
	"github.com/Rzmy7/logLogger/internal/models"
	"github.com/Rzmy7/logLogger/internal/redis"
	kafkaGo "github.com/segmentio/kafka-go"
)

// BatchConfig configures micro-batching parameters.
type BatchConfig struct {
	BulkSize      int
	FlushInterval time.Duration
	ProcessorID   string
}

// BatchProcessor accumulates Kafka messages and flushes them in micro-batches to Elasticsearch and Redis.
type BatchProcessor struct {
	cfg         BatchConfig
	consumer    kafka.MessageConsumer
	esClient    elastic.BulkIndexer
	redisClient redis.MetricsRecorder
	dlqProducer kafka.Producer
}

// NewBatchProcessor creates a new micro-batch processor.
func NewBatchProcessor(
	cfg BatchConfig,
	consumer kafka.MessageConsumer,
	esClient elastic.BulkIndexer,
	redisClient redis.MetricsRecorder,
	dlqProducer kafka.Producer,
) *BatchProcessor {
	if cfg.BulkSize <= 0 {
		cfg.BulkSize = 200
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.ProcessorID == "" {
		cfg.ProcessorID = "processor-1"
	}
	return &BatchProcessor{
		cfg:         cfg,
		consumer:    consumer,
		esClient:    esClient,
		redisClient: redisClient,
		dlqProducer: dlqProducer,
	}
}

// ProcessBatch processes a micro-batch of Kafka messages:
// 1. Poison/unparseable messages are routed to the DLQ and marked for offset commit.
// 2. Valid messages are converted to LogDocument with deterministic IDs and tenant_id.
// 3. Batched to Elasticsearch via _bulk API with retry on transient failures.
// 4. Real-time metrics recorded in Redis in an atomic pipeline for all successful items.
// 5. Offsets safely committed ONLY after downstream operations succeed.
func (b *BatchProcessor) ProcessBatch(ctx context.Context, msgs []kafkaGo.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
	}()

	ingestedAt := time.Now().UTC()
	var (
		validDocs       []*models.LogDocument
		validLogs       []*models.LogMessage
		validRawJSONs   [][]byte
		validMsgIndices []int
		safeCommitMsgs  []kafkaGo.Message
	)

	// 1. Parse each message in the batch
	for i, msg := range msgs {
		logMsg, err := kafka.ParseLogMessage(msg.Value)
		if err != nil {
			log.Printf("[WARN] Malformed message (offset=%d, partition=%d): %v", msg.Offset, msg.Partition, err)
			metrics.KafkaProcessingFailuresTotal.WithLabelValues(kafka.TopicAppLogs, "parse_error").Inc()

			dlqPayload := models.NewDLQMessage(string(msg.Value), err.Error(), b.cfg.ProcessorID)
			dlqBytes, marshalErr := json.Marshal(dlqPayload)
			if marshalErr != nil {
				return fmt.Errorf("failed to marshal DLQ message: %w", marshalErr)
			}

			// Route poison message to DLQ
			if pubErr := b.dlqProducer.PublishToTopic(ctx, kafka.TopicAppLogsDLQ, string(msg.Key), dlqBytes); pubErr != nil {
				log.Printf("[ERROR] Failed to publish message to DLQ topic %q: %v", kafka.TopicAppLogsDLQ, pubErr)
				return fmt.Errorf("dlq publish error: %w", pubErr)
			}
			metrics.KafkaDLQMessagesTotal.WithLabelValues(kafka.TopicAppLogsDLQ).Inc()
			safeCommitMsgs = append(safeCommitMsgs, msg)
			continue
		}

		doc := logMsg.ToDocument(ingestedAt)
		validDocs = append(validDocs, doc)
		validLogs = append(validLogs, logMsg)
		validRawJSONs = append(validRawJSONs, msg.Value)
		validMsgIndices = append(validMsgIndices, i)
	}

	// 2. If valid documents exist, perform Elasticsearch bulk indexing
	if len(validDocs) > 0 {
		var bulkRes *elastic.BulkResult
		backoff := 50 * time.Millisecond

		for {
			res, err := b.esClient.IndexBatch(ctx, validDocs)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("[WARN] Transient ES bulk indexing failure, retrying in %v: %v", backoff, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
					if backoff < 2*time.Second {
						backoff *= 2
					}
					continue
				}
			}
			bulkRes = res
			break
		}

		// Inspect item-level results
		var successfulLogs []*models.LogMessage
		var successfulRawJSONs [][]byte

		for j, success := range bulkRes.ItemSuccess {
			origMsgIdx := validMsgIndices[j]
			origMsg := msgs[origMsgIdx]

			if success {
				successfulLogs = append(successfulLogs, validLogs[j])
				successfulRawJSONs = append(successfulRawJSONs, validRawJSONs[j])
				safeCommitMsgs = append(safeCommitMsgs, origMsg)
			} else {
				// Permanent item error (e.g. mapping parse failure)
				itemErr := elastic.BulkItemError{Index: j, Status: 400, Type: "item_error", Reason: "indexing rejected"}
				for _, e := range bulkRes.Errors {
					if e.Index == j {
						itemErr = e
						break
					}
				}
				log.Printf("[WARN] Document rejected by Elasticsearch (status=%d, type=%s, reason=%s), routing to DLQ",
					itemErr.Status, itemErr.Type, itemErr.Reason)
				metrics.KafkaProcessingFailuresTotal.WithLabelValues(kafka.TopicAppLogs, "es_item_error").Inc()

				dlqPayload := models.NewDLQMessage(string(origMsg.Value), fmt.Sprintf("ES bulk error %d (%s): %s", itemErr.Status, itemErr.Type, itemErr.Reason), b.cfg.ProcessorID)
				dlqBytes, _ := json.Marshal(dlqPayload)
				if pubErr := b.dlqProducer.PublishToTopic(ctx, kafka.TopicAppLogsDLQ, string(origMsg.Key), dlqBytes); pubErr == nil {
					metrics.KafkaDLQMessagesTotal.WithLabelValues(kafka.TopicAppLogsDLQ).Inc()
					safeCommitMsgs = append(safeCommitMsgs, origMsg)
				}
			}
		}

		// 3. Record real-time metrics in Redis for successful documents
		if len(successfulLogs) > 0 {
			redisBackoff := 50 * time.Millisecond
			for {
				if err := b.redisClient.RecordBatch(ctx, successfulLogs, successfulRawJSONs); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					log.Printf("[WARN] Redis batch recording failure, retrying in %v: %v", redisBackoff, err)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(redisBackoff):
						if redisBackoff < 2*time.Second {
							redisBackoff *= 2
						}
						continue
					}
				}
				break
			}
		}
	}

	// 4. Safely commit Kafka offsets for all processed messages
	if len(safeCommitMsgs) > 0 && b.consumer != nil {
		if err := b.consumer.CommitMessages(ctx, safeCommitMsgs...); err != nil {
			if ctx.Err() == nil {
				log.Printf("[ERROR] Failed to commit Kafka batch offsets: %v", err)
			}
		}
	}

	return nil
}

// Run executes the continuous fetch and micro-batching loop until ctx is canceled.
func (b *BatchProcessor) Run(ctx context.Context) error {
	log.Printf("[INFO] Starting micro-batch processing loop (bulk_size=%d, flush_interval=%v)", b.cfg.BulkSize, b.cfg.FlushInterval)

	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	var batch []kafkaGo.Message
	msgChan := make(chan kafkaGo.Message, b.cfg.BulkSize)

	// Background fetcher
	go func() {
		for {
			msg, err := b.consumer.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					close(msgChan)
					return
				}
				log.Printf("[WARN] Error fetching message from Kafka: %v", err)
				select {
				case <-ctx.Done():
					close(msgChan)
					return
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}

			metrics.KafkaMessagesConsumedTotal.WithLabelValues(msg.Topic, kafka.GroupIDLogProcessors).Inc()
			select {
			case <-ctx.Done():
				close(msgChan)
				return
			case msgChan <- msg:
			}
		}
	}()

	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				// Channel closed -> flush remaining batch during shutdown
				if len(batch) > 0 {
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					if err := b.ProcessBatch(shutdownCtx, batch); err != nil {
						log.Printf("[ERROR] Error flushing pending batch during shutdown: %v", err)
					}
					cancel()
				}
				return nil
			}

			batch = append(batch, msg)
			if len(batch) >= b.cfg.BulkSize {
				if err := b.ProcessBatch(ctx, batch); err != nil {
					log.Printf("[ERROR] Batch processing error: %v", err)
				}
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				if err := b.ProcessBatch(ctx, batch); err != nil {
					log.Printf("[ERROR] Batch processing error on interval tick: %v", err)
				}
				batch = batch[:0]
			}

		case <-ctx.Done():
			// Context canceled -> flush remaining batch with dedicated timeout
			if len(batch) > 0 {
				log.Printf("[INFO] Shutdown received, flushing %d pending messages in batch...", len(batch))
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := b.ProcessBatch(shutdownCtx, batch); err != nil {
					log.Printf("[ERROR] Error flushing batch on shutdown: %v", err)
				}
				cancel()
				batch = batch[:0]
			}
			return nil
		}
	}
}
