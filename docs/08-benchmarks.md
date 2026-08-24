# Performance Benchmarks & Bottleneck Analysis

## 1. Executive Summary

This document records the empirical baseline benchmark results of the Log Platform pipeline under controlled load profiles ranging from **100 logs/sec** to **5,000 logs/sec**, concluding with an unthrottled stress test.

All benchmarks were generated using [`cmd/loadgen`](../cmd/loadgen) communicating directly with the Ingestor HTTP API (`http://localhost:8081/api/v1/logs`), with telemetry captured via Prometheus (`:9090`) and Grafana (`:3000`).

---

## 2. Test Environment

| Component | Specification |
|---|---|
| **OS** | Windows 11 (AMD64) |
| **Go Version** | 1.26.4 |
| **Kafka** | Apache Kafka 3.7.0 (Docker, KRaft mode, 3 partitions) |
| **Elasticsearch** | Elasticsearch 8.11.4 (Single Node, Docker) |
| **Redis** | Upstash Redis (TLS `rediss://`) / Redis 7 Alpine |
| **Prometheus** | v2.51.0 (Scrape interval: 2s) |
| **Grafana** | v10.4.0 (Provisioned Dashboard: `log-platform-overview`) |

---

## 3. Empirical Benchmark Results

| Test Run | Target Rate | Workers | Logs Sent | Duration | Achieved Rate | Success Rate | p50 Latency | p95 Latency | p99 Latency | Max Latency |
|---|---|---|---|---|---|---|---|---|---|---|
| **Test 1** | 100 logs/s | 10 | 1,000 | 10.15s | **98.5 logs/s** | 100.0% | 12.45ms | 44.44ms | 120.54ms | 170.05ms |
| **Test 2** | 500 logs/s | 15 | 5,000 | 10.31s | **485.1 logs/s** | 100.0% | 10.89ms | 13.18ms | 56.55ms | 89.37ms |
| **Test 3** | 1,000 logs/s | 20 | 10,000 | 11.26s | **887.8 logs/s** | 100.0% | 8.50ms | 29.35ms | 79.98ms | 123.14ms |
| **Test 4** | 2,000 logs/s | 30 | 10,000 | 6.78s | **1,475.7 logs/s** | 100.0% | 8.15ms | 30.78ms | 83.08ms | 112.59ms |
| **Test 5** | 5,000 logs/s | 50 | 15,000 | 9.58s | **1,565.5 logs/s** | 100.0% | 8.78ms | 64.31ms | 94.46ms | 131.68ms |
| **Test 6 (Stress)** | Unthrottled | 80 | 25,000 | 15.43s | **1,620.4 logs/s** | 100.0% | 48.56ms | 78.78ms | 100.74ms | 152.09ms |

---

## 4. Bottleneck Analysis

### 4.1 Identified Bottleneck: Downstream Single-Document Indexing
During the high-throughput tests (Tests 4–6, >1,000 logs/sec), the **Ingestor** scaled smoothly up to **1,620.4 logs/sec** with 0 HTTP errors. However, observation of Prometheus metric `log_platform_kafka_consumer_lag` revealed substantial growth in consumer lag.

### 4.2 Evidence from Metrics
1. **Ingestor Throughput:** `sum(rate(log_platform_http_requests_total[1m]))` reached peak ~1,620 req/s.
2. **Processor Consumption Rate:** `sum(rate(log_platform_kafka_messages_consumed_total[1m]))` reached ~80–120 msg/s per single processor loop.
3. **Consumer Lag:** `sum(log_platform_kafka_consumer_lag)` climbed during stress testing because incoming message rate exceeded the single-message synchronous processing rate.
4. **Per-Document Latency Breakdown:**
   - Elasticsearch single-document indexing: ~8–12ms per document (`log_platform_elasticsearch_indexing_duration_seconds`).
   - Redis pipelined write: ~1–3ms per document (`log_platform_redis_operation_duration_seconds`).
   - Total synchronous processing per message: ~10–15ms $\implies$ max ~70–100 messages/sec per sequential worker.

### 4.3 Root Cause
The current vertical slice processes Kafka events **one message at a time** synchronously:
$$\text{Kafka Fetch} \longrightarrow \text{Parse} \longrightarrow \text{ES Index (1 doc)} \longrightarrow \text{Redis Write} \longrightarrow \text{Kafka Commit}$$

Because each Elasticsearch document requires a separate HTTP roundtrip over TCP, network latency imposes an upper bound on single-consumer throughput.

### 4.4 Proposed Remediation & Roadmap
1. **Micro-batching / Bulk Indexing (ADR-005):** Transition from single `IndexLog` calls to `_bulk` API micro-batches (e.g., 200 documents or 100ms timeout buffer). This increases indexing throughput by $10\times\text{--}20\times$.
2. **Parallel Consumer Workers:** Run multiple consumer goroutines or horizontal processor instances partitioned across Kafka topic partitions (`app-logs` has 3 partitions).
3. **Status:** The current architecture intentionally used single-document indexing for vertical-slice validation. Bulk indexing is scheduled for the performance optimization milestone.

---

## 5. Observability & Dashboard Summary

The platform provides complete observability out of the box via Prometheus and Grafana:

- **Ingestion Telemetry:** `log_platform_http_requests_total`, `log_platform_http_request_duration_seconds`, `log_platform_http_errors_total`, `log_platform_http_in_flight_requests`
- **Kafka Telemetry:** `log_platform_kafka_messages_produced_total`, `log_platform_kafka_messages_consumed_total`, `log_platform_kafka_consumer_lag`, `log_platform_kafka_dlq_messages_total`, `log_platform_kafka_processing_failures_total`
- **Storage Telemetry:** `log_platform_elasticsearch_indexing_total`, `log_platform_elasticsearch_indexing_duration_seconds`, `log_platform_redis_operations_total`, `log_platform_redis_operation_duration_seconds`
- **Processor Telemetry:** `log_platform_processor_processing_duration_seconds`
