package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTP Metrics
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests processed, partitioned by service, handler, method and status code.",
		},
		[]string{"service", "handler", "method", "code"},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "log_platform",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Histogram of HTTP request latencies in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"service", "handler", "method"},
	)

	HTTPErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "http",
			Name:      "errors_total",
			Help:      "Total number of HTTP error responses partitioned by service, handler and error code.",
		},
		[]string{"service", "handler", "error_code"},
	)

	HTTPInFlightRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "log_platform",
			Subsystem: "http",
			Name:      "in_flight_requests",
			Help:      "Current number of in-flight HTTP requests.",
		},
		[]string{"service", "handler"},
	)

	// Kafka Metrics
	KafkaMessagesProducedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "kafka",
			Name:      "messages_produced_total",
			Help:      "Total number of Kafka messages produced.",
		},
		[]string{"topic", "status"},
	)

	KafkaMessagesConsumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "kafka",
			Name:      "messages_consumed_total",
			Help:      "Total number of Kafka messages consumed by consumer group.",
		},
		[]string{"topic", "consumer_group"},
	)

	KafkaProcessingFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "kafka",
			Name:      "processing_failures_total",
			Help:      "Total message processing failures in consumer.",
		},
		[]string{"topic", "reason"},
	)

	KafkaDLQMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "kafka",
			Name:      "dlq_messages_total",
			Help:      "Total messages routed to the Dead Letter Queue.",
		},
		[]string{"topic"},
	)

	KafkaConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "log_platform",
			Subsystem: "kafka",
			Name:      "consumer_lag",
			Help:      "Current consumer lag (unprocessed messages) per partition/group.",
		},
		[]string{"topic", "consumer_group", "partition"},
	)

	// Storage Metrics (Elasticsearch)
	ElasticsearchIndexingTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "elasticsearch",
			Name:      "indexing_total",
			Help:      "Total documents indexed into Elasticsearch.",
		},
		[]string{"status"},
	)

	ElasticsearchIndexingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "log_platform",
			Subsystem: "elasticsearch",
			Name:      "indexing_duration_seconds",
			Help:      "Elasticsearch document indexing latency in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
	)

	// Storage Metrics (Redis)
	RedisOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "redis",
			Name:      "operations_total",
			Help:      "Total Redis metrics recording and query operations.",
		},
		[]string{"operation", "status"},
	)

	RedisOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "log_platform",
			Subsystem: "redis",
			Name:      "operation_duration_seconds",
			Help:      "Redis operation latency in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"operation"},
	)

	// End-to-end Processing Latency
	ProcessingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "log_platform",
			Subsystem: "processor",
			Name:      "processing_duration_seconds",
			Help:      "End-to-end log processing duration (parse, ES index, Redis metrics) in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
	)

	// Log Lifecycle & Retention Metrics
	RetentionRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "retention",
			Name:      "runs_total",
			Help:      "Total number of retention cycles executed.",
		},
		[]string{"status"},
	)

	RetentionIndicesDeletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "retention",
			Name:      "indices_deleted_total",
			Help:      "Total number of expired log indices deleted by automated retention.",
		},
	)

	RetentionIndicesEvaluatedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "retention",
			Name:      "indices_evaluated_total",
			Help:      "Total number of log indices evaluated during retention cycles.",
		},
	)

	RetentionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "log_platform",
			Subsystem: "retention",
			Name:      "duration_seconds",
			Help:      "Duration of retention execution cycles in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
	)

	AdminDeletionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "log_platform",
			Subsystem: "admin",
			Name:      "deletions_total",
			Help:      "Total number of manual index deletion attempts via administrative API.",
		},
		[]string{"type", "status"},
	)
)

func init() {
	prometheus.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDuration,
		HTTPErrorsTotal,
		HTTPInFlightRequests,
		KafkaMessagesProducedTotal,
		KafkaMessagesConsumedTotal,
		KafkaProcessingFailuresTotal,
		KafkaDLQMessagesTotal,
		KafkaConsumerLag,
		ElasticsearchIndexingTotal,
		ElasticsearchIndexingDuration,
		RedisOperationsTotal,
		RedisOperationDuration,
		ProcessingDuration,
		RetentionRunsTotal,
		RetentionIndicesDeletedTotal,
		RetentionIndicesEvaluatedTotal,
		RetentionDuration,
		AdminDeletionsTotal,
	)
}

// Handler returns the Prometheus HTTP metrics handler.
func Handler() http.Handler {
	return promhttp.Handler()
}

// responseWriterInterceptor captures the status code of HTTP responses.
type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterInterceptor) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// HTTPMetricsMiddleware wraps HTTP handlers to record standard Prometheus metrics.
func HTTPMetricsMiddleware(serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			// Skip /metrics itself to avoid self-scraping skew
			if path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			HTTPInFlightRequests.WithLabelValues(serviceName, path).Inc()
			defer HTTPInFlightRequests.WithLabelValues(serviceName, path).Dec()

			wi := &responseWriterInterceptor{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(wi, r)

			duration := time.Since(start).Seconds()
			codeStr := strconv.Itoa(wi.statusCode)

			HTTPRequestsTotal.WithLabelValues(serviceName, path, r.Method, codeStr).Inc()
			HTTPRequestDuration.WithLabelValues(serviceName, path, r.Method).Observe(duration)

			if wi.statusCode >= 400 {
				HTTPErrorsTotal.WithLabelValues(serviceName, path, codeStr).Inc()
			}
		})
	}
}
