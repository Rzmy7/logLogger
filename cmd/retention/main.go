package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rzmy7/logLogger/internal/config"
	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/metrics"
	"github.com/Rzmy7/logLogger/internal/retention"
	"github.com/go-chi/chi/v5"
)

func startMetricsServer(ctx context.Context, port string) *http.Server {
	r := chi.NewRouter()
	r.Handle("/metrics", metrics.Handler())
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"retention"}`))
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
		log.Printf("[INFO] Retention service metrics server listening on %s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[ERROR] Retention metrics server error: %v", err)
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
	log.Println("[INFO] Initializing Retention Service...")

	// 1. Load Configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load configuration: %v", err)
	}

	// 2. Set up context that listens for termination signals
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 3. Start Metrics HTTP Server for Prometheus scraping (:8084)
	metricsPort := ":" + cfg.RetentionMetricsPort
	startMetricsServer(ctx, metricsPort)

	// 4. Initialize Elasticsearch Lifecycle Client
	esClient, err := elastic.NewClient(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Elasticsearch client: %v", err)
	}

	// 5. Initialize Retention Manager & Periodic Runner
	retentionManager := retention.NewManager(esClient)
	runner := retention.NewRetentionRunner(retentionManager, cfg.LogRetentionDays, cfg.LogRetentionInterval)

	log.Printf("[INFO] Retention Service started (Retention: %d days, Interval: %v)", cfg.LogRetentionDays, cfg.LogRetentionInterval)

	// 6. Run the retention runner (blocks until ctx is canceled)
	runner.Start(ctx)

	log.Println("[INFO] Retention Service stopped cleanly.")
}
