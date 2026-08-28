package retention

import (
	"context"
	"log"
	"sync"
	"time"
)

// RetentionRunner handles periodic background execution of index retention cycles.
type RetentionRunner struct {
	manager       Manager
	retentionDays int
	interval      time.Duration
	mu            sync.Mutex
	isRunning     bool
}

// NewRetentionRunner creates a new RetentionRunner.
func NewRetentionRunner(manager Manager, retentionDays int, interval time.Duration) *RetentionRunner {
	return &RetentionRunner{
		manager:       manager,
		retentionDays: retentionDays,
		interval:      interval,
	}
}

// RunOnce executes a single retention cycle with concurrency protection against overlapping runs.
func (r *RetentionRunner) RunOnce(ctx context.Context) (*RetentionResult, error) {
	r.mu.Lock()
	if r.isRunning {
		r.mu.Unlock()
		log.Println("[RETENTION] Cycle already in progress, skipping overlapping execution")
		return nil, nil
	}
	r.isRunning = true
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.isRunning = false
		r.mu.Unlock()
	}()

	log.Printf("[RETENTION] Starting retention cycle (retention_days=%d)...", r.retentionDays)
	return r.manager.RunRetention(ctx, r.retentionDays)
}

// Start runs the periodic retention loop until ctx is canceled.
func (r *RetentionRunner) Start(ctx context.Context) {
	log.Printf("[INFO] Retention Runner started: checking every %v for indices older than %d days", r.interval, r.retentionDays)

	// Run an initial retention pass on startup
	if _, err := r.RunOnce(ctx); err != nil {
		log.Printf("[WARN] Initial retention pass failed: %v", err)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[INFO] Retention Runner context canceled, stopping background loop")
			return
		case <-ticker.C:
			if _, err := r.RunOnce(ctx); err != nil {
				log.Printf("[ERROR] Periodic retention pass error: %v", err)
			}
		}
	}
}
