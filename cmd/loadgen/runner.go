package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Runner coordinates workers and sends load against the Ingestor.
type Runner struct {
	cfg        *Config
	httpClient *http.Client
	stats      *BenchmarkStats
}

// NewRunner creates a new Runner instance.
func NewRunner(cfg *Config) *Runner {
	transport := &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	return &Runner{
		cfg:        cfg,
		httpClient: httpClient,
		stats:      NewBenchmarkStats(),
	}
}

// Run executes the load benchmark.
func (r *Runner) Run(ctx context.Context) (*Summary, error) {
	fmt.Println("Load Generator")
	fmt.Println("==============")
	if r.cfg.TotalLogs > 0 {
		fmt.Printf("Total Logs:      %d\n", r.cfg.TotalLogs)
	} else {
		fmt.Printf("Duration:        %s\n", r.cfg.Duration)
	}
	if r.cfg.Rate > 0 {
		fmt.Printf("Target Rate:     %d logs/sec\n", r.cfg.Rate)
	} else {
		fmt.Printf("Target Rate:     unthrottled (max)\n")
	}
	fmt.Printf("Workers:         %d\n", r.cfg.Workers)
	fmt.Printf("Services:        %v\n", r.cfg.Services)
	fmt.Printf("Level:           %s\n", r.cfg.Level)
	fmt.Printf("Target Endpoint: %s\n", r.cfg.IngestorURL)
	fmt.Println()

	var limiter <-chan time.Time
	if r.cfg.Rate > 0 {
		interval := time.Second / time.Duration(r.cfg.Rate)
		if interval <= 0 {
			interval = time.Microsecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		limiter = ticker.C
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if r.cfg.TotalLogs <= 0 && r.cfg.Duration > 0 {
		timer := time.AfterFunc(r.cfg.Duration, func() {
			cancel()
		})
		defer timer.Stop()
	}

	var wg sync.WaitGroup

	r.stats.StartTime = time.Now()

	// Progress reporter goroutine
	progressTicker := time.NewTicker(2 * time.Second)
	defer progressTicker.Stop()

	go func() {
		for {
			select {
			case <-runCtx.Done():
				return
			case t := <-progressTicker.C:
				elapsed := t.Sub(r.stats.StartTime).Truncate(time.Second)
				currStats := r.stats.Calculate()
				fmt.Printf("[%02d:%02d] %d logs sent | %.1f req/s | p50: %s | p99: %s\n",
					int(elapsed.Minutes()),
					int(elapsed.Seconds())%60,
					currStats.TotalLogs,
					currStats.AvgRate,
					currStats.P50Latency.Round(time.Millisecond),
					currStats.P99Latency.Round(time.Millisecond),
				)
			}
		}
	}()

	// Worker goroutines
	if r.cfg.TotalLogs > 0 {
		jobs := make(chan struct{}, r.cfg.TotalLogs)
		for i := 0; i < r.cfg.TotalLogs; i++ {
			jobs <- struct{}{}
		}
		close(jobs)

		for i := 0; i < r.cfg.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range jobs {
					select {
					case <-runCtx.Done():
						return
					default:
					}

					if limiter != nil {
						select {
						case <-runCtx.Done():
							return
						case <-limiter:
						}
					}

					r.sendOne(runCtx)
				}
			}()
		}
	} else {
		for i := 0; i < r.cfg.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-runCtx.Done():
						return
					default:
					}

					if limiter != nil {
						select {
						case <-runCtx.Done():
							return
						case <-limiter:
						}
					}

					r.sendOne(runCtx)
				}
			}()
		}
	}

	wg.Wait()
	r.stats.EndTime = time.Now()

	summary := r.stats.Calculate()
	return &summary, nil
}

func (r *Runner) sendOne(ctx context.Context) {
	payload := GenerateLogPayload(r.cfg)
	data, err := json.Marshal(payload)
	if err != nil {
		r.stats.Record(0, 0, err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.IngestorURL, bytes.NewReader(data))
	if err != nil {
		r.stats.Record(0, 0, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := r.httpClient.Do(req)
	latency := time.Since(start)

	if err != nil {
		r.stats.Record(latency, 0, err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	r.stats.Record(latency, resp.StatusCode, nil)
}
