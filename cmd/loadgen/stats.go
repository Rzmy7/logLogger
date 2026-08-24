package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// BenchmarkStats aggregates performance metrics across all workers.
type BenchmarkStats struct {
	mu          sync.Mutex
	TotalSent   int64
	Success     int64
	Failed      int64
	Latencies   []time.Duration
	StatusCodes map[int]int64
	StartTime   time.Time
	EndTime     time.Time
}

// NewBenchmarkStats initializes an empty BenchmarkStats tracker.
func NewBenchmarkStats() *BenchmarkStats {
	return &BenchmarkStats{
		Latencies:   make([]time.Duration, 0, 10000),
		StatusCodes: make(map[int]int64),
	}
}

// Record records the result of a single request.
func (s *BenchmarkStats) Record(latency time.Duration, statusCode int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalSent++
	if err == nil && statusCode >= 200 && statusCode < 300 {
		s.Success++
	} else {
		s.Failed++
	}

	if statusCode > 0 {
		s.StatusCodes[statusCode]++
	}

	s.Latencies = append(s.Latencies, latency)
}

// Summary calculates and formats benchmark statistics.
type Summary struct {
	TotalLogs    int64
	SuccessCount int64
	FailedCount  int64
	Duration     time.Duration
	AvgRate      float64
	MinLatency   time.Duration
	AvgLatency   time.Duration
	MaxLatency   time.Duration
	P50Latency   time.Duration
	P90Latency   time.Duration
	P95Latency   time.Duration
	P99Latency   time.Duration
}

// Calculate computes summary percentiles from recorded measurements.
func (s *BenchmarkStats) Calculate() Summary {
	s.mu.Lock()
	defer s.mu.Unlock()

	duration := s.EndTime.Sub(s.StartTime)
	if duration <= 0 {
		duration = time.Millisecond
	}

	total := s.TotalSent
	success := s.Success
	failed := s.Failed

	if len(s.Latencies) == 0 {
		return Summary{
			TotalLogs:    total,
			SuccessCount: success,
			FailedCount:  failed,
			Duration:     duration,
			AvgRate:      float64(total) / duration.Seconds(),
		}
	}

	sorted := make([]time.Duration, len(s.Latencies))
	copy(sorted, s.Latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, lat := range sorted {
		sum += lat
	}

	minLat := sorted[0]
	maxLat := sorted[len(sorted)-1]
	avgLat := sum / time.Duration(len(sorted))

	percentile := func(p float64) time.Duration {
		idx := int(math.Ceil(float64(len(sorted))*(p/100.0))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}

	return Summary{
		TotalLogs:    total,
		SuccessCount: success,
		FailedCount:  failed,
		Duration:     duration,
		AvgRate:      float64(total) / duration.Seconds(),
		MinLatency:   minLat,
		AvgLatency:   avgLat,
		MaxLatency:   maxLat,
		P50Latency:   percentile(50),
		P90Latency:   percentile(90),
		P95Latency:   percentile(95),
		P99Latency:   percentile(99),
	}
}

// PrintReport prints a formatted benchmark summary.
func (s *Summary) PrintReport() {
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("       Benchmark Results Summary        ")
	fmt.Println("========================================")
	fmt.Printf("Total Logs Sent:   %d\n", s.TotalLogs)
	fmt.Printf("Success (2xx):     %d\n", s.SuccessCount)
	fmt.Printf("Failed:            %d\n", s.FailedCount)
	fmt.Printf("Duration:          %s\n", s.Duration.Round(time.Millisecond))
	fmt.Printf("Achieved Rate:     %.1f logs/sec\n", s.AvgRate)
	fmt.Println("----------------------------------------")
	fmt.Println("Latency Distribution:")
	fmt.Printf("  Min:             %s\n", s.MinLatency.Round(time.Microsecond))
	fmt.Printf("  Avg:             %s\n", s.AvgLatency.Round(time.Microsecond))
	fmt.Printf("  p50 (Median):    %s\n", s.P50Latency.Round(time.Microsecond))
	fmt.Printf("  p90:             %s\n", s.P90Latency.Round(time.Microsecond))
	fmt.Printf("  p95:             %s\n", s.P95Latency.Round(time.Microsecond))
	fmt.Printf("  p99:             %s\n", s.P99Latency.Round(time.Microsecond))
	fmt.Printf("  Max:             %s\n", s.MaxLatency.Round(time.Microsecond))
	fmt.Println("========================================")
}
