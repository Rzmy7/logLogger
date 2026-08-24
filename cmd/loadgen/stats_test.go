package main

import (
	"errors"
	"testing"
	"time"
)

func TestBenchmarkStats_Calculation(t *testing.T) {
	stats := NewBenchmarkStats()
	stats.StartTime = time.Now()

	// Record 100 requests with ascending latencies 1ms to 100ms
	for i := 1; i <= 100; i++ {
		lat := time.Duration(i) * time.Millisecond
		if i == 100 {
			stats.Record(lat, 500, errors.New("server error"))
		} else {
			stats.Record(lat, 202, nil)
		}
	}

	stats.EndTime = stats.StartTime.Add(1 * time.Second)

	summary := stats.Calculate()
	if summary.TotalLogs != 100 {
		t.Errorf("expected TotalLogs 100, got %d", summary.TotalLogs)
	}
	if summary.SuccessCount != 99 {
		t.Errorf("expected SuccessCount 99, got %d", summary.SuccessCount)
	}
	if summary.FailedCount != 1 {
		t.Errorf("expected FailedCount 1, got %d", summary.FailedCount)
	}
	if summary.MinLatency != 1*time.Millisecond {
		t.Errorf("expected MinLatency 1ms, got %v", summary.MinLatency)
	}
	if summary.MaxLatency != 100*time.Millisecond {
		t.Errorf("expected MaxLatency 100ms, got %v", summary.MaxLatency)
	}
	if summary.P50Latency != 50*time.Millisecond {
		t.Errorf("expected P50 50ms, got %v", summary.P50Latency)
	}
	if summary.P95Latency != 95*time.Millisecond {
		t.Errorf("expected P95 95ms, got %v", summary.P95Latency)
	}
	if summary.P99Latency != 99*time.Millisecond {
		t.Errorf("expected P99 99ms, got %v", summary.P99Latency)
	}
}
