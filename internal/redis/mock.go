package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

// MockMetricsRecorder is an in-memory fake for MetricsRecorder interface.
type MockMetricsRecorder struct {
	mu           sync.Mutex
	Counters     map[string]int64
	Leaderboards map[string]map[string]float64
	Sets         map[string]map[string]bool
	Lists        map[string][]string
	Err          error
	Closed       bool
}

// NewMockMetricsRecorder creates a new MockMetricsRecorder.
func NewMockMetricsRecorder() *MockMetricsRecorder {
	return &MockMetricsRecorder{
		Counters:     make(map[string]int64),
		Leaderboards: make(map[string]map[string]float64),
		Sets:         make(map[string]map[string]bool),
		Lists:        make(map[string][]string),
	}
}

// Ping returns simulated error or nil.
func (m *MockMetricsRecorder) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Err
}

// Close marks mock as closed.
func (m *MockMetricsRecorder) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

// RecordLog records metrics in-memory.
func (m *MockMetricsRecorder) RecordLog(ctx context.Context, logMsg *models.LogMessage, rawJSON []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Err != nil {
		return m.Err
	}
	if logMsg == nil {
		return errors.New("nil log message")
	}

	// 1. Total logs
	m.Counters["stats:logs:total"]++

	// 2. Service logs
	m.Counters[fmt.Sprintf("stats:logs:%s", logMsg.Service)]++

	// 3. Level logs
	m.Counters[fmt.Sprintf("stats:logs:level:%s", strings.ToLower(logMsg.Level))]++

	// 4. Service leaderboard
	if m.Leaderboards["leaderboard:services"] == nil {
		m.Leaderboards["leaderboard:services"] = make(map[string]float64)
	}
	m.Leaderboards["leaderboard:services"][logMsg.Service]++

	// 5. Unique IP
	if logMsg.IP != "" {
		today := time.Now().UTC().Format("2006-01-02")
		ipKey := fmt.Sprintf("unique:ips:%s", today)
		if m.Sets[ipKey] == nil {
			m.Sets[ipKey] = make(map[string]bool)
		}
		m.Sets[ipKey][logMsg.IP] = true
	}

	// 6. Errors
	if logMsg.Level == "ERROR" || logMsg.Level == "FATAL" {
		m.Counters[fmt.Sprintf("stats:errors:%s", logMsg.Service)]++
		m.Counters[fmt.Sprintf("stats:errors:last_5m:%s", logMsg.Service)]++

		if m.Leaderboards["leaderboard:errors"] == nil {
			m.Leaderboards["leaderboard:errors"] = make(map[string]float64)
		}
		m.Leaderboards["leaderboard:errors"][logMsg.Message]++

		if len(rawJSON) > 0 {
			recentKey := fmt.Sprintf("recent:errors:%s", logMsg.Service)
			m.Lists[recentKey] = append([]string{string(rawJSON)}, m.Lists[recentKey]...)
			if len(m.Lists[recentKey]) > 100 {
				m.Lists[recentKey] = m.Lists[recentKey][:100]
			}
		}
	}

	return nil
}
