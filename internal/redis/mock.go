package redis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

// MockMetricsRecorder is an in-memory fake for MetricsRecorder and MetricsReader interfaces.
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

// RecordBatch records metrics for a slice of log messages in memory.
func (m *MockMetricsRecorder) RecordBatch(ctx context.Context, logs []*models.LogMessage, rawJSONs [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Err != nil {
		return m.Err
	}

	today := time.Now().UTC().Format("2006-01-02")
	for i, logMsg := range logs {
		if logMsg == nil {
			continue
		}

		var rawJSON []byte
		if i < len(rawJSONs) {
			rawJSON = rawJSONs[i]
		}

		kb := NewKeyBuilder(logMsg.Tenant())

		m.Counters[kb.StatsLogsTotal()]++
		m.Counters[kb.StatsLogsService(logMsg.Service)]++
		m.Counters[kb.StatsLogsLevel(logMsg.Level)]++

		lbKey := kb.LeaderboardServices()
		if m.Leaderboards[lbKey] == nil {
			m.Leaderboards[lbKey] = make(map[string]float64)
		}
		m.Leaderboards[lbKey][logMsg.Service]++

		if logMsg.IP != "" {
			ipKey := kb.UniqueIPs(time.Now().UTC())
			if m.Sets[ipKey] == nil {
				m.Sets[ipKey] = make(map[string]bool)
			}
			m.Sets[ipKey][logMsg.IP] = true
		}

		if logMsg.Level == "ERROR" || logMsg.Level == "FATAL" {
			m.Counters[kb.StatsErrorsService(logMsg.Service)]++
			m.Counters[kb.StatsErrorsLast5m(logMsg.Service)]++

			errLbKey := kb.LeaderboardErrors()
			if m.Leaderboards[errLbKey] == nil {
				m.Leaderboards[errLbKey] = make(map[string]float64)
			}
			m.Leaderboards[errLbKey][logMsg.Message]++

			if len(rawJSON) > 0 {
				recentKey := kb.RecentErrors(logMsg.Service)
				m.Lists[recentKey] = append([]string{string(rawJSON)}, m.Lists[recentKey]...)
				if len(m.Lists[recentKey]) > 100 {
					m.Lists[recentKey] = m.Lists[recentKey][:100]
				}
			}
		}
	}
	_ = today
	return nil
}

// GetLiveMetrics returns mock live metrics.
func (m *MockMetricsRecorder) GetLiveMetrics(ctx context.Context, requestedServices []string) (int64, map[string]ServiceMetrics, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Err != nil {
		return 0, nil, m.Err
	}

	total := m.Counters["stats:logs:total"]
	res := make(map[string]ServiceMetrics)

	services := requestedServices
	if len(services) == 0 || (len(services) == 1 && (services[0] == "" || services[0] == "all")) {
		for svc := range m.Leaderboards["leaderboard:services"] {
			services = append(services, svc)
		}
	}

	for _, svc := range services {
		res[svc] = ServiceMetrics{
			TotalLogs:    m.Counters[fmt.Sprintf("stats:logs:%s", svc)],
			TotalErrors:  m.Counters[fmt.Sprintf("stats:errors:%s", svc)],
			ErrorsLast5m: m.Counters[fmt.Sprintf("stats:errors:last_5m:%s", svc)],
		}
	}

	return total, res, nil
}

// GetTopErrors returns top n errors.
func (m *MockMetricsRecorder) GetTopErrors(ctx context.Context, n int) ([]TopErrorItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Err != nil {
		return nil, m.Err
	}

	var items []TopErrorItem
	for msg, count := range m.Leaderboards["leaderboard:errors"] {
		items = append(items, TopErrorItem{
			Message: msg,
			Count:   int64(count),
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Count > items[j].Count
	})

	if len(items) > n {
		items = items[:n]
	}

	return items, nil
}

// GetTopServices returns top n services.
func (m *MockMetricsRecorder) GetTopServices(ctx context.Context, n int) ([]TopServiceItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Err != nil {
		return nil, m.Err
	}

	var items []TopServiceItem
	for svc, count := range m.Leaderboards["leaderboard:services"] {
		items = append(items, TopServiceItem{
			Service: svc,
			Count:   int64(count),
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Count > items[j].Count
	})

	if len(items) > n {
		items = items[:n]
	}

	return items, nil
}
