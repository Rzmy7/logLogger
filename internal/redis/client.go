package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/metrics"
	"github.com/Rzmy7/logLogger/internal/models"
	redisGo "github.com/redis/go-redis/v9"
)

// ServiceMetrics holds aggregate counter metrics for a single service.
type ServiceMetrics struct {
	TotalLogs    int64 `json:"total_logs"`
	TotalErrors  int64 `json:"total_errors"`
	ErrorsLast5m int64 `json:"errors_last_5m"`
}

// TopErrorItem represents a ranked error message.
type TopErrorItem struct {
	Message string `json:"message"`
	Count   int64  `json:"count"`
}

// TopServiceItem represents a ranked service by log volume.
type TopServiceItem struct {
	Service string `json:"service"`
	Count   int64  `json:"count"`
}

// MetricsRecorder defines the contract for recording real-time metrics in Redis.
type MetricsRecorder interface {
	RecordLog(ctx context.Context, logMsg *models.LogMessage, rawJSON []byte) error
	Ping(ctx context.Context) error
	Close() error
}

// MetricsReader defines the contract for querying real-time metrics from Redis.
type MetricsReader interface {
	GetLiveMetrics(ctx context.Context, services []string) (int64, map[string]ServiceMetrics, error)
	GetTopErrors(ctx context.Context, n int) ([]TopErrorItem, error)
	GetTopServices(ctx context.Context, n int) ([]TopServiceItem, error)
	Ping(ctx context.Context) error
	Close() error
}

// Client wraps the redis client to record and query real-time log metrics.
type Client struct {
	rdb *redisGo.Client
}

// NewClient initializes a Redis client from the given REDIS_URL.
func NewClient(redisURL string) (*Client, error) {
	if strings.TrimSpace(redisURL) == "" {
		return nil, errors.New("redis URL cannot be empty")
	}

	opts, err := redisGo.ParseURL(redisURL)
	if err != nil {
		opts = &redisGo.Options{
			Addr: strings.TrimPrefix(redisURL, "redis://"),
		}
	}

	rdb := redisGo.NewClient(opts)
	return &Client{rdb: rdb}, nil
}

// Ping checks if Redis is responsive.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close closes the underlying Redis client connection.
func (c *Client) Close() error {
	if c.rdb != nil {
		return c.rdb.Close()
	}
	return nil
}

// RecordLog records all documented Redis metrics in an atomic pipeline.
func (c *Client) RecordLog(ctx context.Context, logMsg *models.LogMessage, rawJSON []byte) error {
	if logMsg == nil {
		return errors.New("log message cannot be nil")
	}

	start := time.Now()
	defer func() {
		metrics.RedisOperationDuration.WithLabelValues("record_log").Observe(time.Since(start).Seconds())
	}()

	pipe := c.rdb.Pipeline()

	// 1. Total logs counter (String)
	pipe.Incr(ctx, "stats:logs:total")

	// 2. Total logs per service (String)
	pipe.Incr(ctx, fmt.Sprintf("stats:logs:%s", logMsg.Service))

	// 3. Total logs per level (String)
	levelLower := strings.ToLower(logMsg.Level)
	pipe.Incr(ctx, fmt.Sprintf("stats:logs:level:%s", levelLower))

	// 4. Service leaderboard by volume (Sorted Set)
	pipe.ZIncrBy(ctx, "leaderboard:services", 1, logMsg.Service)

	// 5. Unique IPs seen per day with 24h TTL (Set)
	if logMsg.IP != "" {
		today := time.Now().UTC().Format("2006-01-02")
		ipKey := fmt.Sprintf("unique:ips:%s", today)
		pipe.SAdd(ctx, ipKey, logMsg.IP)
		pipe.Expire(ctx, ipKey, 24*time.Hour)
	}

	// 6. Error metrics on ERROR or FATAL levels
	if logMsg.Level == "ERROR" || logMsg.Level == "FATAL" {
		// All-time error counter per service
		pipe.Incr(ctx, fmt.Sprintf("stats:errors:%s", logMsg.Service))

		// 5-minute sliding window error counter
		windowKey := fmt.Sprintf("stats:errors:last_5m:%s", logMsg.Service)
		pipe.Incr(ctx, windowKey)
		pipe.Expire(ctx, windowKey, 5*time.Minute)

		// Top error messages leaderboard (Sorted Set)
		pipe.ZIncrBy(ctx, "leaderboard:errors", 1, logMsg.Message)

		// Recent errors list (List, capped at 100)
		if len(rawJSON) > 0 {
			recentKey := fmt.Sprintf("recent:errors:%s", logMsg.Service)
			pipe.LPush(ctx, recentKey, rawJSON)
			pipe.LTrim(ctx, recentKey, 0, 99)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		metrics.RedisOperationsTotal.WithLabelValues("record_log", "error").Inc()
		return fmt.Errorf("failed to record Redis metrics: %w", err)
	}

	metrics.RedisOperationsTotal.WithLabelValues("record_log", "success").Inc()
	return nil
}

// GetLiveMetrics queries the real-time metrics for total logs and requested/all services.
func (c *Client) GetLiveMetrics(ctx context.Context, requestedServices []string) (int64, map[string]ServiceMetrics, error) {
	// Total logs
	totalLogsVal, err := c.rdb.Get(ctx, "stats:logs:total").Result()
	var totalLogs int64
	if err == nil {
		totalLogs, _ = strconv.ParseInt(totalLogsVal, 10, 64)
	} else if !errors.Is(err, redisGo.Nil) {
		return 0, nil, fmt.Errorf("failed to get total logs: %w", err)
	}

	servicesToFetch := requestedServices
	if len(servicesToFetch) == 0 || (len(servicesToFetch) == 1 && (servicesToFetch[0] == "" || servicesToFetch[0] == "all")) {
		// Fetch all known services from the leaderboard
		servicesList, err := c.rdb.ZRevRange(ctx, "leaderboard:services", 0, -1).Result()
		if err != nil && !errors.Is(err, redisGo.Nil) {
			return 0, nil, fmt.Errorf("failed to list services: %w", err)
		}
		servicesToFetch = servicesList
	}

	servicesMap := make(map[string]ServiceMetrics)
	if len(servicesToFetch) == 0 {
		return totalLogs, servicesMap, nil
	}

	// Pipeline query per service
	pipe := c.rdb.Pipeline()
	type serviceCmds struct {
		totalLogsCmd    *redisGo.StringCmd
		totalErrorsCmd  *redisGo.StringCmd
		errorsLast5mCmd *redisGo.StringCmd
	}

	cmdsMap := make(map[string]serviceCmds)
	for _, svc := range servicesToFetch {
		svc = strings.TrimSpace(svc)
		if svc == "" {
			continue
		}
		cmdsMap[svc] = serviceCmds{
			totalLogsCmd:    pipe.Get(ctx, fmt.Sprintf("stats:logs:%s", svc)),
			totalErrorsCmd:  pipe.Get(ctx, fmt.Sprintf("stats:errors:%s", svc)),
			errorsLast5mCmd: pipe.Get(ctx, fmt.Sprintf("stats:errors:last_5m:%s", svc)),
		}
	}

	_, _ = pipe.Exec(ctx)

	for svc, cmds := range cmdsMap {
		var metrics ServiceMetrics

		if val, err := cmds.totalLogsCmd.Result(); err == nil {
			metrics.TotalLogs, _ = strconv.ParseInt(val, 10, 64)
		}
		if val, err := cmds.totalErrorsCmd.Result(); err == nil {
			metrics.TotalErrors, _ = strconv.ParseInt(val, 10, 64)
		}
		if val, err := cmds.errorsLast5mCmd.Result(); err == nil {
			metrics.ErrorsLast5m, _ = strconv.ParseInt(val, 10, 64)
		}

		servicesMap[svc] = metrics
	}

	return totalLogs, servicesMap, nil
}

// GetTopErrors returns top n error messages ranked by frequency from leaderboard:errors.
func (c *Client) GetTopErrors(ctx context.Context, n int) ([]TopErrorItem, error) {
	if n <= 0 {
		n = 5
	}
	if n > 100 {
		n = 100
	}

	results, err := c.rdb.ZRevRangeWithScores(ctx, "leaderboard:errors", 0, int64(n-1)).Result()
	if err != nil {
		if errors.Is(err, redisGo.Nil) {
			return []TopErrorItem{}, nil
		}
		return nil, fmt.Errorf("failed to get top errors: %w", err)
	}

	items := make([]TopErrorItem, 0, len(results))
	for _, r := range results {
		msg, ok := r.Member.(string)
		if !ok {
			msg = fmt.Sprint(r.Member)
		}
		items = append(items, TopErrorItem{
			Message: msg,
			Count:   int64(r.Score),
		})
	}

	return items, nil
}

// GetTopServices returns top n services ranked by log volume from leaderboard:services.
func (c *Client) GetTopServices(ctx context.Context, n int) ([]TopServiceItem, error) {
	if n <= 0 {
		n = 5
	}
	if n > 100 {
		n = 100
	}

	results, err := c.rdb.ZRevRangeWithScores(ctx, "leaderboard:services", 0, int64(n-1)).Result()
	if err != nil {
		if errors.Is(err, redisGo.Nil) {
			return []TopServiceItem{}, nil
		}
		return nil, fmt.Errorf("failed to get top services: %w", err)
	}

	items := make([]TopServiceItem, 0, len(results))
	for _, r := range results {
		svc, ok := r.Member.(string)
		if !ok {
			svc = fmt.Sprint(r.Member)
		}
		items = append(items, TopServiceItem{
			Service: svc,
			Count:   int64(r.Score),
		})
	}

	return items, nil
}
