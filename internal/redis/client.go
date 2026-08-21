package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
	redisGo "github.com/redis/go-redis/v9"
)

// MetricsRecorder defines the contract for recording real-time metrics in Redis.
type MetricsRecorder interface {
	RecordLog(ctx context.Context, logMsg *models.LogMessage, rawJSON []byte) error
	Ping(ctx context.Context) error
	Close() error
}

// Client wraps the redis client to record real-time log metrics.
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
		return fmt.Errorf("failed to record Redis metrics: %w", err)
	}

	return nil
}
