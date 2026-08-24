package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

var (
	infoTemplates = []string{
		"Request completed in %dms",
		"User session initiated for account_%d",
		"Payment processed successfully for order_%d",
		"Cache hit for key user_profile_%d",
		"Background reconciliation completed for batch_%d",
	}

	debugTemplates = []string{
		"Executing query: SELECT * FROM items WHERE id = %d",
		"Cache miss for key session_%d",
		"HTTP 200 GET /api/v2/items/%d from upstream",
		"Parsing payload with size %d bytes",
	}

	warnTemplates = []string{
		"Slow database query executed in %dms",
		"Rate limit threshold at 80%% for tenant_%d",
		"Retry attempt 2 for upstream service_%d",
		"Memory usage elevated at %d%%",
	}

	errorTemplates = []string{
		"DB connection timeout after %ds",
		"HTTP 500 from upstream payment gateway",
		"JWT validation failed for expired token",
		"Redis connection refused on host_%d",
		"Timeout waiting for lock on resource_%d",
	}

	fatalTemplates = []string{
		"Process panic: unhandled nil pointer in worker_%d",
		"Critical database corruption detected in partition_%d",
		"OOM killed: heap allocation exceeded limit",
	}
)

func randInt(max int) int {
	if max <= 0 {
		return 0
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max)))
	return int(n.Int64())
}

func randFloat() float64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	return float64(n.Int64()) / 1000000.0
}

func randomTraceID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("trace-%s", hex.EncodeToString(b))
}

func randomIP() string {
	return fmt.Sprintf("192.168.%d.%d", randInt(254)+1, randInt(254)+1)
}

// SelectLevel chooses a level based on configuration weights or error-rate override.
func SelectLevel(cfg *Config) string {
	if cfg.ErrorRate >= 0 {
		if randFloat() < cfg.ErrorRate {
			if randFloat() < 0.2 {
				return "FATAL"
			}
			return "ERROR"
		}
		return "INFO"
	}

	if cfg.Level != "MIXED" {
		return cfg.Level
	}

	// Mixed distribution from docs/03-api-spec.md:
	// INFO: 70%, DEBUG: 15%, WARN: 10%, ERROR: 4%, FATAL: 1%
	r := randFloat()
	switch {
	case r < 0.70:
		return "INFO"
	case r < 0.85:
		return "DEBUG"
	case r < 0.95:
		return "WARN"
	case r < 0.99:
		return "ERROR"
	default:
		return "FATAL"
	}
}

// GenerateLogPayload creates a realistic log payload based on the configuration.
func GenerateLogPayload(cfg *Config) *models.LogMessage {
	level := SelectLevel(cfg)

	service := cfg.Services[0]
	if len(cfg.Services) > 1 {
		service = cfg.Services[randInt(len(cfg.Services))]
	}

	var message string
	num := randInt(1000) + 1

	switch level {
	case "DEBUG":
		tmpl := debugTemplates[randInt(len(debugTemplates))]
		message = fmt.Sprintf(tmpl, num)
	case "WARN":
		tmpl := warnTemplates[randInt(len(warnTemplates))]
		message = fmt.Sprintf(tmpl, num)
	case "ERROR":
		tmpl := errorTemplates[randInt(len(errorTemplates))]
		message = fmt.Sprintf(tmpl, num)
	case "FATAL":
		tmpl := fatalTemplates[randInt(len(fatalTemplates))]
		message = fmt.Sprintf(tmpl, num)
	default: // INFO
		tmpl := infoTemplates[randInt(len(infoTemplates))]
		message = fmt.Sprintf(tmpl, num)
	}

	traceID := ""
	if cfg.IncludeTraceID {
		traceID = randomTraceID()
	}

	ip := ""
	if cfg.IncludeIP {
		ip = randomIP()
	}

	return &models.LogMessage{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level,
		Service:   service,
		Message:   message,
		TraceID:   traceID,
		IP:        ip,
	}
}
