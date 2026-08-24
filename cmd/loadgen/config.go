package main

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

// Config encapsulates configuration for the load generator.
type Config struct {
	Rate           int           // Target logs per second (0 = unthrottled)
	Duration       time.Duration // Duration to run (e.g. 60s, 5m)
	TotalLogs      int           // Number of logs to send (0 = rely on Duration)
	IngestorURL    string        // Target Ingestor API URL
	Services       []string      // List of service names
	Level          string        // mixed, INFO, ERROR, DEBUG, WARN, FATAL
	ErrorRate      float64       // Optional error override (0.0 - 1.0)
	Workers        int           // Number of concurrent worker goroutines
	MessageLen     int           // Average message length
	IncludeTraceID bool          // Whether to include trace IDs
	IncludeIP      bool          // Whether to include IPs
}

// ParseFlags parses and validates command-line flags.
func ParseFlags() (*Config, error) {
	rate := flag.Int("rate", 100, "Target logs per second (0 = unthrottled)")
	duration := flag.Duration("duration", 60*time.Second, "How long to run (e.g. 10s, 1m, 5m)")
	totalLogs := flag.Int("n", 0, "Total number of logs to send (overrides duration if > 0)")
	ingestor := flag.String("ingestor", "http://localhost:8081", "Ingestor base URL")
	service := flag.String("service", "payment-api", "Comma-separated service name(s)")
	level := flag.String("level", "mixed", "Log level: mixed, INFO, ERROR, DEBUG, WARN, FATAL")
	errorRate := flag.Float64("error-rate", -1, "Error rate percentage (0.0 to 1.0, overrides level weights if >= 0)")
	workers := flag.Int("workers", 10, "Number of concurrent workers")
	msgLen := flag.Int("message-len", 50, "Average message length")
	traceID := flag.Bool("trace-id", true, "Include random trace IDs")
	ip := flag.Bool("ip", true, "Include random IP addresses")

	flag.Parse()

	var servicesList []string
	for _, s := range strings.Split(*service, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			servicesList = append(servicesList, s)
		}
	}
	if len(servicesList) == 0 {
		servicesList = []string{"payment-api"}
	}

	url := strings.TrimRight(*ingestor, "/")
	if !strings.HasSuffix(url, "/api/v1/logs") {
		url = url + "/api/v1/logs"
	}

	if *workers <= 0 {
		*workers = 1
	}

	cfg := &Config{
		Rate:           *rate,
		Duration:       *duration,
		TotalLogs:      *totalLogs,
		IngestorURL:    url,
		Services:       servicesList,
		Level:          strings.ToUpper(strings.TrimSpace(*level)),
		ErrorRate:      *errorRate,
		Workers:        *workers,
		MessageLen:     *msgLen,
		IncludeTraceID: *traceID,
		IncludeIP:      *ip,
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate ensures configuration fields are valid.
func (c *Config) Validate() error {
	if c.IngestorURL == "" {
		return fmt.Errorf("ingestor URL cannot be empty")
	}
	if c.Workers <= 0 {
		return fmt.Errorf("workers must be greater than 0")
	}
	validLevels := map[string]bool{
		"MIXED": true,
		"INFO":  true,
		"DEBUG": true,
		"WARN":  true,
		"ERROR": true,
		"FATAL": true,
	}
	if !validLevels[c.Level] {
		return fmt.Errorf("invalid level %q: must be one of MIXED, INFO, DEBUG, WARN, ERROR, FATAL", c.Level)
	}
	return nil
}
