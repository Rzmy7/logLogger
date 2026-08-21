package models

import "time"

// LogMessage represents a structured log event flowing through the pipeline.
type LogMessage struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Service   string `json:"service"`
	Message   string `json:"message"`
	TraceID   string `json:"trace_id,omitempty"`
	IP        string `json:"ip,omitempty"`
}

// ParsedTime parses the RFC3339 timestamp of the LogMessage.
func (l *LogMessage) ParsedTime() (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, l.Timestamp)
	if err != nil {
		return time.Parse(time.RFC3339, l.Timestamp)
	}
	return t, nil
}
