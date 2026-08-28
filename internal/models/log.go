package models

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const DefaultTenantID = "default"

// LogMessage represents a structured log event flowing through the pipeline.
type LogMessage struct {
	TenantID  string `json:"tenant_id,omitempty"`
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Service   string `json:"service"`
	Message   string `json:"message"`
	TraceID   string `json:"trace_id,omitempty"`
	IP        string `json:"ip,omitempty"`
}

// Tenant returns the TenantID or DefaultTenantID if empty.
func (l *LogMessage) Tenant() string {
	if trimmed := strings.TrimSpace(l.TenantID); trimmed != "" {
		return trimmed
	}
	return DefaultTenantID
}

// ParsedTime parses the RFC3339 timestamp of the LogMessage.
func (l *LogMessage) ParsedTime() (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, l.Timestamp)
	if err != nil {
		return time.Parse(time.RFC3339, l.Timestamp)
	}
	return t, nil
}

// LogDocument represents the document indexed into Elasticsearch (with ingested_at and tenant_id).
type LogDocument struct {
	TenantID   string `json:"tenant_id"`
	Timestamp  string `json:"timestamp"`
	Level      string `json:"level"`
	Service    string `json:"service"`
	Message    string `json:"message"`
	TraceID    string `json:"trace_id,omitempty"`
	IP         string `json:"ip,omitempty"`
	IngestedAt string `json:"ingested_at"`
}

// DeterministicID produces a stable 32-hex character identifier from log identity for idempotent indexing retries.
func (d *LogDocument) DeterministicID() string {
	raw := fmt.Sprintf("%s|%s|%s|%s|%s|%s", d.TenantID, d.Service, d.Level, d.Timestamp, d.TraceID, d.Message)
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:16]) // 16 bytes = 32 hex chars
}

// ParsedTime parses the RFC3339 timestamp of the LogDocument.
func (d *LogDocument) ParsedTime() (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, d.Timestamp)
	if err != nil {
		return time.Parse(time.RFC3339, d.Timestamp)
	}
	return t, nil
}

// ToDocument converts a LogMessage to an Elasticsearch LogDocument.
func (l *LogMessage) ToDocument(ingestedAt time.Time) *LogDocument {
	return &LogDocument{
		TenantID:   l.Tenant(),
		Timestamp:  l.Timestamp,
		Level:      l.Level,
		Service:    l.Service,
		Message:    l.Message,
		TraceID:    l.TraceID,
		IP:         l.IP,
		IngestedAt: ingestedAt.UTC().Format(time.RFC3339Nano),
	}
}
