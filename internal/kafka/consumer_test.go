package kafka

import (
	"testing"
)

func TestParseLogMessage_Valid(t *testing.T) {
	data := []byte(`{
		"timestamp": "2026-08-06T10:00:00Z",
		"level": "ERROR",
		"service": "payment-api",
		"message": "DB connection timeout after 30s",
		"trace_id": "abc-123-def-456",
		"ip": "192.168.1.5"
	}`)

	msg, err := ParseLogMessage(data)
	if err != nil {
		t.Fatalf("unexpected error parsing log message: %v", err)
	}

	if msg.Service != "payment-api" {
		t.Errorf("expected service 'payment-api', got %q", msg.Service)
	}
	if msg.Level != "ERROR" {
		t.Errorf("expected level 'ERROR', got %q", msg.Level)
	}
	if msg.TraceID != "abc-123-def-456" {
		t.Errorf("expected trace_id 'abc-123-def-456', got %q", msg.TraceID)
	}
	if msg.IP != "192.168.1.5" {
		t.Errorf("expected ip '192.168.1.5', got %q", msg.IP)
	}

	parsedTime, err := msg.ParsedTime()
	if err != nil {
		t.Fatalf("failed to parse timestamp: %v", err)
	}
	if parsedTime.Year() != 2026 {
		t.Errorf("expected year 2026, got %d", parsedTime.Year())
	}
}

func TestParseLogMessage_MissingFields(t *testing.T) {
	invalidData := []byte(`{"message": "missing timestamp and level"}`)
	_, err := ParseLogMessage(invalidData)
	if err == nil {
		t.Fatal("expected error on missing required fields, got nil")
	}
}

func TestParseLogMessage_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{invalid-json}`)
	_, err := ParseLogMessage(invalidJSON)
	if err == nil {
		t.Fatal("expected error on malformed JSON, got nil")
	}
}

func TestNewConsumer_Validation(t *testing.T) {
	_, err := NewConsumer(nil, GroupIDLogProcessors, TopicAppLogs)
	if err == nil {
		t.Error("expected error for empty brokers, got nil")
	}

	_, err = NewConsumer([]string{"localhost:9092"}, "", TopicAppLogs)
	if err == nil {
		t.Error("expected error for empty groupID, got nil")
	}

	_, err = NewConsumer([]string{"localhost:9092"}, GroupIDLogProcessors, "")
	if err == nil {
		t.Error("expected error for empty topic, got nil")
	}

	consumer, err := NewConsumer([]string{"localhost:9092"}, GroupIDLogProcessors, TopicAppLogs)
	if err != nil {
		t.Fatalf("unexpected error creating consumer: %v", err)
	}
	if consumer == nil {
		t.Fatal("expected non-nil consumer")
	}
	_ = consumer.Close()
}
