package models

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLogMessage_TenantFallback(t *testing.T) {
	// 1. Without tenant_id -> defaults to "default"
	msgDefault := &LogMessage{
		Timestamp: "2026-08-28T12:00:00Z",
		Level:     "INFO",
		Service:   "payment-api",
		Message:   "Payment processed",
	}
	if msgDefault.Tenant() != DefaultTenantID {
		t.Errorf("expected tenant %q, got %q", DefaultTenantID, msgDefault.Tenant())
	}

	docDefault := msgDefault.ToDocument(time.Now().UTC())
	if docDefault.TenantID != DefaultTenantID {
		t.Errorf("expected doc tenant %q, got %q", DefaultTenantID, docDefault.TenantID)
	}

	// 2. With explicit tenant_id
	msgTenant := &LogMessage{
		TenantID:  "tenant-alpha",
		Timestamp: "2026-08-28T12:00:00Z",
		Level:     "WARN",
		Service:   "auth-service",
		Message:   "Rate limit triggered",
	}
	if msgTenant.Tenant() != "tenant-alpha" {
		t.Errorf("expected tenant 'tenant-alpha', got %q", msgTenant.Tenant())
	}

	docTenant := msgTenant.ToDocument(time.Now().UTC())
	if docTenant.TenantID != "tenant-alpha" {
		t.Errorf("expected doc tenant 'tenant-alpha', got %q", docTenant.TenantID)
	}

	// 3. JSON serialization roundtrip
	data, err := json.Marshal(msgTenant)
	if err != nil {
		t.Fatalf("failed to marshal log message: %v", err)
	}

	var parsed LogMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal log message: %v", err)
	}
	if parsed.TenantID != "tenant-alpha" {
		t.Errorf("expected unmarshaled tenant_id 'tenant-alpha', got %q", parsed.TenantID)
	}
}
