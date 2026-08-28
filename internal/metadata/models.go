package metadata

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrTenantNotFound          = errors.New("tenant not found")
	ErrTenantSlugExists        = errors.New("tenant slug already exists")
	ErrAPIKeyNotFound          = errors.New("api key not found")
	ErrAPIKeyExpired           = errors.New("api key is expired")
	ErrAPIKeyRevoked           = errors.New("api key is revoked/inactive")
	ErrServiceNotFound         = errors.New("service not found")
	ErrServiceNameExists       = errors.New("service name already exists for tenant")
	ErrRetentionPolicyNotFound = errors.New("retention policy not found")
	ErrInvalidInput            = errors.New("invalid metadata input")
)

// Tenant represents an organization or system owning application services and logs.
type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	Status    string    `json:"status"` // "active", "suspended", "disabled"
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Validate checks tenant invariants.
func (t *Tenant) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("tenant name cannot be empty")
	}
	if strings.TrimSpace(t.Slug) == "" {
		return errors.New("tenant slug cannot be empty")
	}
	if t.Status == "" {
		t.Status = "active"
	}
	return nil
}

// APIKey represents an authenticated token associated with a tenant.
// The raw key is never stored; only its SHA-256 key_hash is persisted.
type APIKey struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	KeyHash    string     `json:"key_hash"`
	Name       string     `json:"name"`
	Status     string     `json:"status"` // "active", "revoked"
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

// IsValid checks if key is active and not expired.
func (k *APIKey) IsValid() bool {
	if k.Status != "active" {
		return false
	}
	if k.ExpiresAt != nil && time.Now().UTC().After(*k.ExpiresAt) {
		return false
	}
	return true
}

// Service represents a registered log producer belonging to a tenant.
type Service struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Validate checks service invariants.
func (s *Service) Validate() error {
	if strings.TrimSpace(s.TenantID) == "" {
		return errors.New("tenant_id cannot be empty")
	}
	if strings.TrimSpace(s.Name) == "" {
		return errors.New("service name cannot be empty")
	}
	return nil
}

// RetentionPolicy defines log retention configuration for a tenant.
type RetentionPolicy struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	RetentionDays int       `json:"retention_days"`
	Enabled       bool      `json:"enabled"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Validate checks retention policy invariants.
func (r *RetentionPolicy) Validate() error {
	if strings.TrimSpace(r.TenantID) == "" {
		return errors.New("tenant_id cannot be empty")
	}
	if r.RetentionDays <= 0 {
		return errors.New("retention_days must be greater than 0")
	}
	return nil
}
