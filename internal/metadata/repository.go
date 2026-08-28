package metadata

import (
	"context"
	"time"
)

// TenantRepository defines storage operations for tenants.
type TenantRepository interface {
	Create(ctx context.Context, tenant *Tenant) error
	GetByID(ctx context.Context, id string) (*Tenant, error)
	GetBySlug(ctx context.Context, slug string) (*Tenant, error)
	List(ctx context.Context) ([]*Tenant, error)
	Update(ctx context.Context, tenant *Tenant) error
	Delete(ctx context.Context, id string) error
}

// APIKeyRepository defines storage operations for API keys.
type APIKeyRepository interface {
	Create(ctx context.Context, key *APIKey) error
	GetByHash(ctx context.Context, keyHash string) (*APIKey, error)
	ListByTenant(ctx context.Context, tenantID string) ([]*APIKey, error)
	Revoke(ctx context.Context, id string) error
	UpdateLastUsed(ctx context.Context, id string, lastUsed time.Time) error
}

// ServiceRepository defines storage operations for tenant services.
type ServiceRepository interface {
	Create(ctx context.Context, service *Service) error
	GetByName(ctx context.Context, tenantID, name string) (*Service, error)
	ListByTenant(ctx context.Context, tenantID string) ([]*Service, error)
	Delete(ctx context.Context, id string) error
}

// RetentionPolicyRepository defines storage operations for tenant retention policies.
type RetentionPolicyRepository interface {
	Create(ctx context.Context, policy *RetentionPolicy) error
	GetByTenant(ctx context.Context, tenantID string) (*RetentionPolicy, error)
	Update(ctx context.Context, policy *RetentionPolicy) error
}
