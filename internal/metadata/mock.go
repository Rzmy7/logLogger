package metadata

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var idCounter int64

func nextMockID(prefix string) string {
	val := atomic.AddInt64(&idCounter, 1)
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), val)
}

// MockTenantRepo is a thread-safe in-memory TenantRepository for unit tests.
type MockTenantRepo struct {
	mu      sync.RWMutex
	Tenants map[string]*Tenant // keyed by ID
}

func NewMockTenantRepo() *MockTenantRepo {
	return &MockTenantRepo{Tenants: make(map[string]*Tenant)}
}

func (m *MockTenantRepo) Create(ctx context.Context, t *Tenant) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := t.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	for _, existing := range m.Tenants {
		if existing.Slug == t.Slug {
			return ErrTenantSlugExists
		}
	}

	now := time.Now().UTC()
	if t.ID == "" {
		t.ID = nextMockID("tenant")
	}
	t.CreatedAt = now
	t.UpdatedAt = now
	m.Tenants[t.ID] = t
	return nil
}

func (m *MockTenantRepo) GetByID(ctx context.Context, id string) (*Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, exists := m.Tenants[id]
	if !exists {
		return nil, ErrTenantNotFound
	}
	return t, nil
}

func (m *MockTenantRepo) GetBySlug(ctx context.Context, slug string) (*Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, t := range m.Tenants {
		if t.Slug == slug {
			return t, nil
		}
	}
	return nil, ErrTenantNotFound
}

func (m *MockTenantRepo) List(ctx context.Context) ([]*Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Tenant
	for _, t := range m.Tenants {
		result = append(result, t)
	}
	return result, nil
}

func (m *MockTenantRepo) Update(ctx context.Context, t *Tenant) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := t.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	for _, existing := range m.Tenants {
		if existing.Slug == t.Slug && existing.ID != t.ID {
			return ErrTenantSlugExists
		}
	}

	if _, exists := m.Tenants[t.ID]; !exists {
		return ErrTenantNotFound
	}

	t.UpdatedAt = time.Now().UTC()
	m.Tenants[t.ID] = t
	return nil
}

func (m *MockTenantRepo) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.Tenants[id]; !exists {
		return ErrTenantNotFound
	}
	delete(m.Tenants, id)
	return nil
}

// MockAPIKeyRepo is a thread-safe in-memory APIKeyRepository for unit tests.
type MockAPIKeyRepo struct {
	mu   sync.RWMutex
	Keys map[string]*APIKey // keyed by KeyHash
}

func NewMockAPIKeyRepo() *MockAPIKeyRepo {
	return &MockAPIKeyRepo{Keys: make(map[string]*APIKey)}
}

func (m *MockAPIKeyRepo) Create(ctx context.Context, k *APIKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if k.TenantID == "" || k.KeyHash == "" || k.Name == "" {
		return fmt.Errorf("%w: missing required fields", ErrInvalidInput)
	}

	now := time.Now().UTC()
	if k.ID == "" {
		k.ID = nextMockID("key")
	}
	k.CreatedAt = now
	if k.Status == "" {
		k.Status = "active"
	}
	m.Keys[k.KeyHash] = k
	return nil
}

func (m *MockAPIKeyRepo) GetByHash(ctx context.Context, keyHash string) (*APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	k, exists := m.Keys[keyHash]
	if !exists {
		return nil, ErrAPIKeyNotFound
	}
	return k, nil
}

func (m *MockAPIKeyRepo) ListByTenant(ctx context.Context, tenantID string) ([]*APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*APIKey
	for _, k := range m.Keys {
		if k.TenantID == tenantID {
			result = append(result, k)
		}
	}
	return result, nil
}

func (m *MockAPIKeyRepo) Revoke(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, k := range m.Keys {
		if k.ID == id {
			k.Status = "revoked"
			return nil
		}
	}
	return ErrAPIKeyNotFound
}

func (m *MockAPIKeyRepo) UpdateLastUsed(ctx context.Context, id string, lastUsed time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, k := range m.Keys {
		if k.ID == id {
			k.LastUsedAt = &lastUsed
			return nil
		}
	}
	return ErrAPIKeyNotFound
}

// MockServiceRepo is a thread-safe in-memory ServiceRepository for unit tests.
type MockServiceRepo struct {
	mu       sync.RWMutex
	Services map[string]*Service // keyed by id
}

func NewMockServiceRepo() *MockServiceRepo {
	return &MockServiceRepo{Services: make(map[string]*Service)}
}

func (m *MockServiceRepo) Create(ctx context.Context, s *Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := s.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	for _, existing := range m.Services {
		if existing.TenantID == s.TenantID && existing.Name == s.Name {
			return ErrServiceNameExists
		}
	}

	now := time.Now().UTC()
	if s.ID == "" {
		s.ID = nextMockID("svc")
	}
	s.CreatedAt = now
	s.UpdatedAt = now
	m.Services[s.ID] = s
	return nil
}

func (m *MockServiceRepo) GetByName(ctx context.Context, tenantID, name string) (*Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.Services {
		if s.TenantID == tenantID && s.Name == name {
			return s, nil
		}
	}
	return nil, ErrServiceNotFound
}

func (m *MockServiceRepo) ListByTenant(ctx context.Context, tenantID string) ([]*Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Service
	for _, s := range m.Services {
		if s.TenantID == tenantID {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *MockServiceRepo) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.Services[id]; !exists {
		return ErrServiceNotFound
	}
	delete(m.Services, id)
	return nil
}

// MockRetentionPolicyRepo is a thread-safe in-memory RetentionPolicyRepository for unit tests.
type MockRetentionPolicyRepo struct {
	mu       sync.RWMutex
	Policies map[string]*RetentionPolicy // keyed by TenantID
}

func NewMockRetentionPolicyRepo() *MockRetentionPolicyRepo {
	return &MockRetentionPolicyRepo{Policies: make(map[string]*RetentionPolicy)}
}

func (m *MockRetentionPolicyRepo) Create(ctx context.Context, p *RetentionPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := p.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	now := time.Now().UTC()
	if p.ID == "" {
		p.ID = nextMockID("ret")
	}
	p.CreatedAt = now
	p.UpdatedAt = now
	m.Policies[p.TenantID] = p
	return nil
}

func (m *MockRetentionPolicyRepo) GetByTenant(ctx context.Context, tenantID string) (*RetentionPolicy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p, exists := m.Policies[tenantID]
	if !exists {
		return nil, ErrRetentionPolicyNotFound
	}
	return p, nil
}

func (m *MockRetentionPolicyRepo) Update(ctx context.Context, p *RetentionPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := p.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	if _, exists := m.Policies[p.TenantID]; !exists {
		return ErrRetentionPolicyNotFound
	}

	p.UpdatedAt = time.Now().UTC()
	m.Policies[p.TenantID] = p
	return nil
}
