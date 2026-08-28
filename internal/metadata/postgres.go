package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// PostgresTenantRepo implements TenantRepository using PostgreSQL.
type PostgresTenantRepo struct {
	db *sql.DB
}

// NewPostgresTenantRepo creates a new PostgresTenantRepo.
func NewPostgresTenantRepo(db *sql.DB) *PostgresTenantRepo {
	return &PostgresTenantRepo{db: db}
}

func (r *PostgresTenantRepo) Create(ctx context.Context, t *Tenant) error {
	if err := t.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	query := `
	INSERT INTO tenants (name, slug, status, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5)
	RETURNING id, created_at, updated_at;`

	now := time.Now().UTC()
	err := r.db.QueryRowContext(ctx, query, t.Name, t.Slug, t.Status, now, now).Scan(
		&t.ID, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" { // unique_violation
			return ErrTenantSlugExists
		}
		return fmt.Errorf("failed to insert tenant: %w", err)
	}
	return nil
}

func (r *PostgresTenantRepo) GetByID(ctx context.Context, id string) (*Tenant, error) {
	query := `SELECT id, name, slug, status, created_at, updated_at FROM tenants WHERE id = $1;`
	t := &Tenant{}
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query tenant by ID: %w", err)
	}
	return t, nil
}

func (r *PostgresTenantRepo) GetBySlug(ctx context.Context, slug string) (*Tenant, error) {
	query := `SELECT id, name, slug, status, created_at, updated_at FROM tenants WHERE slug = $1;`
	t := &Tenant{}
	err := r.db.QueryRowContext(ctx, query, slug).Scan(
		&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query tenant by slug: %w", err)
	}
	return t, nil
}

func (r *PostgresTenantRepo) List(ctx context.Context) ([]*Tenant, error) {
	query := `SELECT id, name, slug, status, created_at, updated_at FROM tenants ORDER BY created_at ASC;`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tenants: %w", err)
	}
	defer rows.Close()

	var tenants []*Tenant
	for rows.Next() {
		t := &Tenant{}
		if err := rows.Scan(&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, rows.Err()
}

func (r *PostgresTenantRepo) Update(ctx context.Context, t *Tenant) error {
	if err := t.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	query := `
	UPDATE tenants
	SET name = $1, slug = $2, status = $3, updated_at = $4
	WHERE id = $5;`

	res, err := r.db.ExecContext(ctx, query, t.Name, t.Slug, t.Status, time.Now().UTC(), t.ID)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return ErrTenantSlugExists
		}
		return fmt.Errorf("failed to update tenant: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrTenantNotFound
	}
	return nil
}

func (r *PostgresTenantRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, "DELETE FROM tenants WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete tenant: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrTenantNotFound
	}
	return nil
}

// PostgresAPIKeyRepo implements APIKeyRepository using PostgreSQL.
type PostgresAPIKeyRepo struct {
	db *sql.DB
}

// NewPostgresAPIKeyRepo creates a new PostgresAPIKeyRepo.
func NewPostgresAPIKeyRepo(db *sql.DB) *PostgresAPIKeyRepo {
	return &PostgresAPIKeyRepo{db: db}
}

func (r *PostgresAPIKeyRepo) Create(ctx context.Context, k *APIKey) error {
	if k.TenantID == "" || k.KeyHash == "" || k.Name == "" {
		return fmt.Errorf("%w: missing required API key fields", ErrInvalidInput)
	}
	if k.Status == "" {
		k.Status = "active"
	}

	query := `
	INSERT INTO api_keys (tenant_id, key_hash, name, status, created_at, last_used_at, expires_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	RETURNING id, created_at;`

	now := time.Now().UTC()
	err := r.db.QueryRowContext(ctx, query, k.TenantID, k.KeyHash, k.Name, k.Status, now, k.LastUsedAt, k.ExpiresAt).Scan(
		&k.ID, &k.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert api key: %w", err)
	}
	return nil
}

func (r *PostgresAPIKeyRepo) GetByHash(ctx context.Context, keyHash string) (*APIKey, error) {
	query := `
	SELECT id, tenant_id, key_hash, name, status, created_at, last_used_at, expires_at
	FROM api_keys
	WHERE key_hash = $1;`

	k := &APIKey{}
	err := r.db.QueryRowContext(ctx, query, keyHash).Scan(
		&k.ID, &k.TenantID, &k.KeyHash, &k.Name, &k.Status, &k.CreatedAt, &k.LastUsedAt, &k.ExpiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrAPIKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query API key: %w", err)
	}
	return k, nil
}

func (r *PostgresAPIKeyRepo) ListByTenant(ctx context.Context, tenantID string) ([]*APIKey, error) {
	query := `
	SELECT id, tenant_id, key_hash, name, status, created_at, last_used_at, expires_at
	FROM api_keys
	WHERE tenant_id = $1
	ORDER BY created_at ASC;`

	rows, err := r.db.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list API keys: %w", err)
	}
	defer rows.Close()

	var keys []*APIKey
	for rows.Next() {
		k := &APIKey{}
		if err := rows.Scan(&k.ID, &k.TenantID, &k.KeyHash, &k.Name, &k.Status, &k.CreatedAt, &k.LastUsedAt, &k.ExpiresAt); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (r *PostgresAPIKeyRepo) Revoke(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, "UPDATE api_keys SET status = 'revoked' WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to revoke API key: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrAPIKeyNotFound
	}
	return nil
}

func (r *PostgresAPIKeyRepo) UpdateLastUsed(ctx context.Context, id string, lastUsed time.Time) error {
	_, err := r.db.ExecContext(ctx, "UPDATE api_keys SET last_used_at = $1 WHERE id = $2", lastUsed.UTC(), id)
	return err
}

// PostgresServiceRepo implements ServiceRepository using PostgreSQL.
type PostgresServiceRepo struct {
	db *sql.DB
}

// NewPostgresServiceRepo creates a new PostgresServiceRepo.
func NewPostgresServiceRepo(db *sql.DB) *PostgresServiceRepo {
	return &PostgresServiceRepo{db: db}
}

func (r *PostgresServiceRepo) Create(ctx context.Context, s *Service) error {
	if err := s.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	query := `
	INSERT INTO services (tenant_id, name, created_at, updated_at)
	VALUES ($1, $2, $3, $4)
	RETURNING id, created_at, updated_at;`

	now := time.Now().UTC()
	err := r.db.QueryRowContext(ctx, query, s.TenantID, s.Name, now, now).Scan(
		&s.ID, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return ErrServiceNameExists
		}
		return fmt.Errorf("failed to insert service: %w", err)
	}
	return nil
}

func (r *PostgresServiceRepo) GetByName(ctx context.Context, tenantID, name string) (*Service, error) {
	query := `SELECT id, tenant_id, name, created_at, updated_at FROM services WHERE tenant_id = $1 AND name = $2;`
	s := &Service{}
	err := r.db.QueryRowContext(ctx, query, tenantID, name).Scan(
		&s.ID, &s.TenantID, &s.Name, &s.CreatedAt, &s.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrServiceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query service: %w", err)
	}
	return s, nil
}

func (r *PostgresServiceRepo) ListByTenant(ctx context.Context, tenantID string) ([]*Service, error) {
	query := `SELECT id, tenant_id, name, created_at, updated_at FROM services WHERE tenant_id = $1 ORDER BY name ASC;`
	rows, err := r.db.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}
	defer rows.Close()

	var services []*Service
	for rows.Next() {
		s := &Service{}
		if err := rows.Scan(&s.ID, &s.TenantID, &s.Name, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		services = append(services, s)
	}
	return services, rows.Err()
}

func (r *PostgresServiceRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, "DELETE FROM services WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete service: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrServiceNotFound
	}
	return nil
}

// PostgresRetentionPolicyRepo implements RetentionPolicyRepository using PostgreSQL.
type PostgresRetentionPolicyRepo struct {
	db *sql.DB
}

// NewPostgresRetentionPolicyRepo creates a new PostgresRetentionPolicyRepo.
func NewPostgresRetentionPolicyRepo(db *sql.DB) *PostgresRetentionPolicyRepo {
	return &PostgresRetentionPolicyRepo{db: db}
}

func (r *PostgresRetentionPolicyRepo) Create(ctx context.Context, p *RetentionPolicy) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	query := `
	INSERT INTO retention_policies (tenant_id, retention_days, enabled, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5)
	RETURNING id, created_at, updated_at;`

	now := time.Now().UTC()
	err := r.db.QueryRowContext(ctx, query, p.TenantID, p.RetentionDays, p.Enabled, now, now).Scan(
		&p.ID, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert retention policy: %w", err)
	}
	return nil
}

func (r *PostgresRetentionPolicyRepo) GetByTenant(ctx context.Context, tenantID string) (*RetentionPolicy, error) {
	query := `SELECT id, tenant_id, retention_days, enabled, created_at, updated_at FROM retention_policies WHERE tenant_id = $1;`
	p := &RetentionPolicy{}
	err := r.db.QueryRowContext(ctx, query, tenantID).Scan(
		&p.ID, &p.TenantID, &p.RetentionDays, &p.Enabled, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRetentionPolicyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query retention policy: %w", err)
	}
	return p, nil
}

func (r *PostgresRetentionPolicyRepo) Update(ctx context.Context, p *RetentionPolicy) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	query := `
	UPDATE retention_policies
	SET retention_days = $1, enabled = $2, updated_at = $3
	WHERE tenant_id = $4;`

	res, err := r.db.ExecContext(ctx, query, p.RetentionDays, p.Enabled, time.Now().UTC(), p.TenantID)
	if err != nil {
		return fmt.Errorf("failed to update retention policy: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrRetentionPolicyNotFound
	}
	return nil
}
