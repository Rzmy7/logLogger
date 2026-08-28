package metadata

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/Rzmy7/logLogger/internal/config"
	_ "github.com/lib/pq"
)

func TestPostgres_Integration(t *testing.T) {
	cfg, err := config.Load()
	if err != nil || cfg.PostgresURL == "" {
		t.Skip("Skipping PostgreSQL integration test: POSTGRES_URL not configured")
	}

	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("Skipping PostgreSQL integration test (database unreachable): %v", err)
	}

	// 1. Run Migrations
	migrator := NewMigrator(db)
	migrations, err := LoadMigrationsFromDir("../../migrations")
	if err != nil {
		t.Fatalf("failed to load migrations: %v", err)
	}

	if err := migrator.Up(ctx, migrations); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	// 2. Test Tenant Creation
	tenantRepo := NewPostgresTenantRepo(db)
	apiKeyRepo := NewPostgresAPIKeyRepo(db)
	svcRepo := NewPostgresServiceRepo(db)
	retRepo := NewPostgresRetentionPolicyRepo(db)

	testSlug := "test-tenant-" + time.Now().UTC().Format("20060102150405")
	tenant := &Tenant{
		Name: "Test Tenant Alpha",
		Slug: testSlug,
	}
	if err := tenantRepo.Create(ctx, tenant); err != nil {
		t.Fatalf("failed to create tenant in postgres: %v", err)
	}

	// 3. Test API Key Creation
	rawKey, keyHash, err := GenerateAPIKey("ll_live")
	if err != nil {
		t.Fatalf("failed to generate api key: %v", err)
	}

	apiKey := &APIKey{
		TenantID: tenant.ID,
		KeyHash:  keyHash,
		Name:     "Primary Ingestion Key",
	}
	if err := apiKeyRepo.Create(ctx, apiKey); err != nil {
		t.Fatalf("failed to create api key in postgres: %v", err)
	}

	// Verify key retrieval by hash
	fetchedKey, err := apiKeyRepo.GetByHash(ctx, keyHash)
	if err != nil || fetchedKey.TenantID != tenant.ID {
		t.Fatalf("failed to fetch key by hash: %v", err)
	}
	if !VerifyKey(rawKey, fetchedKey.KeyHash) {
		t.Error("raw key verification failed against fetched key hash")
	}

	// 4. Test Service Registration
	svc := &Service{
		TenantID: tenant.ID,
		Name:     "order-api",
	}
	if err := svcRepo.Create(ctx, svc); err != nil {
		t.Fatalf("failed to create service in postgres: %v", err)
	}

	// 5. Test Retention Policy Creation
	policy := &RetentionPolicy{
		TenantID:      tenant.ID,
		RetentionDays: 30,
		Enabled:       true,
	}
	if err := retRepo.Create(ctx, policy); err != nil {
		t.Fatalf("failed to create retention policy in postgres: %v", err)
	}

	// Cleanup test tenant (CASCADE deletes api_keys, services, retention_policies)
	_ = tenantRepo.Delete(ctx, tenant.ID)
}
