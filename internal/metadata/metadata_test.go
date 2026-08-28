package metadata

import (
	"context"
	"testing"
	"time"
)

func TestAPIKey_GenerationAndVerification(t *testing.T) {
	rawKey, keyHash, err := GenerateAPIKey("ll_live")
	if err != nil {
		t.Fatalf("failed to generate API key: %v", err)
	}

	if rawKey == "" || keyHash == "" {
		t.Fatal("expected non-empty rawKey and keyHash")
	}

	// 1. Verify valid key
	if !VerifyKey(rawKey, keyHash) {
		t.Error("expected VerifyKey to return true for valid key and matching hash")
	}

	// 2. Verify invalid key
	if VerifyKey("wrong_key", keyHash) {
		t.Error("expected VerifyKey to return false for incorrect key")
	}

	// 3. Different generation produces different keys and hashes
	rawKey2, keyHash2, err := GenerateAPIKey("ll_live")
	if err != nil {
		t.Fatalf("failed to generate second API key: %v", err)
	}
	if rawKey == rawKey2 {
		t.Error("expected randomly generated keys to differ")
	}
	if keyHash == keyHash2 {
		t.Error("expected different hashes for different keys")
	}
}

func TestAPIKey_Validity(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(1 * time.Hour)
	past := now.Add(-1 * time.Hour)

	activeKey := &APIKey{
		Status:    "active",
		ExpiresAt: &future,
	}
	if !activeKey.IsValid() {
		t.Error("expected active key with future expiration to be valid")
	}

	expiredKey := &APIKey{
		Status:    "active",
		ExpiresAt: &past,
	}
	if expiredKey.IsValid() {
		t.Error("expected expired key to be invalid")
	}

	revokedKey := &APIKey{
		Status: "revoked",
	}
	if revokedKey.IsValid() {
		t.Error("expected revoked key to be invalid")
	}
}

func TestMockTenantRepository(t *testing.T) {
	repo := NewMockTenantRepo()
	ctx := context.Background()

	tenant := &Tenant{
		Name: "Acme Corp",
		Slug: "acme-corp",
	}

	// 1. Create
	if err := repo.Create(ctx, tenant); err != nil {
		t.Fatalf("unexpected error creating tenant: %v", err)
	}
	if tenant.ID == "" {
		t.Fatal("expected tenant ID to be set")
	}

	// 2. Duplicate Slug Rejection
	dup := &Tenant{
		Name: "Acme Duplicate",
		Slug: "acme-corp",
	}
	if err := repo.Create(ctx, dup); err != ErrTenantSlugExists {
		t.Fatalf("expected ErrTenantSlugExists, got %v", err)
	}

	// 3. GetByID
	fetched, err := repo.GetByID(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("unexpected error getting tenant by ID: %v", err)
	}
	if fetched.Slug != "acme-corp" {
		t.Errorf("expected slug 'acme-corp', got %s", fetched.Slug)
	}

	// 4. GetBySlug
	fetchedSlug, err := repo.GetBySlug(ctx, "acme-corp")
	if err != nil {
		t.Fatalf("unexpected error getting tenant by slug: %v", err)
	}
	if fetchedSlug.ID != tenant.ID {
		t.Errorf("expected ID %s, got %s", tenant.ID, fetchedSlug.ID)
	}

	// 5. Update
	tenant.Name = "Acme Global"
	if err := repo.Update(ctx, tenant); err != nil {
		t.Fatalf("unexpected error updating tenant: %v", err)
	}

	// 6. List
	list, err := repo.List(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 tenant in list, got %d (err: %v)", len(list), err)
	}

	// 7. Delete
	if err := repo.Delete(ctx, tenant.ID); err != nil {
		t.Fatalf("unexpected error deleting tenant: %v", err)
	}
	if _, err := repo.GetByID(ctx, tenant.ID); err != ErrTenantNotFound {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
}

func TestMockServiceRepository(t *testing.T) {
	repo := NewMockServiceRepo()
	ctx := context.Background()

	svc := &Service{
		TenantID: "tenant-1",
		Name:     "payment-api",
	}

	if err := repo.Create(ctx, svc); err != nil {
		t.Fatalf("unexpected error creating service: %v", err)
	}

	// Duplicate service name for same tenant -> Error
	dup := &Service{
		TenantID: "tenant-1",
		Name:     "payment-api",
	}
	if err := repo.Create(ctx, dup); err != ErrServiceNameExists {
		t.Fatalf("expected ErrServiceNameExists, got %v", err)
	}

	// Same service name for different tenant -> OK
	otherTenantSvc := &Service{
		TenantID: "tenant-2",
		Name:     "payment-api",
	}
	if err := repo.Create(ctx, otherTenantSvc); err != nil {
		t.Fatalf("unexpected error creating service for other tenant: %v", err)
	}

	fetched, err := repo.GetByName(ctx, "tenant-1", "payment-api")
	if err != nil || fetched.ID != svc.ID {
		t.Fatalf("expected to fetch service, got %v (err: %v)", fetched, err)
	}

	list, err := repo.ListByTenant(ctx, "tenant-1")
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 service for tenant-1, got %d", len(list))
	}
}

func TestMockRetentionPolicyRepository(t *testing.T) {
	repo := NewMockRetentionPolicyRepo()
	ctx := context.Background()

	policy := &RetentionPolicy{
		TenantID:      "tenant-1",
		RetentionDays: 45,
		Enabled:       true,
	}

	if err := repo.Create(ctx, policy); err != nil {
		t.Fatalf("unexpected error creating retention policy: %v", err)
	}

	fetched, err := repo.GetByTenant(ctx, "tenant-1")
	if err != nil || fetched.RetentionDays != 45 {
		t.Fatalf("expected retention 45 days, got %v (err: %v)", fetched, err)
	}

	policy.RetentionDays = 60
	if err := repo.Update(ctx, policy); err != nil {
		t.Fatalf("unexpected error updating retention policy: %v", err)
	}

	fetchedUpdated, err := repo.GetByTenant(ctx, "tenant-1")
	if err != nil || fetchedUpdated.RetentionDays != 60 {
		t.Fatalf("expected updated retention 60 days, got %v", fetchedUpdated)
	}
}
