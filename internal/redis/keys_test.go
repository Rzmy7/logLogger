package redis

import (
	"testing"
	"time"
)

func TestKeyBuilder_TenantIsolation(t *testing.T) {
	defaultKB := NewKeyBuilder("default")
	tenantAKB := NewKeyBuilder("tenant-a")
	tenantBKB := NewKeyBuilder("tenant-b")

	// 1. Total logs counter
	if defaultKB.StatsLogsTotal() != "stats:logs:total" {
		t.Errorf("expected default key 'stats:logs:total', got %s", defaultKB.StatsLogsTotal())
	}
	if tenantAKB.StatsLogsTotal() != "tenant:tenant-a:stats:logs:total" {
		t.Errorf("expected tenant-a key 'tenant:tenant-a:stats:logs:total', got %s", tenantAKB.StatsLogsTotal())
	}
	if tenantBKB.StatsLogsTotal() != "tenant:tenant-b:stats:logs:total" {
		t.Errorf("expected tenant-b key 'tenant:tenant-b:stats:logs:total', got %s", tenantBKB.StatsLogsTotal())
	}

	// 2. Service counter
	if defaultKB.StatsLogsService("payment-api") != "stats:logs:payment-api" {
		t.Errorf("expected default key 'stats:logs:payment-api', got %s", defaultKB.StatsLogsService("payment-api"))
	}
	if tenantAKB.StatsLogsService("payment-api") != "tenant:tenant-a:stats:logs:payment-api" {
		t.Errorf("expected tenant-a key 'tenant:tenant-a:stats:logs:payment-api', got %s", tenantAKB.StatsLogsService("payment-api"))
	}

	// 3. Leaderboard keys
	if defaultKB.LeaderboardServices() != "leaderboard:services" {
		t.Errorf("expected default leaderboard 'leaderboard:services', got %s", defaultKB.LeaderboardServices())
	}
	if tenantAKB.LeaderboardServices() != "tenant:tenant-a:leaderboard:services" {
		t.Errorf("expected tenant-a leaderboard 'tenant:tenant-a:leaderboard:services', got %s", tenantAKB.LeaderboardServices())
	}

	if defaultKB.LeaderboardErrors() != "leaderboard:errors" {
		t.Errorf("expected default error leaderboard 'leaderboard:errors', got %s", defaultKB.LeaderboardErrors())
	}
	if tenantAKB.LeaderboardErrors() != "tenant:tenant-a:leaderboard:errors" {
		t.Errorf("expected tenant-a error leaderboard 'tenant:tenant-a:leaderboard:errors', got %s", tenantAKB.LeaderboardErrors())
	}

	// 4. Sliding window errors
	if tenantAKB.StatsErrorsLast5m("order-svc") != "tenant:tenant-a:stats:errors:last_5m:order-svc" {
		t.Errorf("expected tenant-a 5m error key 'tenant:tenant-a:stats:errors:last_5m:order-svc', got %s", tenantAKB.StatsErrorsLast5m("order-svc"))
	}

	// 5. Unique IPs
	fixedDate := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	if defaultKB.UniqueIPs(fixedDate) != "unique:ips:2026-08-28" {
		t.Errorf("expected 'unique:ips:2026-08-28', got %s", defaultKB.UniqueIPs(fixedDate))
	}
	if tenantAKB.UniqueIPs(fixedDate) != "tenant:tenant-a:unique:ips:2026-08-28" {
		t.Errorf("expected 'tenant:tenant-a:unique:ips:2026-08-28', got %s", tenantAKB.UniqueIPs(fixedDate))
	}
}
