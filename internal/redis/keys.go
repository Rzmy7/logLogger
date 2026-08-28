package redis

import (
	"fmt"
	"strings"
	"time"

	"github.com/Rzmy7/logLogger/internal/models"
)

// KeyBuilder provides centralized, tenant-aware Redis key generation.
// For the "default" tenant, it retains the canonical key format for 100% backward compatibility,
// while prefixing other tenants with "tenant:{tenant_id}:" for complete data isolation.
type KeyBuilder struct {
	tenantID string
}

// NewKeyBuilder creates a KeyBuilder scoped to a tenant.
func NewKeyBuilder(tenantID string) *KeyBuilder {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = models.DefaultTenantID
	}
	return &KeyBuilder{tenantID: tenantID}
}

// Prefix returns the key prefix.
func (k *KeyBuilder) Prefix() string {
	if k.tenantID == models.DefaultTenantID || k.tenantID == "" {
		return ""
	}
	return fmt.Sprintf("tenant:%s:", k.tenantID)
}

// TenantID returns the configured tenant ID.
func (k *KeyBuilder) TenantID() string {
	return k.tenantID
}

// StatsLogsTotal returns key for total logs counter.
func (k *KeyBuilder) StatsLogsTotal() string {
	return k.Prefix() + "stats:logs:total"
}

// StatsLogsService returns key for total logs per service counter.
func (k *KeyBuilder) StatsLogsService(service string) string {
	return k.Prefix() + fmt.Sprintf("stats:logs:%s", service)
}

// StatsLogsLevel returns key for total logs per level counter.
func (k *KeyBuilder) StatsLogsLevel(level string) string {
	return k.Prefix() + fmt.Sprintf("stats:logs:level:%s", strings.ToLower(level))
}

// LeaderboardServices returns key for service volume sorted set.
func (k *KeyBuilder) LeaderboardServices() string {
	return k.Prefix() + "leaderboard:services"
}

// LeaderboardErrors returns key for top error messages sorted set.
func (k *KeyBuilder) LeaderboardErrors() string {
	return k.Prefix() + "leaderboard:errors"
}

// StatsErrorsService returns key for total errors per service counter.
func (k *KeyBuilder) StatsErrorsService(service string) string {
	return k.Prefix() + fmt.Sprintf("stats:errors:%s", service)
}

// StatsErrorsLast5m returns key for 5-minute sliding window error counter.
func (k *KeyBuilder) StatsErrorsLast5m(service string) string {
	return k.Prefix() + fmt.Sprintf("stats:errors:last_5m:%s", service)
}

// RecentErrors returns key for recent errors list.
func (k *KeyBuilder) RecentErrors(service string) string {
	return k.Prefix() + fmt.Sprintf("recent:errors:%s", service)
}

// UniqueIPs returns key for daily unique IPs set.
func (k *KeyBuilder) UniqueIPs(t time.Time) string {
	return k.Prefix() + fmt.Sprintf("unique:ips:%s", t.UTC().Format("2006-01-02"))
}
