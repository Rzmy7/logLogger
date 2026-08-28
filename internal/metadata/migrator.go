package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Migration represents a single versioned SQL migration.
type Migration struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

// Migrator manages transactional database schema migrations.
type Migrator struct {
	db *sql.DB
}

// NewMigrator creates a new Migrator instance.
func NewMigrator(db *sql.DB) *Migrator {
	return &Migrator{db: db}
}

// EnsureSchemaTable creates the schema_migrations table if it doesn't exist.
func (m *Migrator) EnsureSchemaTable(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS schema_migrations (
		version     BIGINT PRIMARY KEY,
		name        VARCHAR(255) NOT NULL,
		applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);`
	_, err := m.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}
	return nil
}

// AppliedVersions retrieves all currently applied migration versions.
func (m *Migrator) AppliedVersions(ctx context.Context) (map[int64]bool, error) {
	if err := m.EnsureSchemaTable(ctx); err != nil {
		return nil, err
	}

	rows, err := m.db.QueryContext(ctx, "SELECT version FROM schema_migrations ORDER BY version ASC")
	if err != nil {
		return nil, fmt.Errorf("failed to query schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]bool)
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	return applied, rows.Err()
}

// LoadMigrationsFromDir loads all .up.sql and .down.sql files from the specified directory.
func LoadMigrationsFromDir(dir string) ([]Migration, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory %q: %w", dir, err)
	}

	migrationMap := make(map[int64]*Migration)

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			continue
		}

		version, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}

		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %q: %w", name, err)
		}

		if migrationMap[version] == nil {
			migrationMap[version] = &Migration{
				Version: version,
				Name:    name,
			}
		}

		if strings.HasSuffix(name, ".up.sql") {
			migrationMap[version].UpSQL = string(content)
			migrationMap[version].Name = strings.TrimSuffix(name, ".up.sql")
		} else if strings.HasSuffix(name, ".down.sql") {
			migrationMap[version].DownSQL = string(content)
		}
	}

	var migrations []Migration
	for _, m := range migrationMap {
		migrations = append(migrations, *m)
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// Up runs all unapplied migrations in ascending version order.
func (m *Migrator) Up(ctx context.Context, migrations []Migration) error {
	applied, err := m.AppliedVersions(ctx)
	if err != nil {
		return err
	}

	for _, mig := range migrations {
		if applied[mig.Version] {
			continue
		}

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to start transaction for migration %d (%s): %w", mig.Version, mig.Name, err)
		}

		if _, err := tx.ExecContext(ctx, mig.UpSQL); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to execute migration %d (%s): %w", mig.Version, mig.Name, err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, $3)",
			mig.Version, mig.Name, time.Now().UTC())
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to record migration %d: %w", mig.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", mig.Version, err)
		}
	}

	return nil
}

// Down rolls back the last applied migration.
func (m *Migrator) Down(ctx context.Context, migrations []Migration) error {
	applied, err := m.AppliedVersions(ctx)
	if err != nil {
		return err
	}

	// Find the highest applied version
	var highestVersion int64 = -1
	var targetMig *Migration

	for i := len(migrations) - 1; i >= 0; i-- {
		mig := migrations[i]
		if applied[mig.Version] && mig.Version > highestVersion {
			highestVersion = mig.Version
			targetMig = &mig
		}
	}

	if targetMig == nil {
		return nil // Nothing to rollback
	}

	if targetMig.DownSQL == "" {
		return fmt.Errorf("migration %d has no down SQL", targetMig.Version)
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin rollback transaction for migration %d: %w", targetMig.Version, err)
	}

	if _, err := tx.ExecContext(ctx, targetMig.DownSQL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to execute rollback for migration %d: %w", targetMig.Version, err)
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version = $1", targetMig.Version); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to remove migration record %d: %w", targetMig.Version, err)
	}

	return tx.Commit()
}
