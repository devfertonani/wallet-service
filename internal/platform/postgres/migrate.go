package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/devfertonani/wallet-service/migrations"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// NewMigrator returns a golang-migrate instance over the embedded migrations.
// databaseURL is a regular postgres:// URL; it must belong to the schema owner,
// not to the application role.
func NewMigrator(databaseURL string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	// The pgx/v5 driver registers the "pgx5" scheme.
	u := databaseURL
	for _, p := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(u, p) {
			u = "pgx5://" + strings.TrimPrefix(u, p)
			break
		}
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, u)
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return m, nil
}

// MigrateUp applies every pending migration. No change is not an error.
func MigrateUp(databaseURL string) error {
	m, err := NewMigrator(databaseURL)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}
