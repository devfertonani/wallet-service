// Package pgtest creates throwaway PostgreSQL databases for integration tests,
// against the real Postgres started by docker compose. Each call gets its own
// database with every migration applied, so tests never share state.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/devfertonani/wallet-service/internal/platform/postgres"
	"github.com/devfertonani/wallet-service/internal/testsupport/dotenv"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ownerUser = "wallet_owner:wallet_owner"
	appUser   = "wallet_service:wallet_service"
)

type DB struct {
	Name     string
	OwnerURL string // schema owner, runs migrations
	AppURL   string // service role, the one the application uses
	App      *pgxpool.Pool
	Owner    *pgxpool.Pool
}

// New creates a database, applies the migrations and returns pools for the
// owner and for the application role. The database is dropped on cleanup.
func New(t testing.TB) *DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dotenv.Load()
	adminURL := os.Getenv("TEST_ADMIN_DATABASE_URL")
	if adminURL == "" {
		adminURL = "postgres://postgres:postgres@localhost:" + dotenv.HostPort() + "/postgres?sslmode=disable"
	}
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgtest: connect admin (is `docker compose up -d postgres` running?): %v", err)
	}
	defer admin.Close(ctx)

	name := "wallet_test_" + strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	for _, q := range []string{
		fmt.Sprintf(`CREATE DATABASE %s OWNER wallet_owner`, name),
		fmt.Sprintf(`REVOKE ALL ON DATABASE %s FROM PUBLIC`, name),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO wallet_app`, name),
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("pgtest: %s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			t.Logf("pgtest: drop %s: %v", name, err)
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
	})

	db := &DB{Name: name, OwnerURL: withUserAndDB(adminURL, ownerUser, name), AppURL: withUserAndDB(adminURL, appUser, name)}
	if err := postgres.MigrateUp(db.OwnerURL); err != nil {
		t.Fatalf("pgtest: migrate: %v", err)
	}
	if db.Owner, err = pgxpool.New(ctx, db.OwnerURL); err != nil {
		t.Fatal(err)
	}
	if db.App, err = pgxpool.New(ctx, db.AppURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.App.Close(); db.Owner.Close() })
	return db
}

func withUserAndDB(base, userinfo, dbName string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	user, pass, _ := strings.Cut(userinfo, ":")
	u.User = url.UserPassword(user, pass)
	u.Path = "/" + dbName
	return u.String()
}
