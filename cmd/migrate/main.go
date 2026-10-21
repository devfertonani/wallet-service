// Command migrate applies or reverts the embedded migrations.
//
//	migrate up          apply every pending migration
//	migrate down 1      revert the last N migrations
//	migrate version     print the current version
//
// It reads MIGRATIONS_DATABASE_URL, which must belong to the schema owner.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/devfertonani/wallet-service/internal/platform/postgres"
	"github.com/golang-migrate/migrate/v4"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	url := os.Getenv("MIGRATIONS_DATABASE_URL")
	if url == "" {
		return errors.New("MIGRATIONS_DATABASE_URL is required")
	}
	if len(args) == 0 {
		return errors.New("usage: migrate up | down N | version")
	}
	m, err := postgres.NewMigrator(url)
	if err != nil {
		return err
	}
	defer m.Close()

	switch args[0] {
	case "up":
		err = m.Up()
	case "down":
		if len(args) != 2 {
			return errors.New("usage: migrate down N (N is required, to avoid reverting everything by accident)")
		}
		n, convErr := strconv.Atoi(args[1])
		if convErr != nil || n <= 0 {
			return fmt.Errorf("invalid N %q", args[1])
		}
		err = m.Steps(-n)
	case "version":
		v, dirty, verr := m.Version()
		if errors.Is(verr, migrate.ErrNilVersion) {
			fmt.Println("no migration applied")
			return nil
		}
		if verr != nil {
			return verr
		}
		fmt.Printf("version %d dirty=%t\n", v, dirty)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if errors.Is(err, migrate.ErrNoChange) {
		fmt.Println("no change")
		return nil
	}
	if err != nil {
		return err
	}
	v, _, _ := m.Version()
	fmt.Printf("ok, version %d\n", v)
	return nil
}
