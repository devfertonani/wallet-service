// Package postgres owns the pgx connection pool and its lifecycle.
package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// NewPool parses the configuration without connecting. The first connection
// and the ping happen in OnStart, so a database that is down fails the start
// with a clear error, and the pool is closed in OnStop after every component
// that depends on it has stopped (Fx runs OnStop hooks in reverse order).
func NewPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DATABASE_URL: %w", err)
	}
	pc.MaxConns = cfg.DBMaxConns
	pc.ConnConfig.ConnectTimeout = cfg.DBConnectTimeout
	pc.ConnConfig.RuntimeParams["application_name"] = "wallet-" + cfg.InstanceID
	// Bounds the wait for a wallet row lock; 55P03 is classified as transient.
	pc.ConnConfig.RuntimeParams["lock_timeout"] = fmt.Sprintf("%dms", cfg.DBLockTimeout.Milliseconds())

	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("postgres: ping: %w", err)
			}
			log.Info("postgres connected", "maxConns", pc.MaxConns)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			// Close waits for connections to be released and says goodbye to
			// the server; with an unresponsive database that could block past
			// the stop budget, so it is bounded by the Fx stop context.
			done := make(chan struct{})
			go func() { pool.Close(); close(done) }()
			select {
			case <-done:
				log.Info("postgres pool closed")
			case <-ctx.Done():
				log.Warn("postgres pool did not close in time; exiting anyway")
			}
			return nil
		},
	})
	return pool, nil
}

var Module = fx.Module("postgres", fx.Provide(NewPool))
