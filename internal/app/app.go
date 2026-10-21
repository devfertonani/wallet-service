// Package app composes the process. It is the only package that knows every
// module; the domain and the use cases do not import Fx.
package app

import (
	"log/slog"

	"github.com/devfertonani/wallet-service/internal/adapters/httpapi"
	"github.com/devfertonani/wallet-service/internal/adapters/outbox"
	"github.com/devfertonani/wallet-service/internal/adapters/pending"
	"github.com/devfertonani/wallet-service/internal/adapters/pgstore"
	"github.com/devfertonani/wallet-service/internal/adapters/sqsconsumer"
	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/health"
	"github.com/devfertonani/wallet-service/internal/platform/auth"
	"github.com/devfertonani/wallet-service/internal/platform/awsclient"
	"github.com/devfertonani/wallet-service/internal/platform/httpserver"
	"github.com/devfertonani/wallet-service/internal/platform/logging"
	"github.com/devfertonani/wallet-service/internal/platform/metrics"
	"github.com/devfertonani/wallet-service/internal/platform/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// wageringModule binds the use cases to their adapters.
var wageringModule = fx.Module("wagering",
	fx.Provide(
		func(pool *pgxpool.Pool) wagering.UnitOfWork { return pgstore.NewUnitOfWork(pool) },
		func(m *metrics.Metrics) wagering.Recorder { return m },
		func(cfg config.Config, uow wagering.UnitOfWork, rec wagering.Recorder, log *slog.Logger) *wagering.Service {
			return wagering.NewService(uow, rec, log.With("component", "wagering"), wager.RetryPolicy{
				BaseDelay: cfg.PendingBaseDelay, MaxDelay: cfg.PendingMaxDelay, TTL: cfg.PendingTTL,
			})
		},
	),
)

// Options returns the full application graph. Tests call it with extra
// fx.Replace/fx.Decorate options to swap configuration.
//
// Order matters for shutdown: Fx stops components in the reverse order of
// their start. The HTTP server is invoked last, so it is the first to stop
// (readiness turns DRAINING, no new requests); then the SQS consumer, the
// pending reference worker and the outbox publisher finish their current work;
// then the AWS clients and the pool are released.
func Options(extra ...fx.Option) fx.Option {
	return fx.Options(
		fx.WithLogger(logging.FxLogger),
		config.Module,
		logging.Module,
		metrics.Module,
		postgres.Module,
		awsclient.Module,
		auth.Module,
		wageringModule,
		outbox.Module,
		pending.Module,
		sqsconsumer.Module,
		health.Module,
		httpapi.Module,
		httpserver.Module,
		fx.Options(extra...),
	)
}
