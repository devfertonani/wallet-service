// Package pending runs the worker that resumes PENDING_REFERENCE operations.
// Every instance runs it; the state lives in the database, so pending
// operations survive a restart of every instance.
package pending

import (
	"context"
	"log/slog"
	"time"

	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/platform/worker"
	"go.uber.org/fx"
)

// BatchSize is how many due operations one cycle looks at.
const BatchSize = 50

type Worker struct{}

func Register(lc fx.Lifecycle, cfg config.Config, svc *wagering.Service, log *slog.Logger) *Worker {
	log = log.With("component", "pending-reference-worker")
	worker.Start(lc, log, "pending-reference", 1, func(ctx context.Context) {
		for ctx.Err() == nil {
			cycle, cancel := context.WithTimeout(ctx, 30*time.Second)
			n, err := svc.ResumePending(cycle, BatchSize)
			cancel()
			if err != nil && ctx.Err() == nil {
				log.Warn("pending reference cycle failed", "error", err)
			}
			if n < BatchSize && !worker.Sleep(ctx, cfg.PendingPollInterval) {
				return
			}
		}
	})
	return &Worker{}
}

var Module = fx.Module("pending", fx.Provide(Register), fx.Invoke(func(*Worker) {}))
