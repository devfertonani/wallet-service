// Command wallet runs the HTTP API, the SQS consumer and the background
// workers of one instance. Several instances can run against the same
// PostgreSQL and SQS; coordination happens in the database.
package main

import (
	"time"

	"github.com/devfertonani/wallet-service/internal/app"
	"github.com/devfertonani/wallet-service/internal/config"
	"go.uber.org/fx"
)

func main() {
	stop := 30 * time.Second
	if c, err := config.Load(); err == nil {
		// Leave Fx a margin over the per-component shutdown budget.
		stop = c.ShutdownTimeout + 5*time.Second
	}
	fx.New(
		app.Options(),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(stop),
	).Run() // Run blocks until SIGINT/SIGTERM, then runs the OnStop hooks.
}
