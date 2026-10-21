// Package worker runs background loops under the Fx lifecycle with an
// observable end: OnStop cancels the loop's context and waits for it to
// return, so the resources it uses (the pool, the AWS clients) are closed only
// after the loop has finished.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"go.uber.org/fx"
)

// Loop is the body of a worker. It must return when ctx is cancelled.
type Loop func(ctx context.Context)

// Start registers n goroutines running loop between OnStart and OnStop.
func Start(lc fx.Lifecycle, log *slog.Logger, name string, n int, loop Loop) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					// A panic is a bug, but a dead publisher or consumer would
					// silently stop the service: log it and restart the loop.
					for ctx.Err() == nil {
						if !runRecovered(ctx, log, name, loop) {
							return
						}
						log.Error("worker restarting in 1s after a panic", "worker", name, "index", i)
						Sleep(ctx, time.Second)
					}
				}()
			}
			log.Info("worker started", "worker", name, "goroutines", n)
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			cancel()
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				log.Info("worker stopped", "worker", name)
				return nil
			case <-stopCtx.Done():
				log.Error("worker did not stop in time", "worker", name)
				return fmt.Errorf("worker %s: %w", name, stopCtx.Err())
			}
		},
	})
}

// runRecovered runs loop and reports whether it ended by panicking.
func runRecovered(ctx context.Context, log *slog.Logger, name string, loop Loop) (panicked bool) {
	defer func() {
		if v := recover(); v != nil {
			panicked = true
			log.Error("worker panic", "worker", name, "panic", fmt.Sprint(v), "stack", string(debug.Stack()))
		}
	}()
	loop(ctx)
	return false
}

// Sleep waits for d or until ctx is cancelled; it reports whether to go on.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
