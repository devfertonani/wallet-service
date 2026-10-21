package worker

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx/fxtest"
)

func TestLoopIsRestartedAfterAPanic(t *testing.T) {
	lc := fxtest.NewLifecycle(t)
	var runs atomic.Int32
	Start(lc, slog.New(slog.NewTextHandler(io.Discard, nil)), "flaky", 1, func(ctx context.Context) {
		if runs.Add(1) == 1 {
			panic("bug")
		}
		<-ctx.Done()
	})
	lc.RequireStart()
	deadline := time.Now().Add(3 * time.Second)
	for runs.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("loop was not restarted after the panic")
		}
		time.Sleep(20 * time.Millisecond)
	}
	lc.RequireStop()
}
