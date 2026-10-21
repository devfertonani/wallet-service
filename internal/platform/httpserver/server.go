// Package httpserver runs the HTTP listener under the Fx lifecycle.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/devfertonani/wallet-service/internal/config"
	"go.uber.org/fx"
)

// Route is contributed by feature modules through the "routes" value group,
// so the server does not import the handlers it serves.
type Route struct {
	Pattern string // Go 1.22+ ServeMux pattern, e.g. "GET /wallets/{walletId}"
	Handler http.Handler
}

func AsRoute(f any) any {
	return fx.Annotate(f, fx.ResultTags(`group:"routes"`))
}

// Draining flips to true when shutdown starts; readiness reads it so a load
// balancer stops routing before the listener closes.
type Draining struct{ v atomic.Bool }

func (d *Draining) Set()         { d.v.Store(true) }
func (d *Draining) Active() bool { return d.v.Load() }

type Params struct {
	fx.In
	Lifecycle fx.Lifecycle
	Config    config.Config
	Log       *slog.Logger
	Draining  *Draining
	Routes    []Route `group:"routes"`
}

func New(p Params) (*http.Server, error) {
	mux := http.NewServeMux()
	for _, r := range p.Routes {
		mux.Handle(r.Pattern, r.Handler)
	}
	srv := &http.Server{
		Addr:              p.Config.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	done := make(chan struct{})
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				return fmt.Errorf("http: listen %s: %w", srv.Addr, err)
			}
			go func() {
				defer close(done)
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					p.Log.Error("http server stopped", "error", err)
				}
			}()
			p.Log.Info("http server listening", "addr", ln.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			p.Draining.Set()
			// Shutdown stops accepting connections and waits for in-flight
			// requests until ctx (the Fx stop timeout) expires.
			err := srv.Shutdown(ctx)
			<-done
			p.Log.Info("http server stopped", "error", err)
			return err
		},
	})
	return srv, nil
}

var Module = fx.Module("http",
	fx.Provide(New, func() *Draining { return &Draining{} }),
	fx.Invoke(func(*http.Server) {}),
)
