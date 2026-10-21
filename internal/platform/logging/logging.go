// Package logging provides the JSON slog logger and the Fx event logger.
package logging

import (
	"log/slog"
	"os"

	"github.com/devfertonani/wallet-service/internal/config"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func New(cfg config.Config) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		level = slog.LevelInfo // debug, info, warn and error are accepted
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", "wallet", "instanceId", cfg.InstanceID)
}

// FxLogger routes Fx lifecycle events to the JSON logger. It must be applied
// at the root of the graph: fx.WithLogger inside a module only covers it.
func FxLogger(l *slog.Logger) fxevent.Logger {
	return &fxevent.SlogLogger{Logger: l.With("component", "fx")}
}

var Module = fx.Module("logging", fx.Provide(New))
