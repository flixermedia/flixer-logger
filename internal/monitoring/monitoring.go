// Package monitoring reports operational failures without exporting log content.
package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

type config struct {
	Enabled *bool   `json:"enabled"`
	DSN     *string `json:"dsn"`
}

// Load waits for the API without blocking logger startup. Settings are applied
// once, like the other Flixer services, and changes take effect after restart.
func Load(ctx context.Context, baseURL, environment string, client *http.Client, interval time.Duration) {
	endpoint := strings.TrimRight(baseURL, "/") + "/api/sentry/config/logger"
	for {
		cfg, err := fetch(ctx, client, endpoint)
		if err == nil {
			dsn := ""
			if *cfg.Enabled {
				dsn = strings.TrimSpace(*cfg.DSN)
			}
			enabled, startErr := Start(dsn, environment)
			if startErr != nil {
				slog.Error("Could not initialize Sentry; error reporting is disabled.")
			} else {
				slog.Info("Sentry configuration loaded", "enabled", enabled)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		slog.Warn("Could not load Sentry configuration from Flixer; retrying with reporting disabled")
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func fetch(ctx context.Context, client *http.Client, endpoint string) (config, error) {
	var cfg config
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return cfg, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return cfg, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cfg, fmt.Errorf("Flixer returned status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&cfg); err != nil {
		return cfg, err
	}
	if cfg.Enabled == nil || (*cfg.Enabled && (cfg.DSN == nil || strings.TrimSpace(*cfg.DSN) == "")) {
		return cfg, errors.New("invalid Sentry configuration")
	}
	return cfg, nil
}

// Start enables reporting only when a DSN is supplied.
func Start(dsn, environment string) (bool, error) {
	if strings.TrimSpace(dsn) == "" {
		return false, nil
	}
	if environment == "" {
		environment = "production"
	}
	err := sentry.Init(sentry.ClientOptions{
		Dsn: dsn, Environment: environment, AttachStacktrace: false,
		BeforeSend: sanitizeEvent,
	})
	return err == nil, err
}

func sanitizeEvent(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
	return &sentry.Event{
		EventID: event.EventID, Timestamp: event.Timestamp, Platform: event.Platform,
		Level: sentry.LevelError, Environment: event.Environment,
		Message: "Flixer logger internal error",
	}
}

func Flush() { sentry.Flush(2 * time.Second) }

// Recover reports a process panic and preserves normal crash behavior.
func Recover() {
	if recovered := recover(); recovered != nil {
		sentry.CaptureMessage("Flixer logger internal error")
		Flush()
		panic(recovered)
	}
}

// Middleware reports a handler panic without sending the request or panic value.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				sentry.CaptureMessage("Flixer logger internal error")
				panic(recovered)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// WrapLogger preserves normal logging and reports error-level failures without their content.
func WrapLogger(logger *slog.Logger) *slog.Logger {
	return slog.New(&handler{Handler: logger.Handler()})
}

type handler struct{ slog.Handler }

func (h *handler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level >= slog.LevelError {
		sentry.CaptureMessage("Flixer logger internal error")
	}
	return h.Handler.Handle(ctx, record)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &handler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{Handler: h.Handler.WithGroup(name)}
}
