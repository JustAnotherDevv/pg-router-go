// HTTP `/metrics` endpoint serving the Prometheus text format.

package stats

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServeMetrics runs an HTTP server on `addr` exposing `path` (typically
// `/metrics`) until ctx is cancelled. Returns the first error
// encountered (or nil on clean shutdown).
//
// Production main calls this in a goroutine alongside the listener.
func ServeMetrics(ctx context.Context, addr, path string, log *slog.Logger) error {
	if addr == "" {
		addr = ":9090"
	}
	if path == "" {
		path = "/metrics"
	}
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(Reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("metrics server listening", "addr", addr, "path", path)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}
