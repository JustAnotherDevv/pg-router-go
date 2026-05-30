// OpenTelemetry tracing setup for pgrouter.
//
// Optional: only initialised when OTEL_EXPORTER_OTLP_ENDPOINT (or one
// of the standard OTel env vars) is set. Otherwise Tracer() returns a
// no-op tracer and spans cost nothing.
//
// Per-query spans are emitted from PooledConn.Serve (Tracer is
// threaded via PooledHandler). Span attributes follow the
// `pg.statement` / `db.system` semantic conventions where reasonable.

package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation name used for all pgrouter spans.
const TracerName = "github.com/JustAnotherDevv/pgrouter"

// Init wires the global TracerProvider when OTel env is present.
// Returns a shutdown closure (no-op if tracing was not configured).
//
// Honoured env (standard OTel):
//   OTEL_EXPORTER_OTLP_ENDPOINT       — e.g. http://collector:4318
//   OTEL_EXPORTER_OTLP_PROTOCOL       — currently only "http/protobuf" supported
//   OTEL_SERVICE_NAME                 — defaults to "pgrouter"
//
// Empty endpoint → no-op TracerProvider stays in place.
func Init(ctx context.Context, version, commit string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		// No-op shutdown.
		return func(context.Context) error { return nil }, nil
	}
	svc := os.Getenv("OTEL_SERVICE_NAME")
	if svc == "" {
		svc = "pgrouter"
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(stripScheme(endpoint)),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp http exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(svc),
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Tracer returns pgrouter's instrumentation Tracer. Always safe to
// call; pre-Init returns a no-op tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// stripScheme removes leading "http://" / "https://" because
// otlptracehttp wants just "host:port".
func stripScheme(s string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(s) > len(p) && s[:len(p)] == p {
			return s[len(p):]
		}
	}
	return s
}
