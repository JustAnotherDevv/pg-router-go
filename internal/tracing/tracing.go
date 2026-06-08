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
	"log/slog"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation name used for all pgrouter spans.
const TracerName = "github.com/JustAnotherDevv/pg-router-go"

// Init wires the global TracerProvider when OTel env is present.
// Returns a shutdown closure (no-op if tracing was not configured).
//
// Honoured env (standard OTel):
//   OTEL_EXPORTER_OTLP_ENDPOINT       â€” e.g. http://collector:4318
//   OTEL_EXPORTER_OTLP_PROTOCOL       â€” currently only "http/protobuf" supported
//   OTEL_SERVICE_NAME                 â€” defaults to "pgrouter"
//   OTEL_TRACES_SAMPLER               â€” parentbased_always_on |
//                                       parentbased_traceidratio | always_on |
//                                       always_off | traceidratio
//                                       (default: parentbased_traceidratio)
//   OTEL_TRACES_SAMPLER_ARG           â€” float ratio for ratio samplers
//                                       (default: 0.01 = 1%)
//
// Empty endpoint â†’ no-op TracerProvider stays in place.
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
		sdktrace.WithSampler(samplerFromEnv()),
	)
	otel.SetTracerProvider(tp)
	enabled = true

	return tp.Shutdown, nil
}

// samplerFromEnv honours the standard OTEL_TRACES_SAMPLER /
// OTEL_TRACES_SAMPLER_ARG variables. Default: 1% ratio under a
// parent-based wrapper so an upstream service's sampling decision is
// respected. 100% sampling at 10k QPS overwhelms collectors â€” see
// SB7 review.
func samplerFromEnv() sdktrace.Sampler {
	ratio := 0.01
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			ratio = f
		} else {
			slog.Warn("OTEL_TRACES_SAMPLER_ARG ignored (not a float 0..1)", "value", v)
		}
	}
	switch os.Getenv("OTEL_TRACES_SAMPLER") {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio)
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	default: // "parentbased_traceidratio" or unset
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
}

// Tracer returns pgrouter's instrumentation Tracer. Always safe to
// call; pre-Init returns a no-op tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// Enabled reports whether OTel tracing is configured (i.e. an exporter
// endpoint is set). When false, callers can skip span creation entirely
// to avoid the attribute-build + Start() overhead on the hot path.
var enabled bool

// Enabled returns true when OTel tracing was successfully initialised
// with a real exporter. Returns false in the no-op default case.
func Enabled() bool { return enabled }

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
