# Multi-stage build: small distroless image for pgrouter.
#
# Two stages:
#
#   builder   — golang:1.26-alpine, fetches deps and produces a fully
#               static linux/amd64 binary (CGO_ENABLED=0).
#   runtime   — gcr.io/distroless/static-debian12:nonroot — no shell,
#               no package manager, runs as uid:gid 65532:65532.
#
# Build:
#   docker build -t pgrouter:dev -f deploy/Dockerfile .
#
# Run with a mounted config:
#   docker run --rm -p 6432:6432 -p 9090:9090 \
#       -v $(pwd)/pgrouter.yaml:/etc/pgrouter/pgrouter.yaml:ro \
#       pgrouter:dev run --config /etc/pgrouter/pgrouter.yaml
#
# Build args:
#   VERSION    — stamped into the binary (`pgrouter version`). default: dev
#   COMMIT     — short git SHA. default: unknown
#   GOPROXY    — if you mirror modules
#
# Healthcheck: the runtime image has no shell, so we use the built-in
# pgrouter binary in a `validate` no-op mode — the binary itself
# returning exit 0 is the liveness signal.

# ──────────────────────────────────────────────────────────────────
# Stage 1: builder
# ──────────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

ARG VERSION=dev
ARG COMMIT=unknown
ARG GOPROXY=https://proxy.golang.org,direct
ARG TARGETOS=linux
ARG TARGETARCH=amd64

ENV CGO_ENABLED=0 \
    GO111MODULE=on \
    GOPROXY=${GOPROXY} \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

WORKDIR /src

# Module cache layer: copy only go.{mod,sum} so this layer is reused
# across source-only changes.
COPY go.mod go.sum ./
RUN go mod download

# Source.
COPY cmd/   ./cmd/
COPY internal/ ./internal/

# Strip debug info + DWARF (smaller binary), stamp version + commit.
RUN go build \
        -trimpath \
        -ldflags "-s -w \
                  -X main.version=${VERSION} \
                  -X main.commit=${COMMIT}" \
        -o /out/pgrouter \
        ./cmd/pgrouter

# ──────────────────────────────────────────────────────────────────
# Stage 2: runtime
# ──────────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

# OCI labels (image inspect / registry UI).
LABEL org.opencontainers.image.title="pgrouter" \
      org.opencontainers.image.description="Modern PostgreSQL connection pooler" \
      org.opencontainers.image.source="https://github.com/JustAnotherDevv/pgrouter" \
      org.opencontainers.image.licenses="Apache-2.0"

# distroless/static-debian12:nonroot already provides uid:gid 65532:65532.

COPY --from=builder /out/pgrouter /usr/local/bin/pgrouter

# Expose: 6432 = pgwire, 9090 = Prometheus + /healthz.
EXPOSE 6432 9090

# distroless has no shell so HEALTHCHECK CMD must use direct exec form.
# `pgrouter version` returns 0 on success — a cheap liveness probe.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/pgrouter", "version"]

ENTRYPOINT ["/usr/local/bin/pgrouter"]
CMD ["run", "--config", "/etc/pgrouter/pgrouter.yaml"]
