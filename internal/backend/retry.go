// Capped exponential-backoff wrapper around Dial. Spec M.7.6:
// handshake auth failures + transient TLS/network errors shouldn't kill
// the next Acquire instantly — back off so a dying backend doesn't get
// hammered. Cap at 6 attempts so wrong creds give up in seconds rather
// than minutes.
//
// Shared by cmd/pgrouter (CLI mode) and pkg/pgrouter (library mode).
// Before extraction, library mode had NO retry — a backend auth failure
// would have it hammer PG at the client's Acquire rate.

package backend

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// DialRetryConfig tunes the retry loop. Zero values produce the
// canonical M.7.6 schedule: 6 attempts, 100ms → 30s capped backoff.
type DialRetryConfig struct {
	MaxAttempts    int           // default 6
	InitialBackoff time.Duration // default 100ms
	MaxBackoff     time.Duration // default 30s
}

func (c DialRetryConfig) withDefaults() DialRetryConfig {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 6
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 100 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	return c
}

// DialWithRetry wraps a single-shot dial function with capped
// exponential backoff. Honors ctx — a cancelled ctx during sleep
// returns ctx.Err() immediately, not "gave up after N attempts".
//
// addr is only used in log lines; the dial closure itself knows what
// to connect to.
func DialWithRetry(
	ctx context.Context,
	addr string,
	log *slog.Logger,
	cfg DialRetryConfig,
	dialOnce func(ctx context.Context) (*Conn, error),
) (*Conn, error) {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	backoff := cfg.InitialBackoff
	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		c, err := dialOnce(ctx)
		if err == nil {
			if attempt > 1 {
				log.Info("backend dial succeeded after retry",
					"attempts", attempt, "addr", addr)
			}
			return c, nil
		}
		lastErr = err
		if attempt == cfg.MaxAttempts {
			break
		}
		log.Warn("backend dial failed; backing off",
			"attempt", attempt, "max", cfg.MaxAttempts,
			"backoff", backoff, "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		backoff *= 2
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}
	return nil, fmt.Errorf("backend dial gave up after %d attempts: %w",
		cfg.MaxAttempts, lastErr)
}
