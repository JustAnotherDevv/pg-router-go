// Package testutil collects shared helpers for pgrouter's unit tests.
//
// Nothing here should be imported by production code. Helpers take
// *testing.T (or are used from _test.go files only).
package testutil

import "log/slog"

// Discard is the canonical shared no-op logger. ~135 tests previously
// wrote `slog.New(slog.DiscardHandler)` inline — use this instead.
var Discard = slog.New(slog.DiscardHandler)
