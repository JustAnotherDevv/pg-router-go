// Package pgrouter is the embedded-library entry point for Go
// services that want to run a PostgreSQL connection pooler in-process
// instead of as a sidecar binary.
//
// Typical use:
//
//	cfg, err := config.Load("pgrouter.yaml")
//	if err != nil { /* ... */ }
//	srv, err := pgrouter.New(cfg, slog.Default())
//	if err != nil { /* ... */ }
//	ctx, cancel := signal.NotifyContext(context.Background(),
//	    os.Interrupt, syscall.SIGTERM)
//	defer cancel()
//	if err := srv.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// Or, for finer control:
//
//	if err := srv.Start(ctx); err != nil { /* ... */ }
//	defer srv.Stop(30 * time.Second)
//
// The library variant exposes the same wire protocol on the
// configured listener — clients can't tell whether they're talking
// to the standalone `pgrouter` binary or an embedded instance.
//
// Internals (auth, pool, replica, stats, etc.) remain in `internal/`;
// this package is the only stable public surface.
package pgrouter
