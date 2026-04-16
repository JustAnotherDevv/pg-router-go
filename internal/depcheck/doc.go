// Package depcheck is a test-only harness that keeps every direct module
// dependency referenced by the binary even if no production code imports
// it yet (e.g. during the early milestones). It is excluded from the
// production build by virtue of all files being _test.go.
package depcheck
