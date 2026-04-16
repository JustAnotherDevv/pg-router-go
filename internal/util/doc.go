// Package util holds small, dependency-free helpers shared across
// pgrouter packages: buffer pool, timer abstractions, simple token-bucket
// rate limiter.
//
// Anything that grows past ~200 LOC should move into its own package.
package util
