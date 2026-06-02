// Package listener accepts TCP and Unix-domain socket client connections,
// optionally parses HAProxy PROXY protocol headers, and dispatches each
// connection to a handler goroutine.
package listener
