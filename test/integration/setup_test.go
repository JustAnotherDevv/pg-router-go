// Integration test harness.
//
// TestMain:
//   1. Reads PG_ADMIN_DSN (default postgres://postgres:postgres@127.0.0.1:25433/postgres).
//   2. Pings it. If unreachable → skip all tests (so `go test` without
//      docker compose up doesn't fail loudly).
//   3. Builds pgrouter from ./cmd/pgrouter into a temp dir.
//   4. Writes a pgrouter.yaml routing `appdb` -> the docker PG.
//   5. Spawns pgrouter on a free port. Waits for it to accept.
//   6. Sets PGROUTER_DSN to that port.
//   7. m.Run().
//   8. Tears down (SIGTERM + best-effort wait).
//
// Tests use Dsn() to get the pgrouter address. Driver-specific tests
// switch the host param to talk through pgrouter.

//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/lib/pq" // database/sql driver registration (TestMain ping)
)

var (
	adminDSN    string
	pgrouterDSN string
	pgrouterCmd *exec.Cmd
)

func TestMain(m *testing.M) {
	adminDSN = envOr("PG_ADMIN_DSN",
		"postgres://postgres:postgres@127.0.0.1:25433/postgres?sslmode=disable")

	if err := pingAdmin(adminDSN); err != nil {
		fmt.Printf("[skip] no admin Postgres reachable at %s: %v\n",
			redactDSN(adminDSN), err)
		// exit 0 — go test treats it as "no tests"; CI knob can flip this.
		os.Exit(0)
	}

	bin, cleanup, err := buildPgrouter()
	if err != nil {
		fmt.Printf("[fatal] build pgrouter: %v\n", err)
		os.Exit(2)
	}
	defer cleanup()

	port, err := freePort()
	if err != nil {
		fmt.Printf("[fatal] free port: %v\n", err)
		os.Exit(2)
	}

	cfgPath, cleanupCfg, err := writeConfig(port, adminDSN)
	if err != nil {
		fmt.Printf("[fatal] write config: %v\n", err)
		os.Exit(2)
	}
	defer cleanupCfg()

	cmd, err := startPgrouter(bin, cfgPath, port)
	if err != nil {
		fmt.Printf("[fatal] start pgrouter: %v\n", err)
		os.Exit(2)
	}
	pgrouterCmd = cmd
	defer stopPgrouter(cmd)

	pgrouterDSN = fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/appdb?sslmode=disable", port)
	os.Setenv("PGROUTER_DSN", pgrouterDSN)

	code := m.Run()
	stopPgrouter(cmd)
	os.Exit(code)
}

// Dsn returns the pgrouter DSN tests should connect through.
func Dsn() string { return pgrouterDSN }

// AdminDsn returns the admin DSN for setup-side ops (CREATE DATABASE,
// TRUNCATE, etc.) — bypasses pgrouter.
func AdminDsn() string { return adminDSN }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// redactDSN swaps the password in a postgres:// DSN with `***` for log
// safety. Best-effort; returns the original on parse failure.
func redactDSN(in string) string {
	i := strings.Index(in, "://")
	if i < 0 {
		return in
	}
	rest := in[i+3:]
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return in
	}
	creds := rest[:at]
	colon := strings.Index(creds, ":")
	if colon < 0 {
		return in
	}
	return in[:i+3] + creds[:colon] + ":***" + rest[at:]
}

// pingAdmin attempts a single Connect+SELECT 1 round trip.
func pingAdmin(dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// buildPgrouter `go build`s the pgrouter binary into a temp dir.
// Returns the binary path + a cleanup func.
func buildPgrouter() (string, func(), error) {
	dir, err := os.MkdirTemp("", "pgrouter-it-bin-")
	if err != nil {
		return "", nil, err
	}
	name := "pgrouter"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	out := filepath.Join(dir, name)
	// Module root is two levels up from test/integration.
	moduleRoot, err := findModuleRoot()
	if err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}
	cmd := exec.Command("go", "build", "-o", out, "./cmd/pgrouter")
	cmd.Dir = moduleRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}
	return out, func() { os.RemoveAll(dir) }, nil
}

// findModuleRoot walks up from the current working directory looking
// for a go.mod file.
func findModuleRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from %s", wd)
		}
		dir = parent
	}
}

// freePort grabs an OS-assigned free TCP port.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// writeConfig emits a minimal pgrouter.yaml routing `appdb` to the
// admin Postgres. The harness's pgrouter listens on `port`.
func writeConfig(port int, adminDSN string) (string, func(), error) {
	host, pgPort, user, pwd, dbname, err := parsePostgresDSN(adminDSN)
	if err != nil {
		return "", nil, err
	}
	dir, err := os.MkdirTemp("", "pgrouter-it-cfg-")
	if err != nil {
		return "", nil, err
	}
	path := filepath.Join(dir, "pgrouter.yaml")
	body := fmt.Sprintf(`server:
  listen_addr: 127.0.0.1
  listen_port: %d
pool:
  mode: transaction
  default_pool_size: 10
  query_timeout: 5s
  query_wait_timeout: 5s
auth:
  type: trust
logging:
  level: warn
  log_sql: off
databases:
  appdb:
    host: %q
    port: %d
    dbname: %q
    user: %q
    password: %q
`, port, host, pgPort, dbname, user, pwd)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}
	return path, func() { os.RemoveAll(dir) }, nil
}

// parsePostgresDSN is a tiny URI parser for our admin DSN shape:
//   postgres://user:pwd@host:port/dbname?sslmode=disable
func parsePostgresDSN(dsn string) (host string, port int, user, pwd, db string, err error) {
	i := strings.Index(dsn, "://")
	if i < 0 {
		err = fmt.Errorf("no scheme in %q", dsn)
		return
	}
	rest := dsn[i+3:]
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		err = fmt.Errorf("no @ in %q", dsn)
		return
	}
	creds := rest[:at]
	hostpart := rest[at+1:]
	if c := strings.Index(creds, ":"); c >= 0 {
		user = creds[:c]
		pwd = creds[c+1:]
	} else {
		user = creds
	}
	if q := strings.Index(hostpart, "?"); q >= 0 {
		hostpart = hostpart[:q]
	}
	slash := strings.Index(hostpart, "/")
	if slash < 0 {
		err = fmt.Errorf("no path in %q", dsn)
		return
	}
	hp := hostpart[:slash]
	db = hostpart[slash+1:]
	if c := strings.LastIndex(hp, ":"); c >= 0 {
		host = hp[:c]
		p, perr := strconv.Atoi(hp[c+1:])
		if perr != nil {
			err = fmt.Errorf("bad port: %w", perr)
			return
		}
		port = p
	} else {
		host = hp
		port = 5432
	}
	return
}

// startPgrouter spawns the pgrouter binary against cfgPath and waits
// until its listen port is accepting (or 8s timeout).
func startPgrouter(bin, cfg string, port int) (*exec.Cmd, error) {
	cmd := exec.Command(bin, "run", "--config", cfg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp",
			net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
			200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return cmd, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	return nil, fmt.Errorf("pgrouter did not start on :%d within 8s", port)
}

// stopPgrouter signals + waits with a small grace deadline.
func stopPgrouter(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		log.Printf("pgrouter did not exit on SIGTERM; killing")
		_ = cmd.Process.Kill()
		<-done
	}
}
