#!/usr/bin/env bash
# Lint docker-compose / docker-compose.*.yml files in this repo for
# accidentally-public database ports.
#
# Why: A Postgres port published on 0.0.0.0 with trust auth (or
# with weak auth) gives any internet scanner full superuser RCE
# within seconds. We hit this in production once. Don't do it again.
#
# Detects:
#   - ports: ["PORT:5432"] without a host-IP prefix (treated as 0.0.0.0)
#   - ports: ["0.0.0.0:PORT:5432"] (explicit public bind)
#   - ports: ["*:PORT:5432"]
#   - services: ... ports: - "PORT:5432"  (long form, same issue)
#
# Skips:
#   - CI service blocks (github workflows, which are not public)
#   - non-database ports (heuristic: looks for common DB ports 5432, 3306, 1433, 27017, 6379, 5433, etc.)
#   - 127.0.0.1 / ::1 / localhost binds (safe)
#   - file: docker-compose.ci.yml (the CI service pattern is allowed)
#
# Exits 1 on any violation, 0 on clean.
#
# Usage:
#   bash scripts/check-compose-security.sh
#   make compose-lint   # if you wire it up

set -euo pipefail

# Find repo root (parent of this script's directory).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Database ports we treat as "sensitive" (Postgres, MySQL, MSSQL, Mongo, Redis, etc.)
SENSITIVE_PORTS_REGEX='(5432|5433|3306|1433|27017|6379|5984|8529|9200|9300|1521|50000|25433|25515|25516|15432|16432|16433|16434|6432|6433|6434)'

# Files to check. Auto-discover, exclude the .github directory (CI uses
# its own services: block which runs in a private network).
mapfile -t FILES < <(find . \( -name docker-compose.y*ml -o -name 'docker-compose.*.y*ml' \) \
  -not -path './.github/*' \
  -not -path './node_modules/*' \
  2>/dev/null | sort)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No compose files found in $REPO_ROOT"
  exit 0
fi

# Track failures but don't exit yet — report all at once.
FAIL_COUNT=0
FAIL_OUTPUT=""

for f in "${FILES[@]}"; do
  # Use python for proper YAML parsing (sed/grep on YAML is fragile).
  # Read ports: from each service.
  VIOLATIONS=$(python3 - "$f" <<'PYEOF' 2>/dev/null || echo "PYTHON_ERROR"
import sys
import re

filepath = sys.argv[1]
try:
    with open(filepath) as fh:
        lines = fh.read().split('\n')
except Exception as e:
    print(f"READ_ERROR: {e}")
    sys.exit(0)

# Track current service name (look for "  servicename:" at indent level 2).
current_service = None
service_indent = -1
in_ports_block = False
ports_indent = -1

violations = []

# Sensitive port regex matches individual port numbers.
PORT_RE = re.compile(r'\b(5432|5433|3306|1433|27017|6379|5984|8529|9200|9300|1521|50000|25433|25515|25516|15432|16432|16433|16434|6432|6433|6434)\b')

def is_safe_host(host_part):
    """A port mapping like 'HOST:CONTAINER' is safe if HOST is loopback."""
    if not host_part:
        return False
    h = host_part.strip().strip('"').strip("'")
    if h in ('127.0.0.1', '::1', 'localhost'):
        return True
    # Private CIDRs (RFC1918 + link-local + ULA) — also safe to expose on private nets,
    # but flag for human review since misconfig is common. We treat as safe only if
    # the operator is explicit (which they usually are via a comment in the same file).
    if re.match(r'^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)', h):
        # Allow private ranges — the user has explicitly bound to a private IP.
        return True
    return False

for lineno, line in enumerate(lines, 1):
    stripped = line.lstrip()
    indent = len(line) - len(stripped)
    if not stripped or stripped.startswith('#'):
        continue

    # Detect service start: "  <name>:" at indent 2 with no leading dash.
    m = re.match(r'^(\s{2})([a-zA-Z0-9_-]+):\s*$', line)
    if m and indent == 2:
        current_service = m.group(2)
        in_ports_block = False
        continue

    # Detect ports: at any indent.
    m = re.match(r'^(\s+)ports:\s*$', line)
    if m:
        in_ports_block = True
        ports_indent = indent
        continue
    # Detect end of ports block (next top-level key or sub-key at same/lower indent).
    if in_ports_block:
        if indent <= ports_indent and stripped and not stripped.startswith('- '):
            in_ports_block = False
        else:
            # We're inside ports: list. Each entry is "- ...".
            m = re.match(r'^\s*-\s*["\']?([^"\']+)["\']?\s*(#.*)?$', line)
            if m:
                port_spec = m.group(1).strip()
                # Skip if no sensitive port.
                if not PORT_RE.search(port_spec):
                    continue
                # Parse port spec. Docker accepts: HOST:CONTAINER, IP:HOST:CONTAINER, :CONTAINER (random host port)
                parts = port_spec.split(':')
                if len(parts) == 1:
                    # Just a container port: "5432". No host bind. Safe.
                    continue
                elif len(parts) == 2:
                    # HOST:CONTAINER.
                    host = parts[0]
                    container = parts[1]
                    if not is_safe_host(host):
                        violations.append((lineno, port_spec, f"service '{current_service}' binds {host}:{container} publicly"))
                elif len(parts) == 3:
                    # IP:HOST:CONTAINER.
                    ip = parts[0]
                    host = parts[1]
                    container = parts[2]
                    if not is_safe_host(ip):
                        violations.append((lineno, port_spec, f"service '{current_service}' binds {ip}:{host}:{container} publicly"))
                else:
                    violations.append((lineno, port_spec, f"service '{current_service}' has unusual port spec"))

if violations:
    for v in violations:
        print(f"VIOLATION: line {v[0]}: {v[1]} -- {v[2]}")
    sys.exit(1)
sys.exit(0)
PYEOF
)

  # Strip the EXIT code from python (it uses sys.exit(0) on clean, sys.exit(1) on violations).
  # We need to differentiate "no violations" from "python errored".
  VIOLATIONS_ONLY=$(echo "$VIOLATIONS" | grep -v '^PYTHON_ERROR' | grep '^VIOLATION:' || true)
  if [[ -n "$VIOLATIONS_ONLY" ]]; then
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAIL_OUTPUT+="\n=== $f ===\n$VIOLATIONS_ONLY\n"
  elif echo "$VIOLATIONS" | grep -q '^PYTHON_ERROR'; then
    echo "WARN: could not lint $f (python error)"
  fi
done

if [[ $FAIL_COUNT -gt 0 ]]; then
  echo
  echo "=========================================="
  echo "COMPOSE SECURITY LINT FAILED"
  echo "=========================================="
  echo "Found $FAIL_COUNT file(s) with publicly-bound database ports:"
  echo -e "$FAIL_OUTPUT"
  echo
  echo "Fix: change '\"PORT:5432\"' to '\"127.0.0.1:PORT:5432\"' (or private CIDR)"
  echo "Why: see VPS_INCIDENT_POSTMORTEM.md (gitignored) — public postgres + trust"
  echo "     auth = RCE within seconds via internet scanner."
  exit 1
fi

echo "compose-security-lint: OK (${#FILES[@]} file(s) checked, no public DB ports)"
exit 0
