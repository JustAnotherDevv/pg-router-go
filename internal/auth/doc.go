// Package auth implements client- and backend-side authentication.
//
// MVP scope:
//   - SCRAM-SHA-256 (server-side: we auth clients; client-side: we auth to backend) — M.5
//   - MD5 password — M.5
//   - Trust auth — already supported by backend dialer
//   - PgBouncer-compat userlist.txt — M.5
//   - auth_query lookup — M.5
//
// Post-MVP (v1.0+): cert auth (mTLS), LDAP, PAM, HBA file format,
// auth cache with TTL.
package auth
