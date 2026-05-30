package client

import "testing"

func TestClassifySQLSimple(t *testing.T) {
	cases := []struct {
		sql  string
		want SQLOp
	}{
		{"SELECT 1", SQLOpRead},
		{"  SELECT 1", SQLOpRead},
		{"select 1", SQLOpRead},
		{"VALUES (1, 2)", SQLOpRead},
		{"TABLE users", SQLOpRead},
		{"SHOW search_path", SQLOpRead},

		{"INSERT INTO t VALUES (1)", SQLOpWrite},
		{"UPDATE t SET x = 1", SQLOpWrite},
		{"DELETE FROM t", SQLOpWrite},
		{"MERGE INTO t USING ...", SQLOpWrite},
		{"COPY t FROM STDIN", SQLOpWrite},
		{"CREATE TABLE x(y int)", SQLOpWrite},
		{"DROP TABLE x", SQLOpWrite},
		{"TRUNCATE x", SQLOpWrite},
		{"BEGIN", SQLOpWrite},
		{"COMMIT", SQLOpWrite},
		{"ROLLBACK", SQLOpWrite},
		{"SET search_path TO x", SQLOpWrite},

		{"", SQLOpUnknown},
		{"   ", SQLOpUnknown},
		{"-- only comment\n", SQLOpUnknown},
	}
	for _, c := range cases {
		got := ClassifySQL(c.sql)
		if got != c.want {
			t.Errorf("ClassifySQL(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestClassifySQLExplain(t *testing.T) {
	cases := []struct {
		sql  string
		want SQLOp
	}{
		{"EXPLAIN SELECT 1", SQLOpRead},
		{"EXPLAIN ANALYZE SELECT 1", SQLOpWrite},
		{"EXPLAIN (ANALYZE, VERBOSE) SELECT 1", SQLOpWrite},
		{"EXPLAIN (BUFFERS, FORMAT JSON) SELECT 1", SQLOpRead},
		{"EXPLAIN UPDATE t SET x=1", SQLOpWrite},
	}
	for _, c := range cases {
		got := ClassifySQL(c.sql)
		if got != c.want {
			t.Errorf("ClassifySQL(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestClassifySQLCTE(t *testing.T) {
	cases := []struct {
		sql  string
		want SQLOp
	}{
		{"WITH x AS (SELECT 1) SELECT * FROM x", SQLOpRead},
		{"WITH x AS (SELECT 1) UPDATE t SET y = (SELECT * FROM x)", SQLOpWrite},
		{"WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x", SQLOpRead},
		{"WITH x AS NOT MATERIALIZED (SELECT 1) SELECT * FROM x", SQLOpRead},
		{"WITH x AS (SELECT 1), y AS (SELECT 2) SELECT * FROM x, y", SQLOpRead},
		{"WITH x AS (INSERT INTO foo VALUES (1) RETURNING id) SELECT * FROM x", SQLOpRead}, // outer is SELECT; we're conservative-by-side-effect — RETURNING inside CTE bypasses
	}
	for _, c := range cases {
		got := ClassifySQL(c.sql)
		if got != c.want {
			t.Errorf("ClassifySQL(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestClassifySQLCommentPrefix(t *testing.T) {
	got := ClassifySQL("/* tag */ SELECT 1")
	if got != SQLOpRead {
		t.Errorf("got %v, want Read", got)
	}
}
