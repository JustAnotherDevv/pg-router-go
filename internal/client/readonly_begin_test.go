package client

import (
	"testing"
)

func TestIsExplicitReadOnlyBeginSQL(t *testing.T) {
	yes := []string{
		"BEGIN READ ONLY",
		"BEGIN WORK READ ONLY",
		"BEGIN TRANSACTION READ ONLY",
		"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
		"BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY",
		"START TRANSACTION READ ONLY",
		"start transaction isolation level serializable read only",
		"SET TRANSACTION READ ONLY",
		"  -- comment\n  BEGIN READ ONLY",
	}
	for _, s := range yes {
		if !IsExplicitReadOnlyBeginSQL(s) {
			t.Errorf("IsExplicitReadOnlyBeginSQL(%q) = false, want true", s)
		}
	}
	no := []string{
		"BEGIN",
		"BEGIN TRANSACTION",
		"BEGIN READ WRITE",
		"SET TRANSACTION READ WRITE",
		"SELECT 1",
		"SELECT 'BEGIN READ ONLY'",
	}
	for _, s := range no {
		if IsExplicitReadOnlyBeginSQL(s) {
			t.Errorf("IsExplicitReadOnlyBeginSQL(%q) = true, want false", s)
		}
	}
}

func TestReadOnlyBeginClassification(t *testing.T) {
	tests := []struct {
		sql  string
		want SQLOp
	}{
		{"SELECT 1", SQLOpRead},
		{"INSERT INTO t VALUES (1)", SQLOpWrite},
		{"BEGIN", SQLOpWrite},
		{"BEGIN READ ONLY", SQLOpWrite}, // BEGIN is always write
		{"SET TRANSACTION READ ONLY", SQLOpWrite},
	}
	for _, tt := range tests {
		if got := ClassifySQL(tt.sql); got != tt.want {
			t.Errorf("ClassifySQL(%q) = %v, want %v", tt.sql, got, tt.want)
		}
	}
}
