package client

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
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

func TestIsReadMessageHonoursReadOnlyBegin(t *testing.T) {
	if !isReadMessage(&pgproto3.Query{String: "BEGIN READ ONLY"}) {
		t.Error("BEGIN READ ONLY should be read")
	}
	if isReadMessage(&pgproto3.Query{String: "BEGIN"}) {
		t.Error("BEGIN should NOT be read")
	}
	if !isReadMessage(&pgproto3.Parse{Query: "START TRANSACTION READ ONLY"}) {
		t.Error("START TRANSACTION READ ONLY (Parse) should be read")
	}
}
