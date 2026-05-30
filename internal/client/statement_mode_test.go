package client

import "testing"

func TestIsExplicitBeginSQL(t *testing.T) {
	yes := []string{
		"BEGIN",
		"begin",
		"BEGIN;",
		"BEGIN WORK",
		"BEGIN TRANSACTION",
		"BEGIN ISOLATION LEVEL SERIALIZABLE",
		"START TRANSACTION",
		"start transaction read only",
		"  BEGIN",
		"\t\nBEGIN",
		"-- comment\nBEGIN",
		"/* block */ BEGIN",
		"/* nested /* deeper */ end */ BEGIN",
	}
	for _, s := range yes {
		if !IsExplicitBeginSQL(s) {
			t.Errorf("IsExplicitBeginSQL(%q) = false, want true", s)
		}
	}
	no := []string{
		"",
		"SELECT 1",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT s1",
		"-- BEGIN is in a comment\nSELECT 1",
		"SELECT 'BEGIN'",
		"INSERT INTO BEGIN VALUES (1)", // BEGIN as table name; can't go further without parser
	}
	for _, s := range no {
		if IsExplicitBeginSQL(s) {
			t.Errorf("IsExplicitBeginSQL(%q) = true, want false", s)
		}
	}
}
