package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestSecondaryIndexDiscovery(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()

	t.Run("pk_and_index", func(t *testing.T) {
		var uniqueString = "g14228"
		var tableName = tb.CreateTable(ctx, t, uniqueString, "(k1 INTEGER PRIMARY KEY, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		var shortName = tableName[strings.Index(tableName, ".")+1:]
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX %s_k23 ON %s (k2, k3)`, shortName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
	})
	t.Run("index_only", func(t *testing.T) {
		var uniqueString = "g26313"
		var tableName = tb.CreateTable(ctx, t, uniqueString, "(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		var shortName = tableName[strings.Index(tableName, ".")+1:]
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX %s_k23 ON %s (k2, k3)`, shortName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
	})
	t.Run("index_and_fk", func(t *testing.T) {
		// It's possible to have multiple constraints on the same table with the same constraint
		// name but different types. This shouldn't matter now that our secondary index discovery
		// query no longer joins against `information_schema.table_constraints` at all, but this
		// test case just makes sure that scenario doesn't break anything.
		var uniqueA, uniqueB = "g26313", "g16025"
		var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
		var shortNameA = tableA[strings.Index(tableA, ".")+1:]
		var tableB = tb.CreateTable(ctx, t, uniqueB, fmt.Sprintf("(id INTEGER NOT NULL, parent_id INTEGER, CONSTRAINT FK_%[1]s_ParentID FOREIGN KEY (parent_id) REFERENCES %[1]s(id), CONSTRAINT FK_%[1]s_ParentID UNIQUE (parent_id))", shortNameA))
		var shortNameB = tableB[strings.Index(tableB, ".")+1:]
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX IX_%[1]s_ParentID ON %s(id)`, shortNameB, tableB))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueB)))
	})
	t.Run("nullable_index", func(t *testing.T) {
		var uniqueString = "g31990"
		var tableName = tb.CreateTable(ctx, t, uniqueString, "(k1 INTEGER, k2 INTEGER, k3 INTEGER, data TEXT)")
		var shortName = tableName[strings.Index(tableName, ".")+1:]
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX %s_k23 ON %s (k2, k3)`, shortName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
	})
	t.Run("nonunique_index", func(t *testing.T) {
		var uniqueString = "g22906"
		var tableName = tb.CreateTable(ctx, t, uniqueString, "(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		var shortName = tableName[strings.Index(tableName, ".")+1:]
		tb.Query(ctx, t, fmt.Sprintf(`CREATE INDEX %s_k23 ON %s (k2, k3)`, shortName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
	})
	t.Run("nothing", func(t *testing.T) {
		var uniqueString = "g14307"
		tb.CreateTable(ctx, t, uniqueString, "(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
	})
}

// TestTrickyEnumValues tests some "gotcha" enum values to make sure the discovery parsing logic is robust.
func TestTrickyEnumValues(t *testing.T) {
	var tb, ctx, uniqueID = mysqlTestBackend(t), context.Background(), uniqueTableID(t)
	tb.CreateTable(ctx, t, uniqueID, `(id INTEGER PRIMARY KEY, category ENUM('A', 'B (Parentheses)', '', 'Internal''Quote', 'Internal,Comma', 'Internal\nNewline', 'Internal;Semicolon', '   Leading Spaces', 'Trailing Spaces   ', 'Z'))`)
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}
