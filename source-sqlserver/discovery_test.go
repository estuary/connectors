package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestSecondaryIndexDiscovery(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var testName = strings.TrimPrefix(t.Name(), "Test")

	t.Run("pk_and_index", func(t *testing.T) {
		var uniqueID = "g14228"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(k1 INTEGER PRIMARY KEY, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		var indexName = fmt.Sprintf("UX_%s_%s_k23", testName, uniqueID)
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX %s ON %s (k2, k3)`, indexName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("index_only", func(t *testing.T) {
		var uniqueID = "g26313"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		var indexName = fmt.Sprintf("UX_%s_%s_k23", testName, uniqueID)
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX %s ON %s (k2, k3)`, indexName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("nullable_index", func(t *testing.T) {
		var uniqueID = "g31990"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(k1 INTEGER, k2 INTEGER, k3 INTEGER, data TEXT)")
		var indexName = fmt.Sprintf("UX_%s_%s_k23", testName, uniqueID)
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX %s ON %s (k2, k3)`, indexName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("nonunique_index", func(t *testing.T) {
		var uniqueID = "g22906"
		var tableName = tb.CreateTable(ctx, t, uniqueID, "(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		var indexName = fmt.Sprintf("UX_%s_%s_k23", testName, uniqueID)
		tb.Query(ctx, t, fmt.Sprintf(`CREATE INDEX %s ON %s (k2, k3)`, indexName, tableName))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("nothing", func(t *testing.T) {
		var uniqueID = "g14307"
		tb.CreateTable(ctx, t, uniqueID, "(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
}

// TestIndexIncludedDiscovery tests discovery when a secondary unique index contains
// some included non-key columns.
func TestIndexIncludedDiscovery(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var testName = strings.TrimPrefix(t.Name(), "Test")

	var uniqueID = "98476798"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)")
	var indexName = fmt.Sprintf("UX_%s_%s_k23", testName, uniqueID)
	tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX %s ON %s (k2, k3) INCLUDE (k1)`, indexName, tableName))
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}
