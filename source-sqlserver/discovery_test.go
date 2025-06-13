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

// TestDiscoverOnlyEnabled tests discovery table filtering when only CDC-enabled tables should be discovered.
func TestDiscoverOnlyEnabled(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()

	// Create tables A, B, and C
	var uniqueA, uniqueB, uniqueC = uniqueTableID(t, "a"), uniqueTableID(t, "b"), uniqueTableID(t, "c")
	var _ = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
	var _ = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data TEXT)")
	var _ = tb.CreateTable(ctx, t, uniqueC, "(id INTEGER PRIMARY KEY, data TEXT)")

	// Delete CDC instance for table B so only tables A and C should be discovered.
	tb.Query(ctx, t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = 'all'`,
		*testSchemaName,
		"test_DiscoverOnlyEnabled_325570", // Known name of table B, a bit hacky to hard-code but it's already snapshotted so whatever.
	))
	t.Run("Disabled", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	})
	t.Run("Enabled", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.EndpointSpec.(*Config).Advanced.DiscoverOnlyEnabled = true
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	})
}
