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
