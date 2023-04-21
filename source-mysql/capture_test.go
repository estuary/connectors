package main

import (
	"context"
	"regexp"
	"testing"

	"github.com/estuary/connectors/sqlcapture/tests"
)

func TestTrickyColumnNames(t *testing.T) {
	// Create a table with some 'difficult' column names (a reserved word, a capitalized
	// name, and one containing special characters which also happens to be the primary key).
	var tb, ctx = mysqlTestBackend(t), context.Background()
	const uniqueString = "fizzed_cupcake"
	var tableA = tb.CreateTable(ctx, t, uniqueString+"_a", "(`Meta/``wtf``~ID` INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueString+"_b", "(`table` INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "aaa"}, {2, "bbb"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{3, "ccc"}, {4, "ddd"}})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, tableA, tableB)
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableA, [][]interface{}{{5, "eee"}, {6, "fff"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{7, "ggg"}, {8, "hhh"}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}
