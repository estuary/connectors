package main

import (
	"context"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
)

func TestPrerequisites(t *testing.T) {
	// Table A exists and contains data, table B exists but is empty, and table C does not exist.
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var tableA = tb.CreateTable(ctx, t, "aaa", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, "bbb", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = strings.ReplaceAll(tableA, "aaa", "ccc")
	tb.Insert(ctx, t, tableA, [][]any{{0, "hello"}, {1, "world"}})
	var cs = tb.CaptureSpec(t, tableA, tableB, tableC)

	t.Run("validate", func(t *testing.T) {
		_, err := cs.Validate(ctx, t)
		if err != nil {
			cupaloy.SnapshotT(t, err.Error())
		} else {
			cupaloy.SnapshotT(t, "no error")
		}
	})
	t.Run("capture", func(t *testing.T) {
		tests.VerifiedCapture(ctx, t, cs)
	})
}
