package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/estuary/flow/go/protocols/flow"
)

func TestPrerequisites(t *testing.T) {
	// Table A exists and contains data, table B exists but is empty, and table C does not exist.
	var tb, ctx = postgresTestBackend(t), context.Background()
	var tableA = tb.CreateTable(ctx, t, "aaa", "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, "bbb", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]any{{0, "hello"}, {1, "world"}})
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tb.config.Advanced.WatermarksTable))

	var bindings = tests.DiscoverBindings(ctx, t, tb, tableA, tableB)
	var bindingA, bindingB = bindings[0], bindings[1]
	var bindingC = tests.BindingReplace(bindingA, "aaa", "ccc")

	t.Run("validateAB", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.Bindings = []*flow.CaptureSpec_Binding{bindingA, bindingB}
		_, err := cs.Validate(ctx, t)
		if err != nil {
			cupaloy.SnapshotT(t, err.Error())
		} else {
			cupaloy.SnapshotT(t, "no error")
		}
	})
	t.Run("validateABC-fails", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.Bindings = []*flow.CaptureSpec_Binding{bindingA, bindingB, bindingC}
		_, err := cs.Validate(ctx, t)
		if err != nil {
			cupaloy.SnapshotT(t, err.Error())
		} else {
			cupaloy.SnapshotT(t, "no error")
		}
	})
	t.Run("captureAB", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.Bindings = []*flow.CaptureSpec_Binding{bindingA, bindingB}
		tests.VerifiedCapture(ctx, t, cs)
	})
	t.Run("captureABC-fails", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.Bindings = []*flow.CaptureSpec_Binding{bindingA, bindingB, bindingC}
		tests.VerifiedCapture(ctx, t, cs)
	})
}
