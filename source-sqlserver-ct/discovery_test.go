package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

// TestDiscoverOnlyEnabled tests discovery table filtering when only CT-enabled tables should be discovered.
func TestDiscoverOnlyEnabled(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create three tables, one (table B) without change tracking
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.QuietExec(t, `ALTER TABLE <NAME>_b DISABLE CHANGE_TRACKING`)

		tc.Discover("Discover Tables (Default behavior: only CT-enabled)")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("DiscoverWithoutCT", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create three tables, one (table B) without change tracking
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.QuietExec(t, `ALTER TABLE <NAME>_b DISABLE CHANGE_TRACKING`)

		require.NoError(t, tc.Capture.EditConfig("advanced.discover_tables_without_ct", true))
		tc.Discover("Discover Tables (Non-default behavior: includ tables without CT)")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}
