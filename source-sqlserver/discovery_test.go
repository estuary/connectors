package main

import (
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestSecondaryIndexDiscovery(t *testing.T) {
	t.Run("pk_and_index", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(k1 INTEGER PRIMARY KEY, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX UX_<ID>_k23 ON <NAME> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("index_only", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX UX_<ID>_k23 ON <NAME> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nullable_index", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(k1 INTEGER, k2 INTEGER, k3 INTEGER, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX UX_<ID>_k23 ON <NAME> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nonunique_index", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE INDEX UX_<ID>_k23 ON <NAME> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nothing", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}

// TestIndexIncludedDiscovery tests discovery when a secondary unique index contains
// some included non-key columns.
func TestIndexIncludedDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
	db.Exec(t, `CREATE UNIQUE INDEX UX_<ID>_k23 ON <NAME> (k2, k3) INCLUDE (k1)`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestDiscoverOnlyEnabled tests discovery table filtering when only CDC-enabled tables should be discovered.
func TestDiscoverOnlyEnabled(t *testing.T) {
	t.Run("Disabled", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create three tables
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)

		// Disable CDC for table B
		var schemaName = *testSchemaName
		var tableB = db.Expand(`<NAME>_b`)
		var tableBShort = tableB[len(schemaName)+1:] // Strip schema prefix
		db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = 'all'`, schemaName, tableBShort))

		tc.Discover("Discover Tables (default - shows all)")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("Enabled", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create three tables
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)

		// Disable CDC for table B
		var schemaName = *testSchemaName
		var tableB = db.Expand(`<NAME>_b`)
		var tableBShort = tableB[len(schemaName)+1:] // Strip schema prefix
		db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = 'all'`, schemaName, tableBShort))

		require.NoError(t, tc.Capture.EditConfig("advanced.discover_only_enabled", true))
		tc.Discover("Discover Tables (only CDC-enabled)")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}
