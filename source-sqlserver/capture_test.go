package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

func TestCaptureInstanceCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// Lower the cleanup interval for the purposes of speedy test execution
	var oldInterval = cdcCleanupInterval
	t.Cleanup(func() { cdcCleanupInterval = oldInterval })
	cdcCleanupInterval = 5 * time.Second

	for _, testCase := range []struct {
		Name    string
		WithDBO bool
	}{
		{"WithDBO", true},
		{"WithoutDBO", false},
	} {
		t.Run(testCase.Name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)

			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
			require.NoError(t, tc.Capture.EditConfig("advanced.change_table_cleanup", true))

			// The db_owner permission is required to automatically clean up change tables
			if testCase.WithDBO {
				db.QuietExec(t, `ALTER ROLE db_owner ADD MEMBER flow_capture`)
				t.Cleanup(func() { db.QuietExec(t, `ALTER ROLE db_owner DROP MEMBER flow_capture`) })
			}

			// Initial backfill
			tc.Discover("Discover Tables")
			tc.Run("Initial Backfill", -1)

			// For the ongoing capture, we need to disable SHUTDOWN_AFTER_POLLING
			os.Unsetenv("SHUTDOWN_AFTER_POLLING")

			// Launch the ongoing capture in a background goroutine
			var captureCtx, cancelCapture = context.WithCancel(context.Background())
			defer cancelCapture()
			var captureDone atomic.Bool
			var captureErr error
			go func() {
				_, captureErr = tc.Capture.RunWithContext(captureCtx, -1)
				captureDone.Store(true)
			}()

			// Traffic generator - insert rows over time
			for i := 0; i < 10; i++ {
				db.QuietExec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES (%d, 'iteration %d row one'), (%d, 'iteration %d row two'), (%d, 'iteration %d row three'), (%d, 'iteration %d row four')`,
					i*100+1, i, i*100+2, i, i*100+3, i, i*100+4, i))
				time.Sleep(1 * time.Second)
			}

			// Wait for cleanup interval to pass
			time.Sleep(cdcCleanupInterval + 2*time.Second)

			// Cancel the capture and wait for it to finish
			cancelCapture()
			for !captureDone.Load() {
				time.Sleep(100 * time.Millisecond)
			}

			// Context cancellation is expected, not an error
			if captureErr != nil && !strings.Contains(captureErr.Error(), "context canceled") {
				t.Logf("capture error (may be expected): %v", captureErr)
			}

			// Query the number of rows in the change table
			var schemaName = *testSchemaName
			var tableName = db.Expand(`<NAME>`)
			var shortName = tableName[len(schemaName)+1:]
			var changeTableName = fmt.Sprintf("cdc.%s_%s_CT", schemaName, shortName)
			var changeTableRows int
			db.QueryRow(t, fmt.Sprintf("SELECT COUNT(*) FROM %s", changeTableName), &changeTableRows)

			// Add the change table row count to transcript
			fmt.Fprintf(tc.Transcript, "# Change Table Row Count: %d\n", changeTableRows)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

func TestAlterationAddColumn(t *testing.T) {
	for _, testCase := range []struct {
		Name     string
		Manual   bool
		Automate bool
	}{
		{"Off", false, false},
		{"Manual", true, false},
		{"Automatic", false, true},
	} {
		t.Run(testCase.Name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)

			// Grant db_owner role required for capture instance automation
			db.QuietExec(t, `ALTER ROLE db_owner ADD MEMBER flow_capture`)
			t.Cleanup(func() { db.QuietExec(t, `ALTER ROLE db_owner DROP MEMBER flow_capture`) })

			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
			if testCase.Automate {
				require.NoError(t, tc.Capture.EditConfig("advanced.capture_instance_management", true))
			}

			// Initial backfill
			db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
			tc.Discover("Discover Tables")
			tc.Run("Initial Backfill", -1)

			// Add first column
			db.Exec(t, `ALTER TABLE <NAME> ADD extra TEXT`)
			db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two', 'aaa'), (3, 'three', 'bbb')`)
			time.Sleep(1 * time.Second) // Let alteration propagate to cdc.ddl_history
			if testCase.Manual {
				// Manually create new capture instance for the altered schema
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s', @role_name = 'flow_capture', @capture_instance = '%s_%s_v2'`,
					schemaName, shortName, schemaName, shortName))
				time.Sleep(1 * time.Second)
			}
			tc.Run("After First Column Add", -1)

			db.Exec(t, `INSERT INTO <NAME> VALUES (4, 'four', 'ccc'), (5, 'five', 'ddd')`)
			tc.Run("More Inserts After First Add", -1)

			if testCase.Manual {
				// Disable old capture instance
				time.Sleep(1 * time.Second)
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = '%s_%s'`,
					schemaName, shortName, schemaName, shortName))
				time.Sleep(1 * time.Second)
			}

			// Add second column
			db.Exec(t, `ALTER TABLE <NAME> ADD evenmore TEXT`)
			db.Exec(t, `INSERT INTO <NAME> VALUES (6, 'six', 'eee', 'foo'), (7, 'seven', 'fff', 'bar')`)
			time.Sleep(1 * time.Second)
			if testCase.Manual {
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s', @role_name = 'flow_capture', @capture_instance = '%s_%s_v3'`,
					schemaName, shortName, schemaName, shortName))
				time.Sleep(1 * time.Second)
			}
			tc.Run("After Second Column Add", -1)

			db.Exec(t, `INSERT INTO <NAME> VALUES (8, 'eight', 'ggg', 'baz'), (9, 'nine', 'hhh', 'asdf')`)
			tc.Run("Final Inserts", -1)

			if testCase.Manual {
				time.Sleep(1 * time.Second)
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = '%s_%s_v2'`,
					schemaName, shortName, schemaName, shortName))
			}

			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

func TestFilegroupAndRole(t *testing.T) {
	var db, tc = blackboxTestSetup(t)

	// Create table without CDC (connector will create CDC instance with specified config)
	db.CreateTableWithoutCDC(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)

	// Grant db_owner so connector can create CDC instance
	db.Exec(t, `ALTER ROLE db_owner ADD MEMBER flow_capture`)
	t.Cleanup(func() { db.QuietExec(t, `ALTER ROLE db_owner DROP MEMBER flow_capture`) })

	require.NoError(t, tc.Capture.EditConfig("advanced.filegroup", "PRIMARY"))
	require.NoError(t, tc.Capture.EditConfig("advanced.role_name", "flow_capture"))
	tc.Discover("Discover Tables")

	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	for _, testCase := range []struct {
		name string
		flag string
	}{
		{"Default", ""},
		{"Enabled", "emit_sourced_schemas"},
		{"Disabled", "no_emit_sourced_schemas"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)
			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(32))`)
			db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'hello'), (2, 'world')`)
			if testCase.flag != "" {
				require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", testCase.flag))
			}
			tc.Discover("Discover Tables")
			tc.Run("Capture", -1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

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

// Since discovered bindings for keyless tables specify the keyless backfill mode,
// the collection key shouldn't impact correctness of the capture even if multiple
// rows have the same collection key value.
func TestDuplicatedScanKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id VARCHAR(8), data VARCHAR(2000))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES ('AAA', '1'), ('BBB', '2'), ('BBB', '3'), ('CCC', '4')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	require.NoError(t, tc.Capture.EditCollection(db.Expand(`<ID>`), "key", []string{"/id"}))
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestReplicationOnly(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	require.NoError(t, tc.Capture.EditBinding(0, "resource.mode", string(sqlcapture.BackfillModeOnlyChanges)))
	tc.Run("Backfill (no documents expected)", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (5, 'five'), (6, 'six'), (7, 'seven'), (8, 'eight')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestKeylessDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(a INTEGER, b VARCHAR(2000), c REAL NOT NULL, d VARCHAR(255))`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestKeylessCapture(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestCatalogPrimaryKey sets up a table with no primary key in the database
// and instead specifies one in the catalog configuration.
func TestCatalogPrimaryKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER, name TEXT, value INTEGER)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'alice', 100), (2, 'bob', 200)`)
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 1))
	require.NoError(t, tc.Capture.EditBinding(0, "resource.mode", string(sqlcapture.BackfillModeAutomatic)))
	require.NoError(t, tc.Capture.EditBinding(0, "resource.primary_key", []string{"id"}))
	require.NoError(t, tc.Capture.EditCollection(db.Expand("<ID>"), "key", []string{"/id"}))
	tc.Run("Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'carol', 300), (4, 'dave', 400)`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
