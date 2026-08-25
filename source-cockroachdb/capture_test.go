package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

// runSessions is a generous upper bound on the number of Flow transactions a test capture will
// produce. Because tests set SHUTDOWN_AFTER_POLLING the capture terminates on its own once caught
// up, so any value at least as large as the real transaction count yields identical output.
const runSessions = 16

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]Config{
		"Basic": {
			Address:  "example.com",
			User:     "flow_capture",
			Password: "secret1234",
			Database: "defaultdb",
		},
		"WithPort": {
			Address:  "example.com:26000",
			User:     "flow_capture",
			Password: "secret1234",
			Database: "defaultdb",
		},
		"RequireSSL": {
			Address:  "example.com",
			User:     "flow_capture",
			Password: "secret1234",
			Database: "defaultdb",
			Advanced: advancedConfig{SSLMode: "verify-full"},
		},
		"InvalidSSL": {
			Address:  "example.com",
			User:     "flow_capture",
			Password: "secret1234",
			Database: "defaultdb",
			Advanced: advancedConfig{SSLMode: "whoops-this-isnt-right"},
		},
		"MissingPassword": {
			Address:  "example.com",
			User:     "flow_capture",
			Database: "defaultdb",
		},
	} {
		t.Run(name, func(t *testing.T) {
			var valid = "config valid"
			if err := cfg.Validate(); err != nil {
				valid = err.Error()
			}
			cfg.SetDefaults()
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", cfg.ToURI(), valid))
		})
	}
}

func TestDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	// A table with a single-column primary key and a variety of column types.
	db.CreateTable(t, "<NAME>_single", "(id INTEGER PRIMARY KEY, name TEXT, weight DECIMAL(10,2), active BOOL, tags TEXT[])")
	// A table with a composite primary key.
	db.CreateTable(t, "<NAME>_composite", "(order_id INTEGER, sku TEXT, quantity INTEGER, PRIMARY KEY (order_id, sku))")
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestBasicCapture(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, "<NAME>", "(id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	tc.Discover("Discover")

	// Initial backfill of preexisting rows.
	db.Exec(t, "INSERT INTO <NAME> VALUES (1, 'alice', 10), (2, 'bob', 20)")
	tc.Run("Initial Backfill", runSessions)

	// Live changes streamed through the changefeed: an insert, an update, and a delete. Resuming
	// from the persisted cursor must not re-emit the backfilled rows (no re-snapshot).
	db.Exec(t, "INSERT INTO <NAME> VALUES (3, 'carol', 30)")
	db.Exec(t, "UPDATE <NAME> SET value = 99 WHERE id = 1")
	db.Exec(t, "DELETE FROM <NAME> WHERE id = 2")
	tc.Run("Replication", runSessions)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestCompositeKeyCapture(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, "<NAME>", "(order_id INTEGER, sku TEXT, quantity INTEGER, PRIMARY KEY (order_id, sku))")
	tc.Discover("Discover")

	db.Exec(t, "INSERT INTO <NAME> VALUES (100, 'AAA', 3), (100, 'BBB', 1), (200, 'AAA', 5)")
	tc.Run("Initial Backfill", runSessions)

	db.Exec(t, "INSERT INTO <NAME> VALUES (300, 'CCC', 7)")
	db.Exec(t, "UPDATE <NAME> SET quantity = 42 WHERE order_id = 100 AND sku = 'AAA'")
	db.Exec(t, "DELETE FROM <NAME> WHERE order_id = 200 AND sku = 'AAA'")
	tc.Run("Replication", runSessions)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestDeleteReduction verifies that a deleted row produces a change event carrying the primary key
// and op='d', which (combined with the generated collection schema's delete reduction) removes the
// row from a materialized destination.
func TestDeleteReduction(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, "<NAME>", "(id INTEGER PRIMARY KEY, payload TEXT)")
	tc.Discover("Discover")

	db.Exec(t, "INSERT INTO <NAME> VALUES (1, 'keep'), (2, 'remove')")
	tc.Run("Initial Backfill", runSessions)

	db.Exec(t, "DELETE FROM <NAME> WHERE id = 2")
	tc.Run("Delete", runSessions)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// sanity check that the control connection works before running the heavier capture tests.
func TestControlConnection(t *testing.T) {
	var db, _ = blackboxTestSetup(t)
	var result int
	require.NoError(t, db.conn.QueryRow(context.Background(), "SELECT 1").Scan(&result))
	require.Equal(t, 1, result)
}

// TestSchemaNameCollision verifies that two captured tables sharing a bare table name in
// different schemas have their streamed events routed to the correct bindings. The changefeed
// is created WITH full_table_name for exactly this reason.
func TestSchemaNameCollision(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	var bareName = strings.TrimPrefix(db.vars["<NAME>"], testSchemaName+".")
	db.QuietExec(t, "CREATE SCHEMA IF NOT EXISTS test2")
	db.CreateTable(t, "<NAME>", "(id INTEGER PRIMARY KEY, v TEXT)")
	db.CreateTable(t, "test2."+bareName, "(id INTEGER PRIMARY KEY, v TEXT)")
	tc.Discover("Discover")

	db.Exec(t, "INSERT INTO <NAME> VALUES (1, 'backfill-test')")
	db.Exec(t, "INSERT INTO test2."+bareName+" VALUES (100, 'backfill-test2')")
	tc.Run("Backfill", runSessions)

	db.Exec(t, "INSERT INTO <NAME> VALUES (2, 'streamed-into-test')")
	db.Exec(t, "INSERT INTO test2."+bareName+" VALUES (200, 'streamed-into-test2')")
	tc.Run("Replication", runSessions)

	cupaloy.SnapshotT(t, tc.Transcript.String())

	// Each streamed row must appear under its own schema's collection.
	var transcript = tc.Transcript.String()
	for _, check := range [][2]string{
		{"streamed-into-test\"", "/test/"},
		{"streamed-into-test2\"", "/test2/"},
	} {
		var idx = strings.Index(transcript, check[0])
		require.NotEqual(t, -1, idx, "streamed row %q missing from transcript", check[0])
		var section = transcript[:idx]
		var collection = section[strings.LastIndex(section, "acmeCo/test_capture"):]
		require.Contains(t, collection, check[1], "row %q routed to wrong collection", check[0])
	}
}

// TestMixedCaseNames verifies changefeed routing for quoted mixed-case schema and table names,
// whose rendering in the changefeed's full_table_name topic is undocumented.
func TestMixedCaseNames(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	var bareName = strings.TrimPrefix(db.vars["<NAME>"], testSchemaName+".")
	db.QuietExec(t, `CREATE SCHEMA IF NOT EXISTS "MixedCase"`)
	db.CreateTable(t, `"MixedCase"."Camel_`+bareName+`"`, "(id INTEGER PRIMARY KEY, v TEXT)")
	tc.Discover("Discover")

	db.Exec(t, `INSERT INTO "MixedCase"."Camel_`+bareName+`" VALUES (1, 'backfilled')`)
	tc.Run("Backfill", runSessions)
	db.Exec(t, `INSERT INTO "MixedCase"."Camel_`+bareName+`" VALUES (2, 'streamed')`)
	tc.Run("Replication", runSessions)
	cupaloy.SnapshotT(t, tc.Transcript.String())

	require.Contains(t, tc.Transcript.String(), `"v": "streamed"`, "streamed row missing: mixed-case changefeed routing failed")
}

// TestHashShardedPK verifies capture of a table with a hash-sharded primary key. The hidden
// shard column is a virtual computed column derived from the visible key, so discovery keys the
// collection on the visible columns and delete reconstruction skips the shard value in the
// changefeed key array.
func TestHashShardedPK(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, "<NAME>", "(id INTEGER PRIMARY KEY USING HASH, v TEXT)")
	tc.Discover("Discover")

	db.Exec(t, "INSERT INTO <NAME> VALUES (1, 'a'), (2, 'b')")
	tc.Run("Backfill", runSessions)

	db.Exec(t, "INSERT INTO <NAME> VALUES (3, 'c')")
	db.Exec(t, "UPDATE <NAME> SET v = 'z' WHERE id = 1")
	db.Exec(t, "DELETE FROM <NAME> WHERE id = 2")
	tc.Run("Replication", runSessions)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestRowidTable verifies capture of a table with no user-defined primary key, which CockroachDB
// keys on a synthesized hidden `rowid`. Discovery adopts rowid as the collection key, the
// backfill merges it into its documents (the changefeed emits it in `after` natively), and
// deletes reconstruct a rowid-keyed document. A multi-row transaction must yield one document
// per row (all rows in a transaction share a commit timestamp, so any timestamp-derived key
// would collapse them).
func TestRowidTable(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, "<NAME>", "(v TEXT, n INTEGER)")
	tc.Discover("Discover")

	db.Exec(t, "INSERT INTO <NAME> VALUES ('x', 1), ('y', 2), ('z', 3)")
	tc.Run("Backfill", runSessions)

	db.Exec(t, "INSERT INTO <NAME> VALUES ('p', 4), ('q', 5)") // One transaction, two rows.
	db.Exec(t, "DELETE FROM <NAME> WHERE n = 2")
	tc.Run("Replication", runSessions)

	var transcript = tc.Transcript.String()
	for _, v := range []string{"x", "y", "z", "p", "q"} {
		require.Contains(t, transcript, `"v": "`+v+`"`, "row missing from capture")
	}
	require.Contains(t, transcript, `"op": "d"`, "delete event missing")
}

// TestUnusableTables verifies that tables this connector can't capture correctly (floating-point
// key columns, multiple column families) are rejected with a clear error at validation time
// rather than captured incorrectly.
func TestUnusableTables(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, "<NAME>_float", "(f FLOAT8 PRIMARY KEY, v TEXT)")
	db.CreateTable(t, "<NAME>_decimal", "(d DECIMAL(10,2) PRIMARY KEY, v TEXT)")
	db.CreateTable(t, "<NAME>_multifam", "(id INTEGER PRIMARY KEY, a TEXT, b TEXT, FAMILY f1 (id, a), FAMILY f2 (b))")
	tc.Discover("Discover")

	db.Exec(t, "INSERT INTO <NAME>_float VALUES (1.5, 'a')")
	tc.Run("Attempt Capture", runSessions)
	cupaloy.SnapshotT(t, tc.Transcript.String())

	require.Contains(t, tc.Transcript.String(), "cannot be used as a collection key", "expected a clear unusable-key error")
	require.Contains(t, tc.Transcript.String(), "column families", "expected a clear multi-column-family error")
}
