package main

import (
	"regexp"
	"testing"


	"github.com/bradleyjkemp/cupaloy"
        "github.com/estuary/connectors/sqlcapture" 
        "github.com/stretchr/testify/require"
)

func TestDiscoveryComplex(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(
		k1             INTEGER NOT NULL,
		foo            TEXT,
		real_          REAL NOT NULL,
		"Bounded Text" VARCHAR(255),
		k2             TEXT,
		doc            JSON,
		"doc/bin"      JSONB NOT NULL,
		PRIMARY KEY(k2, k1)
	)`)
	db.Exec(t, `COMMENT ON COLUMN <NAME>.foo IS 'This is a text field!'`)
	db.Exec(t, `COMMENT ON COLUMN <NAME>.k1 IS 'I think this is a key ?'`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestSecondaryIndexDiscovery(t *testing.T) {
	t.Run("pk_and_index", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER PRIMARY KEY, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("index_only", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nullable_index", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER, k3 INTEGER, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nonunique_index", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nothing", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}

func TestDiscoveryExcludesSystemSchemas(t *testing.T) {
	var _, tc = blackboxTestSetup(t)
	// Override the discovery filter to look for system schemas instead of test tables.
	// No bindings should be discovered since system schemas are excluded.
	tc.Capture.DiscoveryFilter = regexp.MustCompile(`(information_schema|pg_catalog)`)
	tc.Discover("Discover System Schemas")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestPartitionedTableDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(logdate DATE PRIMARY KEY, value TEXT) PARTITION BY RANGE (logdate)`)

	var cleanup = func() {
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q1`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q2`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q3`)
		db.QuietExec(t, `DROP TABLE IF EXISTS <NAME>_2023q4`)
	}
	cleanup()
	db.Exec(t, `CREATE TABLE <NAME>_2023q1 PARTITION OF <NAME> FOR VALUES FROM ('2023-01-01') TO ('2023-04-01')`)
	db.Exec(t, `CREATE TABLE <NAME>_2023q2 PARTITION OF <NAME> FOR VALUES FROM ('2023-04-01') TO ('2023-07-01')`)
	db.Exec(t, `CREATE TABLE <NAME>_2023q3 PARTITION OF <NAME> FOR VALUES FROM ('2023-07-01') TO ('2023-10-01')`)
	db.Exec(t, `CREATE TABLE <NAME>_2023q4 PARTITION OF <NAME> FOR VALUES FROM ('2023-10-01') TO ('2024-01-01')`)
	t.Cleanup(cleanup)

	tc.DiscoverFull("Discover Partitioned Table")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestDiscoveryWithoutPermissions(t *testing.T) {
	var db, tc = blackboxTestSetup(t)

	// Create table in public schema where the capture user may not have full permissions
	db.QuietExec(t, `DROP TABLE IF EXISTS public.tbl<ID>`)
	db.Exec(t, `CREATE TABLE public.tbl<ID> (id INTEGER PRIMARY KEY, data TEXT)`)
	t.Cleanup(func() { db.QuietExec(t, `DROP TABLE IF EXISTS public.tbl<ID>`) })

	tc.DiscoverFull("Discover Table Without Permissions")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestFloatKeyDiscovery(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id DOUBLE PRECISION PRIMARY KEY, val DOUBLE PRECISION)`)
	tc.DiscoverFull("Discover Tables")
	db.Exec(t, `INSERT INTO <NAME> VALUES (3.14, 3.14), (123.456, 123.456), (-12.3456789, -12.3456789), (9999999999.99, 9999999999.99)`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", transactionCountBaseline+1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestDiscoveryCrossSchemaIsolation(t *testing.T) {
	var db, tc = blackboxTestSetup(t)

	db.Exec(t, `CREATE SCHEMA IF NOT EXISTS otherschema`)
	db.Exec(t, `GRANT USAGE ON SCHEMA otherschema TO flow_capture`)
	t.Cleanup(func() { db.Exec(t, `DROP SCHEMA IF EXISTS otherschema CASCADE`) })

	db.CreateTable(t, `<NAME>`, `(k1 INTEGER PRIMARY KEY, foo TEXT)`)
	db.Exec(t, `CREATE TABLE otherschema.tbl<ID> (
		k2 TEXT PRIMARY KEY,
		bar INTEGER,
		gen INTEGER GENERATED ALWAYS AS (bar + 1) STORED
	)`)
	db.Exec(t, `GRANT SELECT ON ALL TABLES IN SCHEMA otherschema TO flow_capture`)
	db.Exec(t, `CREATE UNIQUE INDEX decoy_idx<ID> ON otherschema.tbl<ID> (bar)`)
	db.Exec(t, `COMMENT ON COLUMN otherschema.tbl<ID>.bar IS 'should never appear'`)

	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestSchemaFilterHelpers(t *testing.T) {
	var filter, args = schemaFilter("nc.nspname", nil)
	require.Equal(t, "", filter)
	require.Nil(t, args)

	filter, args = schemaFilter("nc.nspname", []string{"a", "b"})
	require.Equal(t, "AND nc.nspname = ANY($1)", filter)
	require.Equal(t, []any{[]string{"a", "b"}}, args)

	require.Equal(t, []string{"a", "b"},
		distinctSchemas([]sqlcapture.TableID{{Schema: "a"}, {Schema: "b"}, {Schema: "a"}}))
}
