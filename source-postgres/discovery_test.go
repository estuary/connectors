package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func TestDiscoveryComplex(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
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
		var db, tc = postgresBlackboxSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER PRIMARY KEY, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("index_only", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nullable_index", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER, k3 INTEGER, data TEXT)`)
		db.Exec(t, `CREATE UNIQUE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nonunique_index", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		db.Exec(t, `CREATE INDEX idx<ID>_k23 ON <SCHEMA>.tbl<ID> (k2, k3)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("nothing", func(t *testing.T) {
		var db, tc = postgresBlackboxSetup(t)
		db.CreateTable(t, `<SCHEMA>.tbl<ID>`, `(k1 INTEGER, k2 INTEGER NOT NULL, k3 INTEGER NOT NULL, data TEXT)`)
		tc.DiscoverFull("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}

func TestDiscoveryExcludesSystemSchemas(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(`(information_schema|pg_catalog)`))
}

func TestPartitionedTableDiscovery(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var rootName = tb.CreateTable(ctx, t, uniqueID, "(logdate DATE PRIMARY KEY, value TEXT) PARTITION BY RANGE (logdate)")

	var cleanup = func() {
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q1;`, rootName))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q2;`, rootName))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q3;`, rootName))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s_2023q4;`, rootName))
	}
	cleanup()
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q1 PARTITION OF %[1]s FOR VALUES FROM ('2023-01-01') TO ('2023-04-01');`, rootName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q2 PARTITION OF %[1]s FOR VALUES FROM ('2023-04-01') TO ('2023-07-01');`, rootName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q3 PARTITION OF %[1]s FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');`, rootName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %[1]s_2023q4 PARTITION OF %[1]s FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');`, rootName))
	t.Cleanup(cleanup)

	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

func TestDiscoveryWithoutPermissions(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()

	var uniqueID = uniqueTableID(t)
	var tableName = strings.ToLower(fmt.Sprintf("public.%s_%s", strings.TrimPrefix(t.Name(), "Test"), uniqueID))
	var tableDef = "(id INTEGER PRIMARY KEY, data TEXT)"
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	t.Cleanup(func() { tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName)) })
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s %s;`, tableName, tableDef))

	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

func TestFloatKeyDiscovery(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)
	db.CreateTable(t, `<NAME>`, `(id DOUBLE PRECISION PRIMARY KEY, val DOUBLE PRECISION)`)
	tc.DiscoverFull("Discover Tables")
	db.Exec(t, `INSERT INTO <NAME> VALUES (3.14, 3.14), (123.456, 123.456), (-12.3456789, -12.3456789), (9999999999.99, 9999999999.99)`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", captureSessions)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
