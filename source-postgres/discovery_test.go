package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestDiscoveryComplex(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()

	const uniqueString = "cheap_oxygenation"
	var tableName = tb.CreateTable(ctx, t, uniqueString, `(
		k1             INTEGER NOT NULL,
		foo            TEXT,
		real_          REAL NOT NULL,
		"Bounded Text" VARCHAR(255),
		k2             TEXT,
		doc            JSON,
		"doc/bin"      JSONB NOT NULL,
		PRIMARY KEY(k2, k1)
	)`)
	tb.Query(ctx, t, fmt.Sprintf("COMMENT ON COLUMN %s.foo IS 'This is a text field!';", tableName))
	tb.Query(ctx, t, fmt.Sprintf("COMMENT ON COLUMN %s.k1 IS 'I think this is a key ?';", tableName))

	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
}

func TestDiscoveryCapitalization(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var tablePrefix = strings.TrimPrefix(t.Name(), "Test")
	var tableA = tablePrefix + "_AaAaA"                  // Name contains capital letters and will be quoted
	var tableB = tablePrefix + "_BbBbB"                  // Name contains capital letters and will not be quoted
	var tableC = strings.ToLower(tablePrefix + "_CcCcC") // Name is all lowercase and will be quoted

	var cleanup = func() {
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s";`, testSchemaName, tableA))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS "%s".%s;`, testSchemaName, tableB))
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s";`, testSchemaName, tableC))
	}
	cleanup()
	t.Cleanup(cleanup)

	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data TEXT);`, testSchemaName, tableA))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE "%s".%s (id INTEGER PRIMARY KEY, data TEXT);`, testSchemaName, tableB))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data TEXT);`, testSchemaName, tableC))

	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(fmt.Sprintf(`(?i:%s)`, regexp.QuoteMeta(tablePrefix))))
}

func TestSecondaryIndexDiscovery(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()

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

func TestDiscoveryExcludesSystemSchemas(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(`(information_schema|pg_catalog)`))
}

func TestPartitionedTableDiscovery(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()
	var uniqueID = "27339326"
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

	var uniqueID = "117535"
	var tableName = strings.ToLower(fmt.Sprintf("public.%s_%s", strings.TrimPrefix(t.Name(), "Test"), uniqueID))
	var tableDef = "(id INTEGER PRIMARY KEY, data TEXT)"
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	t.Cleanup(func() { tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName)) })
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s %s;`, tableName, tableDef))

	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}
