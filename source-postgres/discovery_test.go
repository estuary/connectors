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

	tb.CaptureSpec(t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
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

	tb.CaptureSpec(t).VerifyDiscover(ctx, t, regexp.MustCompile(fmt.Sprintf(`(?i:%s)`, regexp.QuoteMeta(tablePrefix))))
}
