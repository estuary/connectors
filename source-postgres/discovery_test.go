package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
)

func TestConfigSchema(t *testing.T) {
	tests.VerifySnapshot(t, "", string(configSchema()))
}

func TestDiscoveryComplex(t *testing.T) {
	var tb, ctx = TestBackend, context.Background()

	var tableName = tb.CreateTable(ctx, t, "", `(
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

	// Create the table (with deferred cleanup), perform discovery, and verify
	// that the stream as discovered matches the golden snapshot.
	var catalog, err = sqlcapture.DiscoverCatalog(ctx, tb.GetDatabase())
	if err != nil {
		t.Fatal(err)
	}
	tests.VerifyStream(t, "", catalog, tableName)
}
