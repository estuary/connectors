package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/protocols/airbyte"
)

func TestDiscoverySimple(t *testing.T) {
	var db, ctx = &postgresDatabase{config: &TestDefaultConfig}, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", "(a INTEGER PRIMARY KEY, b TEXT, c REAL NOT NULL, d VARCHAR(255))")

	// Create the table (with deferred cleanup), perform discovery, and verify
	// that the stream as discovered matches the golden snapshot.
	var catalog, err = sqlcapture.DiscoverCatalog(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	verifyStream(t, "", catalog, tableName)
}

func TestDiscoveryComplex(t *testing.T) {
	var db, ctx = &postgresDatabase{config: &TestDefaultConfig}, shortTestContext(t)
	var tableName = createTestTable(ctx, t, "", `(
		k1             INTEGER NOT NULL,
		foo            TEXT,
		real_          REAL NOT NULL,
		"Bounded Text" VARCHAR(255),
		k2             TEXT,
		doc            JSON,
		"doc/bin"      JSONB NOT NULL,
		PRIMARY KEY(k2, k1)
	)`)
	dbQuery(ctx, t, fmt.Sprintf("COMMENT ON COLUMN %s.foo IS 'This is a text field!';", tableName))
	dbQuery(ctx, t, fmt.Sprintf("COMMENT ON COLUMN %s.k1 IS 'I think this is a key ?';", tableName))

	// Create the table (with deferred cleanup), perform discovery, and verify
	// that the stream as discovered matches the golden snapshot.
	var catalog, err = sqlcapture.DiscoverCatalog(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	verifyStream(t, "", catalog, tableName)
}

// verifyStream is a helper function which locates a particular stream by name in
// the discovered catalog and then uses verifySnapshot on that. This is necessary
// because we don't want to make assumptions about what other tables might be
// present in the test database or what order they might be discovered in, we
// just want to test that the specific table we created in our test looks good.
func verifyStream(t *testing.T, suffix string, catalog *airbyte.Catalog, expectedStream string) {
	t.Helper()
	for _, stream := range catalog.Streams {
		t.Logf("catalog stream %q", stream.Name)
		if !strings.EqualFold(stream.Name, expectedStream) {
			continue
		}

		var buf bytes.Buffer
		var enc = json.NewEncoder(&buf)
		enc.SetIndent("", "  ")

		if err := enc.Encode(stream); err != nil {
			t.Fatalf("error marshalling stream %q: %v", expectedStream, err)
		}

		verifySnapshot(t, suffix, buf.String())
		return
	}
	t.Fatalf("test stream %q not found in catalog", expectedStream)
}
