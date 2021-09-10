package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/estuary/protocols/airbyte"
)

// TODO(wgd): Figure out if there's a better way to specify this. Maybe
// a flag with this as default value? How do flags work in `go test` again?
const TestingConnectionURI = "postgres://flow:flow@localhost:5432/flow"

func TestTrivialPass(t *testing.T) {
	t.Logf("Trivially Passing Test: %v", "Woo!")
}

func TestDiscoverySimple(t *testing.T) {
	cfg, ctx := testDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(a INTEGER PRIMARY KEY, b TEXT, c REAL, d VARCHAR(255))")

	// Create the table (with deferred cleanup), perform discovery, and verify
	// that the stream as discovered matches the golden snapshot.
	catalog, err := DiscoverCatalog(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	verifyStream(t, "", catalog, tableName)
}

func verifyStream(t *testing.T, suffix string, catalog *airbyte.Catalog, expectedStream string) {
	t.Helper()
	for _, stream := range catalog.Streams {
		t.Logf("catalog stream %q", stream.Name)
		if !strings.EqualFold(stream.Name, expectedStream) {
			continue
		}
		bs, err := json.Marshal(stream)
		if err != nil {
			t.Fatalf("error marshalling stream %q: %v", expectedStream, err)
		}
		verifySnapshot(t, suffix, string(bs))
		return
	}
	t.Fatalf("test stream %q not found in catalog", expectedStream)
}
