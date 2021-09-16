package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/estuary/protocols/airbyte"
)

func TestDiscoverySimple(t *testing.T) {
	cfg, ctx := TestDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(a INTEGER PRIMARY KEY, b TEXT, c REAL, d VARCHAR(255))")

	// Create the table (with deferred cleanup), perform discovery, and verify
	// that the stream as discovered matches the golden snapshot.
	catalog, err := DiscoverCatalog(ctx, cfg)
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
		bs, err := json.Marshal(stream)
		if err != nil {
			t.Fatalf("error marshalling stream %q: %v", expectedStream, err)
		}
		verifySnapshot(t, suffix, string(bs))
		return
	}
	t.Fatalf("test stream %q not found in catalog", expectedStream)
}
