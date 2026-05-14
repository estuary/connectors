package main

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
)

func TestSecondaryIndexDiscovery(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()

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
	t.Run("index_and_fk", func(t *testing.T) {
		// It's possible to have multiple constraints on the same table with the same constraint
		// name but different types. This shouldn't matter now that our secondary index discovery
		// query no longer joins against `information_schema.table_constraints` at all, but this
		// test case just makes sure that scenario doesn't break anything.
		var uniqueA, uniqueB = "g26313", "g16025"
		var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
		var shortNameA = tableA[strings.Index(tableA, ".")+1:]
		var tableB = tb.CreateTable(ctx, t, uniqueB, fmt.Sprintf("(id INTEGER NOT NULL, parent_id INTEGER, CONSTRAINT FK_%[1]s_ParentID FOREIGN KEY (parent_id) REFERENCES %[1]s(id), CONSTRAINT FK_%[1]s_ParentID UNIQUE (parent_id))", shortNameA))
		var shortNameB = tableB[strings.Index(tableB, ".")+1:]
		tb.Query(ctx, t, fmt.Sprintf(`CREATE UNIQUE INDEX IX_%[1]s_ParentID ON %s(id)`, shortNameB, tableB))
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueB)))
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

// TestTrickyEnumValues tests some "gotcha" enum values to make sure the discovery parsing logic is robust.
func TestTrickyEnumValues(t *testing.T) {
	var tb, ctx, uniqueID = mysqlTestBackend(t), context.Background(), uniqueTableID(t)
	tb.CreateTable(ctx, t, uniqueID, `(id INTEGER PRIMARY KEY, category ENUM('A', 'B (Parentheses)', '', 'Internal''Quote', 'Internal,Comma', 'Internal\nNewline', 'Internal;Semicolon', '   Leading Spaces', 'Trailing Spaces   ', 'Z'))`)
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

// TestDiscoveryFiltersApply exercises the post-query filtering logic for
// various combinations of exclude-schemas and table-name patterns. The
// include-schemas list is intentionally not covered here, since it is pushed
// into the listTables query rather than handled by apply().
func TestDiscoveryFiltersApply(t *testing.T) {
	var input = []sqlcapture.TableID{
		{Schema: "a", Table: "users"},
		{Schema: "a", Table: "users_evt"},
		{Schema: "a", Table: "events_log"},
		{Schema: "b", Table: "users"},
		{Schema: "b", Table: "users_evt"},
		{Schema: "internal", Table: "secret"},
	}
	var cases = []struct {
		name     string
		filters  discoveryFilters
		expected []sqlcapture.TableID
	}{
		{
			name:     "Passthrough",
			filters:  discoveryFilters{},
			expected: input,
		},
		{
			name:    "ExcludeOneSchema",
			filters: discoveryFilters{ExcludeSchemas: []string{"internal"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "a", Table: "users_evt"},
				{Schema: "a", Table: "events_log"},
				{Schema: "b", Table: "users"},
				{Schema: "b", Table: "users_evt"},
			},
		},
		{
			name:    "ExcludeMultipleSchemas",
			filters: discoveryFilters{ExcludeSchemas: []string{"internal", "b"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "a", Table: "users_evt"},
				{Schema: "a", Table: "events_log"},
			},
		},
		{
			// A bare literal pattern matches only the exact table name, proving
			// that the pattern is implicitly anchored. The "users_evt" tables
			// must not be included by the "users" pattern.
			name:    "LiteralPatternIsAnchored",
			filters: discoveryFilters{TablePatterns: []string{"users"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "b", Table: "users"},
			},
		},
		{
			name:    "MultiplePatternsOR",
			filters: discoveryFilters{TablePatterns: []string{"users", "secret"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "b", Table: "users"},
				{Schema: "internal", Table: "secret"},
			},
		},
		{
			// A single pattern with top-level alternation must still anchor
			// against the full table name. Without the implicit `(?:...)`
			// wrapper this would parse as "^users" OR "secret$", which would
			// incorrectly match "users_evt".
			name:    "AlternationInSinglePattern",
			filters: discoveryFilters{TablePatterns: []string{"users|secret"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "b", Table: "users"},
				{Schema: "internal", Table: "secret"},
			},
		},
		{
			name:    "WildcardSuffix",
			filters: discoveryFilters{TablePatterns: []string{".*_evt"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users_evt"},
				{Schema: "b", Table: "users_evt"},
			},
		},
		{
			name: "ExcludeAndPattern",
			filters: discoveryFilters{
				ExcludeSchemas: []string{"b"},
				TablePatterns:  []string{".*_evt", "events_.*"},
			},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users_evt"},
				{Schema: "a", Table: "events_log"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got, err = tc.filters.apply(input)
			if err != nil {
				t.Fatalf("apply() unexpected error: %v", err)
			}
			if !slices.Equal(got, tc.expected) {
				t.Errorf("apply() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestDiscoveryFiltersValidate checks that the table-pattern regex compilation
// catches malformed patterns at config-validation time.
func TestDiscoveryFiltersValidate(t *testing.T) {
	var cases = []struct {
		name    string
		filters discoveryFilters
		wantErr bool
	}{
		{"Empty", discoveryFilters{}, false},
		{"GoodPatterns", discoveryFilters{TablePatterns: []string{"foo", "bar.*", "alpha|beta"}}, false},
		{"BadPattern", discoveryFilters{TablePatterns: []string{"["}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err = tc.filters.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// TestTablePatterns exercises end-to-end discovery against a live database
// with various TablePatterns configurations.
func TestTablePatterns(t *testing.T) {
	var tb, ctx, uniqueID = mysqlTestBackend(t), context.Background(), uniqueTableID(t)
	tb.CreateTable(ctx, t, uniqueID+"_alpha_evt", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.CreateTable(ctx, t, uniqueID+"_beta_evt", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.CreateTable(ctx, t, uniqueID+"_alpha_log", "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.CreateTable(ctx, t, uniqueID+"_beta_log", "(id INTEGER PRIMARY KEY, data TEXT)")

	t.Run("Default", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("EventsOnly", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.EndpointSpec.(*Config).DiscoveryFilters.TablePatterns = []string{`.*_evt`}
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("AlphaAlternation", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.EndpointSpec.(*Config).DiscoveryFilters.TablePatterns = []string{`.*_alpha_evt|.*_alpha_log`}
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("NoMatch", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.EndpointSpec.(*Config).DiscoveryFilters.TablePatterns = []string{`nonexistent`}
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
}

// TestSchemaWhitelist tests table discovery with an explicit whitelist of schema names
func TestSchemaWhitelist(t *testing.T) {
	var tb, ctx, uniqueID = mysqlTestBackend(t), context.Background(), uniqueTableID(t)
	tb.CreateTable(ctx, t, uniqueID, `(id INTEGER PRIMARY KEY, data TEXT)`)
	t.Run("Default", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("Included", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"nonexistent_schema", testSchemaName, "another_nonexistent_schema"}
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	t.Run("Excluded", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t)
		cs.EndpointSpec.(*Config).Advanced.DiscoverSchemas = []string{"nonexistent_schema", "another_nonexistent_schema"}
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
}
