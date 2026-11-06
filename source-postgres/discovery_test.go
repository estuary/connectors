package main

import (
	"fmt"
	"regexp"
	"slices"
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

// TestDiscoveryFiltersApply exercises the post-query filtering logic for various
// combinations of exclude-schemas and table patterns. The include-schemas list
// is not covered here, since it is pushed into the listTables query rather than
// handled by apply().
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
			// A literal basename pattern matches only the exact table name,
			// proving that patterns are implicitly anchored. The "users_evt"
			// tables must not be included by the "users" pattern.
			name:    "BasenameLiteral",
			filters: discoveryFilters{TablePatterns: []string{"users"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "b", Table: "users"},
			},
		},
		{
			name:    "BasenameWildcard",
			filters: discoveryFilters{TablePatterns: []string{"*_evt"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users_evt"},
				{Schema: "b", Table: "users_evt"},
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
			// A pattern containing a '.' is qualified and matches against
			// schema and table independently.
			name:    "QualifiedLiteral",
			filters: discoveryFilters{TablePatterns: []string{"a.users"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
			},
		},
		{
			name:    "QualifiedWildcardTable",
			filters: discoveryFilters{TablePatterns: []string{"a.*"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "a", Table: "users_evt"},
				{Schema: "a", Table: "events_log"},
			},
		},
		{
			name:    "QualifiedWildcardSchema",
			filters: discoveryFilters{TablePatterns: []string{"*.users"}},
			expected: []sqlcapture.TableID{
				{Schema: "a", Table: "users"},
				{Schema: "b", Table: "users"},
			},
		},
		{
			name: "ExcludeAndPattern",
			filters: discoveryFilters{
				ExcludeSchemas: []string{"b"},
				TablePatterns:  []string{"*_evt", "events_*"},
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

// TestDiscoveryFiltersValidate checks that malformed table patterns are caught
// at config-validation time.
func TestDiscoveryFiltersValidate(t *testing.T) {
	var cases = []struct {
		name    string
		filters discoveryFilters
		wantErr bool
	}{
		{"Empty", discoveryFilters{}, false},
		{"GoodPatterns", discoveryFilters{TablePatterns: []string{"foo", "bar_*", "a.b"}}, false},
		{"EmptyTableComponent", discoveryFilters{TablePatterns: []string{"foo."}}, true},
		{"TrailingBackslash", discoveryFilters{TablePatterns: []string{`foo\`}}, true},
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

// TestTablePatterns exercises end-to-end discovery against a live database with
// various TablePatterns configurations.
func TestTablePatterns(t *testing.T) {
	var runSubcase = func(t *testing.T, patterns []string) {
		var db, tc = blackboxTestSetup(t)
		var id = uniqueTableID(t)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_alpha_evt", testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_beta_evt", testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_alpha_log", testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_beta_log", testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
		if patterns != nil {
			require.NoError(t, tc.Capture.EditConfig("discoveryFilters.table_patterns", patterns))
		}
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	}

	t.Run("Default", func(t *testing.T) { runSubcase(t, nil) })
	t.Run("EventsOnly", func(t *testing.T) { runSubcase(t, []string{"*_evt"}) })
	t.Run("AlphaOnly", func(t *testing.T) { runSubcase(t, []string{"*_alpha_evt", "*_alpha_log"}) })
	t.Run("NoMatch", func(t *testing.T) { runSubcase(t, []string{"nonexistent"}) })
}

// TestSchemaWhitelist exercises the schema include filter, including the union
// between the legacy Advanced.DiscoverSchemas and the new
// DiscoveryFilters.IncludeSchemas option.
func TestSchemaWhitelist(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("IncludeViaAdvanced", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		require.NoError(t, tc.Capture.EditConfig("advanced.discover_schemas", []string{testSchemaName}))
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("IncludeViaFilters", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		require.NoError(t, tc.Capture.EditConfig("discoveryFilters.include_schemas", []string{testSchemaName}))
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("Excluded", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		require.NoError(t, tc.Capture.EditConfig("advanced.discover_schemas", []string{"nonexistent_schema"}))
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("UnionOfBoth", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		require.NoError(t, tc.Capture.EditConfig("advanced.discover_schemas", []string{"nonexistent_schema"}))
		require.NoError(t, tc.Capture.EditConfig("discoveryFilters.include_schemas", []string{testSchemaName}))
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}
