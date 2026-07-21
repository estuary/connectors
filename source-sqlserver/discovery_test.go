package main

import (
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestSchemaWhitelist(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("IncludeMatching", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		require.NoError(t, tc.Capture.EditConfig("discoveryFilters.include_schemas", []string{*testSchemaName}))
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("IncludeNonmatching", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		require.NoError(t, tc.Capture.EditConfig("discoveryFilters.include_schemas", []string{"nonexistent_schema"}))
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
	t.Run("Excluded", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		require.NoError(t, tc.Capture.EditConfig("discoveryFilters.exclude_schemas", []string{*testSchemaName}))
		tc.Discover("Discover Tables")
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}

func TestTablePatterns(t *testing.T) {
	var runSubcase = func(t *testing.T, patterns []string) {
		var db, tc = blackboxTestSetup(t)
		var id = uniqueTableID(t)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_alpha_evt", *testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_beta_evt", *testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_alpha_log", *testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, fmt.Sprintf("%s.tp_%s_beta_log", *testSchemaName, id), `(id INTEGER PRIMARY KEY, data TEXT)`)
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
