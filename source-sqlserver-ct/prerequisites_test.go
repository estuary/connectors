package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func TestPrerequisites(t *testing.T) {
	t.Run("validateExistingTables", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create tables A and B - both exist and can be validated
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.Exec(t, `INSERT INTO <NAME>_a VALUES (0, 'hello'), (1, 'world')`)
		tc.Discover("Discover Tables")
		tc.Run("Validate And Capture", -1)
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})

	t.Run("captureMissingTable", func(t *testing.T) {
		var db, tc = blackboxTestSetup(t)
		// Create tables A and B, but drop B after discovery
		db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
		db.Exec(t, `INSERT INTO <NAME>_a VALUES (0, 'hello'), (1, 'world')`)
		tc.Discover("Discover Tables")
		db.Exec(t, `DROP TABLE <NAME>_b`)
		tc.Run("Capture With Missing Table", -1)
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})
}
