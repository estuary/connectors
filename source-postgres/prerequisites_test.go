package main

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

// TestPrerequisites verifies that the connector properly validates table existence
// before capture. It creates three tables, discovers them, then drops one and verifies
// that subsequent capture attempts fail with an appropriate error message.
func TestPrerequisites(t *testing.T) {
	var db, tc = blackboxTestSetup(t)

	// Create tables A (with data), B (empty), and C (will be dropped later)
	db.CreateTable(t, `<NAME>_a`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_b`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.CreateTable(t, `<NAME>_c`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME>_a VALUES (0, 'hello'), (1, 'world')`)

	// Discover all three tables
	tc.Discover("Discover Tables A, B, C")

	// Capture with all tables present - should succeed
	tc.Run("Capture A+B+C (All Tables Exist)", -1)

	// Drop table C and try to capture again - should fail with prerequisite error
	db.Exec(t, `DROP TABLE <NAME>_c`)
	tc.Run("Capture A+B+C (Table C Dropped)", -1)

	cupaloy.SnapshotT(t, tc.Transcript.String())
}
