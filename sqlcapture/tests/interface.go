package tests

import (
	"context"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
)

// TestBackend defines the methods necessary to set up test scenarios on a specific
// database (for instance, PostgreSQL or MySQL) and execute captures against that
// same database.
//
// There are methods to create temporary test tables and add/modify/delete rows in
// those tables, and another method that returns a new `sqlcapture.Database` which
// can be fed into the generic `sqlcapture` machinery to perform discovery/captures.
type TestBackend interface {
	// CreateTable creates a new database table whose name is based on the current test
	// name. If `suffix` is non-empty it should be included at the end of the new table's
	// name. The table will be registered with `t.Cleanup()` to be deleted at the end of
	// the current test.
	CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string
	// Insert adds all provided rows to the specified table in a single transaction.
	Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{})
	// Update modifies preexisting rows to a new value.
	Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{})
	// Delete removes preexisting rows.
	Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{})
	// GetDatabase returns a new sqlcapture.Database which can be used to perform
	// discovery and captures.
	GetDatabase() sqlcapture.Database
}
