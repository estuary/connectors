package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture/tests"
)

// TestMariaDBTemporalTables exercises the connector's handling of MariaDB
// temporal table flavors. Each subtest creates a table with a different
// declaration and then runs the same mutations and capture operations against it.
func TestMariaDBTemporalTables(t *testing.T) {
	if os.Getenv("TEST_MARIADB") != "yes" {
		t.Skip("Skipping MariaDB specific test, set TEST_MARIADB=yes to enable")
	}
	var tb, ctx = mysqlTestBackend(t), context.Background()
	tb.Query(ctx, t, "SET GLOBAL time_zone = 'UTC';")
	tb.Query(ctx, t, "SET SESSION time_zone = 'UTC';")
	// MariaDB refuses to ALTER a system-versioned table unless told what to do
	// with the historical rows. Keeping them is the only setting under which
	// history remains capturable, so it's the case worth testing.
	tb.Query(ctx, t, "SET SESSION system_versioning_alter_history = KEEP;")
	// Capturing system-versioned tables is opt-in via feature flag.
	tb.config.Advanced.FeatureFlags = "system_versioned_tables"

	for _, tc := range []struct {
		name     string
		tableDef string
	}{
		{
			// An application-time period over user-managed valid_from / valid_to columns.
			// There is no system versioning here, the PERIOD declaration is metadata only
			// and the connector should treat the period columns as ordinary data.
			name: "ApplicationVersioned",
			tableDef: `(
				id INT PRIMARY KEY,
				valid_from DATETIME NOT NULL DEFAULT '2026-01-01 00:00:00',
				valid_to DATETIME NOT NULL DEFAULT '2099-12-31 23:59:59',
				data TEXT,
				PERIOD FOR app_period (valid_from, valid_to)
			)`,
		},
		{
			// System versioning in implicit form. The row_start / row_end columns are hidden
			// from SELECT * and from information_schema, so the connector has to synthesize
			// their metadata in discovery and list them explicitly in backfill queries.
			name: "SystemVersionedImplicit",
			tableDef: `(
				id INT PRIMARY KEY,
				data TEXT
			) WITH SYSTEM VERSIONING`,
		},
		{
			// System versioning in explicit form. The row_start / row_end columns appear
			// normally in information_schema.columns, and MariaDB automatically extends
			// the primary key with row_end.
			name: "SystemVersionedExplicit",
			tableDef: `(
				id INT PRIMARY KEY,
				data TEXT,
				row_start TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
				row_end TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
				PERIOD FOR SYSTEM_TIME (row_start, row_end)
			) WITH SYSTEM VERSIONING`,
		},
		{
			// System versioning in explicit form with user-chosen names for the
			// period columns. MariaDB permits any names and the connector should
			// pick them up from information_schema correctly.
			name: "SystemVersionedExplicitRenamed",
			tableDef: `(
				id INT PRIMARY KEY,
				data TEXT,
				sys_start TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
				sys_end TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
				PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
			) WITH SYSTEM VERSIONING`,
		},
		{
			// Bitemporal: system versioning combined with an application time period.
			// The application-period columns should flow through as ordinary data and
			// the system-versioning behavior should be unaffected by their presence.
			name: "Bitemporal",
			tableDef: `(
				id INT PRIMARY KEY,
				valid_from DATETIME NOT NULL DEFAULT '2026-01-01 00:00:00',
				valid_to DATETIME NOT NULL DEFAULT '2099-12-31 23:59:59',
				data TEXT,
				PERIOD FOR app_period (valid_from, valid_to)
			) WITH SYSTEM VERSIONING`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var uniqueID = uniqueTableID(t)
			var table = tb.CreateTable(ctx, t, uniqueID, tc.tableDef)
			tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'one');", table))
			tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (2, 'two');", table))
			tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (3, 'three');", table))

			// Modify rows before the initial backfill so that system-versioned tables
			// already contain historical row versions, which the backfill is expected
			// to capture along with the current ones.
			tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET data = 'two-v2' WHERE id = 2;", table))
			tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE id = 3;", table))

			// Without the feature flag, system-versioned tables must stay out of
			// discovery entirely, while application-period tables are unaffected.
			t.Run("DiscoveryWithoutFlag", func(t *testing.T) {
				var unflagged = tb.CaptureSpec(ctx, t)
				unflagged.EndpointSpec.(*Config).Advanced.FeatureFlags = ""
				unflagged.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
			})

			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			cs.Validator = &st.OrderedCaptureValidator{}

			t.Run("Discovery", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
			t.Run("Backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

			tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (4, 'four');", table))
			tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET data = 'one-v2' WHERE id = 1;", table))
			tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET data = 'one-v3' WHERE id = 1;", table))
			tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET id = 5 WHERE id = 2;", table))
			tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE id = 4;", table))

			t.Run("Replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

			// Adding a column without an explicit position places it after the visible
			// columns but before any implicit row_start/row_end columns, so this checks
			// that replication keeps decoding row images correctly after the change.
			tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra INT;", table))
			tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s (id, data, extra) VALUES (6, 'six', 42);", table))
			tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET extra = 7 WHERE id = 1;", table))

			t.Run("SchemaChange", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
		})
	}
}
