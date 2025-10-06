package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
)

// TestTemporalTables_SystemVersioned tests MariaDB system-versioned (temporal) tables.
// System-versioned tables automatically track historical row versions with ROW_START and ROW_END timestamps.
//
// TODO(wgd): Test with explicit column syntax to see if behavior differs:
//   CREATE TABLE t(
//     x INT,
//     start_timestamp TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
//     end_timestamp TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
//     PERIOD FOR SYSTEM_TIME(start_timestamp, end_timestamp)
//   ) WITH SYSTEM VERSIONING;
// Also experiment with the INVISIBLE modifier to understand when row_start/row_end
// columns are discovered vs. hidden, and whether they become part of the primary key.
func TestTemporalTables_SystemVersioned(t *testing.T) {
	if os.Getenv("TEST_MARIADB") != "yes" {
		t.Skip("skipping MariaDB temporal table test: ${TEST_MARIADB} != \"yes\"")
	}

	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT) WITH SYSTEM VERSIONING")

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	setShutdownAfterCaughtUp(t, true)

	t.Run("Discover", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("Capture", func(t *testing.T) {
		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))

		// Initial backfill
		tb.Insert(ctx, t, tableName, [][]any{{1, "one"}, {2, "two"}, {3, "three"}})
		cs.Capture(ctx, t, nil)

		// Updates and deletes - these create new row versions in the history
		tb.Update(ctx, t, tableName, "id", 1, "data", "one_updated")
		tb.Update(ctx, t, tableName, "id", 2, "data", "two_updated")
		tb.Delete(ctx, t, tableName, "id", 3)
		tb.Insert(ctx, t, tableName, [][]any{{4, "four"}})
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestTemporalTables_ApplicationPeriod tests MariaDB tables with application-time periods.
// Application-time periods allow tracking business validity periods separate from database change history.
func TestTemporalTables_ApplicationPeriod(t *testing.T) {
	if os.Getenv("TEST_MARIADB") != "yes" {
		t.Skip("skipping MariaDB temporal table test: ${TEST_MARIADB} != \"yes\"")
	}

	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, `(
		id INTEGER PRIMARY KEY,
		data TEXT,
		valid_from DATE,
		valid_to DATE,
		PERIOD FOR valid_period(valid_from, valid_to)
	)`)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	setShutdownAfterCaughtUp(t, true)

	t.Run("Discover", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("Capture", func(t *testing.T) {
		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))

		// Initial backfill with application-time period data
		tb.Insert(ctx, t, tableName, [][]any{
			{1, "contract_a", "2024-01-01", "2024-12-31"},
			{2, "contract_b", "2024-06-01", "2025-06-01"},
			{3, "contract_c", "2023-01-01", "2024-03-31"},
		})
		cs.Capture(ctx, t, nil)

		// Updates to application-time periods and regular replication
		tb.Update(ctx, t, tableName, "id", 1, "valid_to", "2025-12-31")
		tb.Insert(ctx, t, tableName, [][]any{
			{4, "contract_d", "2025-01-01", "2026-01-01"},
		})
		tb.Delete(ctx, t, tableName, "id", 3)
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestTemporalTables_Bitemporal tests MariaDB bitemporal tables.
// Bitemporal tables combine system versioning (tracking database changes) with application-time periods
// (tracking business validity), providing both system time and application time dimensions.
func TestTemporalTables_Bitemporal(t *testing.T) {
	if os.Getenv("TEST_MARIADB") != "yes" {
		t.Skip("skipping MariaDB temporal table test: ${TEST_MARIADB} != \"yes\"")
	}

	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = uniqueTableID(t)
	var tableName = tb.CreateTable(ctx, t, uniqueID, `(
		id INTEGER PRIMARY KEY,
		data TEXT,
		valid_from DATE,
		valid_to DATE,
		row_start TIMESTAMP(6) AS ROW START INVISIBLE,
		row_end TIMESTAMP(6) AS ROW END INVISIBLE,
		PERIOD FOR valid_period(valid_from, valid_to),
		PERIOD FOR SYSTEM_TIME(row_start, row_end)
	) WITH SYSTEM VERSIONING`)

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	setShutdownAfterCaughtUp(t, true)

	t.Run("Discover", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("Capture", func(t *testing.T) {
		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))

		// Initial backfill with both system versioning and application-time periods
		tb.Insert(ctx, t, tableName, [][]any{
			{1, "employee_alice", "2024-01-01", "2024-12-31"},
			{2, "employee_bob", "2024-03-01", "2025-03-01"},
		})
		cs.Capture(ctx, t, nil)

		// Updates affect both system time (automatically) and application time (when specified)
		tb.Update(ctx, t, tableName, "id", 1, "data", "employee_alice_promoted")
		tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET valid_to = '2025-12-31' WHERE id = 2", tableName))
		tb.Insert(ctx, t, tableName, [][]any{
			{3, "employee_carol", "2025-01-01", "2026-01-01"},
		})
		tb.Delete(ctx, t, tableName, "id", 1)
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}
