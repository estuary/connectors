package main

import (
	"context"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	log "github.com/sirupsen/logrus"
)

// TestSimpleCapture exercises the simplest use-case of a capture first doing an initial
// backfill and subsequently capturing new rows using an updated_at column for the cursor.
func TestSimpleCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY NOT NULL, data CLOB, updated_at TIMESTAMP)")
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "UPDATED_AT")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestAsyncCapture performs a capture with periodic restarts, in parallel with a bunch of inserts.
func TestAsyncCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INT PRIMARY KEY NOT NULL, data CLOB)")

	// Ignore SourcedSchema updates as their precise position in the change stream is
	// unpredictable when we're restarting the capture in parallel with changes.
	cs.Validator = &st.OrderedCaptureValidator{IncludeSourcedSchemas: false}

	// Have to sanitize the index within the polling interval because we're running
	// the capture in parallel with changes.
	cs.Sanitizers[`"index":999`] = regexp.MustCompile(`"index":[0-9]+`)

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		// Spawn a worker thread which will insert 50 rows of data in parallel with the capture.
		var insertsDone atomic.Bool
		go func() {
			for i := 0; i < 250; i++ {
				time.Sleep(100 * time.Millisecond)
				executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName), i, fmt.Sprintf("Value for row %d", i))
				log.WithField("i", i).Debug("inserted row")
			}
			time.Sleep(1 * time.Second)
			insertsDone.Store(true)
		}()

		// Run the capture over and over for 5 seconds each time until all inserts have finished, then verify results.
		for !insertsDone.Load() {
			var captureCtx, cancelCapture = context.WithCancel(ctx)
			time.AfterFunc(5*time.Second, cancelCapture)
			cs.Capture(captureCtx, t, nil)
		}
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestModificationsAndDeletions exercises the capture of modified and deleted rows
// by performing a series of INSERT, UPDATE, and DELETE operations.
func TestModificationsAndDeletions(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        version INTEGER,
        description CLOB,
        updated_at TIMESTAMP
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "UPDATED_AT")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial inserts
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, version, description, updated_at)
                VALUES (?, ?, ?, ?)`, tableName),
				i, 1, fmt.Sprintf("Initial version of row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Modify some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET version = version + 1,
            description = description || ' (updated)',
            updated_at = ?
            WHERE id IN (1, 3)`, tableName), baseTime.Add(5*time.Minute))
		cs.Capture(ctx, t, nil)

		// Delete some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            DELETE FROM %s WHERE id IN (2, 4)`, tableName))
		cs.Capture(ctx, t, nil)

		// Insert new rows with reused IDs
		executeControlQuery(t, control, fmt.Sprintf(`
            INSERT INTO %s (id, version, description, updated_at)
            VALUES (?, ?, ?, ?), (?, ?, ?, ?)`, tableName),
			2, 1, "Reused ID 2", baseTime.Add(6*time.Minute),
			4, 1, "Reused ID 4", baseTime.Add(6*time.Minute))
		cs.Capture(ctx, t, nil)

		// Final update to all remaining rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET version = version + 1,
            description = description || ' (final update)',
            updated_at = ?`, tableName), baseTime.Add(7*time.Minute))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithEmptyPoll exercises the scenario where a polling interval finds no new rows.
func TestCaptureWithEmptyPoll(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        data CLOB,
        updated_at TIMESTAMP
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "UPDATED_AT")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch of rows
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)`, tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// No changes - this capture should find no rows
		cs.Capture(ctx, t, nil)

		// Second batch of rows
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)`, tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i+10)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestFullRefresh exercises the scenario of a table without a configured cursor,
// which causes the capture to perform a full refresh every time it runs.
func TestFullRefresh(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, "(id INTEGER PRIMARY KEY NOT NULL, data CLOB)")

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0]) // Empty cursor means full refresh behavior

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tableName),
				i, fmt.Sprintf("Value for row %d", i))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureFromView exercises discovery and capture from a view with an updated_at cursor.
func TestCaptureFromView(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var baseTableName, tableID = testTableName(t, uniqueTableID(t))
	var viewName, viewID = testTableName(t, uniqueTableID(t, "view"))

	// Create base table and view
	createTestTable(t, control, baseTableName, `(
		id INTEGER PRIMARY KEY NOT NULL,
		name CLOB,
		visible SMALLINT,
		updated_at TIMESTAMP
	)`)
	executeControlQuery(t, control, fmt.Sprintf(`
		CREATE VIEW %s AS
		SELECT id, name, updated_at
		FROM %s
		WHERE visible = 1`, viewName, baseTableName))
	t.Cleanup(func() { control.Exec(fmt.Sprintf("DROP VIEW %s", viewName)) })

	// By default views should not be discovered.
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithoutViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Enable view discovery and re-discover bindings.
	cs.EndpointSpec.(*Config).Advanced.DiscoverViews = true
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(tableID), regexp.MustCompile(viewID))
	t.Run("DiscoveryWithViews", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	// Update both the base table and the view incrementally using the updated_at column.
	setResourceCursor(t, cs.Bindings[0], "UPDATED_AT")
	setResourceCursor(t, cs.Bindings[1], "UPDATED_AT")

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// First batch: Insert rows into base table, some visible in view
		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s VALUES (?, ?, ?, ?)`, baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Second batch: More rows with later timestamps
		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s VALUES (?, ?, ?, ?)`, baseTableName),
				i, fmt.Sprintf("Row %d", i), i%2 == 0, // Even numbered rows are visible
				baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows to change their visibility
		executeControlQuery(t, control, fmt.Sprintf(`
			UPDATE %s SET visible = 1 - visible, updated_at = ?
			WHERE id IN (2, 3, 4)`, baseTableName),
			baseTime.Add(20*time.Minute))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessCapture exercises discovery and capture from a table without
// a defined primary key, but using a timestamp column for the cursor.
func TestKeylessCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        data CLOB,
        value INTEGER,
        updated_at TIMESTAMP
    )`) // Intentionally no PRIMARY KEY

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "UPDATED_AT")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (data, value, updated_at) VALUES (?, ?, ?)`, tableName),
				fmt.Sprintf("Initial row %d", i), i, baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Add more rows
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (data, value, updated_at) VALUES (?, ?, ?)`, tableName),
				fmt.Sprintf("Additional row %d", i), i, baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Update some rows
		executeControlQuery(t, control, fmt.Sprintf(`
            UPDATE %s SET data = data || ' (updated)', updated_at = ?
            WHERE value >= 3 AND value <= 7`, tableName),
			baseTime.Add(10*time.Minute))
		cs.Capture(ctx, t, nil)

		// Delete and reinsert rows
		executeControlQuery(t, control, fmt.Sprintf(`
            DELETE FROM %s WHERE value = 4 OR value = 6`, tableName))
		executeControlQuery(t, control, fmt.Sprintf(`
            INSERT INTO %s (data, value, updated_at) VALUES (?, ?, ?), (?, ?, ?)`, tableName),
			"Reinserted row 4", 4, baseTime.Add(11*time.Minute),
			"Reinserted row 6", 6, baseTime.Add(11*time.Minute))
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestKeylessFullRefreshCapture exercises discovery and capture from a table
// without a defined primary key, and with the cursor explicitly unset to test
// full-refresh behavior.
func TestKeylessFullRefreshCapture(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(data CLOB, value INTEGER)`) // No PRIMARY KEY

	// Discover the table and verify discovery snapshot
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

	// Empty cursor forces full refresh behavior
	setResourceCursor(t, cs.Bindings[0])

	t.Run("Discovery", func(t *testing.T) {
		cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings))
	})

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// Initial batch of data
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control,
				fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
				fmt.Sprintf("Initial row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Add more rows - these should appear in the next full refresh
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control,
				fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
				fmt.Sprintf("Additional row %d", i), i)
		}
		cs.Capture(ctx, t, nil)

		// Modify some existing rows - changes should appear in next full refresh
		executeControlQuery(t, control, fmt.Sprintf(
			"UPDATE %s SET data = data || ' (updated)' WHERE value >= 3 AND value <= 7",
			tableName))
		cs.Capture(ctx, t, nil)

		// Delete some rows and add new ones - changes should appear in next full refresh
		executeControlQuery(t, control,
			fmt.Sprintf("DELETE FROM %s WHERE value = 4 OR value = 6", tableName))
		executeControlQuery(t, control,
			fmt.Sprintf("INSERT INTO %s (data, value) VALUES (?, ?)", tableName),
			"New row A", 20)
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestSourceTag verifies the output of a capture with /advanced/source_tag set
func TestSourceTag(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY NOT NULL, data CLOB)`)
	cs.EndpointSpec.(*Config).Advanced.SourceTag = "example_source_tag_1234"
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'one'), (2, 'two')", tableName))
		cs.Capture(ctx, t, nil)
		executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (3, 'three'), (4, 'four')", tableName))
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestTypedTable exercises discovery and capture from a "typed table", which is a table
// defined using a user-defined structured type. Typed tables are identified by TYPE='U'
// in SYSCAT.TABLES (vs 'T' for regular tables). This test verifies there are no gotchas
// with typed tables compared to regular tables.
func TestTypedTable(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var uniqueID = uniqueTableID(t)
	var typeName = fmt.Sprintf("%s.TYPED_%s_T", *testSchemaName, uniqueID)
	var tableName = fmt.Sprintf("%s.TYPED_%s", *testSchemaName, uniqueID)

	// Create the structured type with REF USING INTEGER so we can construct OID references.
	control.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	control.Exec(fmt.Sprintf("DROP TYPE %s", typeName))
	executeControlQuery(t, control, fmt.Sprintf(`
		CREATE TYPE %s AS (
			id INTEGER,
			name VARCHAR(100),
			updated_at TIMESTAMP
		) REF USING INTEGER MODE DB2SQL`, typeName))
	t.Cleanup(func() { control.Exec(fmt.Sprintf("DROP TYPE %s", typeName)) })

	// Create a typed table using the structured type.
	// USER GENERATED means we provide the OID values using the type constructor.
	executeControlQuery(t, control, fmt.Sprintf(`
		CREATE TABLE %s OF %s (
			REF IS oid USER GENERATED,
			id WITH OPTIONS NOT NULL PRIMARY KEY
		)`, tableName, typeName))
	t.Cleanup(func() { control.Exec(fmt.Sprintf("DROP TABLE %s", tableName)) })

	// Discover the typed table
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	if len(cs.Bindings) == 0 {
		t.Fatal("typed table was not discovered")
	}
	setResourceCursor(t, cs.Bindings[0], "UPDATED_AT")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)

		// Insert rows into the typed table.
		// With USER GENERATED OID, we provide the OID using the type constructor.
		for i := 0; i < 5; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (oid, id, name, updated_at) VALUES (%s(%d), ?, ?, ?)",
				tableName, typeName, i),
				i, fmt.Sprintf("Row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		// Insert more rows
		for i := 5; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf(
				"INSERT INTO %s (oid, id, name, updated_at) VALUES (%s(%d), ?, ?, ?)",
				tableName, typeName, i),
				i, fmt.Sprintf("Row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		cupaloy.SnapshotT(t, cs.Summary())
	})
}
