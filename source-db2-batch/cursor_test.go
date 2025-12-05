package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
)

// TestBinaryCursorRejected verifies that binary columns cannot be used as cursor columns.
// Binary types don't survive JSON serialization round-trip correctly - []byte becomes
// a base64 string which DB2 won't accept as a binary parameter.
func TestBinaryCursorRejected(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        cursor_col VARBINARY(10),
        data VARCHAR(100)
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	// Use the VARBINARY column as the cursor instead of ID
	setResourceCursor(t, cs.Bindings[0], "CURSOR_COL")

	// Insert rows
	for _, row := range [][]any{
		{1, []byte{0x00, 0x01}, "first"},
		{2, []byte{0x00, 0x02}, "second"},
		{3, []byte{0x00, 0x03}, "third"},
	} {
		executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s (id, cursor_col, data) VALUES (?, ?, ?)`, tableName), row...)
	}

	// Capture should fail with an error about binary cursor columns
	setShutdownAfterQuery(t, true)
	cs.Capture(ctx, t, nil)

	// Check that we got an error about binary cursor columns
	if len(cs.Errors) == 0 {
		t.Fatalf("expected error when using binary cursor column, got success")
	}
	var foundExpectedError bool
	for _, err := range cs.Errors {
		if strings.Contains(err.Error(), "binary column types") && strings.Contains(err.Error(), "cannot be used as cursor columns") {
			foundExpectedError = true
			t.Logf("correctly rejected binary cursor column with error: %v", err)
			break
		}
	}
	if !foundExpectedError {
		t.Fatalf("expected error about binary cursor columns, got: %v", cs.Errors)
	}
}

// TestCaptureWithTwoColumnCursor exercises the use-case of a capture using
// a multiple-column compound cursor.
func TestCaptureWithTwoColumnCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        major INTEGER,
        minor INTEGER,
        data VARCHAR(100)
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "MAJOR", "MINOR")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// First batch with lower version numbers
		for _, row := range [][]any{
			{0, 1, 1, "v1.1"}, {1, 1, 2, "v1.2"}, {2, 1, 3, "v1.3"},
			{3, 2, 1, "v2.1"}, {4, 2, 2, "v2.2"}, {5, 2, 3, "v2.3"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)

		// Second batch with higher version numbers
		for _, row := range [][]any{
			{6, 0, 9, "Value ignored because major is too small"},
			{7, 2, 0, "Value ignored because minor is too small"},
			{8, 3, 1, "v3.1"}, {9, 3, 2, "v3.2"}, {10, 3, 3, "v3.3"},
			{11, 4, 1, "v4.1"}, {12, 4, 2, "v4.2"}, {13, 4, 3, "v4.3"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestNonexistentCursor exercises the situation of a capture whose configured
// cursor column doesn't actually exist.
func TestNonexistentCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY NOT NULL, data CLOB)`)
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'hello'), (2, 'world')", tableName))

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "UPDATED_AT") // Nonexistent column

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestTimestampCursorRoundTrip tests that TIMESTAMP column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestTimestampCursorRoundTrip(t *testing.T) {
	var ctx, control = context.Background(), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY NOT NULL, data VARCHAR(100), updated_at TIMESTAMP)`)

	for _, tc := range []struct {
		name     string
		timezone string
	}{
		{"UTC", "UTC"},
		{"Chicago", "America/Chicago"},
		{"PositiveOffset", "+04:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = testCaptureSpec(t)
			cs.EndpointSpec.(*Config).Advanced.Timezone = tc.timezone
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
			setResourceCursor(t, cs.Bindings[0], "UPDATED_AT")

			setShutdownAfterQuery(t, true)
			baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

			// Clear the table before each timezone test
			control.Exec(fmt.Sprintf("DELETE FROM %s", tableName))

			// Initial batch of rows
			for i := range 5 {
				executeControlQuery(t, control, fmt.Sprintf(
					"INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
					i, fmt.Sprintf("Initial row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
			}
			cs.Capture(ctx, t, nil)

			// Second batch to be captured using cursor
			for i := 5; i < 10; i++ {
				executeControlQuery(t, control, fmt.Sprintf(
					"INSERT INTO %s (id, data, updated_at) VALUES (?, ?, ?)", tableName),
					i, fmt.Sprintf("Second batch row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
			}
			cs.Capture(ctx, t, nil)

			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}

// TestDecimalCursorRoundTrip tests that DECIMAL column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestDecimalCursorRoundTrip(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		id INTEGER PRIMARY KEY NOT NULL,
		data VARCHAR(100),
		sequence DECIMAL(18,2)
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "SEQUENCE")
	setShutdownAfterQuery(t, true)

	for i := range 5 {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, sequence) VALUES (?, ?, ?)", tableName),
			i, fmt.Sprintf("Initial row %d", i), float64(100+i*10)+0.50)
	}
	cs.Capture(ctx, t, nil)
	for i := 5; i < 10; i++ {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, sequence) VALUES (?, ?, ?)", tableName),
			i, fmt.Sprintf("Second batch row %d", i), float64(100+i*10)+0.50)
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestIntegerCursorRoundTrip tests that INTEGER column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestIntegerCursorRoundTrip(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		id INTEGER PRIMARY KEY NOT NULL,
		data VARCHAR(100),
		sequence INTEGER
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "SEQUENCE")
	setShutdownAfterQuery(t, true)

	for i := range 5 {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, sequence) VALUES (?, ?, ?)", tableName),
			i, fmt.Sprintf("Initial row %d", i), 100+i*10)
	}
	cs.Capture(ctx, t, nil)
	for i := 5; i < 10; i++ {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, sequence) VALUES (?, ?, ?)", tableName),
			i, fmt.Sprintf("Second batch row %d", i), 100+i*10)
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestStringCursorRoundTrip tests that VARCHAR column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestStringCursorRoundTrip(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		id INTEGER PRIMARY KEY NOT NULL,
		data VARCHAR(100),
		version VARCHAR(20)
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "VERSION")
	setShutdownAfterQuery(t, true)

	// Use lexicographically ordered version strings
	versions := []string{"v1.0.0", "v1.0.1", "v1.0.2", "v1.1.0", "v1.1.1", "v2.0.0", "v2.0.1", "v2.1.0", "v2.1.1", "v3.0.0"}

	for i := range 5 {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, version) VALUES (?, ?, ?)", tableName),
			i, fmt.Sprintf("Initial row %d", i), versions[i])
	}
	cs.Capture(ctx, t, nil)
	for i := 5; i < 10; i++ {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, version) VALUES (?, ?, ?)", tableName),
			i, fmt.Sprintf("Second batch row %d", i), versions[i])
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}
