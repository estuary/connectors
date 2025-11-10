package main

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

// TestCaptureWithUpdatedAtCursor exercises the use-case of a capture using
// an updated_at timestamp column as the cursor.
func TestCaptureWithUpdatedAtCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        data NVARCHAR(MAX),
        updated_at DATETIME2
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		baseTime := time.Date(2025, 2, 13, 12, 0, 0, 0, time.UTC)

		for i := 0; i < 10; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)

		for i := 10; i < 20; i++ {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3)", tableName),
				i, fmt.Sprintf("Value for row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCaptureWithTwoColumnCursor exercises the use-case of a capture using
// a multiple-column compound cursor.
func TestCaptureWithTwoColumnCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY,
        major INTEGER,
        minor INTEGER,
        data NVARCHAR(MAX)
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "major", "minor")

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)

		// First batch with lower version numbers
		for _, row := range [][]any{
			{0, 1, 1, "v1.1"}, {1, 1, 2, "v1.2"}, {2, 1, 3, "v1.3"},
			{3, 2, 1, "v2.1"}, {4, 2, 2, "v2.2"}, {5, 2, 3, "v2.3"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3, @p4)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)

		// Second batch with higher version numbers
		for _, row := range [][]any{
			{6, 0, 9, "Value ignored because major is too small"},
			{7, 2, 0, "Value ignored because minor is too small"},
			{8, 3, 1, "v3.1"}, {9, 3, 2, "v3.2"}, {10, 3, 3, "v3.3"},
			{11, 4, 1, "v4.1"}, {12, 4, 2, "v4.2"}, {13, 4, 3, "v4.3"},
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s VALUES (@p1, @p2, @p3, @p4)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestQueryTemplates exercises the selection and execution of query templates
// for various combinations of resource spec and stream state.
func TestQueryTemplates(t *testing.T) {
	var testCases = []struct {
		name         string
		cursor       []string
		cursorValues []any
	}{
		{name: "SingleCursorFirstQuery", cursor: []string{"updated_at"}},
		{name: "SingleCursorSubsequentQuery", cursor: []string{"updated_at"}, cursorValues: []any{"2024-02-20 12:00:00"}},
		{name: "MultiCursorFirstQuery", cursor: []string{"major", "minor"}},
		{name: "MultiCursorSubsequentQuery", cursor: []string{"major", "minor"}, cursorValues: []any{1, 2}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var resource = &Resource{
				Name:       "test_foobar",
				SchemaName: "test",
				TableName:  "foobar",
				Cursor:     tc.cursor,
			}
			var state = &streamState{
				CursorNames:  tc.cursor,
				CursorValues: tc.cursorValues,
			}
			var query, err = sqlserverDriver.buildQuery(resource, state)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, query)
		})
	}
}

// TestNonexistentCursor exercises the situation of a capture whose configured
// cursor column doesn't actually exist.
func TestNonexistentCursor(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, data TEXT)`)
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, 'hello'), (2, 'world')", tableName))

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "updated_at") // Nonexistent column

	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestDatetimeCursorRoundTrip tests that DATETIME column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
//
// This is a regression test for a bug where DATETIME cursor values with timezone
// information added during output formatting were not correctly handled when passed
// back to SQL Server as query parameters.
func TestDatetimeCursorRoundTrip(t *testing.T) {
	var ctx, control = context.Background(), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, data NVARCHAR(MAX), updated_at DATETIME)`)

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
			setResourceCursor(t, cs.Bindings[0], "updated_at")

			setShutdownAfterQuery(t, true)
			baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

			// Clear the table before each timezone test
			executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s", tableName))

			// Initial batch of rows
			for i := range 5 {
				executeControlQuery(t, control, fmt.Sprintf(
					"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
					i, fmt.Sprintf("Initial row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
			}
			cs.Capture(ctx, t, nil)

			// Second batch to be captured using cursor
			for i := 5; i < 10; i++ {
				executeControlQuery(t, control, fmt.Sprintf(
					"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
					i, fmt.Sprintf("Second batch row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
			}
			cs.Capture(ctx, t, nil)

			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}

// TestSmallDatetimeCursorRoundTrip tests that SMALLDATETIME column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestSmallDatetimeCursorRoundTrip(t *testing.T) {
	var ctx, control = context.Background(), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, data NVARCHAR(MAX), updated_at SMALLDATETIME)`)

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
			setResourceCursor(t, cs.Bindings[0], "updated_at")

			setShutdownAfterQuery(t, true)
			baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

			// Clear the table before each timezone test
			executeControlQuery(t, control, fmt.Sprintf("DELETE FROM %s", tableName))

			// Initial batch of rows
			for i := range 5 {
				executeControlQuery(t, control, fmt.Sprintf(
					"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
					i, fmt.Sprintf("Initial row %d", i), baseTime.Add(time.Duration(i)*time.Minute))
			}
			cs.Capture(ctx, t, nil)

			// Second batch to be captured using cursor
			for i := 5; i < 10; i++ {
				executeControlQuery(t, control, fmt.Sprintf(
					"INSERT INTO %s (id, data, updated_at) VALUES (@p1, @p2, @p3)", tableName),
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
		id INTEGER PRIMARY KEY,
		data NVARCHAR(MAX),
		sequence DECIMAL(18,2)
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "sequence")
	setShutdownAfterQuery(t, true)

	for i := range 5 {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, sequence) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Initial row %d", i), float64(100+i*10)+0.50)
	}
	cs.Capture(ctx, t, nil)
	for i := 5; i < 10; i++ {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, sequence) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Second batch row %d", i), float64(100+i*10)+0.50)
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestMoneyCursorRoundTrip tests that MONEY column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestMoneyCursorRoundTrip(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		id INTEGER PRIMARY KEY,
		data NVARCHAR(MAX),
		amount MONEY
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "amount")
	setShutdownAfterQuery(t, true)

	for i := range 5 {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, amount) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Initial row %d", i), fmt.Sprintf("%d.50", 100+i*10))
	}
	cs.Capture(ctx, t, nil)
	for i := 5; i < 10; i++ {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, amount) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Second batch row %d", i), fmt.Sprintf("%d.50", 100+i*10))
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestSmallMoneyCursorRoundTrip tests that SMALLMONEY column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestSmallMoneyCursorRoundTrip(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		id INTEGER PRIMARY KEY,
		data NVARCHAR(MAX),
		amount SMALLMONEY
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "amount")
	setShutdownAfterQuery(t, true)

	for i := range 5 {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, amount) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Initial row %d", i), fmt.Sprintf("%d.25", 50+i*5))
	}
	cs.Capture(ctx, t, nil)
	for i := 5; i < 10; i++ {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, amount) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Second batch row %d", i), fmt.Sprintf("%d.25", 50+i*5))
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

// TestUniqueIdentifierCursorRoundTrip tests that UNIQUEIDENTIFIER column values used as cursors
// are correctly round-tripped back to the database for incremental queries.
func TestUniqueIdentifierCursorRoundTrip(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		id INTEGER PRIMARY KEY,
		data NVARCHAR(MAX),
		guid UNIQUEIDENTIFIER
	)`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "guid")
	setShutdownAfterQuery(t, true)

	var testUUIDs = []string{
		"09c58be4-5e1e-4074-9bf6-2113f97c6364",
		"0112bf96-5d4b-4775-bcf5-0216610d9703",
		"b9ecf3c0-253a-425b-b84c-4704fa4b2daf",
		"dd0ee7c0-9f04-4920-b982-7c1ad8998f1b",
		"fe0761c1-e4e9-496d-be20-4fa97adafef2",
		"65c2815a-4dbf-44cf-b08a-ec5fd0559f2b",
	}

	for i := range 3 {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, guid) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Initial row %d", i), testUUIDs[i])
	}
	cs.Capture(ctx, t, nil)
	for i := 3; i < 6; i++ {
		executeControlQuery(t, control, fmt.Sprintf(
			"INSERT INTO %s (id, data, guid) VALUES (@p1, @p2, @p3)", tableName),
			i, fmt.Sprintf("Second batch row %d", i), testUUIDs[i])
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}
