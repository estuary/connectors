package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestTrickyColumnNames(t *testing.T) {
	// Create a table with some 'difficult' column names (a reserved word, a capitalized
	// name, and one containing special characters which also happens to be the primary key).
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueA, uniqueB = "14055203", "28395292"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(`Meta/``wtf``~ID` INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(`table` INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "aaa"}, {2, "bbb"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{3, "ccc"}, {4, "ddd"}})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableA, [][]interface{}{{5, "eee"}, {6, "fff"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{7, "ggg"}, {8, "hhh"}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestTrickyTableNames(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	tb.Query(ctx, t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`UsErS!@#$`", testSchemaName))
	tb.Query(ctx, t, fmt.Sprintf("CREATE TABLE `%s`.`UsErS!@#$` (id INTEGER PRIMARY KEY, data TEXT NOT NULL)", testSchemaName))
	var cs = tb.CaptureSpec(ctx, t)
	t.Run("Discover", func(t *testing.T) {
		cs.VerifyDiscover(ctx, t, regexp.MustCompile(`(?i:users)`))
	})
	var resourceSpecJSON, err = json.Marshal(sqlcapture.Resource{
		Namespace: testSchemaName,
		Stream:    "UsErS!@#$",
	})
	require.NoError(t, err)
	cs.Bindings = []*flow.CaptureSpec_Binding{{
		// Because we're explicitly constructing the collection spec here this test accidentally
		// exercises the "legacy collection without a /_meta/source/txid property" case, so we
		// may as well leave it like that.
		Collection:         flow.CollectionSpec{Name: flow.Collection("acmeCo/test/users____")},
		ResourceConfigJson: resourceSpecJSON,
		ResourcePath:       []string{testSchemaName, "UsErS!@#$"},
	}}
	t.Run("Validate", func(t *testing.T) {
		var _, err = cs.Validate(ctx, t)
		require.NoError(t, err)
	})
	t.Run("Capture", func(t *testing.T) {
		tb.Query(ctx, t, fmt.Sprintf("INSERT INTO `%s`.`UsErS!@#$` VALUES (1, 'Alice'), (2, 'Bob')", testSchemaName))
		tests.VerifiedCapture(ctx, t, cs)
		t.Run("Replication", func(t *testing.T) {
			tb.Query(ctx, t, fmt.Sprintf("INSERT INTO `%s`.`UsErS!@#$` VALUES (3, 'Carol'), (4, 'Dave')", testSchemaName))
			tests.VerifiedCapture(ctx, t, cs)
		})
	})
}

func TestPartitionedTable(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "83812828"
	var tableName = tb.CreateTable(ctx, t, uniqueID, `(
	  grp INTEGER,
	  id INTEGER,
	  data TEXT,
	  PRIMARY KEY (grp, id)
	)
	PARTITION BY RANGE (grp)
	SUBPARTITION BY HASH(id) SUBPARTITIONS 2
	(
        PARTITION p0 VALUES LESS THAN (3),
        PARTITION p1 VALUES LESS THAN (5),
        PARTITION p2 VALUES LESS THAN (7),
        PARTITION p3 VALUES LESS THAN (10)
	);`)

	var rows [][]any
	for group := 0; group < 10; group++ {
		for idx := 0; idx < 10; idx++ {
			rows = append(rows, []any{group, idx, fmt.Sprintf("Group #%d Value #%d", group, idx)})
		}
	}
	tb.Insert(ctx, t, tableName, rows)

	t.Run("Discovery", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("Capture", func(t *testing.T) {
		tests.VerifiedCapture(ctx, t, cs)

		var rows [][]any
		for group := 0; group < 10; group++ {
			for idx := 10; idx < 20; idx++ {
				rows = append(rows, []any{group, idx, fmt.Sprintf("Group #%d Value #%d", group, idx)})
			}
		}
		tb.Insert(ctx, t, tableName, rows)

		t.Run("Replication", func(t *testing.T) {
			tests.VerifiedCapture(ctx, t, cs)
		})
	})
}

func TestDatetimeNormalization(t *testing.T) {
	// Create a table with some 'difficult' column names (a reserved word, a capitalized
	// name, and one containing special characters which also happens to be the primary key).
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "24528211"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, x DATETIME)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	delete(cs.Sanitizers, `"<TIMESTAMP>"`) // Don't sanitize timestamps in this test's output

	// Tell MySQL to act as though we're running in Chicago. This has an effect (in very
	// different ways) on the processing of DATETIME and TIMESTAMP values.
	tb.Query(ctx, t, "SET GLOBAL time_zone = 'America/Chicago';")
	tb.Query(ctx, t, "SET SESSION time_zone = 'America/Chicago';")

	// Permit arbitrarily crazy datetime values
	tb.Query(ctx, t, "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'NO_ZERO_DATE',''));")
	tb.Query(ctx, t, "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'NO_ZERO_IN_DATE',''));")
	tb.Query(ctx, t, "SET SESSION sql_mode=(SELECT CONCAT(@@sql_mode, ',ALLOW_INVALID_DATES'));")

	// Insert various test timestamps
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{100, "1991-08-31 12:34:56.987654"},
		{101, "0000-00-00 00:00:00"},
		{102, "2023-00-00 00:00:00"},
		{103, "2023-07-00 00:00:00"},
	})
	tests.VerifiedCapture(ctx, t, cs)

	t.Run("replication", func(t *testing.T) {
		tb.Insert(ctx, t, tableName, [][]interface{}{
			{200, "1991-08-31 12:34:56.987654"},
			{201, "0000-00-00 00:00:00"},
			{202, "2023-00-00 00:00:00"},
			{203, "2023-07-00 00:00:00"},
		})
		tests.VerifiedCapture(ctx, t, cs)
	})
}

func TestEnumPrimaryKey(t *testing.T) {
	// Create a table whose primary key includes an enum value (whose cases are specified
	// in non-alphabetical order).
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "18676708"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(category ENUM('A', 'C', 'B', 'D') , id INTEGER, data TEXT, PRIMARY KEY (category, id))")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 3

	t.Run("discovery", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
	// Insert various test values and then capture them
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{"A", 1, "A1"}, {"A", 2, "A2"}, {"A", 3, "A3"}, {"A", 4, "A4"},
		{"B", 1, "B1"}, {"B", 2, "B2"}, {"B", 3, "B3"}, {"B", 4, "B4"},
		{"C", 1, "C1"}, {"C", 2, "C2"}, {"C", 3, "C3"}, {"C", 4, "C4"},
		{"D", 1, "D1"}, {"D", 2, "D2"}, {"D", 3, "D3"}, {"D", 4, "D4"},
		{"E", 1, "E1"}, {"E", 2, "E2"}, {"E", 3, "E3"}, {"E", 4, "E4"},
	})
	tests.VerifiedCapture(ctx, t, cs)
}

func TestEnumDecodingFix(t *testing.T) {
	// This test is part of the fix for the enum decoding bug introduced by https://github.com/estuary/connectors/pull/1336
	// and can be deleted after the metadata migration logic is also removed. The metadata migration logic can safely be
	// removed after all live captures we care about have run the fix code and updated their checkpoint metadata.
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "32314857"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, category ENUM('A', 'C', 'B', 'D'))")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}

	t.Run("discovery", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
	// Insert various test values and then capture them via replication
	tb.Insert(ctx, t, tableName, [][]interface{}{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "error"}})
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]interface{}{{6, "A"}, {7, "B"}, {8, "C"}, {9, "D"}, {10, "error"}})
	t.Run("replication1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Manually fiddle with the persisted checkpoint metadata used for enum decoding, to
	// simulate the situation where an old capture with old metadata is used with the newer
	// enum decoding logic.
	cs.Checkpoint = json.RawMessage(strings.ReplaceAll(string(cs.Checkpoint), `"enum":["","A","C","B","D"]`, `"enum":["A","C","B","D",""]`))
	tb.Insert(ctx, t, tableName, [][]interface{}{{11, "A"}, {12, "B"}, {13, "C"}, {14, "D"}, {15, "error"}})
	t.Run("replication2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestBackfillModes(t *testing.T) {
	// Create two tables with 1,000 rows each
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueA, uniqueB = "11837744", "25282936"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id VARCHAR(32) PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id VARCHAR(32) PRIMARY KEY, data TEXT)")

	// TODO: Generate more challenging keys?
	var rows [][]any
	for idx := 0; idx < 1000; idx++ {
		rows = append(rows, []any{fmt.Sprintf("Row %d", idx), fmt.Sprintf("Data for row %d", idx)})
	}
	tb.Insert(ctx, t, tableA, rows)
	tb.Insert(ctx, t, tableB, rows)

	// Capture both tables, one with a precise backfill and one imprecise
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	cs.Validator = &st.OrderedCaptureValidator{}
	var resA, resB sqlcapture.Resource
	require.NoError(t, json.Unmarshal(cs.Bindings[0].ResourceConfigJson, &resA))
	require.NoError(t, json.Unmarshal(cs.Bindings[1].ResourceConfigJson, &resB))
	resA.Mode = sqlcapture.BackfillModeNormal
	resB.Mode = sqlcapture.BackfillModePrecise
	resourceSpecA, err := json.Marshal(resA)
	require.NoError(t, err)
	resourceSpecB, err := json.Marshal(resB)
	require.NoError(t, err)
	cs.Bindings[0].ResourceConfigJson = resourceSpecA
	cs.Bindings[1].ResourceConfigJson = resourceSpecB

	tests.VerifiedCapture(ctx, t, cs)
}

func TestEmptyBlobs(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "11214558"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, a_varchar VARCHAR(32) NOT NULL, a_varbinary VARBINARY(32) NOT NULL)")
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1, "A", []byte{0xAA, 0xAA, 0xAA, 0xAA}},
		{2, "B", []byte{}},
		{3, "", []byte{0xCC, 0xCC, 0xCC, 0xCC}},
	})

	t.Run("Discovery", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
	})

	t.Run("Capture", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
		tests.VerifiedCapture(ctx, t, cs)
	})
}

func TestEnumEmptyString(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "29144777"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, category ENUM('' , 'A' , 'B' , 'C'))")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}

	t.Run("discovery", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
	// Insert various test values and then capture them via replication
	tb.Insert(ctx, t, tableName, [][]any{{1, "A"}, {2, "B"}, {3, "C"}, {4, "error"}, {100, 0}, {101, 1}, {102, ""}, {105, 5}})
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]any{{5, "A"}, {6, "B"}, {7, "C"}, {8, "error"}, {200, 0}, {201, 1}, {202, ""}, {205, 5}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestUnsignedIntegers(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "45511171"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, v1 TINYINT UNSIGNED, v2 SMALLINT UNSIGNED, v3 MEDIUMINT UNSIGNED, v4 INT UNSIGNED, v8 BIGINT UNSIGNED)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}

	t.Run("discovery", func(t *testing.T) { cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
	// Insert various test values and then capture them via replication
	tb.Insert(ctx, t, tableName, [][]any{{1, "222", "55555", "11111111", "3333333333", "17777777777777777777"}})
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
	tb.Insert(ctx, t, tableName, [][]any{{2, "222", "55555", "11111111", "3333333333", "17777777777777777777"}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestPartialRowImages(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()

	tb.Query(ctx, t, "SET SESSION binlog_row_image = 'MINIMAL'")
	t.Cleanup(func() { tb.Query(ctx, t, "SET SESSION binlog_row_image = 'FULL'") })

	var uniqueID = "16824726"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)")
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	cs.Validator = &st.OrderedCaptureValidator{}

	tb.Insert(ctx, t, tableName, [][]any{{0, 0, 0, 0}, {1, 1, 1, 1}, {2, 2, 2, 2}})
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s(id, a) VALUES (3, 3)", tableName))
	tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s(id, b) VALUES (4, 4)", tableName))
	tb.Query(ctx, t, fmt.Sprintf("INSERT INTO %s(id, c) VALUES (5, 5)", tableName))
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET a = 6 WHERE id = 0", tableName))
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET b = 7 WHERE id = 1", tableName))
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET c = 8 WHERE id = 2", tableName))
	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE id = 2", tableName))
	t.Run("delete", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}
