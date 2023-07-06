package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"

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
