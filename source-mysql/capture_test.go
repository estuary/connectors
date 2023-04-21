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
	const uniqueString = "fizzed_cupcake"
	var tableA = tb.CreateTable(ctx, t, uniqueString+"_a", "(`Meta/``wtf``~ID` INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueString+"_b", "(`table` INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "aaa"}, {2, "bbb"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{3, "ccc"}, {4, "ddd"}})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, tableA, tableB)
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
