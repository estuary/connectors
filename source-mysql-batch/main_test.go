package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	response, err := mysqlDriver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestQueryTemplate(t *testing.T) {
	res, err := mysqlDriver.GenerateResource("test_foobar", "test", "foobar", "BASE TABLE")
	require.NoError(t, err)

	tmpl, err := template.New("query").Parse(res.Template)
	require.NoError(t, err)

	for _, tc := range []struct {
		Name    string
		IsFirst bool
		Cursor  []string
	}{
		{Name: "FirstNoCursor", IsFirst: true, Cursor: nil},
		{Name: "SubsequentNoCursor", IsFirst: false, Cursor: nil},
		{Name: "FirstOneCursor", IsFirst: true, Cursor: []string{"`ka`"}},
		{Name: "SubsequentOneCursor", IsFirst: false, Cursor: []string{"`ka`"}},
		{Name: "FirstTwoCursor", IsFirst: true, Cursor: []string{"`ka`", "`kb`"}},
		{Name: "SubsequentTwoCursor", IsFirst: false, Cursor: []string{"`ka`", "`kb`"}},
		{Name: "FirstThreeCursor", IsFirst: true, Cursor: []string{"`ka`", "`kb`", "`kc`"}},
		{Name: "SubsequentThreeCursor", IsFirst: false, Cursor: []string{"`ka`", "`kb`", "`kc`"}},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var buf = new(strings.Builder)
			require.NoError(t, tmpl.Execute(buf, map[string]any{
				"IsFirstQuery": tc.IsFirst,
				"CursorFields": tc.Cursor,
			}))
			cupaloy.SnapshotT(t, buf.String())
		})
	}
}

func TestQueryPlaceholderExpansion(t *testing.T) {
	var querySource = `SELECT * FROM "test"."foobar" WHERE (ka > :0) OR (ka = :0 AND kb > :1) OR (ka = :0 AND kb = :1 AND kc > :2) OR (x > ?) OR (y > ?);`
	var argvals = []any{1, "two", 3.0, "xval", "yval"}
	var query, args = expandQueryPlaceholders(querySource, argvals)
	var buf = new(strings.Builder)
	fmt.Fprintf(buf, "Query: %s\n\n---\n", query)
	for i, arg := range args {
		fmt.Fprintf(buf, "Argument %d: %#v\n", i, arg)
	}
	cupaloy.SnapshotT(t, buf.String())
}
