package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ts(minute int) time.Time {
	return time.Date(2026, 1, 1, 0, minute, 0, 0, time.UTC)
}

func rec(minute int, materialize bool, docs map[string]int64) statsRecord {
	var m = map[string]bindingStats{}
	for coll, n := range docs {
		var b bindingStats
		b.Out = &struct {
			DocsTotal  int64 `json:"docsTotal"`
			BytesTotal int64 `json:"bytesTotal"`
		}{DocsTotal: n, BytesTotal: n * 100}
		m[coll] = b
	}
	var r = statsRecord{TS: ts(minute), TxnCount: 1}
	if materialize {
		r.Materialize = m
	} else {
		r.Capture = m
	}
	return r
}

var testTables = map[string]string{"acme/a": "a", "acme/b": "b"}

func TestAnalyzeCompleteRun(t *testing.T) {
	var in = &inputs{
		materialization: "acme/mat", capture: "acme/cap", matImage: "ghcr.io/estuary/materialize-x:v2", scale: "1",
		tables: testTables,
		matStats: []statsRecord{
			rec(10, true, map[string]int64{"acme/a": 5, "acme/b": 40}), // b completes in the first transaction
			rec(20, true, map[string]int64{"acme/a": 3}),
			rec(30, true, map[string]int64{"acme/a": 2}), // a completes
			rec(31, true, map[string]int64{}),
		},
		capStats: []statsRecord{rec(0, false, map[string]int64{"acme/a": 10, "acme/b": 40})},
	}
	var res = analyze(in, map[string]int64{"a": 10, "b": 40})
	require.True(t, res.Complete)
	require.True(t, res.OK)
	require.Equal(t, ts(10), res.Materialize.StartedAt)
	require.Equal(t, ts(30), res.Materialize.CompletedAt)
	require.Equal(t, 20*60.0, res.Materialize.Seconds)
	require.Equal(t, int64(50), res.Materialize.Docs)
	require.Equal(t, int64(5000), res.Materialize.Bytes)
	require.Equal(t, 3, res.Materialize.Transactions)
	require.Equal(t, 0.0, res.Tables["b"].Seconds)
	require.Equal(t, 20*60.0, res.Tables["a"].Seconds)
	require.Equal(t, int64(10), res.Tables["a"].Captured)
}

func TestAnalyzeIncompleteRun(t *testing.T) {
	var in = &inputs{tables: testTables, matStats: []statsRecord{rec(10, true, map[string]int64{"acme/a": 5, "acme/b": 40})}}
	var res = analyze(in, map[string]int64{"a": 10, "b": 40})
	require.False(t, res.Complete)
	require.False(t, res.OK)
	require.True(t, res.Tables["b"].OK)
	require.False(t, res.Tables["a"].OK)
}

func TestAnalyzeTooManyDocuments(t *testing.T) {
	var in = &inputs{tables: testTables, matStats: []statsRecord{rec(10, true, map[string]int64{"acme/a": 11, "acme/b": 40})}}
	var res = analyze(in, map[string]int64{"a": 10, "b": 40})
	require.True(t, res.Complete, "nothing is still loading")
	require.False(t, res.OK, "a has more rows than dsdgen produces")
}

func TestOracles(t *testing.T) {
	for _, scale := range []string{"0.01", "1", "10", "100"} {
		o, err := loadOracle(scale)
		require.NoError(t, err)
		require.Len(t, o, 24, scale)
	}
	_, err := loadOracle("3")
	require.ErrorContains(t, err, `no row-count oracle for scale "3"`)
}
