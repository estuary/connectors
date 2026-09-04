package testutil

import (
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/stretchr/testify/require"
)

func healthLog(fields string) string {
	return `{"ts":"2026-09-05T00:00:00Z","level":"info","message":"transaction health","fields":` + fields + `,"shard":{"kind":"materialization","name":"test/task"}}`
}

const (
	okTotal = `{"verdict":"ok","fidelity":"total","rounds":2,"firstRound":0,"lastRound":1,"bindings":2,"loadRequests":3,"loaded":1,` +
		`"expected":{"insert":10,"update":2,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":0,"updated":0,"deleted":0,"total":12}}`
	okEmpty = `{"verdict":"ok","fidelity":"none","rounds":1,"firstRound":2,"lastRound":2,"bindings":0,"loadRequests":0,"loaded":0,` +
		`"expected":{"insert":0,"update":0,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":0,"updated":0,"deleted":0,"total":0}}`
	recovery = `{"verdict":"unchecked","fidelity":"total","recovery":true,"rounds":1,"firstRound":3,"lastRound":3,"bindings":1,"loadRequests":0,"loaded":0,` +
		`"expected":{"insert":0,"update":0,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":0,"updated":0,"deleted":0,"total":4}}`
	mismatch = `{"verdict":"mismatch","fidelity":"total","rounds":1,"firstRound":1,"lastRound":1,"bindings":1,"loadRequests":0,"loaded":0,` +
		`"expected":{"insert":3,"update":0,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":0,"updated":0,"deleted":0,"total":2},` +
		`"mismatches":[{"check":"total","resourcePath":["a"],"expected":3,"actual":2}]}`
	pending = `{"verdict":"unchecked","fidelity":"none","pending":2,"rounds":1,"firstRound":1,"lastRound":1,"bindings":2,"loadRequests":0,"loaded":0,` +
		`"expected":{"insert":12,"update":0,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":0,"updated":0,"deleted":0,"total":0}}`
	shardedExpected = `{"verdict":"unchecked","fidelity":"none","sharded":true,"rounds":2,"firstRound":0,"lastRound":1,"bindings":1,"loadRequests":0,"loaded":0,` +
		`"expected":{"insert":5,"update":0,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":0,"updated":0,"deleted":0,"total":0}}`
	shardedPrimary = `{"verdict":"unchecked","fidelity":"exact","sharded":true,"rounds":2,"firstRound":0,"lastRound":1,"bindings":2,"loadRequests":0,"loaded":0,` +
		`"expected":{"insert":7,"update":0,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":12,"updated":0,"deleted":0,"total":12}}`
	shardedRecovery = `{"verdict":"unchecked","fidelity":"exact","sharded":true,"recovery":true,"rounds":1,"firstRound":0,"lastRound":0,"bindings":2,"loadRequests":0,"loaded":0,` +
		`"expected":{"insert":0,"update":0,"delete":0,"softDeleted":0,"skipped":0},"actual":{"inserted":12,"updated":0,"deleted":0,"total":12}}`
)

func TestParseHealthLines(t *testing.T) {
	stderr := strings.Join([]string{
		`INFO flowctl: some text log line`,
		`{"ts":"2026-09-05T00:00:00Z","level":"info","message":"finished materialization commit","fields":{"round":1}}`,
		healthLog(okTotal),
		`not json {`,
		healthLog(recovery),
		"",
	}, "\n")

	lines := ParseHealthLines([]byte(stderr))
	require.Len(t, lines, 2)
	require.Equal(t, "ok", lines[0].Verdict)
	require.Equal(t, "total", lines[0].Fidelity)
	require.Equal(t, int64(12), lines[0].stores())
	require.Equal(t, int64(12), lines[0].Actual.Total)
	require.True(t, lines[1].Recovery)
}

func TestAssertTransactionHealth(t *testing.T) {
	parse := func(entries ...string) []HealthLine {
		var b strings.Builder
		for _, e := range entries {
			b.WriteString(healthLog(e) + "\n")
		}
		return ParseHealthLines([]byte(b.String()))
	}

	for _, tt := range []struct {
		name     string
		lines    []HealthLine
		shards   int
		declared m.Fidelity
		wantFail string
	}{
		{name: "healthy total run", lines: parse(okTotal, okEmpty, recovery), shards: 1, declared: m.FidelityTotal},
		{name: "healthy total run tolerates recovery only line", lines: parse(okTotal, recovery), shards: 1, declared: m.FidelityTotal},
		{name: "non-reporting connector", lines: parse(pending, okEmpty), shards: 1, declared: m.FidelityNone},
		{name: "non-reporting connector, undeclared", lines: parse(pending, okEmpty), shards: 1},
		{name: "sharded run", lines: parse(shardedExpected, shardedPrimary), shards: 2, declared: m.FidelityExact},
		{name: "sharded run, actuals only in recovery", lines: parse(shardedExpected, shardedRecovery), shards: 2, declared: m.FidelityExact},
		{name: "sharded non-reporting run", lines: parse(shardedExpected), shards: 2, declared: m.FidelityNone},

		{name: "mismatch", lines: parse(okTotal, mismatch), shards: 1, declared: m.FidelityTotal, wantFail: "mismatch"},
		{name: "declared fidelity too high", lines: parse(okTotal), shards: 1, declared: m.FidelityExact, wantFail: "differs from the declared"},
		{name: "silently stopped reporting", lines: parse(pending, okEmpty), shards: 1, declared: m.FidelityTotal, wantFail: "differs from the declared"},
		{name: "started reporting without declaring", lines: parse(okTotal), shards: 1, declared: m.FidelityNone, wantFail: "differs from the declared"},
		{name: "no lines at all", lines: nil, shards: 1, declared: m.FidelityTotal, wantFail: "no transaction health lines"},
		{name: "only empty rounds", lines: parse(okEmpty), shards: 1, declared: m.FidelityTotal, wantFail: "covered stored documents"},
		{name: "sharded primary never reported", lines: parse(shardedExpected), shards: 2, declared: m.FidelityExact, wantFail: "carried the declared fidelity"},
		{name: "unsharded line in sharded run", lines: parse(okTotal), shards: 2, declared: m.FidelityTotal, wantFail: "unsharded"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := checkTransactionHealth(tt.lines, tt.shards, tt.declared)
			if tt.wantFail == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantFail)
			}
		})
	}
}
