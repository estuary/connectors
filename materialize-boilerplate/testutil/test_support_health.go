package testutil

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/stretchr/testify/require"
)

// HealthLine is a "transaction health" log line emitted by the materialization
// boilerplate, as parsed from a preview run's JSON task logs.
type HealthLine struct {
	Verdict      string `json:"verdict"`
	Fidelity     string `json:"fidelity"`
	Rounds       int    `json:"rounds"`
	FirstRound   int    `json:"firstRound"`
	LastRound    int    `json:"lastRound"`
	Bindings     int    `json:"bindings"`
	LoadRequests int64  `json:"loadRequests"`
	Loaded       int64  `json:"loaded"`
	Expected     struct {
		Insert      int64 `json:"insert"`
		Update      int64 `json:"update"`
		Delete      int64 `json:"delete"`
		SoftDeleted int64 `json:"softDeleted"`
		Skipped     int64 `json:"skipped"`
	} `json:"expected"`
	Actual struct {
		Inserted int64  `json:"inserted"`
		Updated  int64  `json:"updated"`
		Deleted  int64  `json:"deleted"`
		Total    int64  `json:"total"`
		Staged   *int64 `json:"staged"`
		Loaded   *int64 `json:"loaded"`
	} `json:"actual"`
	Pending    int               `json:"pending"`
	Recovery   bool              `json:"recovery"`
	Sharded    bool              `json:"sharded"`
	Mismatches []json.RawMessage `json:"mismatches"`
}

// stores is the number of documents the line says were handed to the
// connector; skipped hard deletes never are.
func (l HealthLine) stores() int64 {
	return l.Expected.Insert + l.Expected.Update + l.Expected.Delete
}

func (l HealthLine) String() string {
	raw, _ := json.Marshal(l)
	return string(raw)
}

// ParseHealthLines extracts every "transaction health" line from the stderr
// of a `flowctl raw preview-next --log-json` run. Lines that aren't JSON task
// logs are skipped.
func ParseHealthLines(stderr []byte) []HealthLine {
	var out []HealthLine
	scanner := bufio.NewScanner(bytes.NewReader(stderr))
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var entry struct {
			Message string          `json:"message"`
			Fields  json.RawMessage `json:"fields"`
		}
		if err := json.Unmarshal(line, &entry); err != nil || entry.Message != "transaction health" {
			continue
		}
		var parsed HealthLine
		if err := json.Unmarshal(entry.Fields, &parsed); err != nil {
			continue
		}
		out = append(out, parsed)
	}
	return out
}

// AssertTransactionHealth checks a preview run's health lines against the
// fidelity the connector's test declares.
//
// Every line that isn't from a recovery commit must be healthy: verdict `ok`
// for a connector reporting at `total` or `exact`, and `unchecked` for one
// declaring `none`. At least one line must cover stored documents, and each
// such line must carry the declared fidelity, so a connector that silently
// stops reporting fails its own suite. The recovery line from the drain
// session at the end of every run is tolerated.
//
// Under multiple shards nothing can be paired, so every line is `unchecked`
// and tagged `sharded`; the check is then that the primary's actual-only
// lines still carry the declared fidelity.
func AssertTransactionHealth(t *testing.T, lines []HealthLine, shards int, declared m.Fidelity) {
	t.Helper()
	require.NoError(t, checkTransactionHealth(lines, shards, declared))
}

func checkTransactionHealth(lines []HealthLine, shards int, declared m.Fidelity) error {
	if declared == "" {
		declared = m.FidelityNone
	}
	if len(lines) == 0 {
		return fmt.Errorf("no transaction health lines were logged")
	}

	if shards > 1 {
		return checkShardedHealth(lines, declared)
	}

	// Every non-recovery line must be healthy at the declared fidelity; the
	// drain session's recovery line is tolerated.
	var withStores int
	for _, l := range lines {
		if l.Recovery {
			continue
		} else if l.Verdict == "mismatch" {
			return fmt.Errorf("transaction health mismatch: %s", l)
		} else if l.Sharded {
			return fmt.Errorf("single-shard run logged a sharded health line: %s", l)
		} else if l.stores() == 0 {
			continue
		}
		withStores++

		if l.Fidelity != string(declared) {
			return fmt.Errorf("reported fidelity differs from the declared %q: %s", declared, l)
		}
		if declared == m.FidelityNone {
			if l.Verdict != "unchecked" {
				return fmt.Errorf("a connector declaring no fidelity must log unchecked rounds: %s", l)
			}
		} else if l.Verdict != "ok" {
			return fmt.Errorf("transaction health not ok: %s", l)
		} else if l.Pending != 0 {
			return fmt.Errorf("bindings stored documents but never reported: %s", l)
		}
	}

	if withStores == 0 {
		return fmt.Errorf("no transaction health line covered stored documents at fidelity %q:\n%s", declared, describeLines(lines))
	}
	return nil
}

// checkShardedHealth verifies a multi-shard run. Nothing can be paired per
// round, so every line is unchecked and tagged sharded, and the primary's
// actuals often arrive only in the drain session's recovery line. The check
// is that no line is a mismatch and, unless the connector reports nothing,
// some line carries the declared fidelity so a silent downgrade still fails.
func checkShardedHealth(lines []HealthLine, declared m.Fidelity) error {
	var withFidelity int
	for _, l := range lines {
		if l.Verdict == "mismatch" {
			return fmt.Errorf("transaction health mismatch: %s", l)
		} else if !l.Sharded {
			return fmt.Errorf("multi-shard run logged an unsharded health line: %s", l)
		} else if l.Fidelity == string(declared) && declared != m.FidelityNone {
			withFidelity++
		}
	}

	if declared != m.FidelityNone && withFidelity == 0 {
		return fmt.Errorf("no transaction health line carried the declared fidelity %q:\n%s", declared, describeLines(lines))
	}
	return nil
}

func describeLines(lines []HealthLine) string {
	var b strings.Builder
	for _, l := range lines {
		fmt.Fprintln(&b, l.String())
	}
	return b.String()
}
