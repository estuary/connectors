package materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

// healthLine is a decoded "transaction health" log line.
type healthLine struct {
	Observable   bool   `json:"observable"`
	TaskName     string `json:"catalog_task_name"`
	Verdict      string
	Fidelity     string
	Rounds       int
	FirstRound   int
	LastRound    int
	Bindings     int
	LoadRequests int64
	Loaded       int64
	Expected     struct {
		Insert, Update, Delete, SoftDeleted, Skipped int64
	}
	Actual struct {
		Inserted, Updated, Deleted, Total int64
		Staged, Loaded                    *int64
	}
	Pending    int
	Recovery   bool
	Sharded    bool
	Mismatches []healthMismatch
}

func decodeHealthLines(t *testing.T, hook *logtest.Hook) []healthLine {
	t.Helper()
	var out []healthLine
	for _, e := range hook.AllEntries() {
		if e.Message != "transaction health" {
			continue
		}
		require.Equal(t, log.InfoLevel, e.Level)
		raw, err := json.Marshal(e.Data)
		require.NoError(t, err)
		var line healthLine
		require.NoError(t, json.Unmarshal(raw, &line))
		require.True(t, line.Observable, "health lines must be operator-observable")
		require.Equal(t, "test/materialization", line.TaskName)
		out = append(out, line)
	}
	return out
}

// scriptedStream serves a fixed sequence of requests and records responses.
// It returns io.EOF once the script is exhausted, after `hold` (if set) is
// closed.
type scriptedStream struct {
	mu        sync.Mutex
	requests  []pm.Request
	responses []pm.Response
	hold      chan struct{}
}

func (s *scriptedStream) Send(m *pm.Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, *m)
	return nil
}

func (s *scriptedStream) RecvMsg(m *pm.Request) error {
	s.mu.Lock()
	if len(s.requests) == 0 {
		hold := s.hold
		s.mu.Unlock()
		if hold != nil {
			<-hold
		}
		return io.EOF
	}
	*m = s.requests[0]
	s.requests = s.requests[1:]
	s.mu.Unlock()
	return nil
}

// txn is one scripted transaction.
type txn struct {
	loads  []pm.Request
	stores []pm.Request
}

func loadReq(binding int, key string) pm.Request {
	return pm.Request{Load: &pm.Request_Load{
		Binding:   uint32(binding),
		KeyPacked: tuple.Tuple{key}.Pack(),
	}}
}

func storeReq(binding int, key string, exists, deleted bool) pm.Request {
	return pm.Request{Store: &pm.Request_Store{
		Binding:      uint32(binding),
		KeyPacked:    tuple.Tuple{key}.Pack(),
		ValuesPacked: tuple.Tuple{"v"}.Pack(),
		DocJson:      json.RawMessage(`{"k":"` + key + `"}`),
		Exists:       exists,
		Delete:       deleted,
	}}
}

func scriptRequests(txns []txn) []pm.Request {
	var out []pm.Request
	for _, tx := range txns {
		out = append(out, pm.Request{Acknowledge: &pm.Request_Acknowledge{}})
		out = append(out, tx.loads...)
		out = append(out, pm.Request{Flush: &pm.Request_Flush{}})
		out = append(out, tx.stores...)
		out = append(out, pm.Request{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}})
	}
	// Acknowledge the final commit, after which EOF is a graceful shutdown.
	out = append(out, pm.Request{Acknowledge: &pm.Request_Acknowledge{}})
	return out
}

type testBinding struct {
	path  []string
	delta bool
}

// openRequest builds an Open around a real generated specification, with one
// binding per testBinding.
func openRequest(bindings []testBinding, rng *pf.RangeSpec) *pm.Request_Open {
	raw, err := os.ReadFile("../../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	if err != nil {
		panic(err)
	}
	var spec pf.MaterializationSpec
	if err := spec.Unmarshal(raw); err != nil {
		panic(err)
	}
	spec.Name = pf.Materialization("test/materialization")
	template := *spec.Bindings[0]
	spec.Bindings = nil
	for _, b := range bindings {
		binding := template
		binding.ResourcePath = b.path
		binding.DeltaUpdates = b.delta
		spec.Bindings = append(spec.Bindings, &binding)
	}
	if rng == nil {
		rng = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
	}
	return &pm.Request_Open{Materialization: &spec, Range: rng, Version: "v1"}
}

// storedCounts is what the scripted transactor saw for one round, by binding.
type storedCounts map[int]int64

// scriptedTransactor stores documents and reports scripted row stats.
type scriptedTransactor struct {
	be         *BindingEvents
	bindings   []testBinding
	hardDelete bool
	// loadedPerLoad is how many Loaded responses to send per Load request.
	loadedPerLoad int
	// report returns the stats to report for a binding of a round, given
	// what was stored; a nil result reports nothing.
	report func(round, binding int, stored int64) *RowStats
	// deferReports reports from Acknowledge rather than StartCommit.
	deferReports bool
	// recoveryReports are reported from the recovery Acknowledge.
	recoveryReports func(be *BindingEvents)

	mu      sync.Mutex
	pending []func()
	acks    int
}

func (t *scriptedTransactor) NewTransactor(_ context.Context, _ pm.Request_Open, be *BindingEvents) (Transactor, *pm.Response_Opened, *MaterializeOptions, error) {
	t.be = be
	return t, &pm.Response_Opened{}, nil, nil
}

func (t *scriptedTransactor) UnmarshalState(json.RawMessage) error { return nil }
func (t *scriptedTransactor) RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (RuntimeCheckpoint, error) {
	return nil, nil
}
func (t *scriptedTransactor) Destroy() {}

func (t *scriptedTransactor) Load(it *LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		it.WaitForAcknowledged()
		for i := 0; i < t.loadedPerLoad; i++ {
			if err := loaded(it.Binding, json.RawMessage(`{"loaded":true}`)); err != nil {
				return err
			}
		}
	}
	return it.Err()
}

func (t *scriptedTransactor) Store(it *StoreIterator) (StartCommitFunc, error) {
	stored := make(storedCounts)
	for it.Next(t.hardDelete) {
		stored[it.Binding]++
	}
	if it.Err() != nil {
		return nil, it.Err()
	}
	round := it.Round

	doReport := func() {
		if t.report == nil {
			return
		}
		for binding, n := range stored {
			if stats := t.report(round, binding, n); stats != nil {
				t.be.ReportRowStats(round, t.bindings[binding].path, *stats)
			}
		}
	}

	return func(context.Context, *pc.Checkpoint) (*pf.ConnectorState, OpFuture) {
		if t.deferReports {
			t.mu.Lock()
			t.pending = append(t.pending, doReport)
			t.mu.Unlock()
		} else {
			doReport()
		}
		return nil, nil
	}, nil
}

func (t *scriptedTransactor) Acknowledge(context.Context, []json.RawMessage, []string) (*pf.ConnectorState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.acks == 0 && t.recoveryReports != nil {
		t.recoveryReports(t.be)
	}
	t.acks++
	for _, fn := range t.pending {
		fn()
	}
	t.pending = nil
	return nil, nil
}

// runHealthScenario drives RunTransactions over a scripted stream and returns
// the health lines it logged.
func runHealthScenario(t *testing.T, tr *scriptedTransactor, open *pm.Request_Open, txns []txn, hold chan struct{}, beforeEOF func(hook *logtest.Hook)) []healthLine {
	t.Helper()
	hook := logtest.NewGlobal()
	defer hook.Reset()

	stream := &scriptedStream{requests: scriptRequests(txns), hold: hold}
	done := make(chan error, 1)
	go func() {
		done <- RunTransactions(context.Background(), tr, stream, open, log.InfoLevel)
	}()
	if beforeEOF != nil {
		beforeEOF(hook)
	}
	if hold != nil {
		close(hold)
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("RunTransactions did not return")
	}
	return decodeHealthLines(t, hook)
}

// sumLines adds up the countable fields of several health lines.
func sumLines(lines []healthLine) healthLine {
	var out healthLine
	for _, l := range lines {
		out.Rounds += l.Rounds
		out.LoadRequests += l.LoadRequests
		out.Loaded += l.Loaded
		out.Expected.Insert += l.Expected.Insert
		out.Expected.Update += l.Expected.Update
		out.Expected.Delete += l.Expected.Delete
		out.Actual.Inserted += l.Actual.Inserted
		out.Actual.Updated += l.Actual.Updated
		out.Actual.Total += l.Actual.Total
		out.Pending += l.Pending
	}
	return out
}

var twoBindings = []testBinding{{path: []string{"schema", "alpha"}}, {path: []string{"schema", "beta"}}}

func exactReport(inserted, updated, deleted int64) func(int, int, int64) *RowStats {
	return func(int, int, int64) *RowStats {
		s := ExactRowStats(inserted, updated, deleted)
		return &s
	}
}

func TestHealthClassification(t *testing.T) {
	// One document of every (Exists, Delete) combination.
	stores := []pm.Request{
		storeReq(0, "new", false, false),
		storeReq(0, "existing", true, false),
		storeReq(0, "deleted-existing", true, true),
		storeReq(0, "deleted-missing", false, true),
	}

	t.Run("hard deletes on", func(t *testing.T) {
		tr := &scriptedTransactor{
			bindings:   twoBindings,
			hardDelete: true,
			report:     exactReport(1, 1, 1),
		}
		lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), []txn{{stores: stores}}, nil, nil)
		require.Len(t, lines, 1)
		l := lines[0]
		require.Equal(t, "ok", l.Verdict)
		require.Equal(t, "exact", l.Fidelity)
		require.Equal(t, int64(1), l.Expected.Insert)
		require.Equal(t, int64(1), l.Expected.Update)
		require.Equal(t, int64(1), l.Expected.Delete)
		require.Equal(t, int64(0), l.Expected.SoftDeleted)
		require.Equal(t, int64(1), l.Expected.Skipped)
		require.Equal(t, 1, l.Bindings)
		require.Equal(t, 1, l.Rounds)
	})

	t.Run("hard deletes off", func(t *testing.T) {
		tr := &scriptedTransactor{
			bindings: twoBindings,
			report:   exactReport(2, 2, 0),
		}
		lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), []txn{{stores: stores}}, nil, nil)
		require.Len(t, lines, 1)
		l := lines[0]
		require.Equal(t, "ok", l.Verdict)
		require.Equal(t, int64(2), l.Expected.Insert)
		require.Equal(t, int64(2), l.Expected.Update)
		require.Equal(t, int64(0), l.Expected.Delete)
		require.Equal(t, int64(2), l.Expected.SoftDeleted)
		require.Equal(t, int64(0), l.Expected.Skipped)
	})
}

func TestHealthWindowAggregation(t *testing.T) {
	tr := &scriptedTransactor{
		bindings:      twoBindings,
		loadedPerLoad: 1,
		report: func(round, binding int, stored int64) *RowStats {
			s := ExactRowStats(stored, 0, 0)
			return &s
		},
	}
	txns := []txn{
		{loads: []pm.Request{loadReq(0, "a")}, stores: []pm.Request{storeReq(0, "a", false, false), storeReq(1, "b", false, false)}},
		{stores: []pm.Request{storeReq(0, "c", false, false)}},
		{}, // An empty round is healthy and doesn't dilute fidelity.
		{loads: []pm.Request{loadReq(1, "d"), loadReq(1, "e")}, stores: []pm.Request{storeReq(1, "d", false, false)}},
	}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, nil, nil)
	require.Len(t, lines, 2)

	// The first judged round with stores is logged at once; those that follow
	// within the interval roll up into one line, flushed here by EOF.
	first := lines[0]
	require.Equal(t, "ok", first.Verdict)
	require.Equal(t, "exact", first.Fidelity)
	require.Equal(t, 1, first.Rounds)
	require.Equal(t, 0, first.FirstRound)
	require.Equal(t, 0, first.LastRound)
	require.Equal(t, 2, first.Bindings)
	require.Equal(t, int64(1), first.LoadRequests)
	require.Equal(t, int64(1), first.Loaded)
	require.Equal(t, int64(2), first.Expected.Insert)
	require.Equal(t, int64(2), first.Actual.Inserted)

	rest := lines[1]
	require.Equal(t, "ok", rest.Verdict)
	require.Equal(t, "exact", rest.Fidelity)
	require.Equal(t, 3, rest.Rounds)
	require.Equal(t, 1, rest.FirstRound)
	require.Equal(t, 3, rest.LastRound)
	require.Equal(t, 2, rest.Bindings)
	require.Equal(t, int64(2), rest.LoadRequests)
	require.Equal(t, int64(2), rest.Loaded)
	require.Equal(t, int64(2), rest.Expected.Insert)
	require.Equal(t, int64(2), rest.Actual.Inserted)
	require.Equal(t, int64(2), rest.Actual.Total)
	require.Nil(t, rest.Actual.Staged)
	require.Equal(t, 0, rest.Pending)
	require.Empty(t, rest.Mismatches)
	require.False(t, rest.Recovery)
	require.False(t, rest.Sharded)
}

func TestHealthMismatchFastPath(t *testing.T) {
	tr := &scriptedTransactor{
		bindings: twoBindings,
		report: func(round, binding int, stored int64) *RowStats {
			if round == 2 && binding == 1 {
				// A merge that appended instead of updating.
				s := ExactRowStats(stored, 0, 0)
				return &s
			}
			s := ExactRowStats(0, stored, 0)
			return &s
		},
	}
	updates := func(binding int, keys ...string) (out []pm.Request) {
		for _, k := range keys {
			out = append(out, storeReq(binding, k, true, false))
		}
		return out
	}
	txns := []txn{
		{stores: updates(0, "a")},
		{stores: updates(0, "b")},
		{stores: append(updates(0, "c"), updates(1, "d", "e")...)},
		{stores: updates(0, "f")},
	}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, nil, nil)
	require.Len(t, lines, 4)

	// Round 0 is logged at once, round 1 accumulates and is flushed ahead of
	// the mismatching round 2, which goes out on its own; round 3 follows at
	// EOF.
	require.Equal(t, "ok", lines[0].Verdict)
	require.Equal(t, 0, lines[0].FirstRound)
	require.Equal(t, "ok", lines[1].Verdict)
	require.Equal(t, 1, lines[1].Rounds)
	require.Equal(t, 1, lines[1].FirstRound)

	m := lines[2]
	require.Equal(t, "mismatch", m.Verdict)
	require.Equal(t, 1, m.Rounds)
	require.Equal(t, 2, m.FirstRound)
	require.Equal(t, 2, m.LastRound)
	require.Equal(t, 2, m.Bindings)
	require.Equal(t, int64(3), m.Expected.Update)
	require.Equal(t, int64(2), m.Actual.Inserted)
	require.Equal(t, int64(1), m.Actual.Updated)
	require.Equal(t, []healthMismatch{
		{Check: "insert", ResourcePath: []string{"schema", "beta"}, Expected: 0, Actual: 2},
		{Check: "update", ResourcePath: []string{"schema", "beta"}, Expected: 2, Actual: 0},
	}, m.Mismatches)

	require.Equal(t, "ok", lines[3].Verdict)
	require.Equal(t, 1, lines[3].Rounds)
	require.Equal(t, 3, lines[3].FirstRound)
}

func TestHealthChecks(t *testing.T) {
	stores := []pm.Request{
		storeReq(0, "a", false, false),
		storeReq(0, "b", true, false),
		storeReq(0, "c", true, true),
	}
	deltaBindings := []testBinding{{path: []string{"schema", "alpha"}, delta: true}}

	for _, tt := range []struct {
		name       string
		bindings   []testBinding
		hardDelete bool
		stats      RowStats
		want       []healthMismatch
	}{
		{
			name:       "total ok",
			hardDelete: true,
			stats:      TotalRowStats(3),
		},
		{
			name:       "total mismatch",
			hardDelete: true,
			stats:      TotalRowStats(2),
			want:       []healthMismatch{{Check: "total", ResourcePath: []string{"schema", "alpha"}, Expected: 3, Actual: 2}},
		},
		{
			name:       "exact total cross-check",
			hardDelete: true,
			stats:      ExactRowStats(1, 1, 1).WithTotal(4),
			want:       []healthMismatch{{Check: "totalSum", ResourcePath: []string{"schema", "alpha"}, Expected: 3, Actual: 4}},
		},
		{
			name:  "staged equals stores",
			stats: TotalRowStats(3).WithStaged(3).WithLoaded(3),
		},
		{
			name:  "staged lost a row",
			stats: TotalRowStats(3).WithStaged(2).WithLoaded(2),
			want:  []healthMismatch{{Check: "staged", ResourcePath: []string{"schema", "alpha"}, Expected: 3, Actual: 2}},
		},
		{
			name:  "load lost a row",
			stats: TotalRowStats(3).WithStaged(3).WithLoaded(2),
			want:  []healthMismatch{{Check: "loadedRows", ResourcePath: []string{"schema", "alpha"}, Expected: 3, Actual: 2}},
		},
		{
			name:  "loaded without staged compares to stores",
			stats: RowStats{}.WithLoaded(2),
			want:  []healthMismatch{{Check: "loadedRows", ResourcePath: []string{"schema", "alpha"}, Expected: 3, Actual: 2}},
		},
		{
			name:     "delta binding that merged",
			bindings: deltaBindings,
			stats:    ExactRowStats(1, 2, 0),
			want: []healthMismatch{
				{Check: "insert", ResourcePath: []string{"schema", "alpha"}, Expected: 3, Actual: 1},
				{Check: "update", ResourcePath: []string{"schema", "alpha"}, Expected: 0, Actual: 2},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bindings := tt.bindings
			if bindings == nil {
				bindings = twoBindings
			}
			txStores := stores
			if tt.bindings != nil {
				// Delta bindings never see Exists.
				txStores = []pm.Request{
					storeReq(0, "a", false, false),
					storeReq(0, "b", false, false),
					storeReq(0, "c", false, false),
				}
			}
			tr := &scriptedTransactor{
				bindings:   bindings,
				hardDelete: tt.hardDelete,
				report:     func(int, int, int64) *RowStats { s := tt.stats; return &s },
			}
			lines := runHealthScenario(t, tr, openRequest(bindings, nil), []txn{{stores: txStores}}, nil, nil)
			require.Len(t, lines, 1)
			if tt.want == nil {
				require.Equal(t, "ok", lines[0].Verdict)
				require.Empty(t, lines[0].Mismatches)
			} else {
				require.Equal(t, "mismatch", lines[0].Verdict)
				require.Equal(t, tt.want, lines[0].Mismatches)
			}
		})
	}
}

func TestHealthLoadResponses(t *testing.T) {
	tr := &scriptedTransactor{
		bindings:      twoBindings,
		loadedPerLoad: 2, // A load query returning more documents than keys.
		report:        exactReport(1, 0, 0),
	}
	txns := []txn{{
		loads:  []pm.Request{loadReq(0, "a")},
		stores: []pm.Request{storeReq(0, "a", false, false)},
	}}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, nil, nil)
	require.Len(t, lines, 1)
	require.Equal(t, "mismatch", lines[0].Verdict)
	require.Equal(t, int64(1), lines[0].LoadRequests)
	require.Equal(t, int64(2), lines[0].Loaded)
	require.Equal(t, []healthMismatch{{Check: "loadResponses", Expected: 1, Actual: 2}}, lines[0].Mismatches)
}

func TestHealthFidelityNone(t *testing.T) {
	tr := &scriptedTransactor{
		bindings: twoBindings,
		report:   func(int, int, int64) *RowStats { return &RowStats{} },
	}
	txns := []txn{
		{stores: []pm.Request{storeReq(0, "a", false, false)}},
		{stores: []pm.Request{storeReq(1, "b", true, false)}},
	}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, nil, nil)
	require.Len(t, lines, 2)
	for _, l := range lines {
		require.Equal(t, "unchecked", l.Verdict)
		require.Equal(t, "none", l.Fidelity)
		require.Equal(t, 0, l.Pending)
	}
	sum := sumLines(lines)
	require.Equal(t, 2, sum.Rounds)
	require.Equal(t, int64(1), sum.Expected.Insert)
	require.Equal(t, int64(1), sum.Expected.Update)
}

func TestHealthNeverReports(t *testing.T) {
	// A connector that reports nothing still gets its expected counts logged,
	// with every stored binding pending.
	tr := &scriptedTransactor{bindings: twoBindings}
	txns := []txn{
		{stores: []pm.Request{storeReq(0, "a", false, false), storeReq(1, "b", false, false)}},
		{stores: []pm.Request{storeReq(0, "c", false, false)}},
	}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, nil, nil)
	require.Len(t, lines, 2)
	for _, l := range lines {
		require.Equal(t, "unchecked", l.Verdict)
		require.Equal(t, "none", l.Fidelity)
	}
	sum := sumLines(lines)
	require.Equal(t, 2, sum.Rounds)
	require.Equal(t, 3, sum.Pending)
	require.Equal(t, int64(3), sum.Expected.Insert)
	require.Equal(t, int64(0), sum.Actual.Total)
}

func TestHealthPendingBindingAtWindowFlush(t *testing.T) {
	prior := healthFlushInterval
	healthFlushInterval = 20 * time.Millisecond
	t.Cleanup(func() { healthFlushInterval = prior })

	tr := &scriptedTransactor{
		bindings: twoBindings,
		report: func(round, binding int, stored int64) *RowStats {
			if binding == 1 {
				return nil // Never reports.
			}
			s := ExactRowStats(stored, 0, 0)
			return &s
		},
	}
	txns := []txn{
		{stores: []pm.Request{storeReq(0, "a", false, false), storeReq(1, "b", false, false)}},
		{stores: []pm.Request{storeReq(0, "c", false, false)}},
	}

	// Hold the stream open past a window flush, so the flush (not EOF)
	// judges the round with the unreported binding.
	hold := make(chan struct{})
	var atFlush []healthLine
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, hold, func(hook *logtest.Hook) {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if atFlush = decodeHealthLines(t, hook); len(atFlush) >= 2 {
				// A late report for the round just judged as pending.
				tr.be.ReportRowStats(0, []string{"schema", "beta"}, ExactRowStats(1, 0, 0))
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("window flush did not log")
	})

	require.Len(t, atFlush, 2)
	require.Equal(t, "ok", atFlush[0].Verdict)
	require.Equal(t, 1, atFlush[0].Rounds)
	require.Equal(t, 1, atFlush[0].FirstRound)

	require.Equal(t, "unchecked", atFlush[1].Verdict)
	require.Equal(t, 1, atFlush[1].Rounds)
	require.Equal(t, 0, atFlush[1].FirstRound)
	require.Equal(t, 1, atFlush[1].Pending)
	require.Equal(t, int64(2), atFlush[1].Expected.Insert)
	require.Equal(t, int64(1), atFlush[1].Actual.Inserted)

	// The report that finally arrived for the judged round is logged as an
	// actual-only unchecked round rather than lost.
	require.Len(t, lines, 3)
	late := lines[2]
	require.Equal(t, "unchecked", late.Verdict)
	require.Equal(t, 1, late.Rounds)
	require.Equal(t, 0, late.FirstRound)
	require.Equal(t, int64(0), late.Expected.Insert)
	require.Equal(t, int64(1), late.Actual.Inserted)
	require.Equal(t, 0, late.Pending)
}

func TestHealthDeferredReports(t *testing.T) {
	// Reports made from Acknowledge, while the next round's stores are being
	// read, attribute to the round given.
	tr := &scriptedTransactor{
		bindings:     twoBindings,
		deferReports: true,
		report: func(round, binding int, stored int64) *RowStats {
			s := ExactRowStats(stored, 0, 0)
			return &s
		},
	}
	txns := []txn{
		{stores: []pm.Request{storeReq(0, "a", false, false)}},
		{stores: []pm.Request{storeReq(0, "b", false, false), storeReq(0, "c", false, false)}},
		{stores: []pm.Request{storeReq(1, "d", false, false)}},
	}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, nil, nil)
	require.Len(t, lines, 2)
	for _, l := range lines {
		require.Equal(t, "ok", l.Verdict)
	}
	sum := sumLines(lines)
	require.Equal(t, 3, sum.Rounds)
	require.Equal(t, int64(4), sum.Actual.Inserted)
}

func TestHealthRecovery(t *testing.T) {
	tr := &scriptedTransactor{
		bindings: twoBindings,
		report:   exactReport(1, 0, 0),
		recoveryReports: func(be *BindingEvents) {
			// A replayed idempotent merge from the prior session's round 7.
			be.ReportRowStats(7, []string{"schema", "alpha"}, ExactRowStats(5, 2, 0))
			be.ReportRowStats(7, []string{"schema", "beta"}, ExactRowStats(1, 0, 0))
		},
	}
	txns := []txn{{stores: []pm.Request{storeReq(0, "a", false, false)}}}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, nil), txns, nil, nil)
	require.Len(t, lines, 2)

	r := lines[0]
	require.True(t, r.Recovery)
	require.Equal(t, "unchecked", r.Verdict)
	require.Equal(t, "exact", r.Fidelity)
	require.Equal(t, 7, r.FirstRound)
	require.Equal(t, 1, r.Rounds)
	require.Equal(t, 2, r.Bindings)
	require.Equal(t, int64(0), r.Expected.Insert)
	require.Equal(t, int64(6), r.Actual.Inserted)
	require.Equal(t, int64(2), r.Actual.Updated)
	require.Empty(t, r.Mismatches)

	require.False(t, lines[1].Recovery)
	require.Equal(t, "ok", lines[1].Verdict)
	require.Equal(t, 0, lines[1].FirstRound)
}

func TestHealthSharded(t *testing.T) {
	tr := &scriptedTransactor{
		bindings: twoBindings,
		report: func(round, binding int, stored int64) *RowStats {
			// The primary merges its peers' work too, so its actuals exceed
			// its own expected counts.
			s := ExactRowStats(stored+1, 0, 0)
			return &s
		},
	}
	txns := []txn{
		{stores: []pm.Request{storeReq(0, "a", false, false)}},
		{stores: []pm.Request{storeReq(1, "b", false, false)}},
	}
	rng := &pf.RangeSpec{KeyBegin: 0, KeyEnd: math.MaxUint32 / 2, RClockEnd: math.MaxUint32}
	lines := runHealthScenario(t, tr, openRequest(twoBindings, rng), txns, nil, nil)
	require.Len(t, lines, 2)
	for _, l := range lines {
		require.True(t, l.Sharded)
		require.Equal(t, "unchecked", l.Verdict)
		require.Empty(t, l.Mismatches)
	}
	// Round 0's expected side is logged as soon as it is recorded, before its
	// actuals are reported; everything else rolls up to EOF.
	require.Equal(t, "none", lines[0].Fidelity)
	require.Equal(t, "exact", lines[1].Fidelity)
	sum := sumLines(lines)
	require.Equal(t, 2, sum.Rounds)
	require.Equal(t, int64(2), sum.Expected.Insert)
	require.Equal(t, int64(4), sum.Actual.Inserted)
}

func TestHealthUnknownBinding(t *testing.T) {
	// A report for a path the spec doesn't have is surfaced, not dropped.
	tr := &scriptedTransactor{
		bindings: twoBindings,
		report: func(round, binding int, stored int64) *RowStats {
			s := ExactRowStats(stored, 0, 0)
			return &s
		},
	}
	tr.recoveryReports = nil
	txns := []txn{{stores: []pm.Request{storeReq(0, "a", false, false)}}}

	hook := logtest.NewGlobal()
	defer hook.Reset()
	stream := &scriptedStream{requests: scriptRequests(txns)}
	wrapped := &reportingTransactor{scriptedTransactor: tr, extra: func(be *BindingEvents, round int) {
		be.ReportRowStats(round, []string{"schema", "gamma"}, TotalRowStats(3))
	}}
	require.NoError(t, RunTransactions(context.Background(), wrapped, stream, openRequest(twoBindings, nil), log.InfoLevel))
	lines := decodeHealthLines(t, hook)
	require.Len(t, lines, 1)
	require.Equal(t, "mismatch", lines[0].Verdict)
	require.Equal(t, 2, lines[0].Bindings)
	require.Equal(t, []healthMismatch{{Check: "total", ResourcePath: []string{"schema", "gamma"}, Expected: 0, Actual: 3}}, lines[0].Mismatches)
}

// reportingTransactor wraps a scriptedTransactor to make an extra report from
// each StartCommit.
type reportingTransactor struct {
	*scriptedTransactor
	extra func(be *BindingEvents, round int)
}

func (t *reportingTransactor) NewTransactor(ctx context.Context, open pm.Request_Open, be *BindingEvents) (Transactor, *pm.Response_Opened, *MaterializeOptions, error) {
	_, opened, opts, err := t.scriptedTransactor.NewTransactor(ctx, open, be)
	return t, opened, opts, err
}

func (t *reportingTransactor) Store(it *StoreIterator) (StartCommitFunc, error) {
	round := it.Round
	inner, err := t.scriptedTransactor.Store(it)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, cp *pc.Checkpoint) (*pf.ConnectorState, OpFuture) {
		t.extra(t.be, round)
		return inner(ctx, cp)
	}, nil
}

func TestHealthReportWithoutTransactions(t *testing.T) {
	// BindingEvents outside a transactions session, as in an Apply drain,
	// accept reports and do nothing.
	be := NewBindingEvents()
	be.ReportRowStats(0, []string{"a"}, TotalRowStats(1))
}

func TestHealthTrackerPanicSafety(t *testing.T) {
	h := newHealthTracker(openRequest(twoBindings, nil))
	h.log = func(log.Fields, string) { panic(fmt.Errorf("sink is broken")) }
	h.recordExpected(0, 0, 0)
	h.report(0, []string{"schema", "alpha"}, TotalRowStats(0))
	h.acknowledged(0) // Evaluates and logs; the panic is contained.
	h.close()
}
