package boilerplate

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// queryProfileMinDuration is how long a query must run to be profiled.
	queryProfileMinDuration = 5 * time.Second
	// queryProfileQueueSize bounds the queries awaiting their history. A query
	// finishing while it's full isn't profiled.
	queryProfileQueueSize = 256
	// queryProfileBatchSize is the most queries one history fetch asks for.
	queryProfileBatchSize = 50
	// QueryProfileMaxWait is how long a query waits for its history to be
	// final before it's logged without it.
	QueryProfileMaxWait = 10 * time.Minute
	// tableContextTimeout bounds reading a table's context.
	tableContextTimeout = time.Minute
	// queryProfileMaxBindings is the most bindings logged of a query over
	// many of them: those it looks up the most keys of.
	queryProfileMaxBindings = 10
)

// ErrQueryHistoryDenied is wrapped by a QueryHistory error when the
// credentials aren't allowed to read query history.
var ErrQueryHistoryDenied = errors.New("not permitted to read query history")

// QueryHistory reads a warehouse's records of finished queries.
type QueryHistory interface {
	// Fetch returns the fields to log of those of the queries whose history is
	// final, by query ID.
	Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error)
}

// QueryIDReporter has the database driver report the ID of a query run with
// the returned context to setID. The returned func, if not nil, is called
// once the query finishes, before its ID is read. A connector whose driver
// doesn't report IDs sets them with QueryTrace.SetID instead.
type QueryIDReporter func(ctx context.Context, setID func(string)) (context.Context, func())

// TableContext reads what explains a binding's slow queries beyond their
// history, such as its table's layout and maintenance. It's called once per
// binding, in the background, and its fields are added to the binding's first
// logged profile. Fields it returns with an error are still logged.
type TableContext func(ctx context.Context, binding int) (log.Fields, error)

// ProfileBounds are the key bounds a binding's queries are restricted to.
type ProfileBounds interface {
	// ProfileFields describes the bounds as a query profile logs them.
	ProfileFields() map[string]any
}

// QueryProfiler logs slow queries with what the connector knows of them and
// the metrics the warehouse records of them in its query history. History is
// fetched in the background, so profiling never holds up a query.
type QueryProfiler struct {
	history       QueryHistory
	interval      time.Duration
	reportIDs     QueryIDReporter
	resourcePaths [][]string
	minDuration   time.Duration
	queue         chan *profiledQuery
	emit          func(log.Fields)
	warned        bool
	cancel        context.CancelFunc
	done          sync.WaitGroup
	tableContext  TableContext
	contextRead   []bool
}

// profiledQuery is a finished query awaiting its history. One without an ID
// has none to await.
type profiledQuery struct {
	id       string
	fields   log.Fields
	tables   []profiledTable
	finished time.Time
}

// profiledTable is the fields of a binding the query reads or writes, which
// its table's context is added to.
type profiledTable struct {
	binding int
	fields  log.Fields
}

// NewQueryProfiler starts a profiler of the queries of bindings with the given
// resource paths, which fetches the history of finished queries every
// interval.
func NewQueryProfiler(history QueryHistory, interval time.Duration, reportIDs QueryIDReporter, resourcePaths [][]string, tableContext TableContext) *QueryProfiler {
	var ctx, cancel = context.WithCancel(context.Background())
	var p = &QueryProfiler{
		history:       history,
		interval:      interval,
		reportIDs:     reportIDs,
		resourcePaths: resourcePaths,
		minDuration:   queryProfileMinDuration,
		queue:         make(chan *profiledQuery, queryProfileQueueSize),
		emit:          func(fields log.Fields) { log.WithFields(fields).Info("query profile") },
		cancel:        cancel,
		tableContext:  tableContext,
		contextRead:   make([]bool, len(resourcePaths)),
	}
	p.done.Go(func() { p.run(ctx) })
	return p
}

// Stop ends the profiler, logging the queries still awaiting their history
// without it.
func (p *QueryProfiler) Stop() {
	p.cancel()
	p.done.Wait()
}

// QueryProfile collects what the connector learns of each binding over a phase
// of a transaction, such as its loads or its commit, for the profiles of the
// binding's queries in that phase. It's safe for concurrent use.
type QueryProfile struct {
	p        *QueryProfiler
	mu       sync.Mutex
	bindings map[int]*bindingProfile
}

type bindingProfile struct {
	keys   int
	bounds ProfileBounds
	fields log.Fields
}

// NewProfile starts collecting a phase's profile.
func (p *QueryProfiler) NewProfile() *QueryProfile {
	return &QueryProfile{p: p, bindings: make(map[int]*bindingProfile)}
}

// Trace starts measuring a query of the binding that only logs its resource
// path. See QueryProfile.Trace.
func (p *QueryProfiler) Trace(ctx context.Context, binding int, kind string) (context.Context, *QueryTrace) {
	return p.NewProfile().Trace(ctx, binding, kind)
}

func (qp *QueryProfile) binding(binding int) *bindingProfile {
	var b = qp.bindings[binding]
	if b == nil {
		b = &bindingProfile{}
		qp.bindings[binding] = b
	}
	return b
}

// AddKeys counts keys the binding's queries look up.
func (qp *QueryProfile) AddKeys(binding int, n int) {
	qp.mu.Lock()
	qp.binding(binding).keys += n
	qp.mu.Unlock()
}

// SetBounds sets the key bounds the binding's queries are restricted to.
func (qp *QueryProfile) SetBounds(binding int, bounds ProfileBounds) {
	qp.mu.Lock()
	qp.binding(binding).bounds = bounds
	qp.mu.Unlock()
}

// Set sets a field of the profiles of the binding's queries.
func (qp *QueryProfile) Set(binding int, key string, value any) {
	qp.mu.Lock()
	var b = qp.binding(binding)
	if b.fields == nil {
		b.fields = make(log.Fields)
	}
	b.fields[key] = value
	qp.mu.Unlock()
}

// fields are the fields the binding's queries are logged with.
func (qp *QueryProfile) fields(binding int) log.Fields {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	return qp.bindingFields(binding)
}

func (qp *QueryProfile) bindingFields(binding int) log.Fields {
	var out = make(log.Fields)
	if binding >= 0 && binding < len(qp.p.resourcePaths) {
		out["resourcePath"] = qp.p.resourcePaths[binding]
	}
	if b := qp.bindings[binding]; b != nil {
		if b.keys > 0 {
			out["keys"] = b.keys
		}
		if b.bounds != nil {
			out["bounds"] = b.bounds.ProfileFields()
		}
		maps.Copy(out, b.fields)
	}
	return out
}

// tables are the fields of each binding the phase has learned of, for a query
// over all of them, and how many bindings beyond the limit were left out.
func (qp *QueryProfile) tables() (tables []profiledTable, omitted int) {
	qp.mu.Lock()
	defer qp.mu.Unlock()

	var bindings = slices.SortedFunc(maps.Keys(qp.bindings), func(a, b int) int {
		return cmp.Or(cmp.Compare(qp.bindings[b].keys, qp.bindings[a].keys), cmp.Compare(a, b))
	})
	if len(bindings) > queryProfileMaxBindings {
		omitted = len(bindings) - queryProfileMaxBindings
		bindings = bindings[:queryProfileMaxBindings]
	}
	for _, binding := range bindings {
		tables = append(tables, profiledTable{binding: binding, fields: qp.bindingFields(binding)})
	}
	return tables, omitted
}

// QueryTrace measures one query run. Its methods other than SetID must be
// called from one goroutine.
type QueryTrace struct {
	profile   *QueryProfile
	binding   int
	all       bool
	kind      string
	started   time.Time
	collectID func()
	id        atomic.Pointer[string]
	fields    log.Fields

	executeTook    time.Duration
	docs, docBytes int
}

// Trace starts measuring a query of the given kind of the binding, which must
// be run with the returned context for the driver to report its ID.
func (qp *QueryProfile) Trace(ctx context.Context, binding int, kind string) (context.Context, *QueryTrace) {
	var t = &QueryTrace{profile: qp, binding: binding, kind: kind, started: time.Now()}
	if qp.p.reportIDs != nil {
		ctx, t.collectID = qp.p.reportIDs(ctx, t.SetID)
	}
	return ctx, t
}

// TraceAll starts measuring a query over every binding the phase has learned
// of, such as a union of their loads. See Trace.
func (qp *QueryProfile) TraceAll(ctx context.Context, kind string) (context.Context, *QueryTrace) {
	var ctx2, t = qp.Trace(ctx, -1, kind)
	t.all = true
	return ctx2, t
}

// SetID records the ID the warehouse gave the query.
func (t *QueryTrace) SetID(id string) {
	t.id.Store(&id)
}

// Set sets a field of the query's profile, such as a metric its result came
// with.
func (t *QueryTrace) Set(key string, value any) {
	if t.fields == nil {
		t.fields = make(log.Fields)
	}
	t.fields[key] = value
}

// Elapsed is the time since the query was traced.
func (t *QueryTrace) Elapsed() time.Duration {
	return time.Since(t.started)
}

// Slow reports whether the query has run long enough to be profiled, for a
// connector that must ask the warehouse for its ID.
func (t *QueryTrace) Slow() bool {
	return t.Elapsed() >= t.profile.p.minDuration
}

// Executed records that the query's results began to arrive. The profile of a
// query that returns documents then logs how many it returned.
func (t *QueryTrace) Executed() {
	if t.executeTook == 0 {
		t.executeTook = t.Elapsed()
	}
}

// AddDocument counts a document of the given size that the query returned.
func (t *QueryTrace) AddDocument(size int) {
	t.docs++
	t.docBytes += size
}

// Finish queues the query to be logged with its history if it ran long enough.
func (t *QueryTrace) Finish(err error) {
	var took = t.Elapsed()
	if took < t.profile.p.minDuration {
		return
	}
	if t.collectID != nil {
		t.collectID()
	}

	var q = &profiledQuery{finished: time.Now()}
	if t.all {
		var tables, omitted = t.profile.tables()
		var bindings = make([]log.Fields, len(tables))
		for i, table := range tables {
			bindings[i] = table.fields
		}
		q.fields, q.tables = log.Fields{"bindings": bindings}, tables
		if omitted > 0 {
			q.fields["bindingsOmitted"] = omitted
		}
	} else {
		q.fields = t.profile.fields(t.binding)
		q.tables = []profiledTable{{binding: t.binding, fields: q.fields}}
	}
	maps.Copy(q.fields, t.fields)
	maps.Copy(q.fields, log.Fields{"query": t.kind, "took": took.String()})
	if id := t.id.Load(); id != nil {
		q.id = *id
		q.fields["queryId"] = *id
	}
	if t.executeTook > 0 {
		maps.Copy(q.fields, log.Fields{"executeTook": t.executeTook.String(), "docs": t.docs, "docBytes": t.docBytes})
	}
	if err != nil {
		q.fields["failed"] = true
	}

	select {
	case t.profile.p.queue <- q:
	default:
	}
}

func (p *QueryProfiler) run(ctx context.Context) {
	var ticker = time.NewTicker(p.interval)
	defer ticker.Stop()

	var awaiting []*profiledQuery
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case q := <-p.queue:
					awaiting = append(awaiting, q)
				default:
					for _, q := range awaiting {
						p.emit(q.fields)
					}
					return
				}
			}
		case q := <-p.queue:
			awaiting = append(awaiting, q)
		case <-ticker.C:
			awaiting = p.fetch(ctx, awaiting, time.Now())
		}
	}
}

// fetch logs each awaiting query whose history is final, or which has waited
// too long for it, and returns the others.
func (p *QueryProfiler) fetch(ctx context.Context, awaiting []*profiledQuery, now time.Time) []*profiledQuery {
	var ids []string
	for _, q := range awaiting {
		if q.id != "" {
			ids = append(ids, q.id)
		}
	}

	var history = make(map[string]log.Fields)
	for ids := range slices.Chunk(ids, queryProfileBatchSize) {
		if p.history == nil {
			break
		}
		fetched, err := p.history.Fetch(ctx, ids)
		if errors.Is(err, ErrQueryHistoryDenied) {
			log.WithError(err).Warn("not permitted to read query history; query profiles won't include it")
			p.history = nil
		} else if err != nil {
			var entry = log.WithError(err)
			if !p.warned {
				entry.Warn("fetching query history for query profiles failed")
				p.warned = true
			} else {
				entry.Debug("fetching query history for query profiles failed")
			}
		} else {
			maps.Copy(history, fetched)
		}
	}

	var rest = awaiting[:0]
	for _, q := range awaiting {
		var fields, ok = history[q.id]
		if !ok && q.id != "" && p.history != nil && now.Sub(q.finished) < QueryProfileMaxWait {
			rest = append(rest, q)
			continue
		}
		maps.Copy(q.fields, fields)
		for _, table := range q.tables {
			p.addTableContext(ctx, table)
		}
		p.emit(q.fields)
	}
	return rest
}

// addTableContext adds the binding's table context to its first logged
// profile.
func (p *QueryProfiler) addTableContext(ctx context.Context, table profiledTable) {
	var binding = table.binding
	if p.tableContext == nil || binding < 0 || binding >= len(p.contextRead) || p.contextRead[binding] {
		return
	}
	p.contextRead[binding] = true

	ctx, cancel := context.WithTimeout(ctx, tableContextTimeout)
	defer cancel()
	extra, err := p.tableContext(ctx, binding)
	if err != nil {
		log.WithError(err).WithField("resourcePath", p.resourcePaths[binding]).Debug("reading table context for query profiles failed")
	}
	maps.Copy(table.fields, extra)
}

// StatementKind names a query by its leading keyword, such as "merge" or
// "copy".
func StatementKind(query string) string {
	if fields := strings.Fields(query); len(fields) > 0 {
		return strings.ToLower(fields[0])
	}
	return ""
}
