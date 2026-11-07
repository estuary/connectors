package boilerplate

import (
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
	// tableDescribeInterval is how often a table may be described.
	tableDescribeInterval = time.Hour
	// tableDescribeTimeout bounds describing a table.
	tableDescribeTimeout = time.Minute
	// tableDescribeQueueSize bounds the tables awaiting their description. A
	// table queued while it's full isn't described.
	tableDescribeQueueSize = 16
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
// once the query finishes, before its ID is read.
type QueryIDReporter func(ctx context.Context, setID func(string)) (context.Context, func())

// TableDescriber reads what explains the performance of queries of a
// binding's table, such as its layout and maintenance, beyond what query
// history shows. It runs queries of its own, so it's called at most once per
// tableDescribeInterval for each table with slow queries, and never on a
// transaction's path.
type TableDescriber interface {
	DescribeTable(ctx context.Context, binding int) (log.Fields, error)
}

// QueryProfilerOption configures a QueryProfiler.
type QueryProfilerOption func(*QueryProfiler)

// WithTableDescriber has the profiler log a description of the table of a slow
// query's binding alongside its profile.
func WithTableDescriber(d TableDescriber) QueryProfilerOption {
	return func(p *QueryProfiler) { p.describer = d }
}

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

	describer     TableDescriber
	describeQueue chan int
	describedAt   map[int]time.Time
	emitTable     func(log.Fields)
}

// profiledQuery is a finished query awaiting its history.
type profiledQuery struct {
	id       string
	binding  int
	fields   log.Fields
	finished time.Time
}

// NewQueryProfiler starts a profiler of the queries of bindings with the given
// resource paths, which fetches the history of finished queries every
// interval. A nil history logs queries without it.
func NewQueryProfiler(history QueryHistory, interval time.Duration, reportIDs QueryIDReporter, resourcePaths [][]string, opts ...QueryProfilerOption) *QueryProfiler {
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
		describeQueue: make(chan int, tableDescribeQueueSize),
		describedAt:   make(map[int]time.Time),
		emitTable:     func(fields log.Fields) { log.WithFields(fields).Info("query profile table") },
	}
	for _, opt := range opts {
		opt(p)
	}
	p.done.Go(func() { p.run(ctx) })
	if p.describer != nil {
		p.done.Go(func() { p.runDescribes(ctx) })
	}
	return p
}

// Stop ends the profiler, dropping the queries still awaiting their history.
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
	var out = make(log.Fields)
	if binding >= 0 && binding < len(qp.p.resourcePaths) {
		out["resourcePath"] = qp.p.resourcePaths[binding]
	}

	qp.mu.Lock()
	defer qp.mu.Unlock()
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

// QueryTrace measures one query run. Its methods other than SetID must be
// called from one goroutine.
type QueryTrace struct {
	profile   *QueryProfile
	binding   int
	kind      string
	started   time.Time
	collectID func()
	id        atomic.Pointer[string]

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

// SetID records the ID the warehouse gave the query.
func (t *QueryTrace) SetID(id string) {
	t.id.Store(&id)
}

// Elapsed is the time since the query was traced.
func (t *QueryTrace) Elapsed() time.Duration {
	return time.Since(t.started)
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
	var id = t.id.Load()
	if id == nil {
		return
	}

	var q = &profiledQuery{id: *id, binding: t.binding, fields: t.profile.fields(t.binding), finished: time.Now()}
	maps.Copy(q.fields, log.Fields{"query": t.kind, "queryId": *id, "took": took.String()})
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
			return
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
	var history = make(map[string]log.Fields)
	for batch := range slices.Chunk(awaiting, queryProfileBatchSize) {
		if p.history == nil {
			break
		}
		var ids = make([]string, len(batch))
		for i, q := range batch {
			ids[i] = q.id
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
		if fields, ok := history[q.id]; ok {
			var out = maps.Clone(q.fields)
			maps.Copy(out, fields)
			p.emit(out)
			p.requestDescribe(q.binding, now)
		} else if p.history == nil || now.Sub(q.finished) >= QueryProfileMaxWait {
			p.emit(q.fields)
			p.requestDescribe(q.binding, now)
		} else {
			rest = append(rest, q)
		}
	}
	return rest
}

// requestDescribe queues the binding's table to be described, unless it was
// within tableDescribeInterval.
func (p *QueryProfiler) requestDescribe(binding int, now time.Time) {
	if p.describer == nil || binding < 0 || binding >= len(p.resourcePaths) {
		return
	} else if last, ok := p.describedAt[binding]; ok && now.Sub(last) < tableDescribeInterval {
		return
	}
	select {
	case p.describeQueue <- binding:
		p.describedAt[binding] = now
	default:
	}
}

func (p *QueryProfiler) runDescribes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case binding := <-p.describeQueue:
			p.describe(ctx, binding)
		}
	}
}

// describe logs the description of the binding's table. A failure is logged
// at debug level only: it's diagnostics, and must never trouble the task.
func (p *QueryProfiler) describe(ctx context.Context, binding int) {
	ctx, cancel := context.WithTimeout(ctx, tableDescribeTimeout)
	defer cancel()

	var path = p.resourcePaths[binding]
	fields, err := p.describer.DescribeTable(ctx, binding)
	if err != nil {
		log.WithError(err).WithField("resourcePath", path).Debug("describing table for query profiles failed")
	}
	if len(fields) > 0 {
		var out = log.Fields{"resourcePath": path}
		maps.Copy(out, fields)
		p.emitTable(out)
	}
}

// StatementKind names a query by its leading keyword, such as "merge" or
// "copy".
func StatementKind(query string) string {
	if fields := strings.Fields(query); len(fields) > 0 {
		return strings.ToLower(fields[0])
	}
	return ""
}
