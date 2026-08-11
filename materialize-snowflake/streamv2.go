package main

import (
	"cmp"
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// A batch is appended once it holds either this many documents or this many
// bytes. Batching is a pure performance knob rather than checkpoint state:
// recovery resumes from the document index Snowflake reports as committed,
// whatever boundaries the interrupted attempt happened to cut. They are vars
// only so that tests can cut batches without storing tens of thousands of rows.
var (
	streamV2BatchRows  = 10_000
	streamV2BatchBytes = 8 * 1024 * 1024
)

// streamV2MaxBufferedBytes caps the documents buffered across all bindings at
// once. Without it the per-binding cap above would make worst-case resident
// memory a multiple of the binding count, which a wide materialization with
// large transactions could make substantial. Exceeding it appends every
// non-empty buffer, however far short of a full batch they are.
var streamV2MaxBufferedBytes = 128 * 1024 * 1024

// streamV2Item records a binding's channel and how many documents have been
// appended to it.
//
// Unlike every other item of this connector's driver checkpoint, it is not
// pending work: it is durable per-binding state, reconciled against Snowflake's
// committed offset token before the channel's next append, and never cleared by
// Acknowledge.
//
// The checkpoint holds one of these per channel rather than one per binding.
// Every shard of a v2 task merge-patches its connector state into a single
// task-global document, so a binding's item must be keyed by something which
// distinguishes the shard that wrote it, or a sibling shard's reduce overwrites
// it. The channel name is that key: it is derived from the shard's key-begin.
type streamV2Item struct {
	Channel string
	// Counter is the index of the last document appended to Channel, counted
	// from the channel's first document and never reset.
	Counter int64
	// KeyBegin and KeyEnd record the shard key range of the transaction which
	// counted those documents, which is how a later topology recognises the
	// channels of shards whose key range it has taken over — see
	// absorbedChannels.
	KeyBegin uint32
	KeyEnd   uint32
}

// streamV2Range is a shard key range, as a committed offset token reports it.
type streamV2Range struct {
	keyBegin uint32
	keyEnd   uint32
}

func (r streamV2Range) String() string {
	return fmt.Sprintf("[%08x, %08x]", r.keyBegin, r.keyEnd)
}

// streamV2Token renders the offset token of an append: the index of the document
// it ends at, and the shard key range appending it.
//
// The range is in the token because the token is the only thing this connector
// records at the same instant as the rows it accounts for. A checkpoint item
// records a range as well, but only that of the last transaction which
// committed — while what a replay needs to know is which range appended the
// documents standing beyond that transaction, which by definition no checkpoint
// saw. Those are the documents a replay skips, and skipping them is positional,
// so reconcileStreamV2Channel asks the token whose subset of keys produced them.
func streamV2Token(counter int64, keyBegin, keyEnd uint32) string {
	return fmt.Sprintf("%d@%08x-%08x", counter, keyBegin, keyEnd)
}

// parseStreamV2Token reads a committed offset token back as the document count it
// carries and the shard key range which appended it, reporting false for a token
// this write path could not have written.
//
// Every token this path has ever written carries a range, so one without it is
// not this path's: the write path and the range in its token shipped together.
func parseStreamV2Token(token string) (int64, streamV2Range, bool) {
	var count, spec, hasRange = strings.Cut(token, "@")
	if !hasRange {
		return 0, streamV2Range{}, false
	}

	counter, err := strconv.ParseInt(count, 10, 64)
	if err != nil {
		return 0, streamV2Range{}, false
	}

	begin, end, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, streamV2Range{}, false
	}
	keyBegin, err := strconv.ParseUint(begin, 16, 32)
	if err != nil {
		return 0, streamV2Range{}, false
	}
	keyEnd, err := strconv.ParseUint(end, 16, 32)
	if err != nil {
		return 0, streamV2Range{}, false
	}
	return counter, streamV2Range{keyBegin: uint32(keyBegin), keyEnd: uint32(keyEnd)}, true
}

// streamsV2 reports whether a binding's rows are written by the snowpipe
// streaming v2 path, which appends them to a channel as it stores them — something
// only a delta-updates binding does, and only with the key pair JWT credentials
// carry to authenticate its sidecar.
//
// The transactor and Apply must agree on this: it decides both how a binding's
// rows are stored and what a backfill of that binding may do to its table — see
// client.MustRecreateResource.
func streamsV2(cfg *config, deltaUpdates bool, flagEnabled bool) bool {
	return deltaUpdates && cfg.Credentials.AuthType == snowflake_auth.JWT && flagEnabled
}

// streamV2GenerationPrefix introduces the line a streaming v2 table's comment
// carries to name the generation of the binding it belongs to.
const streamV2GenerationPrefix = "flow_generation: "

// streamV2Generation names one generation of one binding: the task, and the
// binding's state key, which a backfill rotates.
//
// The task is named as the materialization specification names it, on both sides:
// the client which writes a generation into a table's comment and the transactor
// which reads it back both take it from that one name.
type streamV2Generation struct {
	materialization string
	stateKey        string
}

// streamV2GenerationComment renders the comment a streaming v2 binding's table
// carries: the comment the table would have anyway, followed by a line naming the
// generation the table belongs to.
//
// The table has to say this because a channel cannot. A backfill re-creates the
// table (client.MustRecreateResource), which takes every channel bound to it, but
// the shards of the generation being replaced are still running and will open
// channels against the table again — and a channel opened against a re-created
// table is indistinguishable from a channel opened against a new one. The table
// itself is the only thing which outlives that drop and knows which generation it
// was created for.
func streamV2GenerationComment(comment string, gen streamV2Generation) string {
	return fmt.Sprintf("%s\n%s%s %s", comment, streamV2GenerationPrefix, gen.materialization, gen.stateKey)
}

// streamV2GenerationOf reads back the generation a table's comment names, and
// reports whether it named one at all. A comment written before this connector
// recorded generations, or one an operator has rewritten, names none.
func streamV2GenerationOf(comment string) (streamV2Generation, bool) {
	for _, line := range strings.Split(comment, "\n") {
		rest, found := strings.CutPrefix(strings.TrimSpace(line), streamV2GenerationPrefix)
		if !found {
			continue
		} else if materialization, stateKey, ok := strings.Cut(rest, " "); ok && materialization != "" && stateKey != "" {
			return streamV2Generation{materialization: materialization, stateKey: stateKey}, true
		}
	}
	return streamV2Generation{}, false
}

// streamV2TableComment reads the comment Snowflake holds for a table, which is
// where a streaming v2 table names the generation of the binding it belongs to. A
// table which does not exist, or which carries no comment, reports the empty
// string — neither names a generation.
//
// The table is named as Snowflake holds it, which is what the dialect's locator
// reports and what an unquoted identifier is folded to: a name in any other casing
// matches nothing here, and so would read as a table naming no generation.
func streamV2TableComment(ctx context.Context, db *stdsql.DB, dialect sql.Dialect, database, schema, table string) (string, error) {
	var query = fmt.Sprintf(
		"SELECT COMMENT FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;",
		dialect.Identifier(database),
	)

	var comment *string
	if err := db.QueryRowContext(ctx, query, schema, table).Scan(&comment); errors.Is(err, stdsql.ErrNoRows) {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("querying the comment of table %s.%s: %w", schema, table, err)
	} else if comment == nil {
		return "", nil
	}
	return *comment, nil
}

// streamV2GenerationConflict refuses a binding whose table records a different
// generation of that same binding as the one it belongs to.
//
// Such a table has been re-created by a backfill of the binding since this task's
// specification was published, so this task is running the specification that
// backfill replaced. Its rows are being re-materialized by the generation which
// now owns the table, and anything this one appends is a duplicate of them.
//
// A comment which names no generation is not read as a conflict: a table created
// before this connector recorded them, or one which a standard-updates generation
// of the binding created, says nothing about which generation may stream into it.
// Neither is a generation of another task: two tasks materializing into one table
// are not generations of each other, and the last of them to create the table is
// the one the comment names.
func streamV2GenerationConflict(comment string, mine streamV2Generation, table string) error {
	owner, ok := streamV2GenerationOf(comment)
	if !ok || owner.materialization != mine.materialization || owner.stateKey == mine.stateKey {
		return nil
	}

	return fmt.Errorf(
		"table %s records state key %q of this binding as the generation materializing into it, while this task is materializing state key %q: the binding has been backfilled, which re-created the table, and this task is running the specification that backfill replaced. The runtime replaces this task with the one that backfill published; appending to the table before it does would duplicate the rows the backfill is re-materializing",
		table, owner.stateKey, mine.stateKey,
	)
}

type streamV2Binding struct {
	database string
	schema   string
	table    string
	channel  string
	// stateKey names the generation of the binding this shard is materializing,
	// which the table it appends to must agree it belongs to.
	stateKey string
	// columns holds the row-object prefix of each Snowflake column, aligned with
	// the output of Table.ConvertAll, for writing the by-name row objects the SDK
	// ingests.
	columns []rowColumn
	// prior is the streaming v2 state the driver checkpoint recorded for this
	// binding, keyed by channel and covering every shard of the task, or nil if
	// it recorded none.
	prior map[string]*streamV2Item
	// opened reports whether the channel has been opened and reconciled.
	opened bool
	// retire names the channels of shards whose key range this shard absorbed and
	// which it has settled, to be deleted from the checkpoint. It is reported in
	// every checkpoint of the session rather than just the first, so that a
	// transaction which never commits does not lose the deletion.
	retire []string

	// counter is the index of the last document counted for this channel: the
	// checkpointed Counter at Open, advancing by one per document stored.
	counter int64
	// recorded is the counter value most recently reported in a checkpoint
	// item, distinguishing a binding which stored nothing this transaction.
	recorded int64
	// skip is the document index Snowflake had already committed when the
	// channel was opened. Documents at or below it were appended by an
	// interrupted attempt of the transaction now being replayed, so they are
	// counted but not appended again. Because indices are absolute, this single
	// threshold suffices for the whole session.
	skip int64
	// committed is the highest index known to be durably committed: Snowflake's
	// committed offset token when the channel was opened, and thereafter each
	// counter this session has committed. A counter at or below it must not be
	// waited on, because the wait is for the channel's committed token to *equal*
	// the requested one — which a channel already past it never will, so the wait
	// would run to its timeout. A replay whose documents Snowflake already holds
	// produces exactly such a counter.
	committed int64

	// buf is the payload of the batch being built: the framed JSON array the
	// append carries, written row by row as documents are stored, so that a
	// batch's bytes are assembled once rather than per row and again per batch.
	// bufRows counts the rows it holds and bufFirst is the index of the first.
	buf      []byte
	bufRows  int
	bufFirst int64
	// bufHint is the size of the batch handed off before this one, which sizes the
	// next buffer. The buffer itself cannot be reused: ownership of it passes to
	// the append it is handed to, which is still writing it to the sidecar while
	// the following batch is being built.
	bufHint int

	// pipe carries at most one append at a time, so Store keeps buffering while
	// a batch is on the wire without ever reordering batches.
	pipe appendPipe
}

// appendPipe runs one append at a time on a goroutine of its own. Submitting
// the next append reports the result of the previous one, so a failure surfaces
// at the following batch boundary or at flush and is never dropped.
type appendPipe struct {
	inflight chan error
}

func (p *appendPipe) submit(fn func() error) error {
	if err := p.wait(); err != nil {
		return err
	}
	p.inflight = make(chan error, 1)
	go func(done chan error) { done <- fn() }(p.inflight)
	return nil
}

func (p *appendPipe) wait() error {
	if p.inflight == nil {
		return nil
	}
	var err = <-p.inflight
	p.inflight = nil
	return err
}

// streamV2Manager implements the high-performance Snowpipe Streaming write path
// by supervising the Python SDK sidecar. Rows are appended to a per-binding
// channel as they are stored and committed as the transaction's checkpoint is
// produced, and the runtime's idempotent replay of an interrupted transaction is
// reconciled against Snowflake's committed offset token before that channel's
// next append. The sidecar is spawned on first use; any sidecar failure is fatal
// to the connector (crash-only).
type streamV2Manager struct {
	cfg             *config
	materialization string
	accountName     string
	channelBase     string
	argv            []string
	keyBegin        uint32
	keyEnd          uint32

	// tableComment reports the comment Snowflake holds for a binding's table,
	// which is where a table names the generation of the binding it belongs to —
	// see streamV2GenerationConflict. The transactor supplies it, since it owns
	// the connection the read needs.
	//
	// It is called as a channel is opened rather than when the session is built,
	// because the backfill it exists to catch lands while the session is running:
	// a comment read at startup would still name this shard's own generation.
	// Where it is nil, or reports no comment, no table names a generation and
	// none is refused.
	tableComment func(ctx context.Context, database, schema, table string) (string, error)

	// procCtx bounds the sidecar process lifetime to the transactor session
	// rather than to whichever caller's context first triggers ensureStarted.
	// A per-call context (e.g. the errgroup context flush awaits its commits on)
	// would otherwise SIGTERM the sidecar as soon as that call returned.
	// procCancel is invoked by stop().
	procCtx    context.Context
	procCancel context.CancelFunc

	mu     sync.Mutex
	sup    *sidecarSupervisor
	client *sidecarClient

	bindings map[int]*streamV2Binding

	// bufBytes is the sum of every binding's buffered bytes, bounded by
	// streamV2MaxBufferedBytes. Written only from Store and flush, which the
	// runtime never overlaps.
	bufBytes int
}

func newStreamV2Manager(ctx context.Context, cfg *config, materialization string, accountName string, keyRange *pf.RangeSpec) *streamV2Manager {
	procCtx, procCancel := context.WithCancel(ctx)
	return &streamV2Manager{
		cfg:             cfg,
		materialization: materialization,
		accountName:     accountName,
		channelBase:     channelName(materialization, keyRange.KeyBegin),
		argv:            defaultSidecarArgv(),
		keyBegin:        keyRange.KeyBegin,
		keyEnd:          keyRange.KeyEnd,
		procCtx:         procCtx,
		procCancel:      procCancel,
		bindings:        make(map[int]*streamV2Binding),
	}
}

// ensureStarted spawns and configures the sidecar exactly once.
func (m *streamV2Manager) ensureStarted(ctx context.Context) (*sidecarClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != nil {
		return m.client, nil
	}

	// The process is anchored to the manager's session-scoped context, not the
	// caller's, so it survives the return of whichever call first started it.
	sup, client, err := startSidecar(m.procCtx, m.argv)
	if err != nil {
		return nil, err
	}
	if err := client.Configure(ctx, sidecarProfile{
		Account:    m.accountName,
		User:       m.cfg.Credentials.User,
		URL:        fmt.Sprintf("https://%s:443", m.cfg.Host),
		PrivateKey: m.cfg.Credentials.PrivateKey,
		Role:       m.cfg.Role,
	}, sup.authToken); err != nil {
		sup.stop(client)
		return nil, fmt.Errorf("configuring sidecar: %w", err)
	}

	m.sup, m.client = sup, client
	return m.client, nil
}

// channelFor names the channel this shard appends a binding's documents to. It
// is named for the binding's state key rather than its table, so that
// backfilling a binding rotates its channel along with its checkpoint item. Both
// then start empty together, which is what makes a backfill the escape from a
// refusal — and what keeps an absent checkpoint item alongside a present
// committed token from ever being a fresh backfill, leaving only an interrupted
// first transaction and a channel an earlier topology left behind to tell apart.
func (m *streamV2Manager) channelFor(stateKey string) string {
	return fmt.Sprintf("%s_%s", m.channelBase, sanitizeAndAppendHash(stateKey))
}

// offsetToken renders the token of a document index this shard appends.
func (m *streamV2Manager) offsetToken(counter int64) string {
	return streamV2Token(counter, m.keyBegin, m.keyEnd)
}

// addBinding registers a binding on the streaming v2 write path, carrying
// forward the document counter the driver checkpoint recorded for it. The
// channel itself is opened on the binding's first document, by ensureChannel.
func (m *streamV2Manager) addBinding(database, schema, table string, target sql.Table, prior map[string]*streamV2Item) {
	var names []string
	for _, col := range target.Columns() {
		names = append(names, unquotedIdentifier(col.Identifier))
	}

	var channel = m.channelFor(target.StateKey)

	var b = &streamV2Binding{
		database: database,
		schema:   schema,
		table:    table,
		channel:  channel,
		stateKey: target.StateKey,
		columns:  rowColumnsOf(names),
		prior:    prior,
	}
	// Only this shard's own channel carries a counter this binding continues;
	// the rest of the map belongs to the task's other shards.
	if own := prior[channel]; own != nil {
		b.counter, b.recorded = own.Counter, own.Counter
	}

	m.bindings[target.Binding] = b
}

// checkpointFor returns the checkpoint entries to record for a binding: the item
// of this shard's own channel, alongside a deletion for every channel this shard
// has retired. The runtime reduces these as a merge patch, so the entries of the
// task's other shards are left as they are.
func (m *streamV2Manager) checkpointFor(binding int, item *streamV2Item) map[string]*streamV2Item {
	var entries = map[string]*streamV2Item{item.Channel: item}
	for _, channel := range m.bindings[binding].retire {
		entries[channel] = nil
	}
	return entries
}

// ensureChannel opens the binding's channel and captures its skip threshold, on
// the binding's first document.
//
// Opening is deferred to the first document, rather than done while the
// transactor is built, because a transactor built only to drain pending work —
// as Apply does before altering a table — stores nothing. Such a transactor must
// neither pay for a sidecar it will not use, nor be reconciled against a key
// range which stands in for the whole task rather than for one shard. Deferral
// costs nothing: the threshold is still captured before the first append, and
// being an absolute document index it then holds for the whole session.
func (m *streamV2Manager) ensureChannel(ctx context.Context, b *streamV2Binding) error {
	if b.opened {
		return nil
	}

	// Settled before a sidecar is started, let alone a channel opened: a shard
	// whose binding's table has been re-created by a backfill has nothing to
	// append there, whatever its own channel would report.
	if m.tableComment != nil {
		comment, err := m.tableComment(ctx, b.database, b.schema, unquotedIdentifier(b.table))
		if err != nil {
			return err
		} else if err := streamV2GenerationConflict(
			comment,
			streamV2Generation{materialization: m.materialization, stateKey: b.stateKey},
			b.table,
		); err != nil {
			return err
		}
	}

	client, err := m.ensureStarted(ctx)
	if err != nil {
		return err
	}

	// Settled before this shard's own channel is opened, so that a join this
	// shard cannot account for is refused before it appends anything.
	if err := m.retireAbsorbedChannels(ctx, client, b); err != nil {
		return err
	}

	status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, b.channel)
	if err != nil {
		return err
	}

	if err := rejectedRowsError(b.channel, b.table, status); err != nil {
		return err
	}

	skip, err := reconcileStreamV2Channel(b.channel, b.table, status.CommittedToken, b.prior, m.keyBegin, m.keyEnd)
	if err != nil {
		return err
	}
	b.skip, b.committed, b.opened = skip, skip, true

	log.WithFields(log.Fields{
		"table":          b.table,
		"channel":        b.channel,
		"committedToken": status.committedToken(),
		"counter":        b.counter,
		"skipThrough":    skip,
		"rowsErrorCount": status.RowsErrorCount,
	}).Info("opened snowpipe streaming v2 channel")
	return nil
}

// rejectedRowsError refuses a channel on which Snowflake has rejected any row,
// and reports nil otherwise.
//
// Snowflake's ingestion discards a row it rejects without failing either the
// append or the commit, and advances the channel's committed offset token past
// it, so this count is the only place the loss is visible. It counts the whole
// life of the channel and never resets, which is what makes one rejection refuse
// the binding for good: the discarded rows cannot be identified, so there is
// nothing to re-send and no state from which the connector could conclude the
// destination is whole again. Only a backfill, which rotates the channel along
// with the binding's checkpoint, clears it.
func rejectedRowsError(channel, table string, status *channelStatusResult) error {
	if status.RowsErrorCount == 0 {
		return nil
	}
	return fmt.Errorf(
		"channel %q had %d row(s) rejected and discarded by Snowflake, which reported: %s. Those rows are not in %s, and Snowflake's committed offset token has advanced past them, so they cannot be identified and re-sent. Backfill this binding",
		channel, status.RowsErrorCount, status.LastErrorMessage, table,
	)
}

// reconcileStreamV2Channel compares Snowflake's committed offset token for a
// channel against the document counter the driver checkpoint recorded for it,
// and reports the highest document index Snowflake already holds — the
// threshold below which a replayed document must not be appended again.
//
// prior holds the checkpoint's items for every channel of the task, of which at
// most one is this channel's. An absent item for this channel reads as a counter
// of zero — the channel's first transaction, whose interrupted attempt Snowflake
// may already hold documents for. Whether that reading is available at all is
// what the rest of the map decides.
//
// Where Snowflake holds documents the counter does not account for, whether they
// may be skipped turns on the key range the token says appended them, which is
// the shard's own or nothing is skipped at all.
func reconcileStreamV2Channel(channel, table string, committedToken *string, prior map[string]*streamV2Item, keyBegin, keyEnd uint32) (int64, error) {
	var own = prior[channel]
	var counter int64
	if own != nil {
		counter = own.Counter
	}

	// Snowflake holds nothing for this channel. With no counter to account for
	// there is nothing to skip; with one, the channel those documents were
	// appended to is gone, and the token which says which of them Snowflake holds
	// went with it.
	//
	// That is what a shard restarting into a backfill of its binding finds, since
	// the backfill re-creates the table and takes every channel bound to it — see
	// client.MustRecreateResource — and refusing is what stops that shard from
	// appending into the table which has been re-materialized under it. The
	// specification it is running is already being replaced, so the replacement
	// settles the refusal rather than the backfill it asks for.
	//
	// Nothing this connector does retires a shard's own channel, so both readings
	// are named: the operator who is looking at a backfill has lost nothing and
	// wants to know it, and the one who is not needs to hear that Snowflake's
	// account of this channel is gone. Neither bounds what the channel had
	// committed beyond the counter, which is why the remedy is the same either way.
	if committedToken == nil {
		if counter > 0 {
			return 0, fmt.Errorf(
				"channel %q has committed nothing while this task's checkpoint records %d documents appended to it. Either the binding has been backfilled, which re-created the table and took every channel bound to it — in which case no rows were lost and the generation which now owns the table is re-materializing them — or Snowflake has lost this channel's committed offset token. This shard cannot tell those apart, and neither leaves it able to identify which of its documents Snowflake still holds. Backfill this binding",
				channel, counter,
			)
		}
		return 0, nil
	}

	committed, appendedBy, ok := parseStreamV2Token(*committedToken)
	if !ok {
		return 0, fmt.Errorf(
			"channel %q reports the committed offset token %q, which is not a document count: this channel was written by something other than this connector's snowpipe_streaming_v2 write path, and continuing could duplicate or drop rows",
			channel, *committedToken,
		)
	}

	if committed < counter {
		return 0, fmt.Errorf(
			"channel %q has committed %d documents but this task's checkpoint records %d as appended: the channel has lost committed data, so the missing rows cannot be identified. Backfill this binding",
			channel, committed, counter,
		)
	}

	if own == nil && committed > 0 && len(prior) > 0 {
		// A token with no item to account for is this channel's own interrupted
		// first transaction only where the checkpoint holds nothing for the binding
		// at all, which a backfill is what produces — the state key rotates, and
		// channel and item start empty together. Where it holds anything, this
		// binding has been materializing under channels of other names, and a name
		// this shard's key-begin derives may just as well be one a join retired and
		// a later split re-created: the same name, holding a token which counts
		// another shard's documents.
		//
		// The two are indistinguishable here, so the line is drawn at any state at
		// all rather than at whether some item still records a range covering this
		// key-begin. An item's range is only as current as the last transaction its
		// shard stored anything in, so that narrower reading turns on the order
		// siblings happen to checkpoint in — and reads the wedge as safe as soon as
		// they have caught up.
		return 0, fmt.Errorf(
			"channel %q has committed %d documents this task's checkpoint cannot account for: it holds no item for this channel, only items its other channels wrote. Such a token is the footprint of a shard which no longer exists, whose channel name this shard's key-begin has derived again, and it cannot be told apart from an interrupted first transaction of this channel — so the documents it counts must not be skipped, or that many of the documents about to be materialized into %s are dropped instead. Backfill this binding",
			channel, committed, table,
		)
	}

	// Nothing outstanding, so no key range bears on anything.
	if committed == counter {
		return committed, nil
	}

	// The documents outstanding beyond the counter are an interrupted attempt of
	// the transaction about to be replayed, and they are skipped by position: the
	// replay must produce those same documents in that same order, which only the
	// key range that appended them produces. A narrower range replays a subset of
	// them and would skip past documents Snowflake does not hold; a wider one
	// replays a superset, and would skip documents of the sibling it absorbed.
	//
	// Under a backfill this is the state nearly every interruption leaves, since a
	// round long enough to be interrupted in is long enough for Snowflake to have
	// committed batches of it. It is also the state a split leaves for as long as
	// it takes the low child to commit a transaction of its own, which is why the
	// range is read off the token rather than off the item.
	var mine = streamV2Range{keyBegin: keyBegin, keyEnd: keyEnd}

	if appendedBy != mine {
		return 0, fmt.Errorf(
			"channel %q has %d documents Snowflake committed beyond the %d this task's checkpoint records, appended by a shard covering %s while this shard covers %s: a replay here carries a different set of documents, so those rows cannot be skipped safely. Backfill this binding, or restore the task's shard key ranges to the topology which appended them, so that the transaction those rows belong to can commit",
			channel, committed, counter, appendedBy, mine,
		)
	}

	return committed, nil
}

// absorbedChannels reports the checkpoint items belonging to shards whose key
// range this shard has taken over — what joining two shards into one produces —
// ordered by the range they covered.
//
// A channel is named for its shard's key-begin, so an item under a channel which
// is not this shard's belongs to another shard. One whose key-begin lies above
// this shard's own and within its range is not a shard which is still running:
// shard ranges tile the key space exactly, so no two live shards can stand in
// that relation. This shard has absorbed it, and now carries the documents it
// was appending to a channel of its own.
func absorbedChannels(channel string, prior map[string]*streamV2Item, keyBegin, keyEnd uint32) []*streamV2Item {
	var absorbed []*streamV2Item
	for ch, item := range prior {
		// A nil item is a channel already retired by an earlier session, whose
		// deletion the runtime has not yet reduced away.
		if ch == channel || item == nil || item.KeyBegin <= keyBegin || item.KeyBegin > keyEnd {
			continue
		}
		absorbed = append(absorbed, item)
	}
	slices.SortFunc(absorbed, func(a, b *streamV2Item) int {
		return cmp.Compare(a.KeyBegin, b.KeyBegin)
	})
	return absorbed
}

// retireAbsorbedChannels retires the channel of every shard whose key range this
// shard has absorbed: it settles the channel against the checkpoint, drops it in
// Snowflake, and marks its checkpoint item for removal.
//
// An absorbed shard's committed rows are in the table and its journal progress is
// recorded in the task's frontier, which is not per-shard, so this shard will not
// be delivered those documents again: the join needs no backfill. What it cannot
// survive is an absorbed shard which was interrupted, having appended documents
// its final transaction never committed. Those rows are in the table while the
// frontier does not account for them, so they will be delivered here and appended
// to this shard's own channel, where the absorbed channel's committed offset token
// cannot skip them. Since this write path serves delta-updates bindings, that
// would duplicate them for good.
//
// Deleting the checkpoint item is not on its own enough to retire the channel, and
// deleting it alone is what wedges a task scaled down and then back up. A split
// re-creates exactly the pivot boundaries a join removed, so the shard which
// arrives derives this same channel name — and finds a channel Snowflake still
// holds, with a committed offset token no checkpoint item is left to contradict.
// That shard is refused, as a shard which cannot tell an interrupted attempt of
// its own from another topology's leftovers must be. Dropping the channel in
// Snowflake is what makes the name safe to derive again, so both halves of the
// retirement happen here: the drop, and the deletion which takes the channel out
// of the checkpoint. Only the drop is durable at once, which is why a retirement
// must be safe to repeat — see absorbedChannelSettled.
//
// Opening an absorbed channel invalidates any opening its own shard still holds,
// which is safe precisely because that shard is gone — the topology it belonged
// to no longer tiles the key space, so the runtime will not start it.
func (m *streamV2Manager) retireAbsorbedChannels(ctx context.Context, client *sidecarClient, b *streamV2Binding) error {
	for _, item := range absorbedChannels(b.channel, b.prior, m.keyBegin, m.keyEnd) {
		status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, item.Channel)
		if err != nil {
			return fmt.Errorf("opening absorbed channel %q: %w", item.Channel, err)
		} else if err := rejectedRowsError(item.Channel, b.table, status); err != nil {
			return err
		} else if err := absorbedChannelSettled(item, status.CommittedToken, m.keyBegin, m.keyEnd); err != nil {
			return err
		}

		// Both checks above are ahead of the drop because the drop takes everything
		// Snowflake holds for the channel, the cumulative row-error count included:
		// a join must not become a way to clear a refusal for rows Snowflake
		// discarded, which are missing from the table whether the channel that
		// delivered them still exists or not.
		if err := retireChannel(ctx, client, item.Channel); err != nil {
			return err
		}

		b.retire = append(b.retire, item.Channel)

		log.WithFields(log.Fields{
			"table":          b.table,
			"channel":        item.Channel,
			"committedToken": status.committedToken(),
			"counter":        item.Counter,
			"absorbedRange":  fmt.Sprintf("[%08x, %08x]", item.KeyBegin, item.KeyEnd),
		}).Info("retiring the channel of a shard whose key range this shard has absorbed")
	}
	return nil
}

// retireChannel takes a channel out of service for good, by dropping it in
// Snowflake rather than only closing the local handle.
//
// Retirement has to reach Snowflake because a channel name is deterministic —
// derived from (task, key-begin, state key) — while the topology it names is
// not. A shard which is deleted and a later shard which happens to be given the
// same key-begin derive the same channel name, and a channel Snowflake still
// holds hands its committed offset token to whoever opens it next, which that
// shard can only refuse — its own documents are not the ones the token counts,
// and it has no way to tell which of them Snowflake holds. Dropping the channel
// is what makes the next open of the same name a genuinely fresh channel, and so
// what keeps a join from costing the next split a backfill.
//
// That the drop is what does this, rather than a plain close of the channel, was
// settled by experiment against live Snowflake rather than read off the SDK's
// contract: a close releases only the local handle, and Snowflake goes on
// reporting the same committed offset token to the next open of that name. The
// retirement subtest of TestStreamV2Manager is that experiment; the same
// sequence against the fake sidecar is TestStreamV2ChannelRetirement.
//
// The channel must already be open in this session, since the drop is expressed
// through the handle an open produced. Its committed rows are untouched: this
// retires the channel, not the data it delivered.
func retireChannel(ctx context.Context, client *sidecarClient, channel string) error {
	if err := client.CloseChannel(ctx, channel, true); err != nil {
		return fmt.Errorf("retiring channel %q: %w", channel, err)
	}
	log.WithFields(log.Fields{"channel": channel}).Info("retired snowpipe streaming v2 channel")
	return nil
}

// absorbedChannelSettled reports whether an absorbed shard's channel holds
// exactly the documents the checkpoint accounts for — or has already been retired
// — and so may be retired now.
func absorbedChannelSettled(item *streamV2Item, committedToken *string, keyBegin, keyEnd uint32) error {
	// A channel holding nothing at all is one this shard has already retired: the
	// drop is what discards a committed offset token, and it is reached only once
	// the channel was found settled against this very item. The two halves of a
	// retirement are not durable together — the drop reaches Snowflake as the
	// channel is settled, while the deletion of this item becomes durable only with
	// the transaction carrying it — so a shard interrupted in between restarts to
	// exactly this, and repeats the retirement.
	//
	// A table re-created under the task reports the same thing, having taken the
	// committed offset token of every channel bound to it. That is settled by the
	// generation the table records and by the reconciliation of this shard's own
	// channel, both of which refuse before anything is appended.
	//
	// Which is why reconcileStreamV2Channel reads the same report as lost data
	// rather than as a retirement: nothing retires a shard's own channel, so there
	// it has no benign explanation.
	if committedToken == nil {
		return nil
	}

	committed, _, ok := parseStreamV2Token(*committedToken)
	if !ok {
		return fmt.Errorf(
			"absorbed channel %q reports the committed offset token %q, which is not a document count: this channel was written by something other than this connector's snowpipe_streaming_v2 write path, and continuing could duplicate or drop rows",
			item.Channel, *committedToken,
		)
	}

	if committed > item.Counter {
		return fmt.Errorf(
			"channel %q of a shard covering [%08x, %08x], which this shard's range [%08x, %08x] has absorbed, has %d documents Snowflake committed beyond the %d this task's checkpoint records: that shard was interrupted, and the documents it did not account for will be replayed to this shard, where they cannot be told apart from new ones. Backfill this binding",
			item.Channel, item.KeyBegin, item.KeyEnd, keyBegin, keyEnd, committed, item.Counter,
		)
	} else if committed < item.Counter {
		return fmt.Errorf(
			"channel %q of a shard covering [%08x, %08x], which this shard's range [%08x, %08x] has absorbed, has committed %d documents but this task's checkpoint records %d as appended: the channel has lost committed data. Backfill this binding",
			item.Channel, item.KeyBegin, item.KeyEnd, keyBegin, keyEnd, committed, item.Counter,
		)
	}

	return nil
}

// writeRow counts one converted document and buffers it for append, sending the
// batch as soon as it reaches either cap. A document Snowflake already committed
// during an interrupted attempt of this transaction is counted but dropped.
func (m *streamV2Manager) writeRow(ctx context.Context, binding int, converted []any) error {
	var b = m.bindings[binding]
	if err := m.ensureChannel(ctx, b); err != nil {
		return err
	}

	b.counter++
	if b.counter <= b.skip {
		return nil
	}

	var grew, err = b.bufferRow(b.counter, converted)
	if err != nil {
		return fmt.Errorf("encoding row for %s: %w", b.table, err)
	}
	m.bufBytes += grew

	if b.bufRows >= streamV2BatchRows || len(b.buf) >= streamV2BatchBytes {
		return m.appendBatch(ctx, b)
	} else if m.bufBytes >= streamV2MaxBufferedBytes {
		return m.appendAllBatches(ctx)
	}
	return nil
}

// bufferRow writes the document at index counter into the payload of the batch
// being built, starting that batch if this is its first row, and reports how many
// bytes the payload grew by. A row the writer refuses leaves the payload holding
// exactly the rows batched before it, since they are still going to be appended
// under the offset token of whichever transaction does commit them.
//
// This is the whole of the encoding this path does: appendRowJSON copies through
// the values which reach it already encoded, and nothing rescans the row after it
// — the bytes it writes here are the bytes the append carries.
func (b *streamV2Binding) bufferRow(counter int64, converted []any) (int, error) {
	var before = len(b.buf)
	if b.bufRows == 0 {
		b.startBatch()
		b.bufFirst = counter
	} else {
		b.buf = append(b.buf, ',')
	}

	var payload, err = appendRowJSON(b.buf, b.columns, converted)
	if err != nil {
		b.buf = b.buf[:before]
		return 0, err
	}

	b.buf = payload
	b.bufRows++
	return len(b.buf) - before, nil
}

// finishBatch closes the batch's payload and gives up ownership of the buffer,
// reporting the payload, the number of rows it holds, and the buffered bytes it
// releases.
//
// Those bytes are counted before the closing bracket is written, because that is
// the byte total bufferRow reported as the batch was built: counting the bracket
// here as well would leave the manager's running total of buffered bytes a byte
// short of zero for every batch of the session, drifting its back-pressure
// ceiling away from the memory it stands for.
func (b *streamV2Binding) finishBatch() (payload []byte, rows int, released int) {
	released = len(b.buf)
	b.buf = append(b.buf, ']')

	payload, rows = b.buf, b.bufRows
	b.bufHint = len(b.buf)
	b.buf, b.bufRows, b.bufFirst = nil, 0, 0
	return payload, rows, released
}

// streamV2MinBatchCapacity is the payload buffer a batch starts with before any
// batch of the binding has been sent to size it.
const streamV2MinBatchCapacity = 8 * 1024

// startBatch opens the payload of a new batch, sized a little over the batch
// before it so that a full one is not grown and copied a dozen times on its way
// to 8 MiB. The buffer is always a new one, since the batch before it owns the
// bytes it was handed.
func (b *streamV2Binding) startBatch() {
	b.buf = append(make([]byte, 0, max(b.bufHint+b.bufHint/8, streamV2MinBatchCapacity)), '[')
}

// appendAllBatches appends every binding's buffered documents, to bring total
// buffered bytes back to zero.
func (m *streamV2Manager) appendAllBatches(ctx context.Context) error {
	for _, b := range m.bindings {
		if b.bufRows == 0 {
			continue
		} else if err := m.appendBatch(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// appendBatch closes the batch's payload and hands it to the binding's append
// pipe. Ownership of the buffer passes to the append, so the next batch starts a
// buffer of its own.
func (m *streamV2Manager) appendBatch(ctx context.Context, b *streamV2Binding) error {
	var first, last = b.bufFirst, b.counter
	payload, rows, released := b.finishBatch()
	m.bufBytes -= released

	// Back-pressure is implicit: a saturated pipe blocks the append, which
	// blocks the next batch's submission, which blocks Store.
	var err = b.pipe.submit(func() error {
		client, err := m.ensureStarted(ctx)
		if err != nil {
			return err
		}
		return client.Append(ctx, b.channel, m.offsetToken(first), m.offsetToken(last), payload, rows)
	})
	if err != nil {
		return fmt.Errorf("appending to channel %q: %w", b.channel, err)
	}
	return nil
}

// flush appends each binding's remaining buffered documents, waits for every
// in-flight append, and then waits for Snowflake to durably commit them. It
// returns a checkpoint item for each binding whose counter advanced during this
// transaction — reported only once Snowflake holds the documents that counter
// accounts for.
//
// The commit is awaited here, as part of producing the checkpoint, rather than
// at Acknowledge. The runtime's own checkpoint is durable before Acknowledge
// runs, so a commit which failed there could no longer fail the transaction
// whose counter it belongs to: the task would resume from a counter Snowflake
// never committed, which the next Open can only refuse, and no replay would
// re-append rows the runtime already considers delivered. Failing here instead
// leaves the transaction to be replayed, and the documents Snowflake did commit
// are skipped by that Open's reconciliation.
func (m *streamV2Manager) flush(ctx context.Context) (map[int]*streamV2Item, error) {
	var items = make(map[int]*streamV2Item)
	for idx, b := range m.bindings {
		if b.bufRows > 0 {
			if err := m.appendBatch(ctx, b); err != nil {
				return nil, err
			}
		}
		if err := b.pipe.wait(); err != nil {
			return nil, fmt.Errorf("appending to channel %q: %w", b.channel, err)
		}

		// A replayed transaction which produced fewer documents than the
		// interrupted attempt committed was not replayed identically, so the skip
		// threshold dropped documents Snowflake does not in fact hold. Only the
		// session's first transaction can trip this, since the counter only
		// grows from there.
		if b.opened && b.counter < b.skip {
			return nil, fmt.Errorf(
				"channel %q holds %d committed documents but the transaction replayed against it produced only %d: it was not replayed identically, so the rows Snowflake already holds cannot be identified. Backfill this binding",
				b.channel, b.skip, b.counter,
			)
		}

		if b.counter == b.recorded {
			continue // stored nothing for this binding
		}
		b.recorded = b.counter

		items[idx] = &streamV2Item{
			Channel:  b.channel,
			Counter:  b.counter,
			KeyBegin: m.keyBegin,
			KeyEnd:   m.keyEnd,
		}
	}

	// Channels commit independently, and the sidecar serializes its ops per
	// channel rather than globally, so a wide materialization does not pay each
	// binding's commit latency in series.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(MaxConcurrentQueries)
	for idx, item := range items {
		var b = m.bindings[idx]
		group.Go(func() error { return m.waitCommit(groupCtx, b, item.Counter) })
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	return items, nil
}

// waitCommit blocks until the binding's channel has durably committed every
// document counted through counter, and fails the transaction if Snowflake
// rejected any of its rows.
func (m *streamV2Manager) waitCommit(ctx context.Context, b *streamV2Binding, counter int64) error {
	// A replay of documents Snowflake already holds appends nothing, so its
	// counter is already committed and must not be waited on.
	if counter <= b.committed {
		return nil
	}

	client, err := m.ensureStarted(ctx)
	if err != nil {
		return err
	}

	// The commit is awaited before StartedCommit reaches the runtime, which is
	// ahead of the runtime's own periodic reporting of a commit in progress, so
	// this wait is otherwise unaccounted for until it returns or times out.
	log.WithFields(log.Fields{
		"table":   b.table,
		"channel": b.channel,
		"counter": counter,
	}).Info("snowpipe streaming v2: awaiting commit")

	var started = time.Now()
	var token = m.offsetToken(counter)
	status, err := client.WaitCommit(ctx, b.channel, token)
	if err != nil {
		return fmt.Errorf("waiting for commit of document %s on channel %q: %w", token, b.channel, err)
	}

	// The token landing is the earliest this transaction's own rejections can be
	// seen, and this is now before its counter is checkpointed: a rejection fails
	// the transaction which produced it rather than a later one.
	if err := rejectedRowsError(b.channel, b.table, status); err != nil {
		return err
	}
	b.committed = counter

	log.WithFields(log.Fields{
		"table":   b.table,
		"channel": b.channel,
		"counter": counter,
		"took":    time.Since(started).String(),
	}).Info("snowpipe streaming v2: committed")
	return nil
}

func (m *streamV2Manager) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sup != nil {
		m.sup.stop(m.client)
	}
	m.procCancel()
}

// unquotedIdentifier undoes SQL identifier quoting, yielding the exact name as
// stored in Snowflake for by-name row construction.
func unquotedIdentifier(ident string) string {
	if strings.HasPrefix(ident, `"`) && strings.HasSuffix(ident, `"`) && len(ident) >= 2 {
		return strings.ReplaceAll(ident[1:len(ident)-1], `""`, `"`)
	}
	return ident
}
