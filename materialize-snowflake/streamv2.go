package main

import (
	"cmp"
	"context"
	"encoding/json"
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
	// KeyBegin and KeyEnd record the shard key range which appended those
	// documents. A replay by a differently-split shard carries a different
	// subset of keys, which positional skipping cannot survive.
	KeyBegin uint32
	KeyEnd   uint32
}

// streamsV2 reports whether a binding's rows are written by the snowpipe
// streaming v2 path: it appends rows to a channel as it stores them, which only a
// delta-updates binding can do, and authenticates its sidecar with the key pair
// JWT credentials carry.
//
// It decides both how the transactor stores a binding's rows and what a backfill
// of that binding may do to its table, which must agree — see
// client.MustRecreateResource.
func streamsV2(cfg *config, deltaUpdates bool, flagEnabled bool) bool {
	return deltaUpdates && cfg.Credentials.AuthType == snowflake_auth.JWT && flagEnabled
}

type streamV2Binding struct {
	database string
	schema   string
	table    string
	channel  string
	// columns holds unquoted Snowflake column names aligned with the output of
	// Table.ConvertAll, for building the by-name row objects the SDK ingests.
	columns []string
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

	// buf accumulates the documents of the batch being built, and bufFirst is
	// the index of buf[0].
	buf      []json.RawMessage
	bufBytes int
	bufFirst int64

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
	cfg         *config
	accountName string
	channelBase string
	argv        []string
	keyBegin    uint32
	keyEnd      uint32

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
		cfg:         cfg,
		accountName: accountName,
		channelBase: channelName(materialization, keyRange.KeyBegin),
		argv:        defaultSidecarArgv(),
		keyBegin:    keyRange.KeyBegin,
		keyEnd:      keyRange.KeyEnd,
		procCtx:     procCtx,
		procCancel:  procCancel,
		bindings:    make(map[int]*streamV2Binding),
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
// refusal — and what makes an absent checkpoint item alongside a present
// committed token unambiguously an interruption of this generation's first
// transaction rather than a fresh backfill.
func (m *streamV2Manager) channelFor(stateKey string) string {
	return fmt.Sprintf("%s_%s", m.channelBase, sanitizeAndAppendHash(stateKey))
}

// addBinding registers a binding on the streaming v2 write path, carrying
// forward the document counter the driver checkpoint recorded for it. The
// channel itself is opened on the binding's first document, by ensureChannel.
func (m *streamV2Manager) addBinding(database, schema, table string, target sql.Table, prior map[string]*streamV2Item) {
	var columns []string
	for _, col := range target.Columns() {
		columns = append(columns, unquotedIdentifier(col.Identifier))
	}

	var channel = m.channelFor(target.StateKey)

	var b = &streamV2Binding{
		database: database,
		schema:   schema,
		table:    table,
		channel:  channel,
		columns:  columns,
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

	skip, err := reconcileStreamV2Channel(b.channel, status.CommittedToken, b.prior, m.keyBegin, m.keyEnd)
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
// of zero.
func reconcileStreamV2Channel(channel string, committedToken *string, prior map[string]*streamV2Item, keyBegin, keyEnd uint32) (int64, error) {
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
	// That is what a shard of an outgoing generation finds when it restarts into a
	// backfill of its binding: the new generation's Apply drops and re-creates the
	// table, taking every channel bound to it (client.MustRecreateResource), and
	// this refusal is what stops the shard from re-creating its channel against
	// the re-materialized table and appending to it again. A shard so refused is
	// running a specification the runtime is already replacing, so the refusal is
	// spent by the replacement rather than needing the backfill it asks for.
	if committedToken == nil {
		if counter > 0 {
			return 0, fmt.Errorf(
				"channel %q has committed nothing while this task's checkpoint records %d documents appended to it: the channel has lost committed data, so the missing rows cannot be identified. Backfill this binding",
				channel, counter,
			)
		}
		return 0, nil
	}

	committed, err := strconv.ParseInt(*committedToken, 10, 64)
	if err != nil {
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
	} else if committed > counter && own != nil && (own.KeyBegin != keyBegin || own.KeyEnd != keyEnd) {
		// Positional skipping is only sound when the replayed transaction holds
		// the same documents in the same order, which a re-split shard's
		// different subset of keys would not. With no prior item there is no
		// recorded range to contradict, and the documents Snowflake holds are
		// still the ones this shard is about to replay.
		return 0, fmt.Errorf(
			"channel %q has %d documents Snowflake committed beyond the %d this task's checkpoint records, and the shard key range has changed from [%08x, %08x] to [%08x, %08x] since they were appended: the interrupted transaction cannot be replayed identically, so the uncommitted rows cannot be skipped safely. Backfill this binding",
			channel, committed, counter, own.KeyBegin, own.KeyEnd, keyBegin, keyEnd,
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

// retireAbsorbedChannels settles the channel of every shard whose key range this
// shard has absorbed, and marks it for removal from the checkpoint.
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
// holds hands its committed offset token to whoever opens it next. That token
// would then be read as documents this generation of the channel had already
// appended, and skipped: rows silently dropped, or a binding wedged against a
// counter it cannot reach. Dropping the channel is what makes the next open of
// the same name a genuinely fresh channel.
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
// exactly the documents the checkpoint accounts for, and so can be retired.
func absorbedChannelSettled(item *streamV2Item, committedToken *string, keyBegin, keyEnd uint32) error {
	if committedToken == nil {
		return fmt.Errorf(
			"channel %q of a shard covering [%08x, %08x], which this shard's range [%08x, %08x] has absorbed, has committed nothing while this task's checkpoint records %d documents appended to it: the channel has lost committed data. Backfill this binding",
			item.Channel, item.KeyBegin, item.KeyEnd, keyBegin, keyEnd, item.Counter,
		)
	}

	committed, err := strconv.ParseInt(*committedToken, 10, 64)
	if err != nil {
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

	// A column absent from the row object defaults to SQL NULL, which is what the
	// classic path stores for a nil. Writing the nil out instead would encode a
	// JSON null, and for a VARIANT column the SDK ingests that as a VARIANT
	// holding null — a value which answers `IS NULL` and `TYPEOF` differently
	// from the SQL NULL the same document produces through a staged file.
	var row = make(map[string]any, len(b.columns))
	for i, col := range b.columns {
		if converted[i] != nil {
			row[col] = converted[i]
		}
	}
	var data, err = json.Marshal(row)
	if err != nil {
		return fmt.Errorf("encoding row for %s: %w", b.table, err)
	}

	if len(b.buf) == 0 {
		b.bufFirst = b.counter
	}
	b.buf = append(b.buf, data)
	b.bufBytes += len(data)
	m.bufBytes += len(data)

	if len(b.buf) >= streamV2BatchRows || b.bufBytes >= streamV2BatchBytes {
		return m.appendBatch(ctx, b)
	} else if m.bufBytes >= streamV2MaxBufferedBytes {
		return m.appendAllBatches(ctx)
	}
	return nil
}

// appendAllBatches appends every binding's buffered documents, to bring total
// buffered bytes back to zero.
func (m *streamV2Manager) appendAllBatches(ctx context.Context) error {
	for _, b := range m.bindings {
		if len(b.buf) == 0 {
			continue
		} else if err := m.appendBatch(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// appendBatch hands the buffered batch to the binding's append pipe. Ownership
// of the buffer passes to the append, so a fresh one is started here.
func (m *streamV2Manager) appendBatch(ctx context.Context, b *streamV2Binding) error {
	var rows, first, last = b.buf, b.bufFirst, b.counter
	m.bufBytes -= b.bufBytes
	b.buf, b.bufBytes, b.bufFirst = nil, 0, 0

	// Back-pressure is implicit: a saturated pipe blocks the append, which
	// blocks the next batch's submission, which blocks Store.
	var err = b.pipe.submit(func() error {
		client, err := m.ensureStarted(ctx)
		if err != nil {
			return err
		}
		return client.Append(ctx, b.channel, strconv.FormatInt(first, 10), strconv.FormatInt(last, 10), rows)
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
		if len(b.buf) > 0 {
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
	var token = strconv.FormatInt(counter, 10)
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
