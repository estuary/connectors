package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
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

type streamV2Binding struct {
	database string
	schema   string
	table    string
	channel  string
	// columns holds unquoted Snowflake column names aligned with the output of
	// Table.ConvertAll, for building the by-name row objects the SDK ingests.
	columns []string
	// prior is the streaming v2 state the driver checkpoint recorded for this
	// binding, or nil if it recorded none.
	prior *streamV2Item
	// opened reports whether the channel has been opened and reconciled.
	opened bool

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
	// committed is the highest index known to be durably committed, so that
	// Acknowledge does not re-wait on a counter which has not moved.
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
// channel as they are stored, and the runtime's idempotent replay of an
// interrupted transaction is reconciled against Snowflake's committed offset
// token before that channel's next append. The sidecar is spawned on first use;
// any sidecar failure is fatal to the connector (crash-only).
type streamV2Manager struct {
	cfg         *config
	accountName string
	channelBase string
	argv        []string
	keyBegin    uint32
	keyEnd      uint32

	// procCtx bounds the sidecar process lifetime to the transactor session
	// rather than to whichever caller's context first triggers ensureStarted.
	// A per-call context (e.g. an Acknowledge errgroup context) would otherwise
	// SIGTERM the sidecar as soon as that call returned. procCancel is invoked
	// by stop().
	procCtx    context.Context
	procCancel context.CancelFunc

	mu     sync.Mutex
	sup    *sidecarSupervisor
	client *sidecarClient

	bindings  map[int]*streamV2Binding
	byChannel map[string]*streamV2Binding

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
		byChannel:   make(map[string]*streamV2Binding),
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

// addBinding registers a binding on the streaming v2 write path, carrying
// forward the document counter the driver checkpoint recorded for it. The
// channel itself is opened on the binding's first document, by ensureChannel.
func (m *streamV2Manager) addBinding(database, schema, table string, target sql.Table, prior *streamV2Item) {
	var columns []string
	for _, col := range target.Columns() {
		columns = append(columns, unquotedIdentifier(col.Identifier))
	}

	var b = &streamV2Binding{
		database: database,
		schema:   schema,
		table:    table,
		// The channel is named for the binding's state key rather than its
		// table, so that backfilling a binding rotates its channel along with
		// its checkpoint item. Both then start empty together, which is what
		// makes a backfill the escape from a refusal — and what makes an absent
		// checkpoint item alongside a present committed token unambiguously an
		// interruption of this generation's first transaction rather than a
		// fresh backfill.
		channel: fmt.Sprintf("%s_%s", m.channelBase, sanitizeAndAppendHash(target.StateKey)),
		columns: columns,
		prior:   prior,
	}
	if prior != nil {
		b.counter, b.recorded = prior.Counter, prior.Counter
	}

	m.bindings[target.Binding] = b
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

	m.mu.Lock()
	m.byChannel[b.channel] = b
	m.mu.Unlock()

	log.WithFields(log.Fields{
		"table":          b.table,
		"channel":        b.channel,
		"committedToken": status.CommittedToken,
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
		"channel %q had %d row(s) rejected and discarded by Snowflake, which reported: %s. Those rows are not in %s, and Snowflake's committed offset token has advanced past them, so they cannot be identified and re-sent. Backfill this binding to materialize it from the beginning",
		channel, status.RowsErrorCount, status.LastErrorMessage, table,
	)
}

// reconcileStreamV2Channel compares Snowflake's committed offset token for a
// channel against the document counter the driver checkpoint recorded for it,
// and reports the highest document index Snowflake already holds — the
// threshold below which a replayed document must not be appended again.
//
// prior is nil when the checkpoint records nothing for the channel, which reads
// as a counter of zero.
func reconcileStreamV2Channel(channel string, committedToken *string, prior *streamV2Item, keyBegin, keyEnd uint32) (int64, error) {
	var counter int64
	if prior != nil {
		counter = prior.Counter
	}

	// Snowflake holds nothing for this channel, so there is nothing to skip.
	if committedToken == nil {
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
			"channel %q has committed %d documents but this task's checkpoint records %d as appended: the channel has lost committed data, so the missing rows cannot be identified. Backfill this binding to materialize it from the beginning",
			channel, committed, counter,
		)
	} else if committed > counter && prior != nil && (prior.KeyBegin != keyBegin || prior.KeyEnd != keyEnd) {
		// Positional skipping is only sound when the replayed transaction holds
		// the same documents in the same order, which a re-split shard's
		// different subset of keys would not. With no prior item there is no
		// recorded range to contradict, and the documents Snowflake holds are
		// still the ones this shard is about to replay.
		return 0, fmt.Errorf(
			"channel %q has %d documents Snowflake committed beyond the %d this task's checkpoint records, and the shard key range has changed from [%08x, %08x) to [%08x, %08x) since they were appended: the interrupted transaction cannot be replayed identically, so the uncommitted rows cannot be skipped safely. Backfill this binding to materialize it from the beginning",
			channel, committed, counter, prior.KeyBegin, prior.KeyEnd, keyBegin, keyEnd,
		)
	}

	return committed, nil
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

// flush appends each binding's remaining buffered documents and waits for every
// in-flight append, so that the counter it reports for a binding is exactly what
// has been handed to Snowflake. It returns a checkpoint item for each binding
// whose counter advanced during this transaction.
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
				"channel %q holds %d committed documents but the transaction replayed against it produced only %d: it was not replayed identically, so the rows Snowflake already holds cannot be identified. Backfill this binding to materialize it from the beginning",
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
	return items, nil
}

// waitCommit blocks until the channel has durably committed every document the
// transaction appended to it, and refuses to acknowledge the transaction if
// Snowflake rejected any of its rows. Nothing is applied here — the rows reached
// Snowflake during Store — but the wait is retained deliberately: without it, a
// failed commit would only surface on the following transaction's append, and a
// rejected row would not surface at all.
func (m *streamV2Manager) waitCommit(ctx context.Context, item *streamV2Item) error {
	m.mu.Lock()
	b, isOpen := m.byChannel[item.Channel]
	m.mu.Unlock()

	if !isOpen {
		// This session appended nothing to the channel — it stored no documents
		// for the binding, or the binding no longer uses this write path — so
		// there is nothing to wait for. The counter stays in the checkpoint
		// against the day the binding appends to the channel again.
		log.WithField("channel", item.Channel).Debug("snowpipe streaming v2: checkpointed counter has no open channel; nothing to commit")
		return nil
	} else if item.Counter <= b.committed {
		return nil
	}

	client, err := m.ensureStarted(ctx)
	if err != nil {
		return err
	}

	var token = strconv.FormatInt(item.Counter, 10)
	status, err := client.WaitCommit(ctx, item.Channel, token)
	if err != nil {
		return fmt.Errorf("waiting for commit of document %s on channel %q: %w", token, item.Channel, err)
	}

	// The token landing is the earliest this transaction's own rejections can be
	// seen. A count which lags even that only delays the refusal to a later
	// transaction or to the channel's next Open, since it never resets.
	if err := rejectedRowsError(item.Channel, b.table, status); err != nil {
		return err
	}
	b.committed = item.Counter

	log.WithFields(log.Fields{
		"table":   b.table,
		"channel": item.Channel,
		"counter": item.Counter,
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
