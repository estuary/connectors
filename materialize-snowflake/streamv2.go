package connector

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

// The manager appends a batch when it holds streamV2BatchRows documents or
// streamV2BatchBytes bytes. These caps are a performance knob, not checkpoint state.
// Recovery resumes from the document index that Snowflake reports as committed,
// whatever batch boundaries the interrupted attempt cut.
var (
	streamV2BatchRows  = 10_000
	streamV2BatchBytes = 8 * 1024 * 1024
)

// streamV2MaxBufferedBytes caps the documents that all bindings buffer at once.
var streamV2MaxBufferedBytes = 128 * 1024 * 1024

// streamV2Item records the channel of a binding, and how many documents this write
// path appended to it.
//
// This item is not pending work, unlike the other items of the driver checkpoint. It
// is durable state, one per binding. The connector reconciles it against Snowflake's
// committed offset token before the next append, and Acknowledge never clears it.
//
// The checkpoint holds one item per channel, not one per binding. Every shard of a v2
// task merge-patches its connector state into one task-global document. The channel
// name is therefore the key, and each shard derives that name from its key-begin.
type streamV2Item struct {
	Channel string
	// Counter is the index of the last document appended to Channel. The count
	// starts at the first document of the channel and never resets.
	Counter int64
	// KeyBegin and KeyEnd are the shard key range of the transaction that counted
	// those documents.
	KeyBegin, KeyEnd uint32
}

// streamV2Range is a shard key range, as a committed offset token reports it.
type streamV2Range struct {
	keyBegin, keyEnd uint32
}

func (r streamV2Range) String() string {
	return fmt.Sprintf("[%08x, %08x]", r.keyBegin, r.keyEnd)
}

// parseStreamV2Token reads a committed offset token back as two values: the document
// count it carries, and the shard key range that appended those documents. It returns
// false for a token that this write path never writes.
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

// streamsV2 reports whether a binding writes through the snowpipe streaming v2 path.
// That path appends rows to a channel as it stores them. Only a delta-updates binding
// can do this, and only with JWT credentials to authenticate its sidecar.
func streamsV2(cfg *config, deltaUpdates bool, flagEnabled bool) bool {
	return deltaUpdates && cfg.Credentials.AuthType == snowflake_auth.JWT && flagEnabled
}

// streamV2PathEnteredWithPendingWork refuses a binding which is entering the
// streaming v2 write path while its checkpoint still holds work that another path
// staged and did not finish.
//
// Acknowledge drains such an item through the manager of the path which staged it.
// This binding no longer has one: its rows now go to the streaming v2 manager, so
// the manager which staged the item holds no channel for the table, and the drain
// fails for as long as the item remains. The item is the only record of documents
// which Snowflake does not hold yet, so it is not discarded here.
//
// The task drains the item if the operator restores the write path it belongs to,
// which loses nothing. A backfill also clears it, at the cost of materializing the
// binding again.
func streamV2PathEnteredWithPendingWork(table string, prior *checkpointItem) error {
	if prior == nil {
		return nil
	}

	var pending []string
	if len(prior.StreamBlobs) > 0 {
		pending = append(pending, fmt.Sprintf("%d Snowpipe Streaming blob(s)", len(prior.StreamBlobs)))
	}
	if len(prior.PipeFiles) > 0 {
		pending = append(pending, fmt.Sprintf("%d file(s) staged for a Snowpipe pipe", len(prior.PipeFiles)))
	}
	if prior.Query != "" {
		pending = append(pending, "a staged-file query")
	}
	if len(pending) == 0 {
		return nil
	}

	return fmt.Errorf(
		"this binding is moving onto the snowpipe_streaming_v2 write path while the task's checkpoint still records work that its previous write path staged into %s and did not finish: %s. That work is the only record of documents which Snowflake does not hold yet, so this write path may not drop it, and it cannot finish it either — the previous path's channels belong to a manager this binding no longer uses. Restore the write path this task was running, which lets the next transaction finish that work before you move the binding onto snowpipe_streaming_v2, or backfill the binding, which discards the work and materializes it again",
		table, strings.Join(pending, ", "),
	)
}

// streamV2GenerationPrefix starts the line in the comment of a streaming v2 table.
// That line names the generation of the binding that the table belongs to.
const streamV2GenerationPrefix = "flow_generation: "

// streamV2Generation names one generation of one binding: the task, and the state key
// of the binding. A backfill rotates that state key.
type streamV2Generation struct {
	materialization string
	stateKey        string
}

// streamV2EmbedGenerationInTableComment builds the comment of a streaming v2 table.
// The comment includes the generation that the table belongs to.
//
// The generation goes in the table comment because a channel has no attribute for it.
// A backfill re-creates the table (client.MustRecreateResource). The drop of the table
// destroys every channel bound to it, and the committed offset token with them.
//
// A later open of the same channel name therefore gets a new channel. That channel
// looks the same as one opened against a table that was new all along. But the shards
// of the replaced generation still run, and they will open channels against the
// re-created table. Only the table outlives the drop, and it records the generation
// that created it.
func streamV2EmbedGenerationInTableComment(comment string, gen streamV2Generation) string {
	return fmt.Sprintf("%s\n%s%s %s", comment, streamV2GenerationPrefix, gen.materialization, gen.stateKey)
}

// streamV2GenerationFromTableComment reads the generation that a table comment names,
// and reports whether the comment named one at all. Two comments name none: one
// written before this connector recorded generations, and one that an operator
// rewrote.
func streamV2GenerationFromTableComment(comment string) (streamV2Generation, bool) {
	for line := range strings.SplitSeq(comment, "\n") {
		rest, found := strings.CutPrefix(strings.TrimSpace(line), streamV2GenerationPrefix)
		if !found {
			continue
		} else if materialization, stateKey, ok := strings.Cut(rest, " "); ok && materialization != "" && stateKey != "" {
			return streamV2Generation{materialization: materialization, stateKey: stateKey}, true
		}
	}
	return streamV2Generation{}, false
}

// streamV2QueryTableComment queries the comment of a table. The comment of a
// streaming v2 table names the generation of the binding it belongs to. A table that
// does not exist, and a table with no comment, both report the empty string. Neither
// names a generation.
func streamV2QueryTableComment(ctx context.Context, db *stdsql.DB, dialect sql.Dialect, database, schema, table string) (string, error) {
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

// streamV2CheckGenerationConflict refuses a binding when its table records a
// different generation of that same binding.
//
// A comment that names no generation is not a conflict. Two tables report no
// generation: one created before this connector recorded them, and one created by a
// standard-updates generation of the binding. Neither says which generation can
// stream into the table.
//
// A generation of another task is not a conflict either. Two tasks that materialize
// into one table are not generations of each other. The comment names whichever of
// them created the table last.
func streamV2CheckGenerationConflict(comment string, mine streamV2Generation, table string) error {
	owner, ok := streamV2GenerationFromTableComment(comment)
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
	// stateKey names the generation of the binding that this shard materializes. The
	// table it appends to must record the same generation.
	stateKey string
	// columns holds the row-object prefix of each Snowflake column, in the same order
	// as the output of Table.ConvertAll. The append writes row objects by name, and the
	// SDK ingests them.
	columns []rowColumn
	// prior is the streaming v2 state that the driver checkpoint recorded for this
	// binding. The map key is the channel name, and the map covers every shard of the
	// task. It is nil when the checkpoint recorded no state.
	prior map[string]*streamV2Item
	// opened reports whether this session opened and reconciled the channel.
	opened bool
	// retire names the channels of the shards whose key range this shard absorbed and
	// settled. The next checkpoint deletes them. Every checkpoint of the session
	// reports them again, not only the first, so that a transaction that never commits
	// does not lose the deletion.
	retire []string

	// counter is the index of the last document counted for this channel. It starts at
	// the checkpointed Counter at Open, and it advances by one for each document
	// stored.
	counter int64
	// recorded is the counter value that the last checkpoint item reported. It is how
	// flush finds a binding that stored nothing this transaction.
	recorded int64
	// skip is the document index that Snowflake already held when this session opened
	// the channel. An interrupted attempt of the transaction now replayed appended the
	// documents at or below that index. The manager counts those documents again, but
	// does not append them again. Document indices are absolute, so this one threshold
	// holds for the whole session.
	skip int64
	// committed is the highest index that Snowflake durably holds. It starts at the
	// committed offset token when this session opened the channel, and then takes each
	// counter this session committed. waitCommit must not wait on a counter at or below
	// it. The wait is for the committed token to *equal* the requested token, and a
	// channel already past that token never reports it again. Such a wait runs to its
	// timeout. A replay of documents that Snowflake already holds produces exactly such
	// a counter.
	committed int64

	// buf is the payload of the batch under construction: the framed JSON array that
	// the append carries. bufferRow writes it row by row as Store stores each document.
	// This way bufferRow assembles the bytes of a batch once, and not once per row and
	// again per batch. bufRows counts the rows it holds, and bufFirst is the index of
	// the first row.
	buf      []byte
	bufRows  int
	bufFirst int64
	// bufHint is the size of the batch handed off before this one, and it sizes the next
	// buffer. The manager cannot reuse the buffer itself. Ownership passes to the
	// append. That append still writes the buffer to the sidecar while bufferRow builds
	// the next batch.
	bufHint int

	// pipe carries one append at a time. Store can buffer more rows while a batch is on
	// the wire, and the batches keep their order.
	pipe appendPipe
}

// appendPipe runs one append at a time, on a goroutine of its own. A submit of the
// next append reports the result of the previous one. A failure therefore surfaces at
// the next batch boundary or at flush, and nothing drops it.
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

// streamV2Manager implements the high-performance Snowpipe Streaming write path. It
// supervises the Python SDK sidecar.
//
// The manager appends rows to a channel of each binding as Store stores them, and
// commits them as the transaction produces its checkpoint. The runtime replays an
// interrupted transaction identically. The manager reconciles that replay against
// Snowflake's committed offset token before the next append to the channel.
//
// The manager spawns the sidecar on first use. Any failure of the sidecar is fatal to
// the connector (crash-only).
type streamV2Manager struct {
	cfg             *config
	materialization string
	accountName     string
	channelBase     string
	argv            []string
	keyBegin        uint32
	keyEnd          uint32

	// tableComment reports the comment that Snowflake holds for the table of a binding.
	// That comment names the generation the table belongs to — see
	// streamV2CheckGenerationConflict. The transactor supplies this function, because it
	// owns the connection that the read needs.
	tableComment func(ctx context.Context, database, schema, table string) (string, error)

	// procCtx bounds the lifetime of the sidecar process to the transactor session.
	procCtx    context.Context
	procCancel context.CancelFunc

	mu     sync.Mutex
	sup    *sidecarSupervisor
	client *sidecarClient

	bindings map[int]*streamV2Binding
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

	// The process uses the session-scoped context of the manager, not the context of
	// the caller. It therefore survives the return of whichever call started it first.
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

// offsetToken renders the offset token of an append. The token holds two facts: the
// index of the last document in the append, and the shard key range that appended it.
//
// The range is in the token because the connector writes the token at the same
// instant as the rows it accounts for. A checkpoint item records a range too, but
// only the range of the last transaction that committed. A replay must know which
// range appended the documents beyond that transaction. No checkpoint saw those
// documents, and a replay skips them by position. This is why
// reconcileStreamV2Channel reads the range from the token and not from the item.
func (m *streamV2Manager) offsetToken(counter int64) string {
	return fmt.Sprintf("%d@%08x-%08x", counter, m.keyBegin, m.keyEnd)
}

// addBinding registers a binding on the streaming v2 write path. It carries forward
// the document counter that the driver checkpoint recorded for the binding.
// ensureChannel opens the channel itself, on the first document of the binding.
func (m *streamV2Manager) addBinding(database, schema, table string, target sql.Table, prior map[string]*streamV2Item) {
	var names []string
	for _, col := range target.Columns() {
		names = append(names, unquotedIdentifier(col.Identifier))
	}

	// The name comes from the state key, not the table. A backfill rotates the state
	// key, so it rotates the channel and the checkpoint item together. Both then start
	// empty. This is why a backfill is the escape from a refusal. That rotation also
	// leaves reconcileStreamV2Channel only two ways to read a token that no item
	// accounts for.
	var channel = fmt.Sprintf("%s_%s", m.channelBase, sanitizeAndAppendHash(target.StateKey))

	var b = &streamV2Binding{
		database: database,
		schema:   schema,
		table:    table,
		channel:  channel,
		stateKey: target.StateKey,
		columns:  rowColumnsOf(names),
		prior:    prior,
	}
	// Only the channel of this shard carries a counter that this binding continues.
	// The other entries in the map belong to the other shards of the task.
	if own := prior[channel]; own != nil {
		b.counter, b.recorded = own.Counter, own.Counter
	}

	m.bindings[target.Binding] = b
}

// checkpointFor returns the checkpoint entries to record for a binding. These are the
// item of the channel of this shard, and a deletion for every channel this shard
// retired. The runtime reduces these entries as a merge patch, so the entries of the
// other shards stay as they are.
func (m *streamV2Manager) checkpointFor(binding int, item *streamV2Item) map[string]*streamV2Item {
	var entries = map[string]*streamV2Item{item.Channel: item}
	for _, channel := range m.bindings[binding].retire {
		entries[channel] = nil
	}
	return entries
}

// ensureChannel opens the channel of the binding and captures its skip threshold. It
// does this on the first document of the binding.
//
// The manager waits for the first document, and does not open the channel while the
// transactor is built. A transactor built only to drain pending work stores nothing,
// as Apply does before it alters a table. Such a transactor must not pay for a sidecar
// it will not use. It must also not reconcile against a key range that stands for the
// whole task instead of one shard.
//
// This delay costs nothing. The manager still captures the threshold before the first
// append. The threshold is an absolute document index, so it then holds for the whole
// session.
func (m *streamV2Manager) ensureChannel(ctx context.Context, b *streamV2Binding) error {
	if b.opened {
		return nil
	}

	// This check runs before the manager starts a sidecar, and before it opens any
	// channel. A backfill that re-created the table of this binding leaves the shard
	// nothing to append there, whatever its own channel reports.
	if m.tableComment != nil {
		comment, err := m.tableComment(ctx, b.database, b.schema, unquotedIdentifier(b.table))
		if err != nil {
			return err
		} else if err := streamV2CheckGenerationConflict(
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

	// This check runs before the manager opens the channel of this shard.
	// retireAbsorbedChannels therefore refuses a join that this shard cannot account
	// for, before the shard appends anything.
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

// rejectedRowsError refuses a channel when Snowflake rejected any row on it. It
// reports nil in every other case.
//
// Snowflake discards a row it rejects, and fails neither the append nor the commit. It
// also advances the committed offset token of the channel past that row. This count is
// therefore the only place where the loss is visible.
//
// The count covers the whole life of the channel and never resets. This is why one
// rejection refuses the binding for good. Nothing can identify the discarded rows, so
// there is nothing to re-send. No state can tell the connector that the destination is
// whole again. Only a backfill clears the count, because it rotates the channel and
// the checkpoint of the binding together.
func rejectedRowsError(channel, table string, status *channelStatusResult) error {
	if status.RowsErrorCount == 0 {
		return nil
	}
	return fmt.Errorf(
		"channel %q had %d row(s) rejected and discarded by Snowflake, which reported: %s. Those rows are not in %s, and Snowflake's committed offset token has advanced past them, so they cannot be identified and re-sent. Backfill this binding",
		channel, status.RowsErrorCount, status.LastErrorMessage, table,
	)
}

// reconcileStreamV2Channel compares Snowflake's committed offset token for a channel
// against the document counter that the driver checkpoint recorded. It reports the
// highest document index that Snowflake already holds. A replay must not append a
// document at or below that threshold again.
//
// prior holds the checkpoint items for every channel of the task. At most one of them
// belongs to this channel. An absent item reads as a counter of zero, which is the
// first transaction of the channel. Snowflake can already hold documents from an
// interrupted attempt of that transaction. The rest of the map decides whether this
// reading is available at all.
//
// Snowflake can hold documents that the counter does not account for. The key range in
// the token says which shard appended them. This shard skips them only when that range
// is its own.
func reconcileStreamV2Channel(channel, table string, committedToken *string, prior map[string]*streamV2Item, keyBegin, keyEnd uint32) (int64, error) {
	var own = prior[channel]
	var counter int64
	if own != nil {
		counter = own.Counter
	}

	// Snowflake holds nothing for this channel. With no counter to account for, there
	// is nothing to skip. With a counter, the channel that held those documents is
	// gone. The token that said which of them Snowflake holds went with that channel.
	//
	// A shard that restarts into a backfill of its binding finds exactly this. The
	// backfill re-creates the table, and the drop destroys every channel bound to it —
	// see client.MustRecreateResource. The refusal is what stops that shard from
	// appending into a table that the new generation re-materializes under it. The
	// runtime already replaces the specification that shard runs, so that replacement
	// settles the refusal, not the backfill the message asks for.
	//
	// Nothing this connector does retires the channel of a shard, so the message names
	// both readings. An operator who is in a backfill lost nothing and wants to know
	// it. An operator who is not must hear that the account Snowflake keeps of this
	// channel is gone. Neither reading bounds what the channel committed beyond the
	// counter, so the remedy is the same either way.
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
		// A token with no item to account for can be the interrupted first transaction
		// of this channel. That reading holds only when the checkpoint holds nothing
		// for the binding at all. A backfill is what produces that state: the state
		// key rotates, so channel and item start empty together.
		//
		// When the checkpoint holds anything, this binding materialized under channels
		// of other names. A name that the key-begin of this shard derives can just as
		// well be one that a join retired and a later split re-created. That name then
		// holds a token that counts the documents of another shard.
		//
		// This code cannot tell the two apart, so it draws the line at any state at
		// all. It does not ask whether some item still records a range that covers this
		// key-begin. The range of an item is only as current as the last transaction in
		// which its shard stored anything. That narrower reading therefore turns on the
		// order in which siblings checkpoint, and it reads the wedge as safe as soon as
		// they catch up.
		return 0, fmt.Errorf(
			"channel %q has committed %d documents this task's checkpoint cannot account for: it holds no item for this channel, only items its other channels wrote. Such a token is the footprint of a shard which no longer exists, whose channel name this shard's key-begin has derived again, and it cannot be told apart from an interrupted first transaction of this channel — so the documents it counts must not be skipped, or that many of the documents about to be materialized into %s are dropped instead. Backfill this binding",
			channel, committed, table,
		)
	}

	// Nothing is outstanding, so no key range bears on the answer.
	if committed == counter {
		return committed, nil
	}

	// The documents beyond the counter are an interrupted attempt of the transaction
	// now replayed, and this shard skips them by position. The replay must produce
	// those same documents in that same order. Only the key range that appended them
	// produces that order. A narrower range replays a subset, and it skips past
	// documents Snowflake does not hold. A wider range replays a superset, and it skips
	// documents of the sibling it absorbed.
	//
	// Under a backfill, nearly every interruption leaves this state. A round long
	// enough to be interrupted in is long enough for Snowflake to commit batches of it.
	// A split leaves this state too, until the low child commits a transaction of its
	// own. This is why the code reads the range from the token and not from the item.
	var mine = streamV2Range{keyBegin: keyBegin, keyEnd: keyEnd}

	if appendedBy != mine {
		return 0, fmt.Errorf(
			"channel %q has %d documents Snowflake committed beyond the %d this task's checkpoint records, appended by a shard covering %s while this shard covers %s: a replay here carries a different set of documents, so those rows cannot be skipped safely. Backfill this binding, or restore the task's shard key ranges to the topology which appended them, so that the transaction those rows belong to can commit",
			channel, committed, counter, appendedBy, mine,
		)
	}

	return committed, nil
}

// absorbedChannels reports the checkpoint items of the shards whose key range this
// shard took over. A join of two shards into one produces that state. The items come
// ordered by the range they covered.
//
// Each shard names its channel from its own key-begin. An item under any other channel
// therefore belongs to another shard. When the key-begin of that item lies above the
// key-begin of this shard and within its range, that shard no longer runs. Shard
// ranges tile the key space exactly, so no two live shards can stand in that relation.
// This shard absorbed it, and now appends the documents that shard was given to a
// channel of its own.
func absorbedChannels(channel string, prior map[string]*streamV2Item, keyBegin, keyEnd uint32) []*streamV2Item {
	var absorbed []*streamV2Item
	for ch, item := range prior {
		// A nil item is a channel that an earlier session retired. The runtime did not
		// yet reduce its deletion away.
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

// retireAbsorbedChannels retires the channel of every shard whose key range this shard
// absorbed. For each one it settles the channel against the checkpoint, drops the
// channel in Snowflake, and marks the checkpoint item for removal.
//
// The committed rows of an absorbed shard are in the table, and the frontier of the
// task records its journal progress. That frontier is not per-shard, so the runtime
// will not deliver those documents here again. The join therefore needs no backfill.
//
// What a join cannot survive is an absorbed shard that was interrupted after it
// appended documents its final transaction never committed. Those rows are in the
// table, and the frontier does not account for them. The runtime will deliver them
// here, and this shard will append them to a channel of its own. The committed offset
// token of the absorbed channel cannot skip them there. This write path serves
// delta-updates bindings, so that duplicates the rows for good.
//
// A deletion of the checkpoint item does not retire the channel on its own. A deletion
// alone is what wedges a task scaled down and then back up. A split re-creates exactly
// the pivot boundaries that a join removed, so the arriving shard derives this same
// channel name. It then finds a channel that Snowflake still holds, with a committed
// offset token that no checkpoint item contradicts. reconcileStreamV2Channel refuses
// that shard, as it must refuse any shard that cannot tell its own interrupted attempt
// from the leftovers of another topology.
//
// The drop in Snowflake is what makes the name safe to derive again. Both halves of
// the retirement therefore happen here: the drop, and the deletion that takes the
// channel out of the checkpoint. Only the drop is durable at once, so a retirement
// must be safe to repeat — see absorbedChannelSettled.
//
// An open of an absorbed channel invalidates any open that its own shard still holds.
// This is safe because that shard is gone. The topology it belonged to no longer tiles
// the key space, so the runtime will not start it.
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

		// Both checks above run ahead of the drop, because the drop discards everything
		// Snowflake holds for the channel, the cumulative row-error count included. A
		// join must not become a way to clear a refusal for rows Snowflake discarded.
		// Those rows are missing from the table whether or not the channel that
		// delivered them still exists.
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

// retireChannel takes a channel out of service for good. It drops the channel in
// Snowflake, and does not only close the local handle.
//
// The retirement must reach Snowflake because a channel name is deterministic. The
// connector derives it from the task, the key-begin, and the state key. The topology
// that name belongs to is not deterministic. A deleted shard and a later shard with
// the same key-begin derive the same channel name.
//
// A channel that Snowflake still holds gives its committed offset token to whoever
// opens it next. That shard can only refuse the token. Its own documents are not the
// ones the token counts, and it cannot tell which of them Snowflake holds. The drop is
// what makes the next open of the same name a fresh channel. The drop is also what
// keeps a join from costing the next split a backfill.
//
// An experiment against live Snowflake settled that the drop does this and a plain
// close does not. The SDK contract does not say so. A close releases only the local
// handle, and Snowflake still reports the same committed offset token to the next open
// of that name. The retirement subtest of TestStreamV2Manager is that experiment.
// TestStreamV2ChannelRetirement runs the same sequence against the fake sidecar.
//
// The channel must already be open in this session, because the drop uses the handle
// that an open produced. The committed rows are untouched. This retires the channel,
// not the data it delivered.
func retireChannel(ctx context.Context, client *sidecarClient, channel string) error {
	if err := client.CloseChannel(ctx, channel, true); err != nil {
		return fmt.Errorf("retiring channel %q: %w", channel, err)
	}
	log.WithFields(log.Fields{"channel": channel}).Info("retired snowpipe streaming v2 channel")
	return nil
}

// absorbedChannelSettled reports whether this shard can retire the channel of an
// absorbed shard now. It can when the channel holds exactly the documents the
// checkpoint accounts for, and also when an earlier session already retired it.
func absorbedChannelSettled(item *streamV2Item, committedToken *string, keyBegin, keyEnd uint32) error {
	// A channel that holds nothing at all is one this shard already retired. The drop
	// is what discards a committed offset token. The code reaches the drop only after
	// it found the channel settled against this very item. The two halves of a
	// retirement are not durable together. The drop reaches Snowflake as the channel
	// settles, and the deletion of this item becomes durable only with the transaction
	// that carries it. A shard interrupted in between therefore restarts to exactly
	// this state, and repeats the retirement.
	//
	// A table re-created under the task reports the same thing, because the drop
	// destroyed the committed offset token of every channel bound to it. Two checks
	// settle that case: the generation the table records, and the reconciliation of the
	// channel of this shard. Both refuse before this shard appends anything.
	//
	// reconcileStreamV2Channel reads the same report as lost data, not as a retirement.
	// Nothing retires the channel of a shard, so that report has no benign explanation
	// there.
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

// streamV2PathAbandoned refuses a binding that materialized through this write path
// and no longer does. The error names the channels that the binding leaves behind.
//
// The prior parameter holds the checkpoint items of the binding, one for each channel
// it appended to. Each item carries the count of documents that channel holds. Only
// this write path maintains those counts, and only they can say which documents of a
// channel Snowflake holds. Every other path treats the checkpoint item of a binding as
// pending work, and clears the whole item once it applied that work. The clear takes
// the counts with it.
//
// The channels themselves stay in Snowflake, named from the state key of the binding,
// and they still report the committed offset tokens they ended on. A later return to
// this path derives those same names. It meets a token that no counter accounts for,
// and reads it as an interrupted first transaction of a fresh channel. Only a backfill
// otherwise leaves a checkpoint with nothing for the binding. The return then skips
// that many of the documents it was about to materialize.
//
// A departure while rows are in flight costs more than the return. The committed offset
// token alone skips the documents of an interrupted transaction, and no other path
// reads that token. The runtime is about to replay those documents to this shard, and
// another path materializes them a second time. This path serves delta-updates
// bindings, so those duplicates are permanent.
//
// The refusal rules both outcomes out, and it applies whatever moved the binding off
// the path. A binding leaves this path when it no longer meets any one of the
// conditions in streamsV2.
//
// A backfill is the way off this path. It rotates the state key of the binding, which
// rotates both the channels and the checkpoint item. Nothing is then left for another
// path to discard, and no channel is left for a later session to find.
func streamV2PathAbandoned(table string, prior map[string]*streamV2Item) error {
	var channels []string
	for channel, item := range prior {
		// A nil item is a channel that this task already retired. The runtime did not
		// yet reduce its deletion away. It records nothing.
		if item != nil {
			channels = append(channels, channel)
		}
	}
	if len(channels) == 0 {
		return nil
	}
	slices.Sort(channels)

	return fmt.Errorf(
		"this binding has materialized into %s through the snowpipe_streaming_v2 write path, which this task's specification no longer selects for it, while the task's checkpoint still records the channel(s) %s it appended to. Those counters are the only account of which documents Snowflake's channels already hold, no other write path maintains them, and the first transaction on another path discards them — after which returning to this write path would skip that many of the documents it materializes. Restore this binding to the snowpipe_streaming_v2 write path — it needs the feature flag, delta updates, and key-pair authentication — or backfill it, which rotates its channels and its checkpoint together",
		table, strings.Join(channels, ", "),
	)
}

// writeRow counts one converted document and buffers it for append. It sends the batch
// as soon as the batch reaches either cap. It counts a document that Snowflake already
// committed during an interrupted attempt of this transaction, but it drops that
// document.
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

// bufferRow writes the document at index counter into the payload of the current
// batch. It starts that batch when this is the first row. It reports how many bytes the
// payload grew by.
//
// A row that the writer refuses leaves the payload with exactly the rows batched before
// it. The append still carries those rows, under the offset token of whichever
// transaction commits them.
//
// This is the whole of the encoding this path does. appendRowJSON copies through the
// values that reach it already encoded, and nothing rescans the row after it. The bytes
// written here are the bytes the append carries.
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

// finishBatch closes the payload of the batch and gives up ownership of the buffer. It
// reports the payload, the number of rows it holds, and the buffered bytes it releases.
//
// finishBatch counts those bytes before it writes the closing bracket, because that is
// the byte total bufferRow reported as it built the batch. A count of the bracket here
// as well leaves the total of buffered bytes one byte short of zero for every batch of
// the session. That drifts the back-pressure ceiling away from the memory it stands
// for.
func (b *streamV2Binding) finishBatch() (payload []byte, rows int, released int) {
	released = len(b.buf)
	b.buf = append(b.buf, ']')

	payload, rows = b.buf, b.bufRows
	b.bufHint = len(b.buf)
	b.buf, b.bufRows, b.bufFirst = nil, 0, 0
	return payload, rows, released
}

// streamV2MinBatchCapacity is the size a payload buffer starts with. It applies until
// the binding sends a batch that can size the next one.
const streamV2MinBatchCapacity = 8 * 1024

// startBatch opens the payload of a new batch. The size is a little over the batch
// before it. A full batch then does not grow and copy a dozen times on its way to 8
// MiB. The buffer is always a new one, because the batch before it owns the bytes it
// received.
func (b *streamV2Binding) startBatch() {
	b.buf = append(make([]byte, 0, max(b.bufHint+b.bufHint/8, streamV2MinBatchCapacity)), '[')
}

// appendAllBatches appends the buffered documents of every binding. This brings the
// total of buffered bytes back to zero.
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

// appendBatch closes the payload of the batch and hands it to the append pipe of the
// binding. Ownership of the buffer passes to the append, so the next batch starts a
// buffer of its own.
func (m *streamV2Manager) appendBatch(ctx context.Context, b *streamV2Binding) error {
	var first, last = b.bufFirst, b.counter
	payload, rows, released := b.finishBatch()
	m.bufBytes -= released

	// Back-pressure is implicit. A saturated pipe blocks the append. The blocked append
	// blocks the next submit, and that blocks Store.
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

// flush appends the remaining buffered documents of each binding. It waits for every
// in-flight append, and then waits for Snowflake to durably commit them. It returns a
// checkpoint item for each binding whose counter advanced during this transaction. It
// reports that item only after Snowflake holds the documents the counter accounts for.
//
// flush awaits the commit here, as part of the checkpoint it produces, and not at
// Acknowledge. The checkpoint of the runtime is durable before Acknowledge runs. A
// commit that failed there can no longer fail the transaction whose counter it belongs
// to. The task resumes from a counter Snowflake never committed, and the next Open can
// only refuse it. No replay re-appends rows the runtime already considers delivered.
//
// A failure here instead leaves the runtime to replay the transaction, and the
// reconciliation of that Open skips the documents Snowflake did commit.
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

		// A replayed transaction that produced fewer documents than the interrupted
		// attempt committed was not replayed identically. The skip threshold then
		// dropped documents that Snowflake does not hold. Only the first transaction of
		// the session can trip this, because the counter only grows from there.
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

	// Channels commit independently, and the sidecar serializes its operations per
	// channel and not globally. A wide materialization therefore does not pay the commit
	// latency of each binding in series.
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

// waitCommit blocks until the channel of the binding durably holds every document
// counted through counter. If Snowflake rejected any of its rows, waitCommit fails the
// transaction.
func (m *streamV2Manager) waitCommit(ctx context.Context, b *streamV2Binding, counter int64) error {
	// A replay of documents Snowflake already holds appends nothing. Its counter is
	// already committed, so waitCommit must not wait on it.
	if counter <= b.committed {
		return nil
	}

	client, err := m.ensureStarted(ctx)
	if err != nil {
		return err
	}

	// This wait happens before StartedCommit reaches the runtime. The runtime reports a
	// commit in progress only after that, so nothing else accounts for this wait until it
	// returns or times out.
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

	// The commit of the token is the earliest point where this transaction can see its
	// own rejections. That point is now before flush checkpoints its counter. A rejection
	// therefore fails the transaction that produced it, and not a later one.
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

// unquotedIdentifier removes SQL identifier quotes. It returns the exact name that
// Snowflake stores, which a row object needs to name its columns.
func unquotedIdentifier(ident string) string {
	if strings.HasPrefix(ident, `"`) && strings.HasSuffix(ident, `"`) && len(ident) >= 2 {
		return strings.ReplaceAll(ident[1:len(ident)-1], `""`, `"`)
	}
	return ident
}
