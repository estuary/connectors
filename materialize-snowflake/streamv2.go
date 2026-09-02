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
	"github.com/estuary/connectors/go/common"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
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

// streamV2PaceBytesPerSecond is the uncompressed byte rate this write path allows
// itself on one channel.
//
// The value is the documented per-channel ceiling.
const streamV2PaceBytesPerSecond = 20 * 1000 * 1000

// streamV2Item records one channel of a binding, and how many documents this write
// path appended to it.
//
// This item is not pending work, unlike the other items of the driver checkpoint. It
// is durable state, one per channel. The connector reconciles it against Snowflake's
// committed offset token before the next append, and Acknowledge never clears it.
//
// The checkpoint holds one item per channel, not one per binding, keyed by the
// channel's key-hash subrange. Every shard of a v2 task merge-patches its connector
// state into one task-global document, and channels are per-subrange, so the
// subrange keys sibling shards write are disjoint. Each shard holds several channels
// — one per subrange of its key range — and a topology change hands whole channels
// to the shards that inherit their subranges.
type streamV2Item struct {
	Channel string
	// Counter is the index of the last document appended to Channel. The count
	// starts at the first document of the channel and never resets.
	Counter int64
	// KeyBegin and KeyEnd are the channel's key-hash subrange. The subrange is a
	// function of the documents the channel receives, not of the shard topology,
	// which is what a split or join inherits whole.
	KeyBegin, KeyEnd uint32
	// Retiring marks a channel that the snowpipe streaming path is taking out of
	// service. The item is written before the channel is dropped, so that a
	// checkpoint which still names the channel after the drop explains why the
	// channel is empty: it was retired, not lost.
	Retiring bool
}

// streamV2Range is a key-hash subrange, as a channel name and a committed offset
// token report it.
type streamV2Range struct {
	keyBegin, keyEnd uint32
}

func (r streamV2Range) String() string {
	return fmt.Sprintf("[%08x, %08x]", r.keyBegin, r.keyEnd)
}

// key renders the subrange as the checkpoint key of its channel's item.
func (r streamV2Range) key() string {
	return fmt.Sprintf("%08x-%08x", r.keyBegin, r.keyEnd)
}

// streamV2ChannelName names the channel of one subrange of one binding. The name
// carries the subrange's begin and end, so subdivision depths never collide: a
// channel covering a half-range and one covering the quarter that starts at the same
// key take different names. The state key is retained so that a backfill rotates the
// binding's channels and its checkpoint items together — both start empty, which is
// what makes a backfill the escape from every refusal in this file.
func streamV2ChannelName(materialization string, r streamV2Range, stateKey string) string {
	return fmt.Sprintf("%s_%08x-%08x_%s",
		sanitizeAndAppendHash(materialization), r.keyBegin, r.keyEnd, sanitizeAndAppendHash(stateKey))
}

// streamV2Token renders the offset token of an append. The token holds two facts:
// the index of the last document in the append, and the channel subrange it was
// appended under.
//
// The subrange is in the token so that a token can be recognized as this channel's
// own. A token whose range is not the channel's subrange was written by something
// else — another connector, or a channel scheme this write path never ran — and
// nothing it counts may be skipped.
func streamV2Token(counter int64, r streamV2Range) string {
	return fmt.Sprintf("%d@%s", counter, r.key())
}

// parseStreamV2Token reads a committed offset token back as two values: the document
// count it carries, and the subrange those documents were appended under. It returns
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

// streamV2Downgrade reports whether a binding is leaving the snowpipe streaming v2
// write path for the snowpipe streaming path by explicit choice: the endpoint
// configuration names snowpipe_streaming itself and does not name
// snowpipe_streaming_v2. The default value of snowpipe_streaming does not count,
// because leaving the path duplicates documents and must be asked for.
func streamV2Downgrade(cfg *config, deltaUpdates bool) bool {
	if cfg.Credentials == nil || cfg.Credentials.AuthType != snowflake_auth.JWT || !deltaUpdates {
		return false
	}
	var configured = common.ParseFeatureFlags(cfg.Advanced.FeatureFlags, nil)
	return configured[flagSnowpipeStreaming] && !configured[flagSnowpipeStreamingV2]
}

// streamV2CannotDrainPendingBlobs reports that a binding moving onto the streaming
// v2 write path carries Snowpipe Streaming blobs which no manager can register.
//
// Acknowledge drains a pending item through the manager of the write path which
// staged it. A binding whose rows now go to streaming v2 still registers with the
// bdec manager for that drain, so this is reached only where the registration is
// impossible: a table whose columns that path does not support, or one it may not
// stream into at all.
//
// The blobs are the only record of documents which Snowflake does not hold yet, so
// they are neither dropped nor left to fail every transaction that tries to drain
// them.
func streamV2CannotDrainPendingBlobs(table string, blobs int, cause error) error {
	var err = fmt.Errorf(
		"this binding is moving onto the snowpipe_streaming_v2 write path while the task's checkpoint still records %d Snowpipe Streaming blob(s) that its previous write path staged into %s and did not finish. Those blobs are the only record of documents which Snowflake does not hold yet, and they can only be finished by the write path that staged them. Restore the write path this task was running, which lets the next transaction finish them before you move the binding onto snowpipe_streaming_v2, or backfill the binding, which discards them and materializes those documents again",
		blobs, table,
	)
	if cause != nil {
		return fmt.Errorf("%w: the previous write path cannot reopen its channel on the table: %w", err, cause)
	}
	return err
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
	if !ok {
		// A table created before this connector recorded generations, or by a write
		// path which records none. Apply records this generation on such a table
		// (adoptStreamV2Generations), so what reaches here is a table Apply could
		// not reach, and it says nothing about which generation may append.
		return nil
	}

	// Two tasks are not generations of each other, and neither one's account covers
	// what the other appended. A backfill of either drops the table, and with it
	// every channel appending to it, including the other task's.
	if owner.materialization != mine.materialization {
		return fmt.Errorf(
			"table %s records materialization %q as the one materializing into it, while this task is %q: each task's channels account only for the documents it appended itself, and a backfill of either task drops the table along with every channel appending to it, so two tasks may not stream into one table. Materialize this binding into a table of its own, or take the other task off this one. If this task was renamed, backfill the binding, which re-creates the table and records the new name",
			table, owner.materialization, mine.materialization,
		)
	}

	if owner.stateKey == mine.stateKey {
		return nil
	}

	return fmt.Errorf(
		"table %s records state key %q of this binding as the generation materializing into it, while this task is materializing state key %q: the binding has been backfilled, which re-created the table, and this task is running the specification that backfill replaced. The runtime replaces this task with the one that backfill published; appending to the table before it does would duplicate the rows the backfill is re-materializing",
		table, owner.stateKey, mine.stateKey,
	)
}

// streamV2Channel is one channel of a binding: the subrange of the key-hash space it
// covers, the document counter it advances, and the batch it is building.
//
// A shard holds several channels per binding. In steady state they are the shard's
// target layout — its key range cut into streamV2ChannelsPerShard equal subranges —
// and after a topology change they are the channels inherited from the shards this
// one replaced, until a rebalance converges them.
type streamV2Channel struct {
	name string
	// r is the subrange of the key-hash space this channel covers. Documents route
	// here when their packed-key hash falls inside it. The subrange is embedded in
	// the channel's name and in every offset token it appends under.
	r streamV2Range

	// counter is the index of the last document counted for this channel. It starts
	// at the checkpointed Counter at open, and it advances by one for each document
	// routed here.
	counter int64
	// recorded is the counter value that the last checkpoint item reported. It is how
	// flush finds a channel that stored nothing this transaction.
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

	// pacer meters the uncompressed bytes this channel hands to Snowflake. One pacer
	// per channel, because Snowflake meters each channel independently. This is why
	// several channels per shard raise the shard's ceiling: four channels are four
	// independently metered pipes, and the shard may legitimately drive all of them.
	pacer *rate.Limiter
}

// offsetToken renders the offset token for a document index on this channel.
func (c *streamV2Channel) offsetToken(counter int64) string {
	return streamV2Token(counter, c.r)
}

type streamV2Binding struct {
	database string
	schema   string
	table    string
	// stateKey names the generation of the binding that this shard materializes. The
	// table it appends to must record the same generation.
	stateKey string
	// columns holds the row-object prefix of each Snowflake column, in the same order
	// as the output of Table.ConvertAll. The append writes row objects by name, and the
	// SDK ingests them.
	columns []rowColumn
	// prior is the streaming v2 state that the driver checkpoint recorded for this
	// binding. The map key is the channel's subrange, and the map covers every shard
	// of the task. It is nil when the checkpoint recorded no state.
	prior map[string]*streamV2Item
	// opened reports whether this session classified the checkpoint's channels,
	// opened its own, and reconciled each of them.
	opened bool
	// retire holds the item keys of the channels this shard has dropped in
	// Snowflake. The next checkpoint deletes their items. Every checkpoint of the
	// session reports them again, not only the first, so that a transaction that
	// never commits does not lose the deletion.
	retire []string

	// channels is the active layout: the channels rows route to, sorted by subrange
	// begin. The layout always tiles the shard's key range exactly, so every document
	// the runtime delivers routes to exactly one of them.
	channels []*streamV2Channel
	// targets is the shard's target layout: its key range cut into
	// streamV2ChannelsPerShard equal subranges. The active layout converges to it.
	targets []streamV2Range
	// declared reports that the last flush wrote the target layout's items into the
	// checkpoint. Acknowledge runs after the runtime made that checkpoint durable,
	// which is what makes the cutover it performs crash-safe: a shard that dies
	// after the cutover's drops restarts to a checkpoint that names the channels it
	// was converging to.
	declared bool
}

// isTargetLayout reports whether the active layout is the target layout.
func (b *streamV2Binding) isTargetLayout() bool {
	if len(b.channels) != len(b.targets) {
		return false
	}
	for i, c := range b.channels {
		if c.r != b.targets[i] {
			return false
		}
	}
	return true
}

// route reports the active channel that covers a key hash, or nil when none does.
func (b *streamV2Binding) route(hash uint32) *streamV2Channel {
	for _, c := range b.channels {
		if c.r.contains(hash) {
			return c
		}
	}
	return nil
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
// The manager appends rows to the channels of each binding as Store stores them, and
// commits them as the transaction produces its checkpoint. The runtime replays an
// interrupted transaction identically. The manager reconciles that replay against
// Snowflake's committed offset tokens before the next append to each channel.
//
// Rows route to a channel by the same packed-key hash the runtime routes documents to
// shards by. A channel's contents are therefore a function of the data, not of the
// shard topology: a split or join hands each surviving shard whole channels, tokens
// and all, and the shard continues them.
//
// The manager spawns the sidecar on first use. Any failure of the sidecar is fatal to
// the connector (crash-only).
type streamV2Manager struct {
	cfg             *config
	materialization string
	accountName     string
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
		argv:            defaultSidecarArgv(),
		keyBegin:        keyRange.KeyBegin,
		keyEnd:          keyRange.KeyEnd,
		procCtx:         procCtx,
		procCancel:      procCancel,
		bindings:        make(map[int]*streamV2Binding),
	}
}

func (m *streamV2Manager) shardRange() streamV2Range {
	return streamV2Range{keyBegin: m.keyBegin, keyEnd: m.keyEnd}
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

// addBinding registers a binding on the streaming v2 write path. ensureOpened builds
// the binding's channels from the driver checkpoint, on the first document of the
// binding.
func (m *streamV2Manager) addBinding(database, schema, table string, target sql.Table, prior map[string]*streamV2Item) {
	var names []string
	for _, col := range target.Columns() {
		names = append(names, unquotedIdentifier(col.Identifier))
	}

	m.bindings[target.Binding] = &streamV2Binding{
		database: database,
		schema:   schema,
		table:    table,
		stateKey: target.StateKey,
		columns:  rowColumnsOf(names),
		prior:    prior,
	}
}

// newChannel builds the in-memory state of one opened, reconciled channel.
func newStreamV2Channel(name string, r streamV2Range, counter, skip int64) *streamV2Channel {
	return &streamV2Channel{
		name:      name,
		r:         r,
		counter:   counter,
		recorded:  counter,
		skip:      skip,
		committed: skip,
		pacer:     rate.NewLimiter(streamV2PaceBytesPerSecond, 2*streamV2PaceBytesPerSecond),
	}
}

// ensureOpened builds the active channel layout of the binding and captures each
// channel's skip threshold. It does this on the first document of the binding.
//
// The manager waits for the first document, and does not open channels while the
// transactor is built. A transactor built only to drain pending work stores nothing,
// as Apply does before it alters a table. Such a transactor must not pay for a sidecar
// it will not use. It must also not reconcile against a key range that stands for the
// whole task instead of one shard.
//
// This delay costs nothing. The manager still captures each threshold before the
// first append. The thresholds are absolute document indices, so they then hold for
// the whole session.
func (m *streamV2Manager) ensureOpened(ctx context.Context, b *streamV2Binding) error {
	if b.opened {
		return nil
	}

	// This check runs before the manager starts a sidecar, and before it opens any
	// channel. A backfill that re-created the table of this binding leaves the shard
	// nothing to append there, whatever its own channels report.
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

	var shard = m.shardRange()
	targets, err := streamV2TargetLayout(m.keyBegin, m.keyEnd)
	if err != nil {
		return err
	}

	// Channels the snowpipe streaming path took out of service after this binding
	// left the write path. Whether or not a drop landed, such a channel holds
	// nothing this binding may skip: open it, drop it, and record the deletion
	// unless the target layout reuses the subrange, where flush writes the live
	// item in its place.
	var prior = make(map[string]*streamV2Item, len(b.prior))
	for key, item := range b.prior {
		if item == nil {
			continue
		}
		if !item.Retiring {
			prior[key] = item
			continue
		}
		if _, err := client.OpenChannel(ctx, b.database, b.schema, b.table, item.Channel); err != nil {
			return fmt.Errorf("opening channel %q: %w", item.Channel, err)
		}
		if err := retireChannel(ctx, client, item.Channel); err != nil {
			return err
		}
		var r = streamV2Range{keyBegin: item.KeyBegin, keyEnd: item.KeyEnd}
		if !slices.Contains(targets, r) {
			b.retire = append(b.retire, r.key())
		}
		log.WithFields(log.Fields{"table": b.table, "channel": item.Channel}).Info("retiring a channel the snowpipe streaming path left behind")
	}
	b.prior = prior

	// Sort the checkpoint's items for this binding into the ones nested in this
	// shard's range and the ones that belong to live siblings. An item that does
	// neither — its subrange crosses this shard's boundary — is the footprint of a
	// split that did not land on channel boundaries. Midpoint-only splits cannot
	// produce it, except by splitting a shard again before its channels converged
	// past the depth the new boundary cuts through. The rows that channel holds
	// beyond its counter belong to both children, so neither can skip them.
	var nested []*streamV2Item
	var targetItems int
	for _, item := range b.prior {
		if item == nil {
			// A channel an earlier session retired. The runtime did not yet reduce
			// its deletion away. It records nothing.
			continue
		}
		var r = streamV2Range{keyBegin: item.KeyBegin, keyEnd: item.KeyEnd}
		switch classifySubrange(r, shard, targets) {
		case streamV2SubrangeTarget:
			targetItems++
			nested = append(nested, item)
		case streamV2SubrangeInherited:
			nested = append(nested, item)
		case streamV2SubrangeSibling:
		case streamV2SubrangeStraddling:
			return fmt.Errorf(
				"channel %q covers %s, which crosses the boundary of this shard's range %s: the shard was split off a boundary its channels do not subdivide along, so the rows that channel holds cannot be attributed to either side. Restore the task's shard key ranges to the topology which appended them, or backfill this binding",
				item.Channel, r, shard,
			)
		}
	}
	slices.SortFunc(nested, func(a, b *streamV2Item) int {
		return cmp.Or(cmp.Compare(a.KeyBegin, b.KeyBegin), cmp.Compare(a.KeyEnd, b.KeyEnd))
	})

	// declared reports that the checkpoint holds an item for every target subrange.
	// flush writes those items — the declaration — strictly before any cutover
	// routes rows to the target channels, so a channel this shard drops always
	// leaves the declaration behind as the durable record of why it is gone.
	var declared = targetItems == len(targets)

	// Open every nested channel and reconcile it. What each one holds decides
	// whether it is live — rows route to it — a relic to retire, or a dormant
	// declaration waiting for a cutover. An empty non-target channel cannot be
	// decided alone: whether it is an inherited channel to continue or an
	// orphaned declaration to retire depends on what the other channels hold,
	// so those wait in candidates until every other channel has been read.
	var liveNonTarget, liveTarget, candidates []*streamV2Channel
	var statuses = make(map[string]*channelStatusResult)
	for _, item := range nested {
		var r = streamV2Range{keyBegin: item.KeyBegin, keyEnd: item.KeyEnd}
		var isTarget = classifySubrange(r, shard, targets) == streamV2SubrangeTarget

		status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, item.Channel)
		if err != nil {
			return fmt.Errorf("opening channel %q: %w", item.Channel, err)
		}
		statuses[item.Channel] = status
		if err := rejectedRowsError(item.Channel, b.table, status); err != nil {
			return err
		}

		if status.CommittedToken == nil {
			if isTarget && item.Counter == 0 {
				// A declaration nothing was appended under. It stays dormant: the
				// item keeps the declaration durable, and the channel joins the
				// layout when a cutover converges to it.
				continue
			}
			if !isTarget && item.Counter == 0 {
				// A channel of another layout which holds nothing at all. A skewed
				// transaction leaves one behind legitimately — a channel of the
				// shard this one inherits from that took no rows — and so does a
				// declaration the declaring shard never converged to before its
				// range changed. Which of the two it is turns on the channels
				// around it, so the decision waits for all of them.
				candidates = append(candidates, newStreamV2Channel(item.Channel, r, 0, 0))
				continue
			}
			if !isTarget && declared {
				// A counter with the declaration in place: an interrupted cutover
				// already dropped the channel — the open above re-created it
				// empty — and only the deletion of its item was lost. The
				// retirement repeats: drop, and re-record the deletion.
				if err := retireChannel(ctx, client, item.Channel); err != nil {
					return err
				}
				b.retire = append(b.retire, r.key())
				log.WithFields(log.Fields{
					"table":   b.table,
					"channel": item.Channel,
					"counter": item.Counter,
				}).Info("re-recording the retirement of a channel an interrupted session dropped")
				continue
			}
			// A counter with no token and no declaration to explain it falls
			// through to the reconciliation, which refuses it as a lost channel.
		}

		skip, err := reconcileStreamV2Channel(item.Channel, b.table, status.CommittedToken, item, r, len(b.prior))
		if err != nil {
			return err
		}
		var c = newStreamV2Channel(item.Channel, r, item.Counter, skip)
		if isTarget {
			liveTarget = append(liveTarget, c)
		} else {
			liveNonTarget = append(liveNonTarget, c)
		}
	}

	// Settle the empty non-target channels. With no live inherited channel, the
	// layout is the target layout and every candidate is an empty declaration or
	// an already-converged-away relic: retire them. With live inherited channels,
	// the layout is the inherited one, and a candidate is part of it exactly when
	// it overlaps no live channel — the empty quarter a skewed transaction left,
	// which the tiling check below demands. A candidate overlapping a live
	// channel is a declaration at another depth, orphaned when the declaring
	// shard's range changed before it converged.
	for _, c := range candidates {
		var overlapsLive = false
		for _, live := range append(liveNonTarget, liveTarget...) {
			if c.r.keyBegin <= live.r.keyEnd && live.r.keyBegin <= c.r.keyEnd {
				overlapsLive = true
				break
			}
		}
		if len(liveNonTarget) > 0 && !overlapsLive {
			liveNonTarget = append(liveNonTarget, c)
			continue
		}
		if err := retireChannel(ctx, client, c.name); err != nil {
			return err
		}
		b.retire = append(b.retire, c.r.key())
		log.WithFields(log.Fields{
			"table":   b.table,
			"channel": c.name,
		}).Info("retiring an empty channel of a layout this shard does not continue")
	}

	// The open-time cutover. The declaration is durable — it arrived with the
	// recovered checkpoint — and every inherited channel is settled at its counter,
	// so the convergence an interrupted session declared completes here: the
	// inherited channels retire, and the target layout takes over.
	if len(liveNonTarget) > 0 && declared {
		var quiescent = true
		for _, c := range liveNonTarget {
			if c.skip != c.counter {
				quiescent = false
				break
			}
		}
		if quiescent {
			for _, c := range liveNonTarget {
				if err := retireChannel(ctx, client, c.name); err != nil {
					return err
				}
				b.retire = append(b.retire, c.r.key())
			}
			liveNonTarget = nil
		}
	}

	// The active layout. Channels inherited from another topology carry rows the
	// replayed transaction must skip, so while any of them stands un-retired it is
	// the layout — mixed subdivision depths included, as a join of children that
	// converged unevenly leaves. Otherwise the layout is the target layout.
	var active []*streamV2Channel
	if len(liveNonTarget) > 0 {
		active = append(liveNonTarget, liveTarget...)
	} else {
		for _, r := range targets {
			var have *streamV2Channel
			for _, c := range liveTarget {
				if c.r == r {
					have = c
					break
				}
			}
			if have != nil {
				active = append(active, have)
				continue
			}

			var name = streamV2ChannelName(m.materialization, r, b.stateKey)
			var status = statuses[name]
			if status == nil {
				if status, err = client.OpenChannel(ctx, b.database, b.schema, b.table, name); err != nil {
					return fmt.Errorf("opening channel %q: %w", name, err)
				}
				if err := rejectedRowsError(name, b.table, status); err != nil {
					return err
				}
			}

			var item = b.prior[r.key()]
			skip, err := reconcileStreamV2Channel(name, b.table, status.CommittedToken, item, r, len(b.prior))
			if err != nil {
				return err
			}
			var counter int64
			if item != nil {
				counter = item.Counter
			}
			active = append(active, newStreamV2Channel(name, r, counter, skip))
		}
	}

	slices.SortFunc(active, func(a, b *streamV2Channel) int {
		return cmp.Compare(a.r.keyBegin, b.r.keyBegin)
	})

	// The layout must tile the shard's range exactly, or some documents the runtime
	// delivers have no channel and some key hashes have two. No topology this
	// connector participates in produces such a layout, so reaching here means the
	// checkpoint and the shard ranges disagree about history.
	var ranges = make([]streamV2Range, len(active))
	for i, c := range active {
		ranges[i] = c.r
	}
	if !streamV2LayoutTiles(ranges, shard) {
		var described = make([]string, len(active))
		for i, c := range active {
			described[i] = c.r.String()
		}
		return fmt.Errorf(
			"the channels this task's checkpoint records for this binding cover %s, which does not tile this shard's range %s exactly: the checkpoint and the shard topology disagree about the ranges that have been appended under. Restore the task's shard key ranges to the topology which appended them, or backfill this binding",
			strings.Join(described, " "), shard,
		)
	}

	b.channels, b.targets, b.opened = active, targets, true

	var layout = make(log.Fields, len(active))
	for _, c := range active {
		layout[c.r.String()] = fmt.Sprintf("counter %d skip %d", c.counter, c.skip)
	}
	log.WithFields(log.Fields{
		"table":    b.table,
		"shard":    shard.String(),
		"channels": len(active),
		"layout":   layout,
		"retiring": b.retire,
	}).Info("opened snowpipe streaming v2 channels")
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
// whole again. Only a backfill clears the count, because it rotates the channels and
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

// reconcileStreamV2Channel compares Snowflake's committed offset token for one
// channel against the counter that the driver checkpoint recorded for it. It reports
// the highest document index that Snowflake already holds. A replay must not append a
// document at or below that threshold again.
//
// item is the checkpoint item of this channel, or nil when the checkpoint holds none.
// priorItems counts every item the checkpoint holds for the binding, across all of
// the task's channels; it is what tells a channel's interrupted first transaction
// apart from a token nothing accounts for.
func reconcileStreamV2Channel(channel, table string, committedToken *string, item *streamV2Item, r streamV2Range, priorItems int) (int64, error) {
	var counter int64
	if item != nil {
		counter = item.Counter
	}

	// Snowflake holds nothing for this channel. With no counter to account for, there
	// is nothing to skip. With a counter, the channel that held those documents is
	// gone, and the token that said which of them Snowflake holds went with it.
	//
	// A shard that restarts into a backfill of its binding finds exactly this. The
	// backfill re-creates the table, and the drop destroys every channel bound to it —
	// see client.MustRecreateResource. The refusal is what stops that shard from
	// appending into a table that the new generation re-materializes under it. The
	// runtime already replaces the specification that shard runs, so that replacement
	// settles the refusal, not the backfill the message asks for.
	//
	// The one drop this connector performs itself — the retirement of a channel a
	// rebalance converged away — is recognized before reconciliation, by the
	// declaration its checkpoint carries. A missing token here has no such record,
	// so the message names both remaining readings. An operator who is in a backfill
	// lost nothing and wants to know it. An operator who is not must hear that the
	// account Snowflake keeps of this channel is gone. Neither reading bounds what
	// the channel committed beyond the counter, so the remedy is the same either way.
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

	// The token's range must be the channel's own subrange, always. The subrange is
	// in the channel's name, so every append this write path makes carries it. A
	// token naming any other range was written under a channel scheme this write
	// path never ran, and nothing it counts may be skipped.
	if appendedBy != r {
		return 0, fmt.Errorf(
			"channel %q covers %s but reports a committed offset token for %d documents appended under %s: that token was not written against this channel's subrange, so the documents it counts cannot be identified. Backfill this binding",
			channel, r, committed, appendedBy,
		)
	}

	if committed < counter {
		return 0, fmt.Errorf(
			"channel %q has committed %d documents but this task's checkpoint records %d as appended: the channel has lost committed data, so the missing rows cannot be identified. Backfill this binding",
			channel, committed, counter,
		)
	}

	if item == nil && committed > 0 && priorItems > 0 {
		// A token with no item to account for can be the interrupted first
		// transaction of this channel. That reading holds only when the checkpoint
		// holds nothing for the binding at all — a backfill produces that state, the
		// state key rotating channels and items together.
		//
		// When the checkpoint holds anything, this state should not exist: flush
		// writes an item for every active channel of the binding, and it declares
		// the target layout's items before any cutover routes rows to those
		// channels. A token no item accounts for is therefore something else
		// appending under this binding's names, and its documents must not be
		// skipped, or that many of the documents about to be materialized into the
		// table are dropped instead.
		return 0, fmt.Errorf(
			"channel %q has committed %d documents this task's checkpoint cannot account for: it holds no item for this channel, only items its other channels wrote, and this write path records an item for every channel before appending to it. The documents that token counts must not be skipped, or that many of the documents about to be materialized into %s are dropped instead. Backfill this binding",
			channel, committed, table,
		)
	}

	// committed == counter is the clean boundary, and committed > counter is an
	// interrupted attempt of the transaction now replayed. The replay produces the
	// same documents in the same order, and the routing hash is a function of each
	// document alone, so this channel receives exactly the documents the token
	// counts, and skips them by position.
	return committed, nil
}

// retireChannel takes a channel out of service for good. It drops the channel in
// Snowflake, and does not only close the local handle.
//
// The retirement must reach Snowflake because a channel name is deterministic. The
// connector derives it from the task, the subrange, and the state key. A later shard
// whose layout includes the same subrange derives the same name.
//
// A channel that Snowflake still holds gives its committed offset token to whoever
// opens it next. The drop is what makes the next open of the same name a fresh
// channel, with nothing to account for.
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

// streamV2ChannelNames lists, sorted, the channels the items of a binding name. A
// nil item is a channel the task already retired and names nothing.
func streamV2ChannelNames(items map[string]*streamV2Item) []string {
	var channels []string
	for _, item := range items {
		if item != nil {
			channels = append(channels, item.Channel)
		}
	}
	slices.Sort(channels)
	return channels
}

// streamV2PathAbandoned refuses a binding that materialized through this write path
// and no longer does, unless the departure is the one exit this connector supports:
// the break-glass downgrade to the snowpipe_streaming path, which streamV2Downgrade
// recognizes and streamV2Retirement carries out. The error names the channels that
// the binding otherwise leaves behind.
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
// A backfill is the other way off this path. It rotates the state key of the binding,
// which rotates both the channels and the checkpoint item. Nothing is then left for
// another path to discard, and no channel is left for a later session to find.
func streamV2PathAbandoned(table string, prior map[string]*streamV2Item) error {
	var channels = streamV2ChannelNames(prior)
	if len(channels) == 0 {
		return nil
	}

	return fmt.Errorf(
		"this binding has materialized into %s through the snowpipe_streaming_v2 write path, which this task's specification no longer selects for it, while the task's checkpoint still records the channel(s) %s it appended to. Those counters are the only account of which documents Snowflake's channels already hold, no other write path maintains them, and the first transaction on another path discards them — after which returning to this write path would skip that many of the documents it materializes. Restore this binding to the snowpipe_streaming_v2 write path — it needs the feature flag, delta updates, and key-pair authentication — or backfill it, which rotates its channels and its checkpoint together",
		table, strings.Join(channels, ", "),
	)
}

// streamV2DowngradeWarning reports what a publication moving a binding from the
// snowpipe streaming v2 write path onto the snowpipe streaming path costs it, and
// "" when the binding names no channel to leave behind.
func streamV2DowngradeWarning(table string, items map[string]*streamV2Item) string {
	var channels = streamV2ChannelNames(items)
	if len(channels) == 0 {
		return ""
	}

	return fmt.Sprintf(
		"binding %s is leaving the snowpipe_streaming_v2 write path for snowpipe_streaming. Every document that its channel(s) %s committed beyond the counter the checkpoint records for them will be materialized again by the snowpipe_streaming path, and this binding uses delta updates, so those duplicates are permanent. The channels are retired by the task's first transaction on the new path",
		table, strings.Join(channels, ", "),
	)
}

// writeRow routes one converted document to a channel of the binding, counts it, and
// buffers it for append. It sends a batch as soon as the batch reaches either cap. It
// counts a document that Snowflake already committed during an interrupted attempt of
// this transaction, but it drops that document.
func (m *streamV2Manager) writeRow(ctx context.Context, binding int, packedKey []byte, converted []any) error {
	var b = m.bindings[binding]
	if err := m.ensureOpened(ctx, b); err != nil {
		return err
	}

	// The routing hash is the runtime's own: the same function, over the same packed
	// key bytes, that assigned this document to this shard. A hash the layout does
	// not cover means the runtime and this connector disagree about routing, and
	// nothing downstream of that disagreement can be trusted — refuse rather than
	// misfile a single row.
	var hash = packedKeyHashHH64(packedKey)
	var c = b.route(hash)
	if c == nil {
		return fmt.Errorf(
			"the key hash %08x of a document stored to %s is covered by no channel of this shard, which covers %s: the runtime and this connector disagree about document routing",
			hash, b.table, m.shardRange(),
		)
	}

	c.counter++
	if c.counter <= c.skip {
		return nil
	}

	var grew, err = c.bufferRow(c.counter, b.columns, converted)
	if err != nil {
		return fmt.Errorf("encoding row for %s: %w", b.table, err)
	}
	m.bufBytes += grew

	if c.bufRows >= streamV2BatchRows || len(c.buf) >= streamV2BatchBytes {
		return m.appendBatch(ctx, c)
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
func (c *streamV2Channel) bufferRow(counter int64, columns []rowColumn, converted []any) (int, error) {
	var before = len(c.buf)
	if c.bufRows == 0 {
		c.startBatch()
		c.bufFirst = counter
	} else {
		c.buf = append(c.buf, ',')
	}

	var payload, err = appendRowJSON(c.buf, columns, converted)
	if err != nil {
		c.buf = c.buf[:before]
		return 0, err
	}

	c.buf = payload
	c.bufRows++
	return len(c.buf) - before, nil
}

// finishBatch closes the payload of the batch and gives up ownership of the buffer. It
// reports the payload, the number of rows it holds, and the buffered bytes it releases.
//
// finishBatch counts those bytes before it writes the closing bracket, because that is
// the byte total bufferRow reported as it built the batch. A count of the bracket here
// as well leaves the total of buffered bytes one byte short of zero for every batch of
// the session. That drifts the back-pressure ceiling away from the memory it stands
// for.
func (c *streamV2Channel) finishBatch() (payload []byte, rows int, released int) {
	released = len(c.buf)
	c.buf = append(c.buf, ']')

	payload, rows = c.buf, c.bufRows
	c.bufHint = len(c.buf)
	c.buf, c.bufRows, c.bufFirst = nil, 0, 0
	return payload, rows, released
}

// streamV2MinBatchCapacity is the size a payload buffer starts with. It applies until
// the channel sends a batch that can size the next one.
const streamV2MinBatchCapacity = 8 * 1024

// startBatch opens the payload of a new batch. The size is a little over the batch
// before it. A full batch then does not grow and copy a dozen times on its way to 8
// MiB. The buffer is always a new one, because the batch before it owns the bytes it
// received.
func (c *streamV2Channel) startBatch() {
	c.buf = append(make([]byte, 0, max(c.bufHint+c.bufHint/8, streamV2MinBatchCapacity)), '[')
}

// appendAllBatches appends the buffered documents of every channel of every binding.
// This brings the total of buffered bytes back to zero.
func (m *streamV2Manager) appendAllBatches(ctx context.Context) error {
	for _, b := range m.bindings {
		for _, c := range b.channels {
			if c.bufRows == 0 {
				continue
			} else if err := m.appendBatch(ctx, c); err != nil {
				return err
			}
		}
	}
	return nil
}

// appendBatch closes the payload of the batch and hands it to the append pipe of the
// channel. Ownership of the buffer passes to the append, so the next batch starts a
// buffer of its own.
func (m *streamV2Manager) appendBatch(ctx context.Context, c *streamV2Channel) error {
	var first, last = c.bufFirst, c.counter
	payload, rows, released := c.finishBatch()
	m.bufBytes -= released

	// Back-pressure is implicit. A saturated pipe blocks the append. The blocked append
	// blocks the next submit, and that blocks Store.
	var err = c.pipe.submit(func() error {
		if err := c.pacer.WaitN(ctx, len(payload)); err != nil {
			return fmt.Errorf("pacing channel %q: %w", c.name, err)
		}
		client, err := m.ensureStarted(ctx)
		if err != nil {
			return err
		}
		return client.Append(ctx, c.name, c.offsetToken(first), c.offsetToken(last), payload, rows)
	})
	if err != nil {
		return fmt.Errorf("appending to channel %q: %w", c.name, err)
	}
	return nil
}

// flush appends the remaining buffered documents of each channel. It waits for every
// in-flight append, and then waits for Snowflake to durably commit them. It returns
// the checkpoint entries of each binding that did anything this transaction, and it
// reports them only after Snowflake holds the documents their counters account for.
//
// The entries hold an item for every active channel, advanced or not. This is the
// declaration invariant: every channel rows can route to has an item in the
// checkpoint no later than the transaction that first routes to it, and the target
// layout a rebalance converges to is declared — written as counter-zero items — at
// least one durable checkpoint before the cutover routes anything there. A token
// with no item to account for therefore never has a benign reading.
//
// flush awaits the commit here, as part of the checkpoint it produces, and not at
// Acknowledge. The checkpoint of the runtime is durable before Acknowledge runs. A
// commit that failed there could no longer fail the transaction whose counter it
// belongs to. The task resumes from a counter Snowflake never committed, and the next
// Open can only refuse it. No replay re-appends rows the runtime already considers
// delivered.
//
// A failure here instead leaves the runtime to replay the transaction, and the
// reconciliation of that Open skips the documents Snowflake did commit.
func (m *streamV2Manager) flush(ctx context.Context) (map[int]map[string]*streamV2Item, error) {
	var entries = make(map[int]map[string]*streamV2Item)
	type commitWait struct {
		b       *streamV2Binding
		c       *streamV2Channel
		counter int64
	}
	var waits []commitWait

	for idx, b := range m.bindings {
		if !b.opened {
			continue // the binding stored nothing this session
		}

		var advanced = false
		for _, c := range b.channels {
			if c.bufRows > 0 {
				if err := m.appendBatch(ctx, c); err != nil {
					return nil, err
				}
			}
			if err := c.pipe.wait(); err != nil {
				return nil, fmt.Errorf("appending to channel %q: %w", c.name, err)
			}

			// A replayed transaction that routed fewer documents to this channel
			// than the interrupted attempt committed was not replayed identically.
			// The skip threshold then dropped documents that Snowflake does not
			// hold. Only the first transaction of the session can trip this,
			// because the counter only grows from there.
			if c.counter < c.skip {
				return nil, fmt.Errorf(
					"channel %q holds %d committed documents but the transaction replayed against it produced only %d: it was not replayed identically, so the rows Snowflake already holds cannot be identified. Backfill this binding",
					c.name, c.skip, c.counter,
				)
			}

			if c.counter != c.recorded {
				advanced = true
			}
		}

		var declaring = !b.isTargetLayout()
		if !advanced && !declaring && len(b.retire) == 0 {
			continue // nothing stored, nothing retiring, nothing to converge
		}

		var items = make(map[string]*streamV2Item)
		for _, c := range b.channels {
			c.recorded = c.counter
			items[c.r.key()] = &streamV2Item{
				Channel:  c.name,
				Counter:  c.counter,
				KeyBegin: c.r.keyBegin,
				KeyEnd:   c.r.keyEnd,
			}
			if c.counter > c.committed {
				waits = append(waits, commitWait{b: b, c: c, counter: c.counter})
			}
		}

		if declaring {
			// The declaration: an item for every target channel the layout has not
			// converged to yet. The cutover at Acknowledge runs only after the
			// checkpoint carrying these is durable.
			for _, r := range b.targets {
				if _, ok := items[r.key()]; !ok {
					var name = streamV2ChannelName(m.materialization, r, b.stateKey)
					items[r.key()] = &streamV2Item{Channel: name, KeyBegin: r.keyBegin, KeyEnd: r.keyEnd}
				}
			}
			b.declared = true
		}

		// Deletions of retired channels ride every checkpoint of the session, not
		// only the first, so that a transaction that never commits does not lose
		// them.
		for _, retired := range b.retire {
			items[retired] = nil
		}

		entries[idx] = items
	}

	// Channels commit independently, and the sidecar serializes its operations per
	// channel and not globally. A wide materialization therefore does not pay the
	// commit latency of each channel in series. The limit scales with the channels
	// per shard so that a binding's channels do not queue behind each other.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(MaxConcurrentQueries * streamV2ChannelsPerShard)
	for _, w := range waits {
		group.Go(func() error { return m.waitCommit(groupCtx, w.b, w.c, w.counter) })
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	return entries, nil
}

// acknowledged runs after the runtime durably committed the transaction whose
// checkpoint the last flush produced. It is where a rebalance cuts over: the
// declaration that flush wrote is durable now, every channel is settled at its
// counter, and the boilerplate serializes this call before the next Store, so no row
// routes anywhere while the layout changes.
//
// The cutover drops every non-target channel of the layout — recording each drop for
// deletion — and opens the target channels in their place. A crash between the drops
// and the durability of the deletions restarts to a checkpoint that still holds the
// dropped channels' items alongside the declaration, which is exactly the state
// ensureOpened re-enters the retirement from.
func (m *streamV2Manager) acknowledged(ctx context.Context) error {
	var indices = make([]int, 0, len(m.bindings))
	for idx := range m.bindings {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	for _, idx := range indices {
		var b = m.bindings[idx]
		if !b.opened || !b.declared {
			continue
		}
		b.declared = false
		if b.isTargetLayout() {
			continue
		}

		// The declaration is only ever written by a flush that settled every
		// channel, and nothing stores between that flush and this call. A channel
		// found unsettled here means that reasoning is broken somewhere, and a
		// deferred convergence costs only throughput — so defer, loudly.
		var quiescent = true
		for _, c := range b.channels {
			if c.committed != c.counter || c.bufRows > 0 {
				quiescent = false
				break
			}
		}
		if !quiescent {
			log.WithFields(log.Fields{"table": b.table}).Warn(
				"snowpipe streaming v2: skipping a channel rebalance because a channel is not settled; this should be impossible")
			continue
		}

		client, err := m.ensureStarted(ctx)
		if err != nil {
			return err
		}

		var next []*streamV2Channel
		for _, c := range b.channels {
			if slices.Contains(b.targets, c.r) {
				next = append(next, c)
				continue
			}
			if err := retireChannel(ctx, client, c.name); err != nil {
				return err
			}
			b.retire = append(b.retire, c.r.key())
		}

		for _, r := range b.targets {
			if slices.ContainsFunc(next, func(c *streamV2Channel) bool { return c.r == r }) {
				continue
			}
			var name = streamV2ChannelName(m.materialization, r, b.stateKey)
			status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, name)
			if err != nil {
				return fmt.Errorf("opening channel %q: %w", name, err)
			}
			if err := rejectedRowsError(name, b.table, status); err != nil {
				return err
			}
			// The channel was declared this session and nothing has routed to it
			// yet, so it must be empty. A token here is something else appending
			// under this binding's names.
			if status.CommittedToken != nil {
				return fmt.Errorf(
					"channel %q was declared by this shard and should hold nothing, but reports the committed offset token %q: something else is appending under this binding's channel names, and continuing could duplicate or drop rows. Backfill this binding",
					name, *status.CommittedToken,
				)
			}
			next = append(next, newStreamV2Channel(name, r, 0, 0))
		}

		slices.SortFunc(next, func(a, b *streamV2Channel) int {
			return cmp.Compare(a.r.keyBegin, b.r.keyBegin)
		})
		b.channels = next

		log.WithFields(log.Fields{
			"table":    b.table,
			"channels": len(next),
			"retiring": b.retire,
		}).Info("snowpipe streaming v2: converged the channel layout to the target layout")
	}
	return nil
}

// waitCommit blocks until the channel durably holds every document counted through
// counter. If Snowflake rejected any of its rows, waitCommit fails the transaction.
func (m *streamV2Manager) waitCommit(ctx context.Context, b *streamV2Binding, c *streamV2Channel, counter int64) error {
	// A replay of documents Snowflake already holds appends nothing. Its counter is
	// already committed, so waitCommit must not wait on it.
	if counter <= c.committed {
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
		"channel": c.name,
		"counter": counter,
	}).Info("snowpipe streaming v2: awaiting commit")

	var started = time.Now()
	var token = c.offsetToken(counter)
	status, err := client.WaitCommit(ctx, c.name, token)
	if err != nil {
		return fmt.Errorf("waiting for commit of document %s on channel %q: %w", token, c.name, err)
	}

	// The commit of the token is the earliest point where this transaction can see its
	// own rejections. That point is now before flush checkpoints its counter. A rejection
	// therefore fails the transaction that produced it, and not a later one.
	if err := rejectedRowsError(c.name, b.table, status); err != nil {
		return err
	}
	c.committed = counter

	log.WithFields(log.Fields{
		"table":   b.table,
		"channel": c.name,
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
