package connector

import (
	"bytes"
	"cmp"
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake/v2"
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

// streamV2ChannelCheckpointItem represents one channel of a binding, and the
// document index last routed to it.
type streamV2ChannelCheckpointItem struct {
	// ChannelName is the name of the Snowpipe channel.
	ChannelName string
	// Routed is the ordinal index of the last document routed to the channel. The
	// channel's first document has index 1, so zero means nothing was routed.
	Routed int64
}

// streamV2Checkpoint is the streaming v2 state of all channels of a binding.
type streamV2Checkpoint = map[streamV2Range]*streamV2ChannelCheckpointItem

// streamV2Range is a key range of the key-hash space, as a channel name and a
// committed offset token report it.
type streamV2Range struct {
	keyBegin, keyEnd uint32
}

// contains reports whether the key range covers a key hash. Bounds are inclusive
// on both ends, as RangeSpec bounds are.
func (r streamV2Range) contains(hash uint32) bool {
	return r.keyBegin <= hash && hash <= r.keyEnd
}

func (r streamV2Range) String() string {
	return fmt.Sprintf("[%08x, %08x]", r.keyBegin, r.keyEnd)
}

func (r streamV2Range) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "%08x-%08x", r.keyBegin, r.keyEnd), nil
}

func (r *streamV2Range) UnmarshalText(text []byte) error {
	begin, end, ok := bytes.Cut(text, []byte("-"))
	if !ok {
		return fmt.Errorf("key range %q lacks a hyphen", text)
	}
	keyBegin, err := strconv.ParseUint(string(begin), 16, 32)
	if err != nil {
		return fmt.Errorf("key range %q: %w", text, err)
	}
	keyEnd, err := strconv.ParseUint(string(end), 16, 32)
	if err != nil {
		return fmt.Errorf("key range %q: %w", text, err)
	}
	*r = streamV2Range{keyBegin: uint32(keyBegin), keyEnd: uint32(keyEnd)}
	return nil
}

// streamV2FormatChannelName builds a Snowpipe channel name as
// "<materialization>_<epoch>_<keyBegin>-<keyEnd>_<stateKey>".
func streamV2FormatChannelName(materialization string, epoch int, keyRange streamV2Range, stateKey string) string {
	return fmt.Sprintf("%s_%d_%08x-%08x_%s",
		sanitizeAndAppendHash(materialization), epoch, keyRange.keyBegin, keyRange.keyEnd, sanitizeAndAppendHash(stateKey))
}

// streamV2ChannelNameShape is the shape of the names that streamV2FormatChannelName
// produces.
var streamV2ChannelNameShape = regexp.MustCompile(
	`^(.+?_[0-9A-F]{16})_(\d+)_([0-9a-f]{8})-([0-9a-f]{8})_(.+_[0-9A-F]{16})$`)

type streamV2ChannelNameParts struct {
	materialization string // as formatted by sanitizeAndAppendHash
	epoch           int
	keyRange        streamV2Range
	stateKey        string // as formatted by sanitizeAndAppendHash
}

// streamV2SplitChannelName reads a channel name back into its parts, and reports
// whether the name has the shape of a v2 channel name at all.
func streamV2SplitChannelName(channelName string) (streamV2ChannelNameParts, bool) {
	var groups = streamV2ChannelNameShape.FindStringSubmatch(channelName)
	if groups == nil {
		return streamV2ChannelNameParts{}, false
	}
	epoch, err := strconv.Atoi(groups[2])
	if err != nil {
		return streamV2ChannelNameParts{}, false
	}
	keyBegin, err := strconv.ParseUint(groups[3], 16, 32)
	if err != nil {
		return streamV2ChannelNameParts{}, false
	}
	keyEnd, err := strconv.ParseUint(groups[4], 16, 32)
	if err != nil {
		return streamV2ChannelNameParts{}, false
	}
	return streamV2ChannelNameParts{
		materialization: groups[1],
		epoch:           epoch,
		keyRange:        streamV2Range{keyBegin: uint32(keyBegin), keyEnd: uint32(keyEnd)},
		stateKey:        groups[5],
	}, true
}

// streamV2ParseChannelName reads the epoch and key range of a channel name this
// materialization and state key derived, and reports whether the name is such a
// channel.
func streamV2ParseChannelName(channelName, materialization, stateKey string) (int, streamV2Range, bool) {
	var parts, ok = streamV2SplitChannelName(channelName)
	if !ok || parts.materialization != sanitizeAndAppendHash(materialization) || parts.stateKey != sanitizeAndAppendHash(stateKey) {
		return 0, streamV2Range{}, false
	}
	return parts.epoch, parts.keyRange, true
}

// streamV2ForeignTaskError rejects a table on which another Flow task's v2 channels
// stand. The connector cannot tell a live task from a deleted or renamed one whose
// channels outlived it, so the message carries the remedy for each.
func streamV2ForeignTaskError(table string, foreignChannelNames map[string][]string) error {
	var materializationNames = make([]string, 0, len(foreignChannelNames))
	for materialization := range foreignChannelNames {
		materializationNames = append(materializationNames, materialization)
	}
	slices.Sort(materializationNames)

	var described = make([]string, len(materializationNames))
	for i, materialization := range materializationNames {
		var channelNames = foreignChannelNames[materialization]
		slices.Sort(channelNames)
		described[i] = fmt.Sprintf("%s (channels %s)", materialization, strings.Join(channelNames, ", "))
	}

	return fmt.Errorf(
		"table %s already receives snowpipe streaming v2 rows from another Flow task, whose channel names begin with %s. Two tasks may not stream into one table. If that task still exists, materialize this binding into a table of its own, or remove this table from that task. If that task was deleted or renamed, its channels have outlived it: backfill this binding with the always_drop_tables_on_backfill feature flag set, which drops the table and every channel standing on it",
		table, strings.Join(described, "; "),
	)
}

// streamV2Token renders the offset token of an append. The token holds two facts:
// the index of the last document in the append, and the channel key range it was
// appended under.
//
// The key range is in the token so that a token can be recognized as this channel's
// own. A token whose range is not the channel's key range was written by something
// else — another connector, or a channel scheme this write path never ran — and
// nothing it counts may be skipped.
func streamV2Token(index int64, keyRange streamV2Range) string {
	var spec, _ = keyRange.MarshalText()
	return fmt.Sprintf("%d@%s", index, spec)
}

// parseStreamV2Token reads a committed offset token back as two values: the document
// index it carries, and the key range those documents were appended under. It returns
// false for a token that this write path never writes.
func parseStreamV2Token(token string) (int64, streamV2Range, bool) {
	var count, spec, hasRange = strings.Cut(token, "@")
	if !hasRange {
		return 0, streamV2Range{}, false
	}

	index, err := strconv.ParseInt(count, 10, 64)
	if err != nil {
		return 0, streamV2Range{}, false
	}

	var keyRange streamV2Range
	if err := keyRange.UnmarshalText([]byte(spec)); err != nil {
		return 0, streamV2Range{}, false
	}
	return index, keyRange, true
}

// streamV2CannotDrainPendingBlobs reports that a binding moving onto the streaming
// v2 write path carries Snowpipe Streaming blobs which no manager can register.
//
// Acknowledge drains a pending item through the manager of the write path which
// staged it. A binding whose rows go to streaming v2 still registers with the
// bdec manager for that drain, so this is reached only where the registration is
// impossible: a table whose columns that path does not support, or one it may not
// stream into at all.
//
// The blobs are the only record of documents which Snowflake does not hold yet, so
// they are neither dropped nor left to fail every transaction that tries to drain
// them.
func streamV2CannotDrainPendingBlobs(table string, blobs int, cause error) error {
	var err = fmt.Errorf(
		"this binding is moving onto the snowpipe_streaming_v2 write path while the task's checkpoint still records %d Snowpipe Streaming blob(s) that the snowpipe_streaming write path staged into %s and did not finish, and only the snowpipe_streaming path can finish them. Restore that path for one transaction before you move the binding onto snowpipe_streaming_v2, or backfill the binding, which discards them and materializes those documents again",
		blobs, table,
	)
	if cause != nil {
		return fmt.Errorf("%w: the snowpipe_streaming path cannot reopen its channel on the table: %w", err, cause)
	}
	return err
}

// streamV2DefaultPipeSuffix ends the name Snowflake gives the pipe it auto-creates
// for a table streamed into through the high-performance architecture: the table's
// own name, followed by this suffix.
const streamV2DefaultPipeSuffix = "-STREAMING"

// streamV2ErrObjectNotExistOrAuthorized is the Snowflake SQL compilation error
// number for an object that does not exist, or that the role may not see; Snowflake
// does not tell the two apart.
const streamV2ErrObjectNotExistOrAuthorized = 2003

// streamV2ListChannels reports, sorted, the channels on a table and on its default
// pipe. A table nothing has streamed into has no default pipe and lists no channels
// for it.
//
// database, schema, and table are the unquoted names Snowflake stores.
func streamV2ListChannels(ctx context.Context, db *stdsql.DB, dialect sql.Dialect, database, schema, table string) ([]string, error) {
	var channelNames []string
	for _, in := range []string{
		"TABLE " + dialect.Identifier(database, schema, table),
		"PIPE " + dialect.Identifier(database, schema, table+streamV2DefaultPipeSuffix),
	} {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SHOW CHANNELS IN %s;", in))
		if err != nil {
			// Snowflake reports an absent pipe with the same error as one the role
			// may not see, so the miss is logged.
			var sfErr *sf.SnowflakeError
			if errors.As(err, &sfErr) && sfErr.Number == streamV2ErrObjectNotExistOrAuthorized {
				log.WithFields(log.Fields{"in": in, "error": err.Error()}).Info("no channels are visible to this role")
				continue
			}
			return nil, fmt.Errorf("listing channels in %s: %w", in, err)
		}
		defer rows.Close()

		// SHOW reports many columns; only "name" is read, and the rest are scanned
		// into a sink, so the column order Snowflake chooses does not matter.
		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("reading columns of the channel listing in %s: %w", in, err)
		}
		var nameAt = slices.Index(columns, "name")
		if nameAt < 0 {
			return nil, fmt.Errorf("the channel listing in %s reports no \"name\" column", in)
		}

		for rows.Next() {
			var channelName string
			var dest = make([]any, len(columns))
			for i := range dest {
				dest[i] = new(any)
			}
			dest[nameAt] = &channelName
			if err := rows.Scan(dest...); err != nil {
				return nil, fmt.Errorf("scanning the channel listing in %s: %w", in, err)
			}
			channelNames = append(channelNames, channelName)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating the channel listing in %s: %w", in, err)
		}
	}
	slices.Sort(channelNames)
	return slices.Compact(channelNames), nil
}

// streamV2Channel is a handle for one Snowpipe channel of one connector binding.
type streamV2Channel struct {
	// channelName is the name of the Snowpipe channel.
	channelName string
	// keyRange is the key range this channel covers. The key range is embedded in
	// the channel's name and in every offset token it appends with.
	keyRange streamV2Range

	// progress tracks the position of the channel along its ordinal document index.
	// The first document the shard routes to the channel has index 1, each later
	// document is assigned the next index, and the index never resets.
	//
	// committed exceeds routed only while a replayed transaction routes documents
	// that an interrupted attempt already committed.
	progress struct {
		// routed leads progress. Its value is the index of the most recent
		// document that the shard routed to this channel.
		routed int64
		// checkpointed is the index that the last checkpoint item carried.
		checkpointed int64
		// committed is the highest index that Snowflake durably holds.
		committed int64
	}

	// buf is the payload of the batch under construction
	buf      []byte
	bufRows  int
	bufFirst int64
	// bufHint is the size of the batch handed off before this one, and it sizes the next
	// buffer.
	bufHint int

	// pipe carries one append at a time. Store can buffer more rows while a batch is on
	// the wire, and the batches keep their order.
	pipe appendPipe

	// limiter meters the uncompressed bytes this channel hands to Snowflake. One limiter
	// per channel, because Snowflake meters each channel independently.
	limiter *rate.Limiter
}

// offsetToken renders the offset token for a document index on this channel.
func (c *streamV2Channel) offsetToken(index int64) string {
	return streamV2Token(index, c.keyRange)
}

type streamV2Binding struct {
	database    string
	schema      string
	table       string
	stateKey    string
	columnNames []columnName
	// prior is the streaming v2 state that the driver checkpoint recorded for this
	// binding. The map key is the channel's key range, and the map covers every shard
	// of the task. It is nil when the checkpoint recorded no state.
	prior streamV2Checkpoint
	// opened reports whether this session classified the checkpoint's channels,
	// opened its own, and reconciled each of them.
	opened bool
	// abandoned holds the key ranges of the channels this shard no longer routes to.
	// The next checkpoint deletes their items so a later session does not reconcile
	// against a channel this layout left behind. Every checkpoint of the session
	// reports them again, not only the first, so a transaction that never commits
	// does not lose the deletion. The channels themselves are left standing for the
	// sweep to drop, since a fresh epoch keeps their names out of any live layout.
	abandoned []streamV2Range
	// targetEpoch is the epoch of the channels this shard converges to. It continues
	// the epoch the shard's own target items already run at, or is minted one past
	// every epoch the binding has used, so the target names collide with nothing.
	targetEpoch int

	// channels is the active layout: the channels rows route to, sorted by
	// keyRangeBegin. The layout always covers the shard's key range with no gaps or
	// overlaps, so every document the runtime delivers routes to exactly one of them.
	channels []*streamV2Channel
	// targets is the shard's target layout: its key range cut into
	// streamV2ChannelsPerShard equal key ranges. The active layout converges to it.
	targets []streamV2Range
	// declared reports that the last flush wrote the target layout's items into the
	// checkpoint. Acknowledge runs after the runtime made that checkpoint durable,
	// which is what makes the switch it performs crash-safe.
	declared bool
}

// isTargetLayout reports whether the active layout is the target layout.
func (b *streamV2Binding) isTargetLayout() bool {
	if len(b.channels) != len(b.targets) {
		return false
	}
	for i, c := range b.channels {
		if c.keyRange != b.targets[i] {
			return false
		}
	}
	return true
}

// route reports the active channel that covers a key hash, or nil when none does.
func (b *streamV2Binding) route(hash uint32) *streamV2Channel {
	for _, c := range b.channels {
		if c.keyRange.contains(hash) {
			return c
		}
	}
	return nil
}

// appendPipe runs one append at a time, on a goroutine of its own. A submit of the
// next append reports the result of the previous one. A failure therefore surfaces at
// the next batch boundary or at flush, and nothing drops it.
type appendPipe struct {
	pending chan error
}

func (p *appendPipe) submit(fn func() error) error {
	if err := p.wait(); err != nil {
		return err
	}
	p.pending = make(chan error, 1)
	go func(done chan error) { done <- fn() }(p.pending)
	return nil
}

func (p *appendPipe) wait() error {
	if p.pending == nil {
		return nil
	}
	var err = <-p.pending
	p.pending = nil
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

	// listChannels reports the channels on a table and its default pipe. It is nil
	// when no connection backs the manager.
	listChannels func(ctx context.Context, database, schema, table string) ([]string, error)
	// dropStreamingChannel drops a snowpipe_streaming channel. It is nil when that
	// path is unavailable.
	dropStreamingChannel func(ctx context.Context, schema, table, name string) error

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
func (m *streamV2Manager) addBinding(database, schema, table string, target sql.Table, prior streamV2Checkpoint) {
	var names []string
	for _, col := range target.Columns() {
		names = append(names, unquotedIdentifier(col.Identifier))
	}

	m.bindings[target.Binding] = &streamV2Binding{
		database:    database,
		schema:      schema,
		table:       table,
		stateKey:    target.StateKey,
		columnNames: columnNamesOf(names),
		prior:       prior,
	}
}

// newStreamV2Channel builds the in-memory state of one opened, reconciled channel.
func newStreamV2Channel(channelName string, keyRange streamV2Range, routed, committed int64) *streamV2Channel {
	var c = &streamV2Channel{
		channelName: channelName,
		keyRange:    keyRange,
		limiter:     rate.NewLimiter(streamV2PaceBytesPerSecond, 2*streamV2PaceBytesPerSecond),
		progress: struct {
			routed       int64
			checkpointed int64
			committed    int64
		}{
			routed:       routed,
			checkpointed: routed,
			committed:    committed,
		},
	}
	return c
}

// ensureOpened builds the active channel layout of the binding and captures each
// channel's committed index. It does this on the first document of the binding.
//
// The manager waits for the first document, and does not open channels while the
// transactor is built. A transactor built only to drain pending work stores nothing,
// as Apply does before it alters a table. Such a transactor must not pay for a sidecar
// it will not use. It must also not reconcile against a key range that stands for the
// whole task instead of one shard.
//
// This delay costs nothing. The manager still captures each committed index before
// the first append.
func (m *streamV2Manager) ensureOpened(ctx context.Context, b *streamV2Binding) error {
	if b.opened {
		return nil
	}

	// A table another task streams into is rejected before this shard creates any
	// channel of its own. A name of no v2 shape is left alone here.
	if m.listChannels != nil {
		channelNames, err := m.listChannels(ctx, b.database, b.schema, unquotedIdentifier(b.table))
		if err != nil {
			return err
		}

		var own = sanitizeAndAppendHash(m.materialization)
		var foreignChannelNames = make(map[string][]string)
		for _, channelName := range channelNames {
			if parts, ok := streamV2SplitChannelName(channelName); !ok {
				log.WithFields(log.Fields{"table": b.table, "channel": channelName}).Info("a channel of no snowpipe streaming v2 shape stands on the table")
				continue
			} else if parts.materialization != own {
				foreignChannelNames[parts.materialization] = append(foreignChannelNames[parts.materialization], channelName)
			}
		}
		if len(foreignChannelNames) > 0 {
			return streamV2ForeignTaskError(b.table, foreignChannelNames)
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

	// A nil item is a channel a layout left behind, which the runtime has not yet
	// reduced out of the checkpoint. Drop them so the live-item count below, and the
	// epoch minted from the items, reason only about channels that still stand.
	var prior = make(streamV2Checkpoint, len(b.prior))
	for key, sv2ChannelCheckpointItem := range b.prior {
		if sv2ChannelCheckpointItem != nil {
			prior[key] = sv2ChannelCheckpointItem
		}
	}
	b.prior = prior

	// The epoch of the target layout. The shard's own target items, where the
	// checkpoint holds them, fix it, so a restart continues the channels it already
	// ran rather than rotating them. Otherwise it is minted one past every epoch a
	// checkpoint item nested in this shard's range has used, so the target names
	// collide with nothing a rebalance abandoned and a sweep has yet to drop.
	// Channels outside this shard's range are a sibling's, and their names, at
	// whatever epoch, never collide with this shard's, so a sibling's convergence
	// does not push this shard's epoch.
	//
	// The epoch is read from the checkpoint alone, never from the channels standing
	// on the pipe: an interrupted first transaction leaves a committed channel with
	// no checkpoint item, and that channel must be reopened and its committed
	// documents skipped, not stepped over by a fresh epoch that would materialize
	// them twice.
	b.targetEpoch = -1
	for _, keyRange := range targets {
		if sv2ChannelCheckpointItem := b.prior[keyRange]; sv2ChannelCheckpointItem != nil {
			if epoch, _, ok := streamV2ParseChannelName(sv2ChannelCheckpointItem.ChannelName, m.materialization, b.stateKey); ok {
				b.targetEpoch = epoch
				break
			}
		}
	}
	if b.targetEpoch < 0 {
		var maxEpoch = -1
		for _, sv2ChannelCheckpointItem := range b.prior {
			if epoch, keyRange, ok := streamV2ParseChannelName(sv2ChannelCheckpointItem.ChannelName, m.materialization, b.stateKey); ok &&
				keyRange.keyBegin >= shard.keyBegin && keyRange.keyEnd <= shard.keyEnd {
				maxEpoch = max(maxEpoch, epoch)
			}
		}
		b.targetEpoch = maxEpoch + 1
	}

	// Sort the checkpoint's items for this binding into the ones nested in this
	// shard's range and the ones that belong to live siblings. An item that does
	// neither — its key range crosses this shard's boundary — is the footprint of a
	// split that did not land on channel boundaries. Midpoint-only splits cannot
	// produce it, except by splitting a shard again before its channels converged
	// past the depth the new boundary cuts through. The rows that channel holds
	// beyond its routed index belong to both children, so neither can skip them.
	var nested []streamV2Range
	var targetItems int
	for keyRange, sv2ChannelCheckpointItem := range b.prior {
		if sv2ChannelCheckpointItem == nil {
			// A channel an earlier session dropped. The runtime did not yet reduce
			// its deletion away. It records nothing.
			continue
		}
		switch classifyKeyRange(keyRange, shard, targets) {
		case streamV2KeyRangeTarget:
			targetItems++
			nested = append(nested, keyRange)
		case streamV2KeyRangeInherited:
			nested = append(nested, keyRange)
		case streamV2KeyRangeSibling:
		case streamV2KeyRangeStraddling:
			return fmt.Errorf(
				"channel %q covers %s, which crosses the boundary of this shard's range %s: the shard was split off a boundary its channels do not subdivide along, so the rows that channel holds cannot be attributed to either side. Restore the task's shard key ranges to the topology which appended them, or backfill this binding",
				sv2ChannelCheckpointItem.ChannelName, keyRange, shard,
			)
		}
	}
	slices.SortFunc(nested, func(a, b streamV2Range) int {
		return cmp.Or(cmp.Compare(a.keyBegin, b.keyBegin), cmp.Compare(a.keyEnd, b.keyEnd))
	})

	// declared reports that the checkpoint holds an item for every target key range.
	// flush writes those items — the declaration — strictly before any switch
	// routes rows to the target channels, so a channel this shard drops always
	// leaves the declaration behind as the durable record of why it is gone.
	var declared = targetItems == len(targets)

	// Open every nested channel and reconcile it. What each one holds decides
	// whether it is live — rows route to it — a relic to drop, or a dormant
	// declaration waiting for a switch. An empty non-target channel cannot be
	// decided alone: whether it is an inherited channel to continue or an
	// orphaned declaration to drop depends on what the other channels hold,
	// so those wait in candidates until every other channel has been read.
	var liveNonTarget, liveTarget, candidates []*streamV2Channel
	var statuses = make(map[string]*channelStatusResult)
	for _, keyRange := range nested {
		var sv2ChannelCheckpointItem = b.prior[keyRange]
		var isTarget = classifyKeyRange(keyRange, shard, targets) == streamV2KeyRangeTarget

		status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, sv2ChannelCheckpointItem.ChannelName)
		if err != nil {
			return fmt.Errorf("opening channel %q: %w", sv2ChannelCheckpointItem.ChannelName, err)
		}
		statuses[sv2ChannelCheckpointItem.ChannelName] = status
		if err := rejectedRowsError(sv2ChannelCheckpointItem.ChannelName, b.table, status); err != nil {
			return err
		}

		if status.CommittedToken == nil {
			if isTarget && sv2ChannelCheckpointItem.Routed == 0 {
				// A declaration nothing was appended under. It stays dormant: the
				// item keeps the declaration durable, and the channel joins the
				// layout when a switch converges to it.
				continue
			}
			if !isTarget && sv2ChannelCheckpointItem.Routed == 0 {
				// A channel of another layout which holds nothing at all. A skewed
				// transaction leaves one behind legitimately — a channel of the
				// shard this one inherits from that took no rows — and so does a
				// declaration the declaring shard never converged to before its
				// range changed. Which of the two it is turns on the channels
				// around it, so the decision waits for all of them.
				candidates = append(candidates, newStreamV2Channel(sv2ChannelCheckpointItem.ChannelName, keyRange, 0, 0))
				continue
			}
			if !isTarget && declared {
				// A routed index with the declaration in place: an interrupted switch
				// already abandoned the channel — the open above re-created it
				// empty — and only the deletion of its item was lost. The deletion
				// is re-recorded, and the sweep drops the empty channel.
				b.abandoned = append(b.abandoned, keyRange)
				log.WithFields(log.Fields{
					"table":   b.table,
					"channel": sv2ChannelCheckpointItem.ChannelName,
					"routed":  sv2ChannelCheckpointItem.Routed,
				}).Info("re-recording the deletion of a channel an interrupted session abandoned")
				continue
			}
			// A routed index with no token and no declaration to explain it falls
			// through to the reconciliation, which rejects it as a lost channel.
		}

		committed, err := reconcileStreamV2Channel(sv2ChannelCheckpointItem.ChannelName, b.table, status.CommittedToken, sv2ChannelCheckpointItem, keyRange, len(b.prior))
		if err != nil {
			return err
		}
		var c = newStreamV2Channel(sv2ChannelCheckpointItem.ChannelName, keyRange, sv2ChannelCheckpointItem.Routed, committed)
		if isTarget {
			liveTarget = append(liveTarget, c)
		} else {
			liveNonTarget = append(liveNonTarget, c)
		}
	}

	// Decide the empty non-target channels. With no live inherited channel, the
	// layout is the target layout and every candidate is an empty declaration or
	// an already-converged-away relic: abandon them. With live inherited channels,
	// the layout is the inherited one, and a candidate is part of it exactly when
	// it overlaps no live channel — the empty quarter a skewed transaction left,
	// which the coverage check below demands. A candidate overlapping a live
	// channel is a declaration at another depth, orphaned when the declaring
	// shard's range changed before it converged.
	for _, c := range candidates {
		var overlapsLive = false
		for _, live := range append(liveNonTarget, liveTarget...) {
			if c.keyRange.keyBegin <= live.keyRange.keyEnd && live.keyRange.keyBegin <= c.keyRange.keyEnd {
				overlapsLive = true
				break
			}
		}
		if len(liveNonTarget) > 0 && !overlapsLive {
			liveNonTarget = append(liveNonTarget, c)
			continue
		}
		b.abandoned = append(b.abandoned, c.keyRange)
		log.WithFields(log.Fields{
			"table":   b.table,
			"channel": c.channelName,
		}).Info("abandoning an empty channel of a layout this shard does not continue")
	}

	// The open-time switch. The declaration is durable — it arrived with the
	// recovered checkpoint — and every inherited channel is settled, so the
	// convergence an interrupted session declared completes here: the
	// inherited channels are abandoned, and the target layout takes over.
	if len(liveNonTarget) > 0 && declared {
		var idle = true
		for _, c := range liveNonTarget {
			if c.progress.routed != c.progress.committed {
				idle = false
				break
			}
		}
		if idle {
			for _, c := range liveNonTarget {
				b.abandoned = append(b.abandoned, c.keyRange)
			}
			liveNonTarget = nil
		}
	}

	// The active layout. Channels inherited from another topology carry rows the
	// replayed transaction must skip, so while any of them stands undropped it is
	// the layout — mixed subdivision depths included, as a join of children that
	// converged unevenly leaves. Otherwise the layout is the target layout.
	var active []*streamV2Channel
	if len(liveNonTarget) > 0 {
		active = append(liveNonTarget, liveTarget...)
	} else {
		for _, keyRange := range targets {
			var have *streamV2Channel
			for _, c := range liveTarget {
				if c.keyRange == keyRange {
					have = c
					break
				}
			}
			if have != nil {
				active = append(active, have)
				continue
			}

			var channelName = streamV2FormatChannelName(m.materialization, b.targetEpoch, keyRange, b.stateKey)
			var status = statuses[channelName]
			if status == nil {
				if status, err = client.OpenChannel(ctx, b.database, b.schema, b.table, channelName); err != nil {
					return fmt.Errorf("opening channel %q: %w", channelName, err)
				}
				if err := rejectedRowsError(channelName, b.table, status); err != nil {
					return err
				}
			}

			var sv2ChannelCheckpointItem = b.prior[keyRange]
			committed, err := reconcileStreamV2Channel(channelName, b.table, status.CommittedToken, sv2ChannelCheckpointItem, keyRange, len(b.prior))
			if err != nil {
				return err
			}
			var routed int64
			if sv2ChannelCheckpointItem != nil {
				routed = sv2ChannelCheckpointItem.Routed
			}
			active = append(active, newStreamV2Channel(channelName, keyRange, routed, committed))
		}
	}

	slices.SortFunc(active, func(a, b *streamV2Channel) int {
		return cmp.Compare(a.keyRange.keyBegin, b.keyRange.keyBegin)
	})

	// The layout must cover the shard's range with no gaps or overlaps, or some
	// documents the runtime delivers have no channel and some key hashes have two.
	// No topology this connector participates in produces such a layout, so
	// reaching here means the checkpoint and the shard ranges disagree about history.
	var ranges = make([]streamV2Range, len(active))
	for i, c := range active {
		ranges[i] = c.keyRange
	}
	if !streamV2LayoutCovers(ranges, shard) {
		var described = make([]string, len(active))
		for i, c := range active {
			described[i] = c.keyRange.String()
		}
		return fmt.Errorf(
			"the channels this task's checkpoint records for this binding cover %s, which does not cover this shard's range %s with no gaps or overlaps: the checkpoint and the shard topology disagree about the ranges that have been appended under. Restore the task's shard key ranges to the topology which appended them, or backfill this binding",
			strings.Join(described, " "), shard,
		)
	}

	b.channels, b.targets, b.opened = active, targets, true

	var layout = make(log.Fields, len(active))
	for _, c := range active {
		layout[c.keyRange.String()] = fmt.Sprintf("routed %d committed %d", c.progress.routed, c.progress.committed)
	}
	log.WithFields(log.Fields{
		"table":      b.table,
		"shardRange": shard.String(),
		"channels":   len(active),
		"layout":     layout,
		"abandoned":  b.abandoned,
	}).Info("opened snowpipe streaming v2 channels")

	// Drop the channels this layout leaves behind before the first append.
	return m.sweep(ctx, b.database, b.schema, b.table, b.stateKey, m.layoutNames(b))
}

// layoutNames is the set of channel names the binding's active and target layouts hold.
func (m *streamV2Manager) layoutNames(b *streamV2Binding) map[string]bool {
	var keep = make(map[string]bool, len(b.channels)+len(b.targets))
	for _, c := range b.channels {
		keep[c.channelName] = true
	}
	for _, keyRange := range b.targets {
		keep[streamV2FormatChannelName(m.materialization, b.targetEpoch, keyRange, b.stateKey)] = true
	}
	return keep
}

// sweep drops the channels this task derived for the binding that it no longer uses:
// the snowpipe_streaming_v2 channels of stateKey not in keep, those of a state key a
// backfill rotated away, and the snowpipe_streaming channel of this shard. Only
// channels within this shard's range are its to drop. Without a listing it does
// nothing.
func (m *streamV2Manager) sweep(ctx context.Context, database, schema, table, stateKey string, keep map[string]bool) error {
	if m.listChannels == nil {
		return nil
	}
	names, err := m.listChannels(ctx, database, schema, unquotedIdentifier(table))
	if err != nil {
		return err
	}

	var shard = m.shardRange()
	var own, ownStateKey = sanitizeAndAppendHash(m.materialization), sanitizeAndAppendHash(stateKey)
	var client *sidecarClient
	for _, channelName := range names {
		if keep[channelName] {
			continue
		}
		if keyBegin, ok := streamingChannelKeyBegin(channelName, m.materialization); ok {
			if keyBegin < shard.keyBegin || keyBegin > shard.keyEnd || m.dropStreamingChannel == nil {
				continue
			}
			if err := m.dropStreamingChannel(ctx, schema, table, channelName); err != nil {
				return err
			}
			continue
		}
		parts, ok := streamV2SplitChannelName(channelName)
		if !ok || parts.materialization != own {
			continue
		}
		if parts.stateKey == ownStateKey {
			// A channel of the live binding is this shard's to drop only when its key
			// range nests in the shard's.
			if parts.keyRange.keyBegin < shard.keyBegin || parts.keyRange.keyEnd > shard.keyEnd {
				continue
			}
		} else if parts.keyRange.keyBegin < shard.keyBegin || parts.keyRange.keyBegin > shard.keyEnd {
			// A backfill rotates a binding's state key and never restores one, so a
			// channel under any other state key belongs to a binding that no longer
			// exists. The shard whose range holds its key-begin drops it.
			continue
		}

		if client == nil {
			if client, err = m.ensureStarted(ctx); err != nil {
				return err
			}
		}

		// The drop needs an open handle. A channel this session opened — one a
		// rebalance just abandoned — already has one, and re-opening it is an error,
		// so the drop is tried first and a channel this session never opened, a
		// prior session's orphan, is opened only when the drop reports none.
		var err = dropChannel(ctx, client, channelName)
		if unknownChannel(err) {
			if _, err = client.OpenChannel(ctx, database, schema, table, channelName); err != nil {
				return fmt.Errorf("opening channel %q to drop it: %w", channelName, err)
			}
			err = dropChannel(ctx, client, channelName)
		}
		if err != nil {
			return err
		}
		log.WithFields(log.Fields{"table": table, "channel": channelName}).Info("swept a snowpipe streaming v2 channel a layout left behind")
	}
	return nil
}

// streamingChannelKeyBegin reads the key-begin of a snowpipe_streaming channel this
// task derived. Snowflake lists those names upper-cased, so the match ignores case.
func streamingChannelKeyBegin(channelName, materialization string) (uint32, bool) {
	var prefix = sanitizeAndAppendHash(materialization) + "_"
	if len(channelName) != len(prefix)+8 || !strings.EqualFold(channelName[:len(prefix)], prefix) {
		return 0, false
	}
	keyBegin, err := strconv.ParseUint(channelName[len(prefix):], 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(keyBegin), true
}

// rejectedRowsError rejects a channel when Snowflake rejected any row on it. It
// reports nil in every other case.
//
// Snowflake discards a row it rejects, and fails neither the append nor the commit. It
// also advances the committed offset token of the channel past that row. This count is
// therefore the only place where the loss is visible.
//
// The count covers the whole life of the channel and never resets. This is why one
// rejected row fails the binding for good. Nothing can identify the discarded rows, so
// there is nothing to re-send. No state can tell the connector that the destination is
// whole again. Only a backfill clears the count, because it rotates the channels and
// the checkpoint of the binding together.
func rejectedRowsError(channelName, table string, status *channelStatusResult) error {
	if status.RowsErrorCount == 0 {
		return nil
	}
	return fmt.Errorf(
		"channel %q had %d row(s) rejected and discarded by Snowflake, which reported: %s. Those rows are not in %s, and Snowflake's committed offset token has advanced past them, so they cannot be identified and re-sent. Backfill this binding",
		channelName, status.RowsErrorCount, status.LastErrorMessage, table,
	)
}

// reconcileStreamV2Channel compares Snowflake's committed offset token for one
// channel against the routed index that the driver checkpoint recorded for it. It reports
// the highest document index that Snowflake already holds. A replay must not append a
// document at or below that threshold again.
//
// sv2ChannelCheckpointItem is the checkpoint item of this channel, or nil when the
// checkpoint holds none. priorItems counts every item the checkpoint holds for the
// binding, across all of the task's channels; it is what tells a channel's interrupted
// first transaction apart from a token nothing accounts for.
func reconcileStreamV2Channel(channelName, table string, committedToken *string, sv2ChannelCheckpointItem *streamV2ChannelCheckpointItem, keyRange streamV2Range, priorItems int) (int64, error) {
	var routed int64
	if sv2ChannelCheckpointItem != nil {
		routed = sv2ChannelCheckpointItem.Routed
	}

	// Snowflake holds nothing for this channel. With nothing routed to account for,
	// there is nothing to skip. With routed documents, the channel that held them is
	// gone, and the token that said which of them Snowflake holds went with it.
	//
	// The one drop this connector performs itself — a rebalance abandoning a channel
	// it converges away from — is recognized before reconciliation reaches here, by
	// the declaration its checkpoint carries. A missing token here has no such
	// record, so it names an account Snowflake itself has lost.
	if committedToken == nil {
		if routed > 0 {
			return 0, fmt.Errorf(
				"channel %q has committed nothing while this task's checkpoint records %d documents appended to it: Snowflake has lost this channel's committed offset token, so this shard cannot identify which of its documents Snowflake still holds. Backfill this binding",
				channelName, routed,
			)
		}
		return 0, nil
	}

	committed, appendedBy, ok := parseStreamV2Token(*committedToken)
	if !ok {
		return 0, fmt.Errorf(
			"channel %q reports the committed offset token %q, which is not a document count: this channel was written by something other than this connector's snowpipe_streaming_v2 write path, and continuing could duplicate or drop rows",
			channelName, *committedToken,
		)
	}

	// The token's range must be the channel's own key range, always. The key range is
	// in the channel's name, so every append this write path makes carries it. A
	// token naming any other range was written under a channel scheme this write
	// path never ran, and nothing it counts may be skipped.
	if appendedBy != keyRange {
		return 0, fmt.Errorf(
			"channel %q covers %s but reports a committed offset token for %d documents appended under %s: that token was not written against this channel's key range, so the documents it counts cannot be identified. Backfill this binding",
			channelName, keyRange, committed, appendedBy,
		)
	}

	if committed < routed {
		return 0, fmt.Errorf(
			"channel %q has committed %d documents but this task's checkpoint records %d as appended: the channel has lost committed data, so the missing rows cannot be identified. Backfill this binding",
			channelName, committed, routed,
		)
	}

	if sv2ChannelCheckpointItem == nil && committed > 0 && priorItems > 0 {
		// A token with no item to account for can be the interrupted first
		// transaction of this channel. That reading holds only when the checkpoint
		// holds nothing for the binding at all — a backfill produces that state, the
		// state key rotating channels and items together.
		//
		// When the checkpoint holds anything, this state should not exist: flush
		// writes an item for every active channel of the binding, and it declares
		// the target layout's items before any switch routes rows to those
		// channels. A token no item accounts for is therefore something else
		// appending under this binding's names, and its documents must not be
		// skipped, or that many of the documents about to be materialized into the
		// table are dropped instead.
		return 0, fmt.Errorf(
			"channel %q has committed %d documents this task's checkpoint cannot account for: it holds no item for this channel, only items its other channels wrote, and this write path records an item for every channel before appending to it. The documents that token counts must not be skipped, or that many of the documents about to be materialized into %s are dropped instead. Backfill this binding",
			channelName, committed, table,
		)
	}

	// committed == routed is the clean boundary, and committed > routed is an
	// interrupted attempt of the transaction now replayed. The replay produces the
	// same documents in the same order, and the routing hash is a function of each
	// document alone, so this channel receives exactly the documents the token
	// counts, and skips them by position.
	return committed, nil
}

// dropChannel takes a channel out of service for good. It drops the channel in
// Snowflake, and does not only close the local handle.
//
// The drop must reach Snowflake because a channel name is deterministic. The
// connector derives it from the task, the key range, and the state key. A later shard
// whose layout includes the same key range derives the same name.
//
// A channel that Snowflake still holds gives its committed offset token to whoever
// opens it next. The drop is what makes the next open of the same name a fresh
// channel, with nothing to account for.
//
// An experiment against live Snowflake showed that the drop does this and a plain
// close does not. The SDK contract does not say so. A close releases only the local
// handle, and Snowflake still reports the same committed offset token to the next open
// of that name. The channel-drop subtest of TestStreamV2Manager is that experiment.
// TestStreamV2DropChannel runs the same sequence against the fake sidecar.
//
// The channel must already be open in this session, because the drop uses the handle
// that an open produced. The committed rows are untouched. This drops the channel,
// not the data it delivered.
func dropChannel(ctx context.Context, client *sidecarClient, channelName string) error {
	if err := client.CloseChannel(ctx, channelName, true); err != nil {
		return fmt.Errorf("dropping channel %q: %w", channelName, err)
	}
	log.WithFields(log.Fields{"channel": channelName}).Info("dropped snowpipe streaming v2 channel")
	return nil
}

// unknownChannel reports whether err is the sidecar's rejection of an operation on
// a channel it holds no open handle for — because it was never opened this session,
// or was closed since.
func unknownChannel(err error) bool {
	var se *sidecarError
	return errors.As(err, &se) && se.Code == "unknown_channel"
}

// streamV2ChannelNames lists, sorted, the channels the items of a binding name. A
// nil item is a channel the task already dropped and names nothing.
func streamV2ChannelNames(sv2Checkpoint streamV2Checkpoint) []string {
	var channelNames []string
	for _, sv2ChannelCheckpointItem := range sv2Checkpoint {
		if sv2ChannelCheckpointItem != nil {
			channelNames = append(channelNames, sv2ChannelCheckpointItem.ChannelName)
		}
	}
	slices.Sort(channelNames)
	return channelNames
}

// streamV2PathOrphaned rejects a binding that materialized through this write path
// and no longer does, unless the departure is the one exit this connector supports:
// the escape-hatch downgrade to the snowpipe_streaming path, which streamV2Downgrade
// recognizes and sweep carries out. The error names the channels that
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
// this path derives those same names. It meets a token that no item accounts for,
// and reads it as an interrupted first transaction of a fresh channel. Only a backfill
// otherwise leaves a checkpoint with nothing for the binding. The return then skips
// that many of the documents it was about to materialize.
//
// A departure while rows are pending costs more than the return. The committed offset
// token alone skips the documents of an interrupted transaction, and no other path
// reads that token. The runtime is about to replay those documents to this shard, and
// another path materializes them a second time. This path serves delta-updates
// bindings, so those duplicates are permanent.
//
// A backfill is the other way off this path. It rotates the state key of the binding,
// which rotates both the channels and the checkpoint item. Nothing is then left for
// another path to discard, and no channel is left for a later session to find.
func streamV2PathOrphaned(table string, prior streamV2Checkpoint) error {
	var channelNames = streamV2ChannelNames(prior)
	if len(channelNames) == 0 {
		return nil
	}

	return fmt.Errorf(
		"this binding has materialized into %s through the snowpipe_streaming_v2 write path, which this task's specification no longer selects for it, while the task's checkpoint still records the channel(s) %s it appended to. Those items are the only account of which documents Snowflake's channels already hold, no other write path maintains them, and the first transaction on another path discards them — after which returning to this write path would skip that many of the documents it materializes. Restore this binding to the snowpipe_streaming_v2 write path — it needs the feature flag, delta updates, and key-pair authentication — or backfill it, which rotates its channels and its checkpoint together",
		table, strings.Join(channelNames, ", "),
	)
}

// streamV2DowngradeWarning reports what a publication moving a binding from the
// snowpipe streaming v2 write path onto the snowpipe streaming path costs it, and
// "" when the binding names no channel to leave behind.
func streamV2DowngradeWarning(table string, sv2Checkpoint streamV2Checkpoint) string {
	var channelNames = streamV2ChannelNames(sv2Checkpoint)
	if len(channelNames) == 0 {
		return ""
	}

	return fmt.Sprintf(
		"binding %s is leaving the snowpipe_streaming_v2 write path for snowpipe_streaming. Every document that its channel(s) %s committed beyond the index the checkpoint records for them will be materialized again by the snowpipe_streaming path, and this binding uses delta updates, so those duplicates are permanent. The channels are dropped when the task next opens on the new path",
		table, strings.Join(channelNames, ", "),
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
	// nothing downstream of that disagreement can be trusted — reject rather than
	// misfile a single row.
	var hash = packedKeyHashHH64(packedKey)
	var c = b.route(hash)
	if c == nil {
		return fmt.Errorf(
			"the key hash %08x of a document stored to %s is covered by no channel of this shard, which covers %s: the runtime and this connector disagree about document routing",
			hash, b.table, m.shardRange(),
		)
	}

	c.progress.routed++
	var index = c.progress.routed
	if index <= c.progress.committed {
		return nil
	}

	var grew, err = c.bufferRow(index, b.columnNames, converted)
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

// bufferRow writes the document at index into the payload of the current
// batch. It starts that batch when this is the first row. It reports how many bytes the
// payload grew by.
//
// A row that the writer rejects leaves the payload with exactly the rows batched before
// it. The append still carries those rows, under the offset token of whichever
// transaction commits them.
//
// This is the whole of the encoding this path does. appendRowJSON copies through the
// values that reach it already encoded, and nothing rescans the row after it. The bytes
// written here are the bytes the append carries.
func (c *streamV2Channel) bufferRow(index int64, columnNames []columnName, converted []any) (int, error) {
	var before = len(c.buf)
	if c.bufRows == 0 {
		c.startBatch()
		c.bufFirst = index
	} else {
		c.buf = append(c.buf, ',')
	}

	var payload, err = appendRowJSON(c.buf, columnNames, converted)
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
	var first, last = c.bufFirst, c.progress.routed
	payload, rows, released := c.finishBatch()
	m.bufBytes -= released

	// Back-pressure is implicit. A saturated pipe blocks the append. The blocked append
	// blocks the next submit, and that blocks Store.
	var err = c.pipe.submit(func() error {
		if err := c.limiter.WaitN(ctx, len(payload)); err != nil {
			return fmt.Errorf("pacing channel %q: %w", c.channelName, err)
		}
		client, err := m.ensureStarted(ctx)
		if err != nil {
			return err
		}
		return client.Append(ctx, c.channelName, c.offsetToken(first), c.offsetToken(last), payload, rows)
	})
	if err != nil {
		return fmt.Errorf("appending to channel %q: %w", c.channelName, err)
	}
	return nil
}

// flush appends the remaining buffered documents of each channel. It waits for every
// pending append, and then waits for Snowflake to durably commit them. It returns
// the checkpoint entries of each binding that did anything this transaction, and it
// reports them only after Snowflake holds the documents their items account for.
//
// The entries hold an item for every active channel, advanced or not. This is the
// declaration invariant: every channel rows can route to has an item in the
// checkpoint no later than the transaction that first routes to it, and the target
// layout a rebalance converges to is declared — written as items with nothing routed — at
// least one durable checkpoint before the switch routes anything there. A token
// with no item to account for therefore never has a benign reading.
//
// flush awaits the commit here, as part of the checkpoint it produces, and not at
// Acknowledge. The checkpoint of the runtime is durable before Acknowledge runs. A
// commit that failed there could no longer fail the transaction whose item it
// belongs to. The task resumes from an index Snowflake never committed, and the next
// Open can only reject it. No replay re-appends rows the runtime already considers
// delivered.
//
// A failure here instead leaves the runtime to replay the transaction, and the
// reconciliation of that Open skips the documents Snowflake did commit.
func (m *streamV2Manager) flush(ctx context.Context) (map[int]streamV2Checkpoint, error) {
	var entries = make(map[int]streamV2Checkpoint)
	type commitWait struct {
		b     *streamV2Binding
		c     *streamV2Channel
		index int64
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
				return nil, fmt.Errorf("appending to channel %q: %w", c.channelName, err)
			}

			// A replayed transaction that routed fewer documents to this channel
			// than the interrupted attempt committed was not replayed identically.
			// Store then withheld documents that Snowflake does not hold. Only the
			// first transaction of the session can trip this, because the routed
			// index only grows from there.
			if c.progress.routed < c.progress.committed {
				return nil, fmt.Errorf(
					"channel %q holds %d committed documents but the transaction replayed against it produced only %d: it was not replayed identically, so the rows Snowflake already holds cannot be identified. Backfill this binding",
					c.channelName, c.progress.committed, c.progress.routed,
				)
			}

			if c.progress.routed != c.progress.checkpointed {
				advanced = true
			}
		}

		var declaring = !b.isTargetLayout()
		if !advanced && !declaring && len(b.abandoned) == 0 {
			continue // nothing stored, nothing abandoned, nothing to converge
		}

		var sv2Checkpoint = make(streamV2Checkpoint)
		for _, c := range b.channels {
			c.progress.checkpointed = c.progress.routed
			sv2Checkpoint[c.keyRange] = &streamV2ChannelCheckpointItem{
				ChannelName: c.channelName,
				Routed:      c.progress.routed,
			}
			if c.progress.routed > c.progress.committed {
				waits = append(waits, commitWait{b: b, c: c, index: c.progress.routed})
			}
		}

		if declaring {
			// The declaration: an item for every target channel the layout has not
			// converged to yet. The switch at Acknowledge runs only after the
			// checkpoint carrying these is durable.
			for _, keyRange := range b.targets {
				if _, ok := sv2Checkpoint[keyRange]; !ok {
					var channelName = streamV2FormatChannelName(m.materialization, b.targetEpoch, keyRange, b.stateKey)
					sv2Checkpoint[keyRange] = &streamV2ChannelCheckpointItem{ChannelName: channelName}
				}
			}
			b.declared = true
		}

		// Deletions of abandoned channels ride every checkpoint of the session, not
		// only the first, so that a transaction that never commits does not lose
		// them.
		for _, key := range b.abandoned {
			sv2Checkpoint[key] = nil
		}

		entries[idx] = sv2Checkpoint
	}

	// Channels commit independently, and the sidecar serializes its operations per
	// channel and not globally. A wide materialization therefore does not pay the
	// commit latency of each channel in series. The limit scales with the channels
	// per shard so that a binding's channels do not queue behind each other.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(MaxConcurrentQueries * streamV2ChannelsPerShard)
	for _, w := range waits {
		group.Go(func() error { return m.waitCommit(groupCtx, w.b, w.c, w.index) })
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	return entries, nil
}

// acknowledged runs after the runtime durably committed the transaction whose
// checkpoint the last flush produced. It is where a rebalance switches: the
// declaration that flush wrote is durable now, every channel is settled, and the
// boilerplate serializes this call before the next Store, so no row
// routes anywhere while the layout changes.
//
// The switch abandons every non-target channel of the layout — recording each
// deletion — opens the target channels in their place, and sweeps the abandoned
// channels off the pipe. A crash between the switch and the durability of the
// deletions restarts to a checkpoint that still holds the abandoned channels' items
// alongside the declaration, which is exactly the state ensureOpened re-enters the
// switch from.
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

		// The declaration is only ever written by a flush that committed every
		// channel, and nothing stores between that flush and this call. A channel
		// found uncommitted here means that reasoning is broken somewhere, and a
		// deferred convergence costs only throughput — so defer, loudly.
		var idle = true
		for _, c := range b.channels {
			if c.progress.routed != c.progress.committed || c.bufRows > 0 {
				idle = false
				break
			}
		}
		if !idle {
			log.WithFields(log.Fields{"table": b.table}).Warn(
				"snowpipe streaming v2: skipping a channel rebalance because a channel is not committed; this should be impossible")
			continue
		}

		client, err := m.ensureStarted(ctx)
		if err != nil {
			return err
		}

		var next []*streamV2Channel
		for _, c := range b.channels {
			if slices.Contains(b.targets, c.keyRange) {
				next = append(next, c)
				continue
			}
			b.abandoned = append(b.abandoned, c.keyRange)
		}

		for _, keyRange := range b.targets {
			if slices.ContainsFunc(next, func(c *streamV2Channel) bool { return c.keyRange == keyRange }) {
				continue
			}
			var channelName = streamV2FormatChannelName(m.materialization, b.targetEpoch, keyRange, b.stateKey)
			status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, channelName)
			if err != nil {
				return fmt.Errorf("opening channel %q: %w", channelName, err)
			}
			if err := rejectedRowsError(channelName, b.table, status); err != nil {
				return err
			}
			// The channel was declared this session and nothing has routed to it
			// yet, so it must be empty. A token here is something else appending
			// under this binding's names.
			if status.CommittedToken != nil {
				return fmt.Errorf(
					"channel %q was declared by this shard and should hold nothing, but reports the committed offset token %q: something else is appending under this binding's channel names, and continuing could duplicate or drop rows. Backfill this binding",
					channelName, *status.CommittedToken,
				)
			}
			next = append(next, newStreamV2Channel(channelName, keyRange, 0, 0))
		}

		slices.SortFunc(next, func(a, b *streamV2Channel) int {
			return cmp.Compare(a.keyRange.keyBegin, b.keyRange.keyBegin)
		})
		b.channels = next

		log.WithFields(log.Fields{
			"table":     b.table,
			"channels":  len(next),
			"abandoned": b.abandoned,
		}).Info("snowpipe streaming v2: converged the channel layout to the target layout")

		// The inherited channels are out of the layout now; drop them from the pipe.
		if err := m.sweep(ctx, b.database, b.schema, b.table, b.stateKey, m.layoutNames(b)); err != nil {
			return err
		}
	}
	return nil
}

// waitCommit blocks until the channel durably holds every document through index. If
// Snowflake rejected any of its rows, waitCommit fails the transaction.
func (m *streamV2Manager) waitCommit(ctx context.Context, b *streamV2Binding, c *streamV2Channel, index int64) error {
	// A replay of documents Snowflake already holds appends nothing. Its index is
	// already committed, so waitCommit must not wait on it.
	if index <= c.progress.committed {
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
		"channel": c.channelName,
		"index":   index,
	}).Info("snowpipe streaming v2: awaiting commit")

	var started = time.Now()
	var token = c.offsetToken(index)
	status, err := client.WaitCommit(ctx, c.channelName, token)
	if err != nil {
		return fmt.Errorf("waiting for commit of document %s on channel %q: %w", token, c.channelName, err)
	}

	// The commit of the token is the earliest point where this transaction can see its
	// own rejections. That point is before flush checkpoints its index. A rejection
	// therefore fails the transaction that produced it, and not a later one.
	if err := rejectedRowsError(c.channelName, b.table, status); err != nil {
		return err
	}
	c.progress.committed = index

	log.WithFields(log.Fields{
		"table":   b.table,
		"channel": c.channelName,
		"index":   index,
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
// Snowflake stores, which is the name each row's JSON entry is keyed by.
func unquotedIdentifier(ident string) string {
	if strings.HasPrefix(ident, `"`) && strings.HasSuffix(ident, `"`) && len(ident) >= 2 {
		return strings.ReplaceAll(ident[1:len(ident)-1], `""`, `"`)
	}
	return ident
}
