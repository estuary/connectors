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
// Recovery resumes from the offset that Snowflake reports as committed,
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
// offset of the last document routed to it as of the checkpoint.
type streamV2ChannelCheckpointItem struct {
	// ChannelName is the name of the Snowpipe channel.
	ChannelName string
	// Routed is the offset of the last document routed to the channel as of the
	// checkpoint. The channel's first document has offset 1, so zero means nothing
	// was routed.
	Routed int64
}

// streamV2Checkpoint is the streaming v2 state of all channels of a binding.
type streamV2Checkpoint map[streamV2Range]*streamV2ChannelCheckpointItem

// channelNames lists, sorted, the channels the non-nil checkpoint items name.
func (cp streamV2Checkpoint) channelNames() []string {
	var channelNames []string
	for _, sv2ChannelCheckpointItem := range cp {
		if sv2ChannelCheckpointItem != nil {
			channelNames = append(channelNames, sv2ChannelCheckpointItem.ChannelName)
		}
	}
	slices.Sort(channelNames)
	return channelNames
}

// validateNotOrphaned fails a binding that leaves this write path while the
// checkpoint still records channels for it.
func (cp streamV2Checkpoint) validateNotOrphaned(table string) error {
	var channelNames = cp.channelNames()
	if len(channelNames) == 0 {
		return nil
	}

	return fmt.Errorf(
		"this binding materialized into %s through the snowpipe_streaming_v2 write path, which this task's specification no longer selects for it, and the task's checkpoint still records its channel(s) %s. Leaving this path discards that record, and returning later would skip the documents those channels already hold. Restore the snowpipe_streaming_v2 write path — it needs the feature flag, delta updates, and key-pair authentication — or backfill this binding",
		table, strings.Join(channelNames, ", "),
	)
}

type streamV2Range struct {
	keyBegin, keyEnd uint32
}

func (r streamV2Range) contains(keyHash uint32) bool {
	return r.keyBegin <= keyHash && keyHash <= r.keyEnd
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

// streamV2FormatOffsetToken renders the offset token of an append. It holds two
// facts: the offset of the last document in the append, and the channel key range
// it was appended under.
//
// The key range is in the offset token so that it can be recognized as this
// channel's own. An offset token whose range is not the channel's key range was
// written by something else — another connector, or a channel scheme this write
// path never ran — and nothing up to its offset may be skipped.
func streamV2FormatOffsetToken(offset int64, keyRange streamV2Range) string {
	var spec, _ = keyRange.MarshalText()
	return fmt.Sprintf("%d@%s", offset, spec)
}

// streamV2ParseOffsetToken reads a committed offset token back as two values: the
// offset it carries, and the key range its documents were appended under. It returns
// false for an offset token that this write path never writes.
func streamV2ParseOffsetToken(token string) (int64, streamV2Range, bool) {
	var offsetText, spec, hasRange = strings.Cut(token, "@")
	if !hasRange {
		return 0, streamV2Range{}, false
	}

	offset, err := strconv.ParseInt(offsetText, 10, 64)
	if err != nil {
		return 0, streamV2Range{}, false
	}

	var keyRange streamV2Range
	if err := keyRange.UnmarshalText([]byte(spec)); err != nil {
		return 0, streamV2Range{}, false
	}
	return offset, keyRange, true
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

	// progress tracks the channel's offsets. A document's offset is its 1-based
	// position among the documents the shard has routed to the channel, over the
	// whole life of the channel, so the offset of a channel is that of its last
	// document, and zero means none. Offsets never reset.
	//
	// committed exceeds routed only while a replayed transaction routes documents
	// that an interrupted attempt already committed.
	progress struct {
		// routed leads progress. It is the offset of the most recent document that
		// the shard routed to this channel.
		routed int64
		// checkpointed is the offset that the last checkpoint item carried.
		checkpointed int64
		// committed is the highest offset that Snowflake durably holds.
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

// offsetToken renders the offset token for an offset on this channel.
func (c *streamV2Channel) offsetToken(offset int64) string {
	return streamV2FormatOffsetToken(offset, c.keyRange)
}

// bufferRow encodes the document at offset onto the payload of the channel's current
// batch. When the payload is empty it starts the batch and records offset as the
// batch's first. It reports how many bytes the payload grew by.
//
// A document that fails to encode leaves the payload exactly as it was, so the batch
// keeps the rows before it and none of the failed row.
func (c *streamV2Channel) bufferRow(offset int64, columnNames []columnName, converted []any) (int, error) {
	var before = len(c.buf)
	if c.bufRows == 0 {
		c.startBatch()
		c.bufFirst = offset
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

type streamV2Binding struct {
	database    string
	schema      string
	table       string
	stateKey    string
	columnNames []columnName

	// priorCheckpoint is the streaming v2 state that the driver checkpoint recorded for this
	// binding. The map key is the channel's key range, and the map covers every shard
	// of the task. It is nil when the checkpoint recorded no state.
	priorCheckpoint streamV2Checkpoint
	// targetRanges is the target layout (the channel key ranges we want rows to route to)
	// in order by key range. See streamV2TargetLayout.
	targetRanges []streamV2Range
	// abandonedRanges holds the key ranges of the channels this shard dropped from its
	// layout. Their checkpoint items are written as null, because the checkpoint is
	// a JSON merge patch and an item that is merely omitted survives in the stored state.
	abandonedRanges []streamV2Range
	// activeChannels is the active layout (the channels that rows route to) in order by
	// key range. The layout covers the shard's key range with no gaps or overlaps,
	// so every document the runtime delivers routes to exactly one channel.
	activeChannels []*streamV2Channel

	// opened reports whether ensureOpened has been called successfully.
	opened bool
	// targetEpoch is the epoch in the target layout's channel names.
	targetEpoch int
	// targetRangesDeclared reports that the last flush wrote a checkpoint item for every
	// target range.
	targetRangesDeclared bool
}

// isTargetLayout reports whether the active layout is the target layout.
func (b *streamV2Binding) isTargetLayout() bool {
	if len(b.activeChannels) != len(b.targetRanges) {
		return false
	}
	for i, c := range b.activeChannels {
		if c.keyRange != b.targetRanges[i] {
			return false
		}
	}
	return true
}

// route reports the active channel that covers a key hash, or nil when none does.
func (b *streamV2Binding) route(keyHash uint32) *streamV2Channel {
	for _, c := range b.activeChannels {
		if c.keyRange.contains(keyHash) {
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

// streamV2Manager is the Snowpipe Streaming V2 write path. It owns the channels of
// every binding and the sidecar process that holds the Snowflake SDK.
//
// A row's channel is chosen by the same key hash that chose the row's shard, so a
// channel's contents depend only on the data and never on the shard topology.
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

	mu     sync.Mutex // guards sup and client
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

// parseChannelName reads the epoch and key range of a channel name this task
// derived for stateKey, and reports whether the name is such a channel.
func (m *streamV2Manager) parseChannelName(channelName, stateKey string) (int, streamV2Range, bool) {
	var parts, ok = streamV2SplitChannelName(channelName)
	if !ok || parts.materialization != sanitizeAndAppendHash(m.materialization) || parts.stateKey != sanitizeAndAppendHash(stateKey) {
		return 0, streamV2Range{}, false
	}
	return parts.epoch, parts.keyRange, true
}

// streamingChannelKeyBegin reads the key-begin of a snowpipe_streaming channel this
// task derived. Snowflake lists those names upper-cased, so the match ignores case.
func (m *streamV2Manager) streamingChannelKeyBegin(channelName string) (uint32, bool) {
	var prefix = sanitizeAndAppendHash(m.materialization) + "_"
	if len(channelName) != len(prefix)+8 || !strings.EqualFold(channelName[:len(prefix)], prefix) {
		return 0, false
	}
	keyBegin, err := strconv.ParseUint(channelName[len(prefix):], 16, 32)
	if err != nil {
		return 0, false
	}
	return uint32(keyBegin), true
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
		database:        database,
		schema:          schema,
		table:           table,
		stateKey:        target.StateKey,
		columnNames:     columnNamesOf(names),
		priorCheckpoint: prior,
	}
}

// ensureOpened opens the binding's channels on its first document. It builds the
// active layout, reconciles each channel against Snowflake, and captures each
// committed offset before anything is appended.
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
		var foreignChannelNames []string
		for _, channelName := range channelNames {
			if parts, ok := streamV2SplitChannelName(channelName); !ok {
				log.WithFields(log.Fields{"table": b.table, "channel": channelName}).Info("a channel of no snowpipe streaming v2 shape stands on the table")
				continue
			} else if parts.materialization != own {
				foreignChannelNames = append(foreignChannelNames, channelName)
			}
		}
		if len(foreignChannelNames) > 0 {
			slices.Sort(foreignChannelNames)
			return fmt.Errorf(
				"table %s already receives snowpipe streaming v2 rows from another Flow task through channel(s) %s. Two tasks may not stream into one table. If that task still exists, materialize this binding into a table of its own, or remove this table from that task. If that task was deleted or renamed, its channels have outlived it: backfill this binding with the always_drop_tables_on_backfill feature flag set, which drops the table and every channel standing on it",
				b.table, strings.Join(foreignChannelNames, ", "),
			)
		}
	}

	client, err := m.ensureStarted(ctx)
	if err != nil {
		return err
	}

	var shardKeyRange = m.shardRange()
	targetChannelKeyRanges, err := streamV2TargetLayout(m.keyBegin, m.keyEnd)
	if err != nil {
		return err
	}

	// A nil checkpoint item is a channel this binding abandoned. Drop them, so that
	// the loops below need no nil checks and len(b.priorCheckpoint) counts only live
	// channels.
	var priorCheckpoint = make(streamV2Checkpoint, len(b.priorCheckpoint))
	for keyRange, sv2ChannelCheckpointItem := range b.priorCheckpoint {
		if sv2ChannelCheckpointItem != nil {
			priorCheckpoint[keyRange] = sv2ChannelCheckpointItem
		}
	}
	b.priorCheckpoint = priorCheckpoint

	// The target epoch. A target range that already has a checkpoint item sets it,
	// so a restart continues the channels it already ran. Otherwise it is one past
	// the highest epoch among the channels this shard owns, so the new names
	// collide with no channel a sweep has yet to drop.
	b.targetEpoch = -1
	for _, keyRange := range targetChannelKeyRanges {
		if sv2ChannelCheckpointItem := b.priorCheckpoint[keyRange]; sv2ChannelCheckpointItem != nil {
			if epoch, _, ok := m.parseChannelName(sv2ChannelCheckpointItem.ChannelName, b.stateKey); ok {
				b.targetEpoch = epoch
				break
			}
		}
	}
	if b.targetEpoch < 0 {
		var maxEpoch = -1
		for _, sv2ChannelCheckpointItem := range b.priorCheckpoint {
			if epoch, keyRange, ok := m.parseChannelName(sv2ChannelCheckpointItem.ChannelName, b.stateKey); ok &&
				keyRange.keyBegin >= shardKeyRange.keyBegin && keyRange.keyEnd <= shardKeyRange.keyEnd {
				maxEpoch = max(maxEpoch, epoch)
			}
		}
		b.targetEpoch = maxEpoch + 1
	}

	// Bucket the prior checkpoint's channels by where each one's key range falls
	// relative to this shard. Owned channels, those whose range lies inside this
	// shard's, are this shard's to open. A sibling's are left alone. A straddling
	// range is rejected, because the rows that channel holds belong to both sides
	// of the boundary.
	var ownedKeyRanges []streamV2Range
	var targetKeyRangeCount int
	for keyRange, sv2ChannelCheckpointItem := range b.priorCheckpoint {
		switch classifyKeyRange(keyRange, shardKeyRange, targetChannelKeyRanges) {
		case streamV2KeyRangeTarget:
			targetKeyRangeCount++
			ownedKeyRanges = append(ownedKeyRanges, keyRange)
		case streamV2KeyRangeInherited:
			ownedKeyRanges = append(ownedKeyRanges, keyRange)
		case streamV2KeyRangeSibling:
		case streamV2KeyRangeStraddling:
			return fmt.Errorf(
				"channel %q covers %s, which crosses the boundary of this shard's range %s: the shard was split off a boundary its channels do not subdivide along, so the rows that channel holds cannot be attributed to either side. Restore the task's shard key ranges to the topology which appended them, or backfill this binding",
				sv2ChannelCheckpointItem.ChannelName, keyRange, shardKeyRange,
			)
		}
	}
	slices.SortFunc(ownedKeyRanges, func(a, b streamV2Range) int {
		return cmp.Or(cmp.Compare(a.keyBegin, b.keyBegin), cmp.Compare(a.keyEnd, b.keyEnd))
	})

	// priorDeclaresTargets reports that the prior checkpoint holds an item for every
	// target range. The declaration is written before any switch, so when it holds,
	// every non-target channel is one a switch was about to abandon.
	var priorDeclaresTargets = targetKeyRangeCount == len(targetChannelKeyRanges)

	// Open every owned channel and reconcile it. Each becomes a live channel, a
	// range to abandon, or a candidate, and an empty target channel is left for a
	// later switch. A candidate is an empty non-target channel. The next loop
	// decides those, because whether one is inherited or orphaned depends on what
	// the other channels hold.
	var liveNonTargetChannels, liveTargetChannels, candidateChannels []*streamV2Channel
	var channelStatusByName = make(map[string]*channelStatusResult)
	for _, keyRange := range ownedKeyRanges {
		var sv2ChannelCheckpointItem = b.priorCheckpoint[keyRange]
		var isTarget = classifyKeyRange(keyRange, shardKeyRange, targetChannelKeyRanges) == streamV2KeyRangeTarget

		status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, sv2ChannelCheckpointItem.ChannelName)
		if err != nil {
			return fmt.Errorf("opening channel %q: %w", sv2ChannelCheckpointItem.ChannelName, err)
		}
		channelStatusByName[sv2ChannelCheckpointItem.ChannelName] = status
		if err := status.validateRejectedRows(sv2ChannelCheckpointItem.ChannelName, b.table); err != nil {
			return err
		}

		if status.CommittedToken == nil {
			if isTarget && sv2ChannelCheckpointItem.Routed == 0 {
				// A declared target channel that took no rows has nothing to reconcile yet.
				continue
			}
			if !isTarget && sv2ChannelCheckpointItem.Routed == 0 {
				// An empty channel of another layout may be inherited or orphaned, and only
				// the channels around it can tell, so the decision waits for all of them.
				candidateChannels = append(candidateChannels, newStreamV2Channel(sv2ChannelCheckpointItem.ChannelName, keyRange, 0, 0))
				continue
			}
			if !isTarget && priorDeclaresTargets {
				// A non-target channel Snowflake no longer holds, under a declared target
				// layout, was abandoned by a switch whose checkpoint deletion never landed.
				b.abandonedRanges = append(b.abandonedRanges, keyRange)
				log.WithFields(log.Fields{
					"table":   b.table,
					"channel": sv2ChannelCheckpointItem.ChannelName,
					"routed":  sv2ChannelCheckpointItem.Routed,
				}).Info("re-recording the deletion of a channel an interrupted session abandoned")
				continue
			}
		}

		committedOffset, err := streamV2ValidateCommittedToken(sv2ChannelCheckpointItem.ChannelName, b.table, status.CommittedToken, sv2ChannelCheckpointItem, keyRange, len(b.priorCheckpoint))
		if err != nil {
			return err
		}
		var c = newStreamV2Channel(sv2ChannelCheckpointItem.ChannelName, keyRange, sv2ChannelCheckpointItem.Routed, committedOffset)
		if isTarget {
			liveTargetChannels = append(liveTargetChannels, c)
		} else {
			liveNonTargetChannels = append(liveNonTargetChannels, c)
		}
	}

	// An empty channel joins the non-target layout when one exists and the channel
	// fits a gap in it. Any other empty channel belongs to no layout this shard runs.
	for _, c := range candidateChannels {
		var overlapsLive = false
		for _, live := range append(liveNonTargetChannels, liveTargetChannels...) {
			if c.keyRange.keyBegin <= live.keyRange.keyEnd && live.keyRange.keyBegin <= c.keyRange.keyEnd {
				overlapsLive = true
				break
			}
		}
		if len(liveNonTargetChannels) > 0 && !overlapsLive {
			liveNonTargetChannels = append(liveNonTargetChannels, c)
			continue
		}
		b.abandonedRanges = append(b.abandonedRanges, c.keyRange)
		log.WithFields(log.Fields{
			"table":   b.table,
			"channel": c.channelName,
		}).Info("abandoning an empty channel of a layout this shard does not continue")
	}

	// A declared target layout alongside idle non-target channels is a switch a
	// prior session declared and did not finish, so it finishes here.
	if len(liveNonTargetChannels) > 0 && priorDeclaresTargets {
		var idle = !slices.ContainsFunc(liveNonTargetChannels, func(ch *streamV2Channel) bool {
			return ch.progress.routed != ch.progress.committed
		})
		if idle {
			for _, c := range liveNonTargetChannels {
				b.abandonedRanges = append(b.abandonedRanges, c.keyRange)
			}
			liveNonTargetChannels = nil
		}
	}

	// The active layout. Channels inherited from another topology carry rows the
	// replayed transaction must skip, so while any of them stands undropped it is
	// the layout — mixed subdivision depths included, as a join of children that
	// converged unevenly leaves. Otherwise the layout is the target layout.
	var activeChannels []*streamV2Channel
	if len(liveNonTargetChannels) > 0 {
		activeChannels = append(liveNonTargetChannels, liveTargetChannels...)
	} else {
		for _, keyRange := range targetChannelKeyRanges {
			if i := slices.IndexFunc(liveTargetChannels, func(ch *streamV2Channel) bool { return ch.keyRange == keyRange }); i >= 0 {
				activeChannels = append(activeChannels, liveTargetChannels[i])
				continue
			}

			var channelName = streamV2FormatChannelName(m.materialization, b.targetEpoch, keyRange, b.stateKey)
			var status = channelStatusByName[channelName]
			if status == nil {
				if status, err = client.OpenChannel(ctx, b.database, b.schema, b.table, channelName); err != nil {
					return fmt.Errorf("opening channel %q: %w", channelName, err)
				}
				if err := status.validateRejectedRows(channelName, b.table); err != nil {
					return err
				}
			}

			var sv2ChannelCheckpointItem = b.priorCheckpoint[keyRange]
			committedOffset, err := streamV2ValidateCommittedToken(channelName, b.table, status.CommittedToken, sv2ChannelCheckpointItem, keyRange, len(b.priorCheckpoint))
			if err != nil {
				return err
			}
			var routed int64
			if sv2ChannelCheckpointItem != nil {
				routed = sv2ChannelCheckpointItem.Routed
			}
			activeChannels = append(activeChannels, newStreamV2Channel(channelName, keyRange, routed, committedOffset))
		}
	}

	slices.SortFunc(activeChannels, func(a, b *streamV2Channel) int {
		return cmp.Compare(a.keyRange.keyBegin, b.keyRange.keyBegin)
	})

	// The layout must cover the shard's range with no gaps or overlaps, or some
	// documents the runtime delivers have no channel and some key hashes have two.
	// No topology this connector participates in produces such a layout, so
	// reaching here means the checkpoint and the shard ranges disagree about history.
	var ranges = make([]streamV2Range, len(activeChannels))
	for i, c := range activeChannels {
		ranges[i] = c.keyRange
	}
	if !streamV2LayoutCovers(ranges, shardKeyRange) {
		var described = make([]string, len(activeChannels))
		for i, c := range activeChannels {
			described[i] = c.keyRange.String()
		}
		return fmt.Errorf(
			"the channels this task's checkpoint records for this binding cover %s, which does not cover this shard's range %s with no gaps or overlaps: the checkpoint and the shard topology disagree about the ranges that have been appended under. Restore the task's shard key ranges to the topology which appended them, or backfill this binding",
			strings.Join(described, " "), shardKeyRange,
		)
	}

	b.activeChannels, b.targetRanges, b.opened = activeChannels, targetChannelKeyRanges, true

	var layout = make(log.Fields, len(activeChannels))
	for _, c := range activeChannels {
		layout[c.keyRange.String()] = fmt.Sprintf("routed %d committed %d", c.progress.routed, c.progress.committed)
	}
	log.WithFields(log.Fields{
		"table":           b.table,
		"shardRange":      shardKeyRange.String(),
		"activeChannels":  len(activeChannels),
		"layout":          layout,
		"abandonedRanges": b.abandonedRanges,
	}).Info("opened snowpipe streaming v2 channels")

	// Drop the channels this layout leaves behind before the first append.
	return m.sweep(ctx, b.database, b.schema, b.table, b.stateKey, m.layoutNames(b))
}

// layoutNames is the set of channel names the binding's active and target layouts hold.
func (m *streamV2Manager) layoutNames(b *streamV2Binding) map[string]bool {
	var keep = make(map[string]bool, len(b.activeChannels)+len(b.targetRanges))
	for _, c := range b.activeChannels {
		keep[c.channelName] = true
	}
	for _, keyRange := range b.targetRanges {
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
		if keyBegin, ok := m.streamingChannelKeyBegin(channelName); ok {
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
		var err = client.CloseChannel(ctx, channelName, true)
		var se *sidecarError
		if errors.As(err, &se) && se.Code == "unknown_channel" {
			// Channel never opened this session, or has been closed since it was opened.
			if _, err = client.OpenChannel(ctx, database, schema, table, channelName); err != nil {
				return fmt.Errorf("opening channel %q to drop it: %w", channelName, err)
			}
			err = client.CloseChannel(ctx, channelName, true)
		}
		if err != nil {
			return fmt.Errorf("dropping channel %q: %w", channelName, err)
		}
		log.WithFields(log.Fields{"table": table, "channel": channelName}).Info("swept a snowpipe streaming v2 channel a layout left behind")
	}
	return nil
}

// writeRow writes one converted document to the channel its key hash routes to, under
// that channel's next offset. It sends the channel's batch once the batch reaches its
// row or byte cap, and every channel's batch once the bytes buffered across the whole
// manager reach their ceiling.
//
// A document at or below the channel's committed offset was already committed by an
// interrupted attempt of this transaction. It still consumes its offset, so that later
// offsets stay aligned with Snowflake's, but it is not written again.
func (m *streamV2Manager) writeRow(ctx context.Context, binding int, packedKey []byte, converted []any) error {
	var b = m.bindings[binding]
	if err := m.ensureOpened(ctx, b); err != nil {
		return err
	}

	// packedKeyHashHH64 mirrors the runtime's shard-routing key hash, so a key hash
	// no channel of this shard owns means the two disagree about where this
	// document belongs.
	var keyHash = packedKeyHashHH64(packedKey)
	var c = b.route(keyHash)
	if c == nil {
		return fmt.Errorf(
			"the key hash %08x of a document stored to %s is covered by no channel of this shard, which covers %s: the runtime and this connector disagree about document routing",
			keyHash, b.table, m.shardRange(),
		)
	}

	c.progress.routed++
	var offset = c.progress.routed
	if offset <= c.progress.committed {
		return nil
	}

	var grew, err = c.bufferRow(offset, b.columnNames, converted)
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

// appendAllBatches appends the buffered documents of every channel of every binding.
// This brings the total of buffered bytes back to zero.
func (m *streamV2Manager) appendAllBatches(ctx context.Context) error {
	for _, b := range m.bindings {
		for _, c := range b.activeChannels {
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

// flush appends every channel's remaining documents, waits for Snowflake to
// commit them, and returns a checkpoint entry for each binding that stored,
// abandoned, or declared anything. Each entry holds an item for every active
// channel and for every target channel not yet in the layout. The wait is here
// because a commit failure must fail the transaction that produced it, and
// only flush can still do that.
func (m *streamV2Manager) flush(ctx context.Context) (map[int]streamV2Checkpoint, error) {
	var entries = make(map[int]streamV2Checkpoint)
	type commitWait struct {
		b      *streamV2Binding
		c      *streamV2Channel
		offset int64
	}
	var waits []commitWait

	for idx, b := range m.bindings {
		if !b.opened {
			continue // the binding stored nothing this session
		}

		var advanced = false
		for _, c := range b.activeChannels {
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
			// offset only grows from there.
			if c.progress.routed < c.progress.committed {
				return nil, fmt.Errorf(
					"channel %q reports committed offset %d but the transaction replayed against it reached only offset %d: it was not replayed identically, so the rows Snowflake already holds cannot be identified. Backfill this binding",
					c.channelName, c.progress.committed, c.progress.routed,
				)
			}

			if c.progress.routed != c.progress.checkpointed {
				advanced = true
			}
		}

		var declaring = !b.isTargetLayout()
		if !advanced && !declaring && len(b.abandonedRanges) == 0 {
			continue // nothing stored, nothing abandoned, nothing to converge
		}

		var sv2Checkpoint = make(streamV2Checkpoint)
		for _, c := range b.activeChannels {
			c.progress.checkpointed = c.progress.routed
			sv2Checkpoint[c.keyRange] = &streamV2ChannelCheckpointItem{
				ChannelName: c.channelName,
				Routed:      c.progress.routed,
			}
			if c.progress.routed > c.progress.committed {
				waits = append(waits, commitWait{b: b, c: c, offset: c.progress.routed})
			}
		}

		if declaring {
			// The declaration: a checkpoint item for every target channel the layout has not
			// converged to yet. The switch at Acknowledge runs only after the
			// checkpoint carrying these is durable.
			for _, keyRange := range b.targetRanges {
				if _, ok := sv2Checkpoint[keyRange]; !ok {
					var channelName = streamV2FormatChannelName(m.materialization, b.targetEpoch, keyRange, b.stateKey)
					sv2Checkpoint[keyRange] = &streamV2ChannelCheckpointItem{ChannelName: channelName}
				}
			}
			b.targetRangesDeclared = true
		}

		// Deletions of abandoned channels ride every checkpoint of the session, not
		// only the first, so that a transaction that never commits does not lose
		// them.
		for _, keyRange := range b.abandonedRanges {
			sv2Checkpoint[keyRange] = nil
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
		group.Go(func() error { return m.waitCommit(groupCtx, w.b, w.c, w.offset) })
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	return entries, nil
}

// acknowledged runs the channel rebalance switch once its declaration is durable.
// It abandons every non-target channel, opens the target channels in their
// place, and drops the abandoned channels.
//
// A crash between the switch and the durability of the resulting deletions
// leaves a checkpoint that holds both the declaration and the abandoned
// channels' items — the state this function resumes a switch from.
func (m *streamV2Manager) acknowledged(ctx context.Context) error {
	var indices = make([]int, 0, len(m.bindings))
	for idx := range m.bindings {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	for _, idx := range indices {
		var b = m.bindings[idx]
		if !b.opened || !b.targetRangesDeclared {
			continue
		}
		b.targetRangesDeclared = false
		if b.isTargetLayout() {
			continue
		}

		// The declaration is only ever written by a flush that committed every
		// channel, and nothing stores between that flush and this call. A channel
		// found uncommitted here means that reasoning is broken somewhere, and a
		// deferred convergence costs only throughput — so defer, loudly.
		var idle = true
		for _, c := range b.activeChannels {
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
		for _, c := range b.activeChannels {
			if slices.Contains(b.targetRanges, c.keyRange) {
				next = append(next, c)
				continue
			}
			b.abandonedRanges = append(b.abandonedRanges, c.keyRange)
		}

		for _, keyRange := range b.targetRanges {
			if slices.ContainsFunc(next, func(c *streamV2Channel) bool { return c.keyRange == keyRange }) {
				continue
			}
			var channelName = streamV2FormatChannelName(m.materialization, b.targetEpoch, keyRange, b.stateKey)
			status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, channelName)
			if err != nil {
				return fmt.Errorf("opening channel %q: %w", channelName, err)
			}
			if err := status.validateRejectedRows(channelName, b.table); err != nil {
				return err
			}
			// The channel was declared this session and nothing has routed to it
			// yet, so it must be empty. An offset token here is something else
			// appending under this binding's names.
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
		b.activeChannels = next

		log.WithFields(log.Fields{
			"table":           b.table,
			"activeChannels":  len(next),
			"abandonedRanges": b.abandonedRanges,
		}).Info("snowpipe streaming v2: converged the channel layout to the target layout")

		// The inherited channels are out of the layout now; drop them from the pipe.
		if err := m.sweep(ctx, b.database, b.schema, b.table, b.stateKey, m.layoutNames(b)); err != nil {
			return err
		}
	}
	return nil
}

// waitCommit blocks until the channel durably holds every document through offset. If
// Snowflake rejected any of its rows, waitCommit fails the transaction.
func (m *streamV2Manager) waitCommit(ctx context.Context, b *streamV2Binding, c *streamV2Channel, offset int64) error {
	client, err := m.ensureStarted(ctx)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"table":   b.table,
		"channel": c.channelName,
		"offset":  offset,
	}).Info("snowpipe streaming v2: awaiting commit")

	var started = time.Now()
	var token = c.offsetToken(offset)
	status, err := client.WaitCommit(ctx, c.channelName, token)
	if err != nil {
		return fmt.Errorf("waiting for commit of offset token %s on channel %q: %w", token, c.channelName, err)
	}

	// Snowflake reports rejected rows only after the commit, and flush has not yet
	// returned its checkpoint, so a rejection here fails the transaction that caused it.
	if err := status.validateRejectedRows(c.channelName, b.table); err != nil {
		return err
	}
	c.progress.committed = offset

	log.WithFields(log.Fields{
		"table":   b.table,
		"channel": c.channelName,
		"offset":  offset,
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

// streamV2ValidateCommittedToken checks Snowflake's committed offset token against the channel's checkpoint item and returns the committed offset, at or below which a replay must not append.
func streamV2ValidateCommittedToken(channelName, table string, committedToken *string, sv2ChannelCheckpointItem *streamV2ChannelCheckpointItem, keyRange streamV2Range, priorItems int) (int64, error) {
	var routed int64
	if sv2ChannelCheckpointItem != nil {
		routed = sv2ChannelCheckpointItem.Routed
	}

	if committedToken == nil {
		if routed > 0 {
			return 0, fmt.Errorf(
				"channel %q reports no committed offset token while this task's checkpoint records offset %d for it: Snowflake has lost this channel's committed offset token, so this shard cannot identify which of its documents Snowflake still holds. Backfill this binding",
				channelName, routed,
			)
		}
		return 0, nil
	}

	committedOffset, appendedBy, ok := streamV2ParseOffsetToken(*committedToken)
	if !ok {
		return 0, fmt.Errorf(
			"channel %q reports the committed offset token %q, which carries no offset: this channel was written by something other than this connector's snowpipe_streaming_v2 write path, and continuing could duplicate or drop rows",
			channelName, *committedToken,
		)
	}

	if appendedBy != keyRange {
		return 0, fmt.Errorf(
			"channel %q covers %s but reports committed offset %d under key range %s: that offset token was not written against this channel's key range, so the documents up to its offset cannot be identified. Backfill this binding",
			channelName, keyRange, committedOffset, appendedBy,
		)
	}

	if committedOffset < routed {
		return 0, fmt.Errorf(
			"channel %q reports committed offset %d, below the offset %d this task's checkpoint records for it: the channel has lost committed data, so the missing rows cannot be identified. Backfill this binding",
			channelName, committedOffset, routed,
		)
	}

	if sv2ChannelCheckpointItem == nil && committedOffset > 0 && priorItems > 0 {
		return 0, fmt.Errorf(
			"channel %q reports committed offset %d, which this task's checkpoint does not account for. Skipping the documents up to that offset would drop that many documents materialized into %s. Backfill this binding",
			channelName, committedOffset, table,
		)
	}

	return committedOffset, nil
}
