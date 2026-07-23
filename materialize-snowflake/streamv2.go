package main

import (
	"bufio"
	"compress/gzip"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	sql "github.com/estuary/connectors/materialize-sql"
	log "github.com/sirupsen/logrus"
)

// Batch caps bound the size of a single append RPC. Every append carries its
// own offset token: rows appended without a token could be committed by the
// SDK's background flusher without moving the committed token, making them
// unattributable and so duplicated by a recovery replay. The caps in effect
// for a transaction are recorded in its checkpoint item so that recovery
// reproduces identical batch boundaries even across connector upgrades that
// change these defaults.
const (
	streamV2DefaultBatchRows  = 50_000
	streamV2DefaultBatchBytes = 8 * 1024 * 1024
)

// streamV2Item is the driver-checkpoint record of a transaction's staged data
// for one binding, applied idempotently by Acknowledge.
type streamV2Item struct {
	Database   string
	Schema     string
	Table      string
	Channel    string
	StageDir   string
	LocalDir   string
	Files      []spillV2File
	BaseToken  string
	BatchRows  int
	BatchBytes int
}

type streamV2Binding struct {
	database string
	schema   string
	table    string
	channel  string
	// columns holds unquoted Snowflake column names aligned with the output of
	// Table.ConvertAll, for building the by-name row objects the SDK ingests.
	columns []string
	spill   *streamV2Spill
}

// streamV2Manager implements the high-performance Snowpipe Streaming write
// path by supervising the Python SDK sidecar. It owns per-binding channels and
// the idempotent Acknowledge apply. The sidecar is spawned lazily on first
// use; any sidecar failure is fatal to the connector (crash-only), and
// recovery replays from Snowflake's committed offset tokens.
type streamV2Manager struct {
	cfg         *config
	accountName string
	channelBase string
	argv        []string

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
	// committed offset token per channel as reported at open. Consulted only
	// by the recovery pass of Acknowledge.
	openChannels map[string]*string

	bindings map[int]*streamV2Binding
}

func newStreamV2Manager(ctx context.Context, cfg *config, materialization string, accountName string, keyBegin uint32) *streamV2Manager {
	procCtx, procCancel := context.WithCancel(ctx)
	return &streamV2Manager{
		cfg:          cfg,
		accountName:  accountName,
		channelBase:  channelName(materialization, keyBegin),
		argv:         defaultSidecarArgv(),
		procCtx:      procCtx,
		procCancel:   procCancel,
		openChannels: make(map[string]*string),
		bindings:     make(map[int]*streamV2Binding),
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

func (m *streamV2Manager) openChannel(ctx context.Context, database, schema, table, channel string) (*string, error) {
	client, err := m.ensureStarted(ctx)
	if err != nil {
		return nil, err
	}

	committed, err := client.OpenChannel(ctx, database, schema, table, channel)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.openChannels[channel] = committed
	m.mu.Unlock()
	return committed, nil
}

// addBinding opens a streaming channel for the binding's table. Any error is
// fatal to the streaming v2 write path; the caller does not degrade to another
// strategy.
func (m *streamV2Manager) addBinding(ctx context.Context, database, schema, table string, target sql.Table) error {
	var channel = fmt.Sprintf("%s_%s", m.channelBase, sanitizeAndAppendHash(table))

	committed, err := m.openChannel(ctx, database, schema, table, channel)
	if err != nil {
		return err
	}

	var columns []string
	for _, col := range target.Columns() {
		columns = append(columns, unquotedIdentifier(col.Identifier))
	}

	m.bindings[target.Binding] = &streamV2Binding{
		database: database,
		schema:   schema,
		table:    table,
		channel:  channel,
		columns:  columns,
		spill:    newStreamV2Spill(os.TempDir()),
	}

	var committedValue = "<none>"
	if committed != nil {
		committedValue = *committed
	}
	log.WithFields(log.Fields{
		"table":          table,
		"channel":        channel,
		"committedToken": committedValue,
	}).Info("opened snowpipe streaming v2 channel")
	return nil
}

// writeRow spills one converted row during Store. Rows never reach the sidecar
// here: the SDK commits appended rows autonomously, so appends are deferred to
// Acknowledge, after the recovery log has committed.
func (m *streamV2Manager) writeRow(ctx context.Context, db *stdsql.DB, binding int, converted []any) error {
	var b = m.bindings[binding]
	if err := b.spill.start(ctx, db); err != nil {
		return err
	}

	var row = make(map[string]any, len(b.columns))
	for i, col := range b.columns {
		row[col] = converted[i]
	}
	return b.spill.writeRow(row)
}

// flush concludes the Store phase, returning a checkpoint item per binding
// that spilled data. baseToken is this transaction's unique token prefix.
func (m *streamV2Manager) flush(baseToken string) (map[int]*streamV2Item, error) {
	var items = make(map[int]*streamV2Item)
	for idx, b := range m.bindings {
		if !b.spill.started {
			continue
		}
		stageDir, localDir, files, err := b.spill.flush()
		if err != nil {
			return nil, fmt.Errorf("flushing spill for %s: %w", b.table, err)
		}
		if len(files) == 0 {
			continue
		}
		items[idx] = &streamV2Item{
			Database:   b.database,
			Schema:     b.schema,
			Table:      b.table,
			Channel:    b.channel,
			StageDir:   stageDir,
			LocalDir:   localDir,
			Files:      files,
			BaseToken:  baseToken,
			BatchRows:  streamV2DefaultBatchRows,
			BatchBytes: streamV2DefaultBatchBytes,
		}
	}
	return items, nil
}

// apply idempotently ingests a checkpoint item's staged rows during
// Acknowledge. On the recovery pass it consults Snowflake's committed offset
// token to skip batches that were already committed by a previous attempt of
// this same transaction.
func (m *streamV2Manager) apply(ctx context.Context, db *stdsql.DB, item *streamV2Item, isRecovery bool) error {
	client, err := m.ensureStarted(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	committed, isOpen := m.openChannels[item.Channel]
	m.mu.Unlock()
	if !isOpen {
		// A checkpoint item whose channel wasn't opened by addBinding, e.g.
		// because the binding has since fallen back to another write strategy.
		if committed, err = m.openChannel(ctx, item.Database, item.Schema, item.Table, item.Channel); err != nil {
			return fmt.Errorf("reopening channel %q: %w", item.Channel, err)
		}
	}

	// skip is the highest batch sequence already committed, or -1.
	var skip = -1
	if isRecovery && committed != nil {
		if seq, ok := strings.CutPrefix(*committed, item.BaseToken+":"); ok {
			if n, err := strconv.Atoi(seq); err == nil {
				skip = n
			}
		}
	}

	var rows, batches int64
	var seq = -1
	var batch []json.RawMessage
	var batchBytes int

	var sendBatch = func() error {
		if len(batch) == 0 {
			return nil
		}
		seq++
		defer func() { batch, batchBytes = batch[:0], 0 }()

		if seq <= skip {
			return nil // already committed by a prior attempt
		}
		batches++
		rows += int64(len(batch))
		return client.Append(ctx, item.Channel, fmt.Sprintf("%s:%d", item.BaseToken, seq), batch)
	}

	var reader = newSpillV2Reader(db, item)
	defer reader.close()
	for {
		line, err := reader.next(ctx)
		if err != nil {
			return err
		} else if line == nil {
			break
		}
		batch = append(batch, line)
		batchBytes += len(line)
		if len(batch) >= item.BatchRows || batchBytes >= item.BatchBytes {
			if err := sendBatch(); err != nil {
				return err
			}
		}
	}
	if err := sendBatch(); err != nil {
		return err
	}

	if seq < 0 {
		return nil // no data at all
	} else if seq <= skip {
		log.WithFields(log.Fields{"channel": item.Channel, "token": *committed}).Info("snowpipe streaming v2: all batches were already committed")
		return nil
	}

	var finalToken = fmt.Sprintf("%s:%d", item.BaseToken, seq)
	if err := client.WaitCommit(ctx, item.Channel, finalToken); err != nil {
		return fmt.Errorf("waiting for commit of %q: %w", finalToken, err)
	}

	log.WithFields(log.Fields{
		"table":   item.Table,
		"channel": item.Channel,
		"rows":    rows,
		"batches": batches,
		"token":   finalToken,
	}).Info("snowpipe streaming v2: committed")
	return nil
}

// cleanup removes a fully-applied item's local and staged files.
func (m *streamV2Manager) cleanup(item *streamV2Item) {
	if err := os.RemoveAll(item.LocalDir); err != nil {
		log.WithError(err).Warn("removing local spill dir")
	}
}

func (m *streamV2Manager) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sup != nil {
		m.sup.stop(m.client)
	}
	m.procCancel()
}

// spillV2Reader streams the NDJSON rows of an item's spill files in order,
// reading local files when they still exist and fetching them back from the
// internal stage otherwise (i.e. during recovery after a restart).
type spillV2Reader struct {
	db   *stdsql.DB
	item *streamV2Item

	fileIdx int
	file    *os.File
	gz      *gzip.Reader
	scanner *bufio.Scanner
	fetched string // temp dir of GET-fetched files, if any
}

func newSpillV2Reader(db *stdsql.DB, item *streamV2Item) *spillV2Reader {
	return &spillV2Reader{db: db, item: item}
}

// next returns the following row line, nil at end of data.
func (r *spillV2Reader) next(ctx context.Context) (json.RawMessage, error) {
	for {
		if r.scanner == nil {
			if r.fileIdx >= len(r.item.Files) {
				return nil, nil
			}
			if err := r.openFile(ctx, r.item.Files[r.fileIdx].Name); err != nil {
				return nil, err
			}
			r.fileIdx++
		}

		if r.scanner.Scan() {
			// The scanner's buffer is reused; the batch outlives it.
			var line = make(json.RawMessage, len(r.scanner.Bytes()))
			copy(line, r.scanner.Bytes())
			return line, nil
		} else if err := r.scanner.Err(); err != nil {
			return nil, fmt.Errorf("reading spill file: %w", err)
		}
		r.closeFile()
	}
}

func (r *spillV2Reader) openFile(ctx context.Context, name string) error {
	var path = filepath.Join(r.item.LocalDir, name)
	if _, err := os.Stat(path); err != nil {
		fetched, err := r.fetch(ctx, name)
		if err != nil {
			return err
		}
		path = fetched
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening spill file: %w", err)
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("reading spill gzip header: %w", err)
	}
	r.file, r.gz = file, gz
	r.scanner = bufio.NewScanner(gz)
	r.scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	return nil
}

// fetch GETs a spill file back from the internal stage.
func (r *spillV2Reader) fetch(ctx context.Context, name string) (string, error) {
	if r.fetched == "" {
		dir, err := os.MkdirTemp("", "sfssget")
		if err != nil {
			return "", err
		}
		r.fetched = dir
	}

	var query = fmt.Sprintf(`GET %s/%s file://%s;`, r.item.StageDir, name, r.fetched)
	var file, size, status, message stdsql.NullString
	if err := r.db.QueryRowContext(ctx, query).Scan(&file, &size, &status, &message); err != nil {
		return "", fmt.Errorf("fetching spill file %q from stage: %w", name, err)
	} else if !strings.EqualFold(status.String, "downloaded") {
		return "", fmt.Errorf("fetching spill file %q from stage: status %q (%s)", name, status.String, message.String)
	}
	log.WithFields(log.Fields{"file": name, "stageDir": r.item.StageDir}).Info("snowpipe streaming v2: recovered spill file from stage")
	return filepath.Join(r.fetched, name), nil
}

func (r *spillV2Reader) closeFile() {
	if r.gz != nil {
		r.gz.Close()
		r.gz = nil
	}
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
	r.scanner = nil
}

func (r *spillV2Reader) close() {
	r.closeFile()
	if r.fetched != "" {
		os.RemoveAll(r.fetched)
	}
}

// unquotedIdentifier undoes SQL identifier quoting, yielding the exact name as
// stored in Snowflake for by-name row construction.
func unquotedIdentifier(ident string) string {
	if strings.HasPrefix(ident, `"`) && strings.HasSuffix(ident, `"`) && len(ident) >= 2 {
		return strings.ReplaceAll(ident[1:len(ident)-1], `""`, `"`)
	}
	return ident
}
