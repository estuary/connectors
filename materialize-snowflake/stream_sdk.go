package main

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	"github.com/klauspost/pgzip"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/estuary/connectors/go/encrow"
)

// sdkChunkSizeLimit bounds the uncompressed size of a single chunk of NDJSON
// rows, since the append-rows endpoint limits the size of a single request.
// Requests are sent gzip-compressed, so the on-the-wire size will be
// considerably smaller than this.
const sdkChunkSizeLimit = 3_500_000

// sdkChunk describes one durable chunk of NDJSON rows from a transaction. It
// is persisted in the driver checkpoint so that a crashed commit can be
// replayed: the chunk bytes are read from the local file when it still exists,
// or retrieved from the internal stage otherwise.
type sdkChunk struct {
	StageDir  string `json:"stage_dir"`  // internal stage directory, e.g. "uuid"
	FileName  string `json:"file_name"`  // chunk file name within the directory
	LocalPath string `json:"local_path"` // local copy of the chunk file
	Token     string `json:"token"`      // offset token registering this chunk
}

// sdkTableStream is the per-binding state of a channel of a table's default
// streaming pipe.
type sdkTableStream struct {
	schema string // stored (translated) schema name
	pipe   string // stored pipe name
	fields []string
	shape  *encrow.Shape

	continuationToken string
	// lastCommitted mirrors the channel's last committed offset token as of
	// the most recent open or confirmed write.
	lastCommitted *string
	// errorRows is the channel's error-row count baseline, used to detect rows
	// that the server parsed but refused to ingest.
	errorRows int

	// Local chunk accumulation for the current transaction.
	dir     string
	chunkN  int
	current *sdkChunkWriter
	chunks  []sdkChunk
}

// sdkChunkWriter writes gzipped NDJSON rows to a local file while tracking the
// uncompressed size for chunking decisions.
type sdkChunkWriter struct {
	file         *os.File
	gz           *pgzip.Writer
	uncompressed int
	buf          []byte
}

func newSdkChunkWriter(path string) (*sdkChunkWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating chunk file: %w", err)
	}
	return &sdkChunkWriter{file: file, gz: pgzip.NewWriter(file)}, nil
}

func (w *sdkChunkWriter) writeRow(shape *encrow.Shape, row []any) error {
	var err error
	w.buf = w.buf[:0]
	if w.buf, err = shape.Encode(w.buf, row); err != nil {
		return fmt.Errorf("encoding row: %w", err)
	}
	w.buf = append(w.buf, '\n')

	if _, err := w.gz.Write(w.buf); err != nil {
		return fmt.Errorf("writing row: %w", err)
	}
	w.uncompressed += len(w.buf)

	return nil
}

func (w *sdkChunkWriter) close() error {
	if err := w.gz.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	} else if err := w.file.Close(); err != nil {
		return fmt.Errorf("closing chunk file: %w", err)
	}
	return nil
}

// sdkStreamManager orchestrates Snowpipe Streaming over the high-performance
// REST API. Rows are accumulated into local NDJSON chunk files during Store and
// made durable by PUTing them to the connector's internal stage before the
// driver checkpoint referencing them is built. The chunks are appended to each
// binding's channel during Acknowledge, using "base:n" offset tokens so that
// chunks already committed by a prior, interrupted attempt are skipped.
type sdkStreamManager struct {
	c           *sdkStreamClient
	db          *stdsql.DB
	channelName string
	tempdir     string

	// mu guards streams, which commit replays of concurrent Acknowledge
	// checkpoint items may add to via ensureStream.
	mu      sync.Mutex
	streams map[int]*sdkTableStream
}

func newSdkStreamManager(cfg *config, materialization string, account string, keyBegin uint32, db *stdsql.DB) (*sdkStreamManager, error) {
	c, err := newSdkStreamClient(cfg, account)
	if err != nil {
		return nil, fmt.Errorf("newSdkStreamClient: %w", err)
	}

	return &sdkStreamManager{
		c:           c,
		db:          db,
		channelName: channelName(materialization, keyBegin),
		tempdir:     os.TempDir(),
		streams:     make(map[int]*sdkTableStream),
	}, nil
}

// defaultPipeName is the name of the streaming pipe that Snowflake
// automatically creates for a table at first ingest.
func defaultPipeName(tableName string) string {
	return tableName + "-STREAMING"
}

// ensureStream returns the stream of the channel of the pipe, opening the
// channel if needed. Checkpoint replay can reference a pipe with no stream
// opened by addBinding, such as when the SDK streaming feature flag was
// turned off with a transaction's chunks still pending.
func (m *sdkStreamManager) ensureStream(ctx context.Context, schema string, pipe string) (*sdkTableStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.streams {
		if s.schema == schema && s.pipe == pipe {
			return s, nil
		}
	}

	res, err := m.c.openChannel(ctx, schema, pipe, m.channelName)
	if err != nil {
		return nil, fmt.Errorf("openChannel: %w", err)
	}
	stream := &sdkTableStream{
		schema:            schema,
		pipe:              pipe,
		continuationToken: res.NextContinuationToken,
		lastCommitted:     res.ChannelStatus.LastCommittedOffsetToken,
		errorRows:         res.ChannelStatus.RowsErrorCount,
	}
	// Key by a binding number that no real binding uses, since this stream
	// exists only to replay a checkpoint.
	m.streams[-1-len(m.streams)] = stream

	log.WithFields(log.Fields{
		"schema":        schema,
		"pipe":          pipe,
		"channel":       m.channelName,
		"lastCommitted": res.ChannelStatus.LastCommittedOffsetToken,
	}).Info("opened streaming channel for checkpoint replay")

	return stream, nil
}

func (m *sdkStreamManager) addBinding(ctx context.Context, schema string, tableName string, columnNames []string, target sql.Table) error {
	pipe := defaultPipeName(tableName)

	res, err := m.c.openChannel(ctx, schema, pipe, m.channelName)
	if err != nil {
		return fmt.Errorf("openChannel: %w", err)
	}

	m.streams[target.Binding] = &sdkTableStream{
		schema:            schema,
		pipe:              pipe,
		fields:            columnNames,
		shape:             newSdkShape(columnNames),
		continuationToken: res.NextContinuationToken,
		lastCommitted:     res.ChannelStatus.LastCommittedOffsetToken,
		errorRows:         res.ChannelStatus.RowsErrorCount,
	}

	log.WithFields(log.Fields{
		"schema":        schema,
		"pipe":          pipe,
		"channel":       m.channelName,
		"lastCommitted": res.ChannelStatus.LastCommittedOffsetToken,
	}).Info("opened streaming channel")

	return nil
}

func newSdkShape(fields []string) *encrow.Shape {
	shape := encrow.NewShape(fields)
	// Pre-serialized JSON from the runtime is trusted as-is, and values are
	// not HTML-escaped, matching how rows are encoded for staged files.
	shape.SetFlags(json.TrustRawMessage)
	shape.SetSkipNulls(true)
	return shape
}

func (m *sdkStreamManager) writeRow(ctx context.Context, binding int, row []any) error {
	stream, ok := m.streams[binding]
	if !ok {
		return fmt.Errorf("no stream for binding %d", binding)
	}

	if stream.current == nil {
		if stream.dir == "" {
			if err := m.startTransaction(ctx, stream); err != nil {
				return fmt.Errorf("starting transaction: %w", err)
			}
		}
		w, err := newSdkChunkWriter(filepath.Join(stream.dir, fmt.Sprintf("%06d.ndjson.gz", stream.chunkN)))
		if err != nil {
			return err
		}
		stream.current = w
	}

	if err := stream.current.writeRow(stream.shape, row); err != nil {
		return err
	}

	if stream.current.uncompressed >= sdkChunkSizeLimit {
		if err := m.finishChunk(ctx, stream); err != nil {
			return fmt.Errorf("finishing chunk: %w", err)
		}
	}

	return nil
}

// startTransaction initializes the local scratch directory for a binding's
// chunks of the current transaction, named by a fresh UUID that also serves as
// the chunk directory within the internal stage.
func (m *sdkStreamManager) startTransaction(ctx context.Context, stream *sdkTableStream) error {
	id := uuid.NewString()
	dir := filepath.Join(m.tempdir, id)
	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("creating scratch directory: %w", err)
	}
	stream.dir = dir
	stream.chunkN = 0
	stream.chunks = nil

	return nil
}

// finishChunk closes the current chunk file and PUTs it to the internal stage,
// making it durable for commit replay.
func (m *sdkStreamManager) finishChunk(ctx context.Context, stream *sdkTableStream) error {
	if stream.current == nil {
		return nil
	}
	if err := stream.current.close(); err != nil {
		return err
	}
	stream.current = nil

	stageDir := filepath.Base(stream.dir)
	fileName := fmt.Sprintf("%06d.ndjson.gz", stream.chunkN)
	localPath := filepath.Join(stream.dir, fileName)

	query := fmt.Sprintf(
		`PUT file://%s @flow_v1/%s AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE;`,
		localPath, stageDir,
	)
	// Retry a few times on errors since there are occasionally transient
	// network errors when running PUT queries.
	for attempt := 1; ; attempt++ {
		var source, target, sourceSize, targetSize, sourceCompression, targetCompression, status, message string
		err := m.db.QueryRowContext(ctx, query).Scan(&source, &target, &sourceSize, &targetSize, &sourceCompression, &targetCompression, &status, &message)
		if err == nil {
			break
		} else if attempt > 3 {
			return fmt.Errorf("PUT chunk to stage: %w", err)
		}
		log.WithError(err).WithField("attempt", attempt).Info("retrying PUT of chunk to stage")
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	stream.chunks = append(stream.chunks, sdkChunk{
		StageDir:  stageDir,
		FileName:  fileName,
		LocalPath: localPath,
	})
	stream.chunkN++

	return nil
}

// flush finalizes all bindings' chunks for the transaction, assigning offset
// tokens "baseToken:0", "baseToken:1", ... per binding, and returns the chunk
// descriptors to be persisted in the driver checkpoint.
func (m *sdkStreamManager) flush(ctx context.Context, baseToken string) (map[int][]sdkChunk, error) {
	out := make(map[int][]sdkChunk)
	for binding, stream := range m.streams {
		if err := m.finishChunk(ctx, stream); err != nil {
			return nil, fmt.Errorf("finishing chunk: %w", err)
		}
		if len(stream.chunks) == 0 {
			continue
		}
		for i := range stream.chunks {
			stream.chunks[i].Token = blobToken(baseToken, i)
		}
		out[binding] = stream.chunks
		stream.chunks = nil
		stream.dir = ""
	}

	return out, nil
}

// write appends a transaction's chunks to the channel of the table identified
// by schema and pipe, skipping chunks whose offset tokens the channel has
// already committed, and returns once the channel reports the final chunk's
// token as durably committed. All chunks must be for the same binding.
func (m *sdkStreamManager) write(ctx context.Context, schema string, pipe string, chunks []sdkChunk) error {
	stream, err := m.ensureStream(ctx, schema, pipe)
	if err != nil {
		return fmt.Errorf("ensuring stream for pipe %s.%s: %w", schema, pipe, err)
	}

	logger := log.WithFields(log.Fields{
		"schema": schema,
		"pipe":   pipe,
	})
	ctx = WithLogger(ctx, logger)

	var finalToken string
	for _, chunk := range chunks {
		finalToken = chunk.Token
		if shouldWrite, err := shouldWriteNextToken(ctx, chunk.Token, stream.lastCommitted); err != nil {
			return fmt.Errorf("shouldWriteNextToken: %w", err)
		} else if !shouldWrite {
			continue
		}

		rows, err := m.chunkBytes(ctx, chunk)
		if err != nil {
			return fmt.Errorf("reading chunk %s: %w", chunk.FileName, err)
		}

		next, err := m.c.appendRows(ctx, schema, pipe, m.channelName, stream.continuationToken, chunk.Token, rows)
		if err != nil {
			return fmt.Errorf("appendRows: %w", err)
		}
		stream.continuationToken = next
		token := chunk.Token
		stream.lastCommitted = &token
	}

	if err := m.waitForTokenCommitted(ctx, stream, finalToken); err != nil {
		return err
	}

	return m.cleanupChunks(ctx, chunks)
}

// chunkBytes returns the gzipped NDJSON bytes of a chunk, reading the local
// file when present or retrieving the durable copy from the internal stage
// when replaying a commit after a restart.
func (m *sdkStreamManager) chunkBytes(ctx context.Context, chunk sdkChunk) ([]byte, error) {
	if b, err := os.ReadFile(chunk.LocalPath); err == nil {
		return b, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading local chunk: %w", err)
	}

	dir, err := os.MkdirTemp(m.tempdir, "sdk-chunk-restore")
	if err != nil {
		return nil, fmt.Errorf("creating restore directory: %w", err)
	}
	defer os.RemoveAll(dir)

	query := fmt.Sprintf(`GET @flow_v1/%s/%s file://%s;`, chunk.StageDir, chunk.FileName, dir)
	var file, size, status, message string
	if err := m.db.QueryRowContext(ctx, query).Scan(&file, &size, &status, &message); err != nil {
		return nil, fmt.Errorf("GET chunk from stage: %w", err)
	} else if status != "DOWNLOADED" {
		return nil, fmt.Errorf("GET chunk from stage: unexpected status %q: %s", status, message)
	}

	return os.ReadFile(filepath.Join(dir, chunk.FileName))
}

// waitForTokenCommitted polls the channel status until the given offset token
// is reported as durably committed. It also fails loudly if the server reports
// rows that could not be ingested, since those rows would otherwise be
// silently dropped.
func (m *sdkStreamManager) waitForTokenCommitted(ctx context.Context, stream *sdkTableStream, token string) error {
	logger := Logger(ctx)
	maxBackoff := 2 * time.Second
	backoff := 100 * time.Millisecond
	ts := time.Now()
	for n := 1; ; n++ {
		statuses, err := m.c.channelStatus(ctx, stream.schema, stream.pipe, []string{m.channelName})
		if err != nil {
			if !errors.Is(err, ErrTemporary) {
				return err
			}
			logger.WithField("attempt", n).WithError(err).Info("temporary error getting channel status")
		} else if status, ok := statuses[m.channelName]; !ok {
			return fmt.Errorf("channel %q missing from status response", m.channelName)
		} else if status.RowsErrorCount > stream.errorRows {
			return fmt.Errorf(
				"channel %q reports %d rows that could not be ingested: %s",
				m.channelName, status.RowsErrorCount-stream.errorRows, deref(status.LastErrorMessage),
			)
		} else if status.LastCommittedOffsetToken != nil && *status.LastCommittedOffsetToken == token {
			break
		}

		if n%10 == 0 {
			logger.WithFields(log.Fields{
				"attempt": n,
				"token":   token,
			}).Info("channel offset token not yet committed")
		}

		if time.Since(ts) > 5*time.Minute {
			return fmt.Errorf("channel offset token not committed: max retries reached")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}

	logger.WithFields(log.Fields{
		"token": token,
		"took":  time.Since(ts).String(),
	}).Info("channel offset token committed")

	return nil
}

// cleanupChunks removes the durable staged copies and local files of a
// transaction's chunks once their data is committed.
func (m *sdkStreamManager) cleanupChunks(ctx context.Context, chunks []sdkChunk) error {
	dirs := make(map[string]struct{})
	for _, chunk := range chunks {
		dirs[chunk.StageDir] = struct{}{}
		if chunk.LocalPath != "" {
			os.Remove(chunk.LocalPath)
			os.Remove(filepath.Dir(chunk.LocalPath)) // succeeds only once empty
		}
	}

	for dir := range dirs {
		if _, err := m.db.ExecContext(ctx, fmt.Sprintf(`REMOVE @flow_v1/%s;`, dir)); err != nil {
			return fmt.Errorf("removing staged chunks: %w", err)
		}
	}

	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
