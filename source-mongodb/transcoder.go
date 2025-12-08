package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

// Binary protocol output message type discriminators
const (
	msgTypeSkip     uint8 = 0x01
	msgTypeDocument uint8 = 0x02
)

// Binary protocol input message type discriminators
const (
	inputTypeChangeEvent  uint8 = 0x01
	inputTypeRawDocument  uint8 = 0x02
)

// Document message flags
const (
	flagCompletedSplitEvent uint8 = 0x01
)

// Transcoder manages a subprocess that converts BSON change events to JSON.
// It reads length-prefixed BSON documents from stdin and writes JSON lines to stdout.
// The transcoder handles fragment merging internally.
type Transcoder struct {
	cmd       *exec.Cmd
	stdinPipe io.WriteCloser  // underlying pipe for closing
	stdin     *bufio.Writer   // buffered writer for efficiency
	stdout    *bufio.Reader
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
}

// NewTranscoder creates and starts a new transcoder subprocess.
func NewTranscoder(ctx context.Context) (*Transcoder, error) {
	ctx, cancel := context.WithCancel(ctx)

	cmd := exec.CommandContext(ctx, "bson-transcoder")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("getting stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("getting stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("getting stderr pipe: %w", err)
	}

	// Copy stderr to the connector's stderr for logging.
	go func() {
		_, err := io.Copy(os.Stderr, stderr)
		if err != nil && err != io.EOF {
			log.WithError(err).Debug("error copying transcoder stderr")
		}
	}()

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("starting transcoder: %w", err)
	}

	log.Debug("bson-transcoder started")

	return &Transcoder{
		cmd:       cmd,
		stdinPipe: stdin,
		stdin:     bufio.NewWriter(stdin),
		stdout:    bufio.NewReader(stdout),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// flushSignal is a 4-byte zero-length frame that signals the transcoder to flush its output.
var flushSignal = []byte{0, 0, 0, 0}

// writeChangeEvent writes a change event message to the transcoder.
// Format: [length: u32 LE][type: u8 = 0x01][bson_doc]
func (t *Transcoder) writeChangeEvent(raw bson.Raw) error {
	// Message length = 1 (type byte) + len(bson)
	msgLen := uint32(1 + len(raw))
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], msgLen)

	if _, err := t.stdin.Write(lenBuf[:]); err != nil {
		return err
	}
	if err := t.stdin.WriteByte(inputTypeChangeEvent); err != nil {
		return err
	}
	if _, err := t.stdin.Write(raw); err != nil {
		return err
	}
	return nil
}

// writeRawDocument writes a raw document message to the transcoder.
// Format: [length: u32 LE][type: u8 = 0x02][db_len: u8][db][coll_len: u8][coll][bson_doc]
func (t *Transcoder) writeRawDocument(raw bson.Raw, database, collection string) error {
	dbBytes := []byte(database)
	collBytes := []byte(collection)

	// Message length = 1 (type) + 1 (db_len) + len(db) + 1 (coll_len) + len(coll) + len(bson)
	msgLen := uint32(1 + 1 + len(dbBytes) + 1 + len(collBytes) + len(raw))
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], msgLen)

	if _, err := t.stdin.Write(lenBuf[:]); err != nil {
		return err
	}
	if err := t.stdin.WriteByte(inputTypeRawDocument); err != nil {
		return err
	}
	if err := t.stdin.WriteByte(uint8(len(dbBytes))); err != nil {
		return err
	}
	if _, err := t.stdin.Write(dbBytes); err != nil {
		return err
	}
	if err := t.stdin.WriteByte(uint8(len(collBytes))); err != nil {
		return err
	}
	if _, err := t.stdin.Write(collBytes); err != nil {
		return err
	}
	if _, err := t.stdin.Write(raw); err != nil {
		return err
	}
	return nil
}

// transcodeBatch handles concurrent writing and reading to/from the transcoder subprocess.
// It writes documents using the provided writeAll function, then reads count responses.
//
// Writes and reads happen concurrently to avoid pipe deadlock: without this, if we
// write all docs before reading, the pipes can fill up (Rust blocks on stdout, Go
// blocks on stdin) causing deadlock.
//
// The writer goroutine always sends exactly one value on writeErr (via defer),
// which is checked after all reads complete.
func (t *Transcoder) transcodeBatch(count int, writeAll func() error) ([]*TranscoderResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	writeErr := make(chan error, 1)
	go func() {
		var err error
		defer func() { writeErr <- err }()

		if err = writeAll(); err != nil {
			return
		}
		if err = t.stdin.Flush(); err != nil {
			err = fmt.Errorf("flushing to transcoder: %w", err)
			return
		}
		if _, err = t.stdinPipe.Write(flushSignal); err != nil {
			err = fmt.Errorf("writing flush signal: %w", err)
		}
	}()

	responses := make([]*TranscoderResponse, count)
	for i := 0; i < count; i++ {
		resp, err := t.readResponse()
		if err != nil {
			// Check if write failed first - it may explain the read error
			select {
			case werr := <-writeErr:
				if werr != nil {
					return nil, fmt.Errorf("write failed: %w (read error: %v)", werr, err)
				}
			default:
			}
			return nil, fmt.Errorf("reading response %d: %w", i, err)
		}
		responses[i] = resp
	}

	if err := <-writeErr; err != nil {
		return nil, err
	}
	return responses, nil
}

// TranscodeBatch sends multiple BSON change event documents and reads all responses.
// The responses are returned in the same order as the input documents.
func (t *Transcoder) TranscodeBatch(rawDocs []bson.Raw) ([]*TranscoderResponse, error) {
	return t.transcodeBatch(len(rawDocs), func() error {
		for _, raw := range rawDocs {
			if err := t.writeChangeEvent(raw); err != nil {
				return fmt.Errorf("writing to transcoder: %w", err)
			}
		}
		return nil
	})
}

// RawDocumentInput represents a raw document with its metadata for transcoding.
type RawDocumentInput struct {
	Raw        bson.Raw
	Database   string
	Collection string
}

// TranscodeRawDocuments sends multiple raw BSON documents (not change events) and reads all responses.
// This is used for backfill documents where we have the raw document and its db/collection metadata.
// The transcoder will sanitize the document and add _meta with snapshot: true.
func (t *Transcoder) TranscodeRawDocuments(docs []RawDocumentInput) ([]*TranscoderResponse, error) {
	return t.transcodeBatch(len(docs), func() error {
		for _, doc := range docs {
			if err := t.writeRawDocument(doc.Raw, doc.Database, doc.Collection); err != nil {
				return fmt.Errorf("writing raw document to transcoder: %w", err)
			}
		}
		return nil
	})
}

// TranscoderResponse represents the parsed response from the transcoder.
type TranscoderResponse struct {
	// IsSkip is true if the event was skipped (not a document event).
	IsSkip bool
	// Document is the JSON document (only set if IsSkip is false).
	Document json.RawMessage
	// CompletedSplitEvent is true if this document completed a split event (multiple fragments merged).
	CompletedSplitEvent bool
	// Database is the database name from the event (extracted from _meta.source for documents).
	Database string
	// Collection is the collection name from the event (extracted from _meta.source for documents).
	Collection string
}

// readFrame reads a length-prefixed binary frame from the reader.
// Returns the payload bytes (without the length prefix).
func (t *Transcoder) readFrame() ([]byte, error) {
	// Read 4-byte length prefix (little-endian)
	var lenBuf [4]byte
	if _, err := io.ReadFull(t.stdout, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("reading frame length: %w", err)
	}

	length := binary.LittleEndian.Uint32(lenBuf[:])

	// Sanity check: reject unreasonably large frames (> 100MB)
	if length > 100*1024*1024 {
		return nil, fmt.Errorf("frame too large: %d bytes", length)
	}

	// Read payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(t.stdout, payload); err != nil {
		return nil, fmt.Errorf("reading frame payload: %w", err)
	}

	return payload, nil
}

// parseDocumentMessage parses a document message payload.
// Payload format: [type: u8][flags: u8][db_len: u8][db: bytes][coll_len: u8][coll: bytes][json: bytes]
func parseDocumentMessage(payload []byte) (
	doc json.RawMessage,
	completedSplit bool,
	database string,
	collection string,
	err error,
) {
	// Minimum: type(1) + flags(1) + db_len(1) + coll_len(1) + json(at least 2 for "{}")
	if len(payload) < 6 {
		return nil, false, "", "", fmt.Errorf("document message too short: %d bytes", len(payload))
	}

	offset := 1 // Skip message type (already validated by caller)

	// Parse flags
	flags := payload[offset]
	completedSplit = (flags & flagCompletedSplitEvent) != 0
	offset++

	// Parse database (length-prefixed string)
	dbLen := int(payload[offset])
	offset++

	if offset+dbLen > len(payload) {
		return nil, false, "", "", fmt.Errorf("database length exceeds payload: %d+%d > %d", offset, dbLen, len(payload))
	}

	database = string(payload[offset : offset+dbLen])
	offset += dbLen

	// Parse collection (length-prefixed string)
	if offset >= len(payload) {
		return nil, false, "", "", fmt.Errorf("no collection length byte at offset %d", offset)
	}

	collLen := int(payload[offset])
	offset++

	if offset+collLen > len(payload) {
		return nil, false, "", "", fmt.Errorf("collection length exceeds payload: %d+%d > %d", offset, collLen, len(payload))
	}

	collection = string(payload[offset : offset+collLen])
	offset += collLen

	// Remaining bytes are the JSON document
	if offset >= len(payload) {
		return nil, false, "", "", fmt.Errorf("no JSON document data at offset %d", offset)
	}

	doc = json.RawMessage(payload[offset:])

	return doc, completedSplit, database, collection, nil
}

// readResponse reads and parses a response from the transcoder using binary protocol.
// This is an internal method - callers should use Transcode or TranscodeBatch instead.
func (t *Transcoder) readResponse() (*TranscoderResponse, error) {
	// Read length-prefixed frame
	payload, err := t.readFrame()
	if err != nil {
		return nil, fmt.Errorf("reading frame from transcoder: %w", err)
	}

	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload from transcoder")
	}

	// Dispatch based on message type
	msgType := payload[0]

	switch msgType {
	case msgTypeSkip:
		// Skip message - just 1 byte, no additional data
		return &TranscoderResponse{
			IsSkip: true,
		}, nil

	case msgTypeDocument:
		doc, completedSplit, database, collection, err := parseDocumentMessage(payload)
		if err != nil {
			return nil, fmt.Errorf("parsing document message: %w", err)
		}

		// Database and collection now come from binary protocol header
		// No JSON parsing needed - this is the optimization!
		resp := &TranscoderResponse{
			IsSkip:              false,
			Document:            doc,
			CompletedSplitEvent: completedSplit,
			Database:            database,
			Collection:          collection,
		}

		return resp, nil

	default:
		return nil, fmt.Errorf("unknown message type: 0x%02x", msgType)
	}
}

// Stop terminates the transcoder subprocess.
func (t *Transcoder) Stop() {
	if t == nil {
		return
	}
	if t.cmd != nil && t.cmd.Process != nil {
		// Close stdin pipe to signal EOF to the transcoder.
		t.stdinPipe.Close()
		// Using negative pid signals kill to a process group.
		syscall.Kill(-t.cmd.Process.Pid, syscall.SIGTERM)
	}
	t.cancel()
}

// Wait waits for the transcoder to exit and returns any error.
func (t *Transcoder) Wait() error {
	if t == nil || t.cmd == nil {
		return nil
	}
	return t.cmd.Wait()
}
