package main

import (
	"bufio"
	"context"
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

// Transcoder manages a subprocess that converts BSON change events to JSON.
// It reads length-prefixed BSON documents from stdin and writes JSON lines to stdout.
// The transcoder handles fragment merging internally.
type Transcoder struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
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
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdout),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Transcode writes a raw BSON change event to the transcoder and reads the response.
// The rawBSON should be the complete change event document (bson.Raw from ChangeStream.Current).
// This operation is thread-safe - only one transcode can happen at a time.
func (t *Transcoder) Transcode(rawBSON bson.Raw) (*TranscoderResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// bson.Raw is already length-prefixed BSON bytes.
	if _, err := t.stdin.Write(rawBSON); err != nil {
		return nil, fmt.Errorf("writing to transcoder: %w", err)
	}

	return t.readResponse()
}

// TranscoderResponse represents the parsed response from the transcoder.
type TranscoderResponse struct {
	// IsSkip is true if the event was skipped (not a document event).
	IsSkip bool
	// Reason is the reason for skipping (only set if IsSkip is true).
	Reason string
	// Database is the database name from the event.
	Database string
	// Collection is the collection name from the event.
	Collection string
	// Document is the JSON document (only set if IsSkip is false).
	Document json.RawMessage
	// CompletedSplitEvent is true if this document completed a split event (multiple fragments merged).
	CompletedSplitEvent bool
}

// readResponse reads and parses a response from the transcoder.
// This is an internal method - callers should use Transcode instead.
func (t *Transcoder) readResponse() (*TranscoderResponse, error) {
	var line []byte
	for {
		segment, isPrefix, err := t.stdout.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("reading from transcoder: %w", err)
		}
		line = append(line, segment...)
		if !isPrefix {
			break
		}
	}

	// Parse the response to check if it's a skip or a document
	var parsed struct {
		Skip                bool   `json:"_skip"`
		Reason              string `json:"reason"`
		DB                  string `json:"db"`
		Collection          string `json:"collection"`
		CompletedSplitEvent bool   `json:"_completedSplitEvent"`
		Meta                *struct {
			Source struct {
				DB         string `json:"db"`
				Collection string `json:"collection"`
			} `json:"source"`
		} `json:"_meta"`
	}

	if err := json.Unmarshal(line, &parsed); err != nil {
		return nil, fmt.Errorf("parsing transcoder response: %w", err)
	}

	resp := &TranscoderResponse{}
	if parsed.Skip {
		resp.IsSkip = true
		resp.Reason = parsed.Reason
		resp.Database = parsed.DB
		resp.Collection = parsed.Collection
	} else {
		resp.IsSkip = false
		resp.Document = json.RawMessage(line)
		resp.CompletedSplitEvent = parsed.CompletedSplitEvent
		if parsed.Meta != nil {
			resp.Database = parsed.Meta.Source.DB
			resp.Collection = parsed.Meta.Source.Collection
		}
	}

	return resp, nil
}

// Stop terminates the transcoder subprocess.
func (t *Transcoder) Stop() {
	if t == nil {
		return
	}
	if t.cmd != nil && t.cmd.Process != nil {
		// Close stdin to signal EOF to the transcoder.
		t.stdin.Close()
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
