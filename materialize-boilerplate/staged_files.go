package boilerplate

import (
	"context"
	"fmt"
	"io"

	"github.com/estuary/connectors/go/blob"
	"github.com/google/uuid"
)

// Encoder is any streaming encoder that can be used to write files.
type Encoder interface {
	Encode(row []any) error
	Written() int
	Close() error
}

// StagedFileClient is a specific implementation of a client that interacts with
// a staging file system, usually an object store of some kind.
type StagedFileClient interface {
	// NewEncoder creates a new Encoder that writes rows to a writer.
	NewEncoder(w io.WriteCloser, fields []string) Encoder

	// NewKey creates a new file key from keyParts, which include the bucketPath
	// (if set), a random UUID prefix if prefixFiles is true, and a random UUID
	// for the specific key.
	NewKey(keyParts []string) string
}

type StagedFiles struct {
	client             StagedFileClient
	bucket             blob.Bucket
	fileSizeLimit      int
	bucketPath         string
	flushOnNextBinding bool
	prefixFiles        bool
	stagedFiles        []stagedFile
	lastBinding        int
}

// NewStagedFiles creates a StagedFiles instance, which is used for staging data
// on a remote system for later committing to a materialized destination.
//
// fileSizeLimit controls how large the staged files are in bytes. If there is a
// lot of data to stage for a single binding, the data will be split into
// multiple files with each one being approximately fileSizeLimit in size.
//
// flushOnNextBinding can be set to flush the file stream whenever a new binding
// (by index) has a row encoded for it. Flushing the file stream will result in
// the current streaming encoder being closed and flushed, which concludes the
// current file being written. Any further writes to that same binding will
// start a new file, so this should usually only be enabled for encoding rows
// received from Store requests, where the documents are always in monotonic
// order with respect to their binding index.
//
// prefixFiles generates an additional random UUID to prefix each file name,
// which is unique for each binding and each transaction cycle. This can be used
// to provide additional grouping of files for a binding within a transaction so
// that all of its files are in the same "folder".
func NewStagedFiles(
	client StagedFileClient,
	bucket blob.Bucket,
	fileSizeLimit int,
	bucketPath string,
	flushOnNextBinding bool,
	prefixFiles bool,
) *StagedFiles {
	return &StagedFiles{
		client:             client,
		bucket:             bucket,
		fileSizeLimit:      fileSizeLimit,
		bucketPath:         bucketPath,
		flushOnNextBinding: flushOnNextBinding,
		lastBinding:        -1,
		prefixFiles:        prefixFiles,
	}
}

// AddBinding adds a binding. Bindings must be added in order of their binding
// index, starting from 0. Fields may be `nil` if they are not needed, ex:
// encoding CSV files without headers, or encoding JSONL as arrays of values
// instead of objects.
func (sf *StagedFiles) AddBinding(binding int, fields []string) {
	if binding != len(sf.stagedFiles) {
		panic("bindings must be added monotonically increasing order starting with binding 0")
	}

	sf.stagedFiles = append(sf.stagedFiles, stagedFile{
		client:        sf.client,
		bucket:        sf.bucket,
		fileSizeLimit: sf.fileSizeLimit,
		bucketPath:    sf.bucketPath,
		prefixFiles:   sf.prefixFiles,
		fields:        fields,
	})
}

// EncodeRow encodes a row of data for the binding.
func (sf *StagedFiles) EncodeRow(ctx context.Context, binding int, row []any) error {
	if sf.flushOnNextBinding && sf.lastBinding != -1 && binding != sf.lastBinding {
		if err := sf.stagedFiles[sf.lastBinding].flushFile(); err != nil {
			return fmt.Errorf("flushing prior binding [%d]: %w", sf.lastBinding, err)
		}
	}
	sf.lastBinding = binding

	return sf.stagedFiles[binding].encodeRow(ctx, row)
}

// Flush flushes the current encoder and closes the file.
func (sf *StagedFiles) Flush(binding int) ([]string, error) {
	return sf.stagedFiles[binding].flush()
}

// CleanupCurrentTransaction attempts to delete all of the known staged files
// for the current transaction. It is safe to call multiple times, and if called
// again will not re-attempt to delete the same files. This makes it convenient
// to use both as part of a deferred call, and an inline call to check for
// deletion errors.
func (sf *StagedFiles) CleanupCurrentTransaction(ctx context.Context) error {
	var uris []string
	for i := range sf.stagedFiles {
		f := &sf.stagedFiles[i]
		if len(f.uploaded) == 0 {
			continue
		}

		for _, u := range f.uploaded {
			uris = append(uris, sf.bucket.URI(u))
		}
		f.uploaded = nil
	}
	if len(uris) == 0 {
		return nil
	}

	return sf.bucket.Delete(ctx, uris)
}

// CleanupCheckpoint deletes files specified in the list of URIs. This can be
// used to cleanup files that have been staged for a materialization using the
// post-commit apply pattern, where staged files must remain across connector
// restarts.
func (sf *StagedFiles) CleanupCheckpoint(ctx context.Context, uris []string) error {
	return sf.bucket.Delete(ctx, uris)
}

// Started indicates if any rows were encoded for the binding during this
// transaction.
func (sf *StagedFiles) Started(binding int) bool {
	return sf.stagedFiles[binding].started
}

type stagedFile struct {
	client        StagedFileClient
	bucket        blob.Bucket
	fileSizeLimit int
	bucketPath    string
	prefixFiles   bool
	uuidPrefix    string
	fields        []string
	encoder       Encoder
	started       bool
	uploaded      []string
}

func (f *stagedFile) encodeRow(ctx context.Context, row []any) error {
	if !f.started {
		f.uuidPrefix = uuid.NewString()
		f.uploaded = nil
		f.started = true
	}

	if f.encoder == nil {
		f.newFile(ctx)
	}

	if err := f.encoder.Encode(row); err != nil {
		return fmt.Errorf("encoding row: %w", err)
	}

	if f.encoder.Written() >= f.fileSizeLimit {
		if err := f.flushFile(); err != nil {
			return err
		}
	}

	return nil
}

func (f *stagedFile) flush() ([]string, error) {
	if err := f.flushFile(); err != nil {
		return nil, err
	}

	var uploaded []string
	for _, obj := range f.uploaded {
		uploaded = append(uploaded, f.bucket.URI(obj))
	}
	f.started = false

	return uploaded, nil
}

func (f *stagedFile) newFile(ctx context.Context) {
	var nameParts []string
	if f.bucketPath != "" {
		nameParts = append(nameParts, f.bucketPath)
	}
	if f.prefixFiles {
		nameParts = append(nameParts, f.uuidPrefix)
	}
	nameParts = append(nameParts, uuid.NewString())

	key := f.client.NewKey(nameParts)
	f.uploaded = append(f.uploaded, key)
	f.encoder = f.client.NewEncoder(f.bucket.NewWriter(ctx, key), f.fields)
}

func (f *stagedFile) flushFile() error {
	if f.encoder == nil {
		return nil
	}

	if err := f.encoder.Close(); err != nil {
		return fmt.Errorf("closing encoder: %w", err)
	}
	f.encoder = nil

	return nil
}
