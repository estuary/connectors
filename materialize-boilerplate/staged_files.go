package boilerplate

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// Encoder is any streaming encoder that can be used to write files.
type Encoder interface {
	Encode(row []any) error
	Written() int
	Close() error
}

// StagedFileStorageClient is a specific implementation of a client that
// interacts with a staging file system, usually an object store of some kind.
type StagedFileStorageClient[T any] interface {
	// NewEncoder creates a new Encoder that writes rows to a writer.
	NewEncoder(w io.WriteCloser, fields []string) Encoder

	// NewObject creates a new file object handle from a random UUID. The UUID
	// should be part of the file name, and the specifics of the object
	// construction are generic to the implementation.
	NewObject(uuid string) T

	// URI creates an identifier from T.
	URI(T) string

	// UploadStream streams data from a reader to the destination represented by
	// the object T. For example, this may read bytes from r and write the bytes
	// to a blob at a specific location represented by T.
	UploadStream(ctx context.Context, object T, r io.Reader) error

	// Delete deletes the objects per the list of URIs. This is used for
	// removing the temporary staged files after transactions are applied.
	Delete(ctx context.Context, uris []string) error
}

type StagedFiles[T any] struct {
	client             StagedFileStorageClient[T]
	fileSizeLimit      int
	flushOnNextBinding bool
	stagedFiles        []stagedFile[T]
	lastBinding        int
	didCleanupCurrent  bool
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
func NewStagedFiles[T any](client StagedFileStorageClient[T], fileSizeLimit int, flushOnNextBinding bool) *StagedFiles[T] {
	return &StagedFiles[T]{
		client:             client,
		fileSizeLimit:      fileSizeLimit,
		flushOnNextBinding: flushOnNextBinding,
		lastBinding:        -1,
	}
}

// AddBinding adds a binding. Bindings must be added in order of their binding
// index, starting from 0.
func (sf *StagedFiles[T]) AddBinding(binding int, fields []string) {
	if binding != len(sf.stagedFiles) {
		panic("bindings must be added monotonically increasing order starting with binding 0")
	}

	sf.stagedFiles = append(sf.stagedFiles, stagedFile[T]{
		client:        sf.client,
		fileSizeLimit: sf.fileSizeLimit,
		fields:        fields,
	})
}

// EncodeRow encodes a row of data for the binding.
func (sf *StagedFiles[T]) EncodeRow(ctx context.Context, binding int, row []any) error {
	if sf.flushOnNextBinding && sf.lastBinding != -1 && binding != sf.lastBinding {
		if err := sf.stagedFiles[sf.lastBinding].flushFile(); err != nil {
			return fmt.Errorf("flushing prior binding [%d]: %w", sf.lastBinding, err)
		}
	}
	sf.lastBinding = binding
	sf.didCleanupCurrent = false

	return sf.stagedFiles[binding].encodeRow(ctx, row)
}

// Flush flushes the current encoder and closes the file.
func (sf *StagedFiles[T]) Flush(binding int) ([]string, error) {
	return sf.stagedFiles[binding].flush()
}

// CleanupCurrentTransaction attempts to delete all of the known staged files
// for the current transaction. It is safe to call multiple times, and if called
// again will not re-attempt to delete the same files. This makes it convenient
// to use both as part of a deferred call, and an inline call to check for
// deletion errors.
func (sf *StagedFiles[T]) CleanupCurrentTransaction(ctx context.Context) error {
	if !sf.didCleanupCurrent {
		sf.didCleanupCurrent = true

		var uris []string
		for _, f := range sf.stagedFiles {
			if !f.started {
				continue
			}

			for _, u := range f.uploaded {
				uris = append(uris, sf.client.URI(u))
			}
		}

		return sf.client.Delete(ctx, uris)
	}

	return nil
}

// CleanupCheckpoint deletes files specified int he list of URIs. This can be
// used to cleanup files that have been staged for a materialization using the
// post-commit apply pattern, where staged files must remain across connector
// restarts.
func (sf *StagedFiles[T]) CleanupCheckpoint(ctx context.Context, uris []string) error {
	return sf.client.Delete(ctx, uris)
}

// Started indicates if any rows were encoded for the binding during this
// transaction.
func (sf *StagedFiles[T]) Started(binding int) bool {
	return sf.stagedFiles[binding].started
}

type stagedFile[T any] struct {
	client        StagedFileStorageClient[T]
	fileSizeLimit int
	fields        []string
	encoder       Encoder
	group         *errgroup.Group
	started       bool
	uploaded      []T
}

func (f *stagedFile[T]) encodeRow(ctx context.Context, row []any) error {
	if !f.started {
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

func (f *stagedFile[T]) flush() ([]string, error) {
	if err := f.flushFile(); err != nil {
		return nil, err
	}

	var uploaded []string
	for _, obj := range f.uploaded {
		uploaded = append(uploaded, f.client.URI(obj))
	}
	f.started = false

	return uploaded, nil
}

func (f *stagedFile[T]) newFile(ctx context.Context) {
	r, w := io.Pipe()
	f.encoder = f.client.NewEncoder(w, f.fields)

	group, groupCtx := errgroup.WithContext(ctx)
	f.group = group
	obj := f.client.NewObject(uuid.NewString())
	f.uploaded = append(f.uploaded, obj)

	f.group.Go(func() error {
		if err := f.client.UploadStream(groupCtx, obj, r); err != nil {
			r.CloseWithError(err)
			return fmt.Errorf("uploading file: %w", err)
		}

		return nil
	})
}

func (f *stagedFile[T]) flushFile() error {
	if f.encoder == nil {
		return nil
	}

	if err := f.encoder.Close(); err != nil {
		return fmt.Errorf("closing encoder: %w", err)
	} else if err := f.group.Wait(); err != nil {
		return err
	}

	f.encoder = nil

	return nil
}
