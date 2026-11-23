package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/filesink"
	"github.com/estuary/connectors/go/smb"
	"github.com/google/uuid"
	"github.com/hirochachacha/go-smb2"
	log "github.com/sirupsen/logrus"
)

type smbUpload struct {
	Key string `json:"key"`
	// Location is the \\host\share the file was staged on. It is empty in
	// state written by earlier versions.
	Location string `json:"location,omitempty"`
}

func (u *smbUpload) FileKey() string { return u.Key }

type smbFile interface {
	io.WriterAt
	Sync() error
	Close() error
}

type smbShare interface {
	WithContext(context.Context) smbShare
	MkdirAll(string, os.FileMode) error
	OpenFile(string, int, os.FileMode) (smbFile, error)
	Stat(string) (os.FileInfo, error)
	Remove(string) error
	Rename(string, string) error
}

type shareAdapter struct{ *smb2.Share }

func (s shareAdapter) WithContext(ctx context.Context) smbShare {
	return shareAdapter{s.Share.WithContext(ctx)}
}

func (s shareAdapter) OpenFile(name string, flag int, mode os.FileMode) (smbFile, error) {
	f, err := s.Share.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	return f, nil
}

type smbConnection interface {
	WithShare(context.Context, func(smbShare) error) error
	Close() error
}

type connectionAdapter struct{ *smb.Connection }

func (c connectionAdapter) WithShare(ctx context.Context, fn func(smbShare) error) error {
	return c.Connection.WithShare(ctx, func(share *smb2.Share) error {
		return fn(shareAdapter{share})
	})
}

type smbStore struct {
	mu         sync.Mutex
	connection smbConnection
	location   string
	// staged holds the size of each file this process staged and has not
	// completed yet.
	staged map[string]int64
}

var _ filesink.Store[*smbUpload] = (*smbStore)(nil)

// go-smb2 sets no socket deadlines, so a server that stops answering would
// block a call forever while holding mu. opTimeout bounds each SMB call; an
// upload as a whole has no bound, since a large backfill file can take hours.
var opTimeout = 5 * time.Minute

func newStore(ctx context.Context, cfg config) (*smbStore, error) {
	conn, err := smb.Dial(ctx, smb.Config{
		Host: cfg.Host, Share: cfg.Share,
		Username: cfg.Username, Password: cfg.Password, Domain: cfg.Domain,
		DialContext: smbDialContext,
	}, net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, err
	}
	return &smbStore{connection: connectionAdapter{conn}, location: `\\` + cfg.Host + `\` + cfg.Share}, nil
}

var smbDialContext func(context.Context, string, string) (net.Conn, error)

func (s *smbStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connection.Close()
}

func (s *smbStore) SupportsPathPatternExpansion() bool { return false }

func (s *smbStore) StageObject(ctx context.Context, r io.Reader, key string) (*smbUpload, error) {
	if err := smb.ValidatePath("/" + strings.TrimPrefix(key, "/")); err != nil {
		return nil, err
	}
	// go-smb2 binds an open file to the context it was opened with, so one
	// context covers the upload and a watchdog cancels it when a single call
	// runs longer than opTimeout. Time spent waiting for rows is not counted.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timedOut := fmt.Errorf("SMB call took longer than %s: %w", opTimeout, context.DeadlineExceeded)
	watchdog := time.AfterFunc(opTimeout, func() { cancel(timedOut) })
	watchdog.Stop()
	s.mu.Lock()
	defer s.mu.Unlock()

	call := func(fn func(smbShare) error) error {
		watchdog.Reset(opTimeout)
		defer watchdog.Stop()
		err := s.connection.WithShare(ctx, fn)
		if cause := context.Cause(ctx); err != nil && cause == timedOut {
			err = fmt.Errorf("%w: %w", cause, err)
		}
		return err
	}
	var file smbFile
	defer func() {
		if file != nil {
			// An error left the file open; bound the close like any other call.
			watchdog.Reset(opTimeout)
			file.Close()
			watchdog.Stop()
		}
	}()
	partial := smb.Path(key + ".partial")
	err := call(func(share smbShare) error {
		if dir := path.Dir(key); dir != "." {
			if err := share.MkdirAll(smb.Path(dir), 0755); err != nil {
				return fmt.Errorf("creating upload directory: %w", err)
			}
		}
		var err error
		file, err = share.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("creating partial file %q: %w", key, err)
	}

	var offset int64
	openFile := func(share smbShare) error {
		if file == nil {
			// A server that restarted or failed over can come back without
			// writes it had acknowledged. Resuming would leave a hole, and the
			// stream can't be replayed, so fail the upload instead.
			if info, err := share.Stat(partial); err != nil {
				return err
			} else if info.Size() < offset {
				return fmt.Errorf("partial file has %d bytes after reconnecting, expected at least %d", info.Size(), offset)
			}
			var err error
			file, err = share.OpenFile(partial, os.O_WRONLY, 0600)
			return err
		}
		return nil
	}

	buf := make([]byte, 1024*1024)
	for {
		// filesink can abandon the stream after a Store error, so don't hold mu
		// while waiting on it or Close would block forever.
		s.mu.Unlock()
		n, readErr := r.Read(buf)
		s.mu.Lock()
		if n > 0 {
			err := call(func(share smbShare) error {
				if err := openFile(share); err != nil {
					return err
				}
				// Rewriting a chunk at the same offset is safe even if the server
				// accepted it before the connection was lost.
				written, err := file.WriteAt(buf[:n], offset)
				if err != nil {
					file.Close()
					file = nil
				}
				if err == nil && written != n {
					return io.ErrShortWrite
				}
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("writing partial file %q: %w", key, err)
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		} else if readErr != nil {
			return nil, fmt.Errorf("reading upload stream: %w", readErr)
		}
	}

	if err := call(func(share smbShare) error {
		if err := openFile(share); err != nil {
			return err
		}
		err := errors.Join(file.Sync(), file.Close())
		file = nil
		return err
	}); err != nil {
		return nil, fmt.Errorf("closing partial file %q: %w", key, err)
	}
	if s.staged == nil {
		s.staged = make(map[string]int64)
	}
	s.staged[key] = offset
	return &smbUpload{Key: key, Location: s.location}, nil
}

func (s *smbStore) CompleteObject(ctx context.Context, upload *smbUpload) error {
	if upload == nil {
		return fmt.Errorf("missing SMB upload")
	} else if err := smb.ValidatePath("/" + strings.TrimPrefix(upload.Key, "/")); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.connection.WithShare(ctx, func(share smbShare) error {
		key := smb.Path(upload.Key)
		partial := key + ".partial"
		if _, err := share.Stat(partial); os.IsNotExist(err) {
			if info, err := share.Stat(key); err == nil && !info.IsDir() {
				return nil
			} else if os.IsNotExist(err) {
				// A repeated Acknowledge can follow a downstream job moving or
				// deleting the completed file.
				if upload.Location != "" && upload.Location != s.location {
					// The endpoint changed since staging. The file was most likely
					// completed there already, but it may be left as a .partial.
					log.WithFields(log.Fields{
						"key":            upload.Key,
						"stagedLocation": upload.Location,
						"location":       s.location,
					}).Warn("file was staged on a different share and is missing here; if the original share still has its .partial file, rename it to remove the .partial suffix")
					return nil
				}
				if _, ok := s.staged[upload.Key]; ok {
					// A server that failed over after the commit can lose a staged
					// file, even during a rename that never applied. A rename whose
					// response was lost also lands here if a downstream job takes the
					// completed file while the session reconnects; a restart then
					// accepts it as below.
					return fmt.Errorf("partial file %q staged by this task is missing; unless a downstream job already took the completed file, its rows were lost and the binding needs a backfill", upload.Key)
				}
				// After a restart the upload comes from state, and a file completed
				// and picked up downstream can't be told apart from one the server
				// lost. checkStaged covers losses before the commit.
				log.WithField("key", upload.Key).Warn("partial and completed files are both missing, assuming already completed and picked up downstream; if it was not, its rows were lost and the binding needs a backfill")
				return nil
			} else if err != nil {
				return fmt.Errorf("checking completed file %q: %w", upload.Key, err)
			}
			return fmt.Errorf("completed path %q is a directory", upload.Key)
		} else if err != nil {
			return fmt.Errorf("checking partial file %q: %w", upload.Key, err)
		}

		// go-smb2 Rename does not replace existing files. Only remove the final
		// file after confirming the staged replacement still exists.
		if err := share.Remove(key); err == nil {
			// A single task never stages a key whose final file exists, so another
			// writer is using the same directory and binding path.
			log.WithField("key", upload.Key).Warn("replaced an existing completed file; another materialization may be writing to the same directory and path")
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("replacing completed file %q: %w", upload.Key, err)
		}
		if err := share.Rename(partial, key); err != nil {
			return fmt.Errorf("completing file %q: %w", upload.Key, err)
		}
		return nil
	})
	if err == nil {
		delete(s.staged, upload.Key)
	}
	return err
}

// checkStaged confirms that every file this process staged and has not
// completed is still on the share. It runs before a transaction commits, while
// a failure still makes the runtime replay the transaction: a server that
// failed over can lose files it had acknowledged, and once the commit records
// them a lost file can't be told apart from one picked up downstream.
func (s *smbStore) checkStaged(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, size := range s.staged {
		var info os.FileInfo
		err := func() error {
			ctx, cancel := context.WithTimeout(ctx, opTimeout)
			defer cancel()
			return s.connection.WithShare(ctx, func(share smbShare) error {
				var err error
				info, err = share.Stat(smb.Path(key + ".partial"))
				return err
			})
		}()
		if os.IsNotExist(err) {
			return fmt.Errorf("partial file %q staged by this task is missing before commit", key)
		} else if err != nil {
			return fmt.Errorf("checking partial file %q: %w", key, err)
		} else if info.Size() != size {
			// A server that failed over can come back with an older or shorter
			// copy of the file.
			return fmt.Errorf("partial file %q staged by this task has %d bytes before commit, expected %d", key, info.Size(), size)
		}
	}
	return nil
}

func (s *smbStore) checkWritable(ctx context.Context, directory string) (err error) {
	key := path.Join(directory, ".estuary-validate-"+uuid.NewString())
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		s.mu.Lock()
		defer s.mu.Unlock()
		if cleanupErr := s.connection.WithShare(ctx, func(share smbShare) error {
			delete(s.staged, key)
			err := share.Remove(smb.Path(key + ".partial"))
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("deleting SMB write probe: %w", cleanupErr))
		}
	}()
	_, err = s.StageObject(ctx, strings.NewReader("estuary\n"), key)
	return err
}
