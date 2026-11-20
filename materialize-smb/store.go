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

type smbStore struct {
	mu           sync.Mutex
	share        smbShare
	closeSession func() error
	connect      func(context.Context) (smbShare, func() error, error)
	closed       bool
	location     string
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
	s := &smbStore{
		location: shareLocation(cfg),
		connect: func(ctx context.Context) (smbShare, func() error, error) {
			return connectSMB(ctx, cfg)
		},
	}
	var err error
	s.share, s.closeSession, err = s.connect(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func shareLocation(cfg config) string {
	return `\\` + cfg.Host + `\` + cfg.Share
}

func connectSMB(ctx context.Context, cfg config) (smbShare, func() error, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to SMB server: %w", err)
	}
	dialer := smb2.Dialer{
		Negotiator: smb2.Negotiator{RequireMessageSigning: true},
		Initiator: &smb2.NTLMInitiator{
			User:     cfg.Username,
			Password: cfg.Password,
			Domain:   cfg.Domain,
		},
	}
	session, err := dialer.DialContext(ctx, conn)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("authenticating SMB session with NTLM and message signing: %w", err)
	}
	share, err := mountShare(session.WithContext(ctx), cfg)
	if err != nil {
		session.WithContext(ctx).Logoff()
		conn.Close()
		return nil, nil, fmt.Errorf("mounting SMB share %q: %w", cfg.Share, err)
	}

	return shareAdapter{share}, func() error {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		unmountErr := share.WithContext(closeCtx).Umount()
		logoffErr := session.WithContext(closeCtx).Logoff()
		closeErr := conn.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
		return errors.Join(unmountErr, logoffErr, closeErr)
	}, nil
}

type mounter interface {
	Mount(string) (*smb2.Share, error)
}

// Mount the full \\host\share path. A bare share name makes go-smb2 send the
// resolved IP address as the server name, which servers that pick a share by
// name (such as clustered file servers) can reject.
func mountShare(m mounter, cfg config) (*smb2.Share, error) {
	return m.Mount(shareLocation(cfg))
}

func (s *smbStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.closeSession != nil {
		return s.closeSession()
	}
	return nil
}

func (s *smbStore) SupportsPathPatternExpansion() bool { return false }

func smbPath(key string) string { return strings.ReplaceAll(key, "/", `\`) }

// go-smb2 wraps every net.Conn failure in a TransportError. A bare net.Error is
// not checked because syscall.Errno satisfies it, so a share-side error such
// as ENOTDIR would otherwise be taken for a lost connection.
func isTransportError(err error) bool {
	var transport *smb2.TransportError
	return errors.As(err, &transport) || errors.Is(err, net.ErrClosed)
}

// Callers hold mu and share a retry budget across each complete upload or rename.
func (s *smbStore) retry(ctx context.Context, retried *bool, fn func() error) error {
	if s.closed {
		return fmt.Errorf("SMB store is closed")
	} else if err := ctx.Err(); err != nil {
		return err
	}
	err := fn()
	if !isTransportError(err) || *retried || ctx.Err() != nil {
		return err
	}
	*retried = true
	s.closeSession()
	s.share, s.closeSession, err = s.connect(ctx)
	if err != nil {
		s.closed = true
		return fmt.Errorf("reconnecting SMB session: %w", err)
	}
	return fn()
}

func (s *smbStore) StageObject(ctx context.Context, r io.Reader, key string) (*smbUpload, error) {
	if err := validatePath(key); err != nil {
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

	var retried bool
	call := func(fn func() error) error {
		watchdog.Reset(opTimeout)
		defer watchdog.Stop()
		err := s.retry(ctx, &retried, fn)
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
	partial := smbPath(key + ".partial")
	err := call(func() error {
		share := s.share.WithContext(ctx)
		if dir := path.Dir(key); dir != "." {
			if err := share.MkdirAll(smbPath(dir), 0755); err != nil {
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
	openFile := func() error {
		if file == nil {
			share := s.share.WithContext(ctx)
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
			err := call(func() error {
				if err := openFile(); err != nil {
					return err
				}
				// Rewriting a chunk at the same offset is safe even if the server
				// accepted it before the connection was lost.
				written, err := file.WriteAt(buf[:n], offset)
				if isTransportError(err) {
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

	if err := call(func() error {
		if err := openFile(); err != nil {
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
	} else if err := validatePath(upload.Key); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	s.mu.Lock()
	defer s.mu.Unlock()

	var retried bool
	err := s.retry(ctx, &retried, func() error {
		share := s.share.WithContext(ctx)
		key := smbPath(upload.Key)
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

	var retried bool
	for key, size := range s.staged {
		var info os.FileInfo
		err := func() error {
			ctx, cancel := context.WithTimeout(ctx, opTimeout)
			defer cancel()
			return s.retry(ctx, &retried, func() error {
				var err error
				info, err = s.share.WithContext(ctx).Stat(smbPath(key + ".partial"))
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
		var retried bool
		if cleanupErr := s.retry(ctx, &retried, func() error {
			delete(s.staged, key)
			err := s.share.WithContext(ctx).Remove(smbPath(key + ".partial"))
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
