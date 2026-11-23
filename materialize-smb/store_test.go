package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/estuary/connectors/go/smb"
	"github.com/hirochachacha/go-smb2"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

const testKey = "exports/flow/events/raw/v0000000000/00000000000000000000.csv.gz"

type testShare struct {
	t               *testing.T
	root            string
	failures        map[string][]error
	files           []*os.File
	opens           []int
	connections     int
	closes          int
	renameCommitted bool
}

type testConnection struct {
	share     smbShare
	state     *testShare
	reconnect func()
	closed    bool
}

func newTestStore(t *testing.T) (*smbStore, *testShare) {
	share := &testShare{t: t, root: t.TempDir(), failures: make(map[string][]error), connections: 1}
	s := &smbStore{connection: &testConnection{share: share, state: share}}
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, share
}

func (c *testConnection) Close() error {
	if !c.closed {
		c.closed = true
		c.state.closes++
		for _, f := range c.state.files {
			f.Close()
		}
	}
	return nil
}

func (c *testConnection) WithShare(ctx context.Context, fn func(smbShare) error) error {
	if err := ctx.Err(); err != nil {
		return err
	} else if c.closed {
		return os.ErrClosed
	}
	err := fn(c.share.WithContext(ctx))
	var transport *smb2.TransportError
	if !errors.As(err, &transport) || ctx.Err() != nil {
		return err
	}
	c.Close()
	if c.reconnect != nil {
		c.reconnect()
	}
	c.closed = false
	c.state.connections++
	return fn(c.share.WithContext(ctx))
}

func (s *testShare) fail(operation string) error {
	if len(s.failures[operation]) == 0 {
		return nil
	}
	err := s.failures[operation][0]
	s.failures[operation] = s.failures[operation][1:]
	return err
}

func (s *testShare) local(name string) string {
	s.t.Helper()
	require.NotContains(s.t, name, "/", "SMB paths must use backslashes")
	return filepath.Join(s.root, filepath.FromSlash(strings.ReplaceAll(name, `\`, "/")))
}

func (s *testShare) WithContext(context.Context) smbShare { return s }

func (s *testShare) MkdirAll(name string, mode os.FileMode) error {
	if err := s.fail("mkdir"); err != nil {
		return err
	}
	return os.MkdirAll(s.local(name), mode)
}

func (s *testShare) OpenFile(name string, flag int, mode os.FileMode) (smbFile, error) {
	s.opens = append(s.opens, flag)
	if err := s.fail("open"); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.local(name), flag, mode)
	if err != nil {
		return nil, err
	}
	s.files = append(s.files, f)
	return &testFile{File: f, share: s}, nil
}

func (s *testShare) Stat(name string) (os.FileInfo, error) {
	if err := s.fail("stat"); err != nil {
		return nil, err
	}
	return os.Stat(s.local(name))
}

func (s *testShare) ReadDir(name string) ([]os.FileInfo, error) {
	entries, err := os.ReadDir(s.local(name))
	if err != nil {
		return nil, err
	}
	var infos []os.FileInfo
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (s *testShare) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(s.local(name))
}

func (s *testShare) Remove(name string) error {
	if err := s.fail("remove"); err != nil {
		return err
	}
	return os.Remove(s.local(name))
}

func (s *testShare) Rename(old, new string) error {
	err := s.fail("rename")
	if err != nil && !s.renameCommitted {
		return err
	}
	if _, statErr := s.Stat(new); statErr == nil {
		return os.ErrExist
	}
	if renameErr := os.Rename(s.local(old), s.local(new)); renameErr != nil {
		return renameErr
	}
	return err
}

type testFile struct {
	*os.File
	share *testShare
}

func (f *testFile) WriteAt(b []byte, offset int64) (int, error) {
	if err := f.share.fail("write"); err != nil {
		var transport *smb2.TransportError
		if errors.As(err, &transport) {
			n, _ := f.File.WriteAt(b[:len(b)/2], offset)
			return n, err
		}
		return 0, err
	}
	return f.File.WriteAt(b, offset)
}

func (f *testFile) Sync() error {
	if err := f.share.fail("sync"); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *testFile) Close() error {
	return errors.Join(f.File.Close(), f.share.fail("close"))
}

func TestStageAndComplete(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()
	s, share := newTestStore(t)
	require.False(t, s.SupportsPathPatternExpansion())
	ctx := context.Background()
	upload, err := s.StageObject(ctx, strings.NewReader("first contents"), testKey)
	require.NoError(t, err)
	require.Equal(t, testKey, upload.FileKey())
	require.NoFileExists(t, share.local(smb.Path(testKey)))
	staged, err := os.ReadFile(share.local(smb.Path(testKey + ".partial")))
	require.NoError(t, err)
	require.Equal(t, "first contents", string(staged))

	state, err := json.Marshal(upload)
	require.NoError(t, err)
	var recovered smbUpload
	require.NoError(t, json.Unmarshal(state, &recovered))
	require.Equal(t, upload, &recovered)
	require.NoError(t, s.CompleteObject(ctx, &recovered))
	require.NoFileExists(t, share.local(smb.Path(testKey+".partial")))
	require.NoError(t, s.CompleteObject(ctx, &recovered))
	final, err := os.ReadFile(share.local(smb.Path(testKey)))
	require.NoError(t, err)
	require.Equal(t, "first contents", string(final))
	require.Nil(t, hook.LastEntry())

	// Replayed staging replaces an uncommitted partial, then replaces the final.
	_, err = s.StageObject(ctx, strings.NewReader("discarded longer contents"), testKey)
	require.NoError(t, err)
	upload, err = s.StageObject(ctx, strings.NewReader("new"), testKey)
	require.NoError(t, err)
	require.NoError(t, s.CompleteObject(ctx, upload))
	require.Contains(t, hook.LastEntry().Message, "replaced an existing completed file")
	final, err = os.ReadFile(share.local(smb.Path(testKey)))
	require.NoError(t, err)
	require.Equal(t, "new", string(final))
	require.NoError(t, os.Mkdir(share.local(smb.Path(testKey+".dir")), 0755))
	require.ErrorContains(t, s.CompleteObject(ctx, &smbUpload{Key: testKey + ".dir"}), "is a directory")
}

func TestCompleteAfterPickup(t *testing.T) {
	s, share := newTestStore(t)
	ctx := context.Background()
	upload, err := s.StageObject(ctx, strings.NewReader("contents"), testKey)
	require.NoError(t, err)
	require.NoError(t, s.CompleteObject(ctx, upload))

	// A downstream job moves the completed file before Acknowledge repeats.
	require.NoError(t, os.Remove(share.local(smb.Path(testKey))))
	require.NoError(t, s.CompleteObject(ctx, upload))
	require.NoFileExists(t, share.local(smb.Path(testKey)))
	require.NoFileExists(t, share.local(smb.Path(testKey+".partial")))
}

func TestCompleteAfterShareChange(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()
	s, share := newTestStore(t)
	s.location = `\\old-nas\exports`
	ctx := context.Background()
	upload, err := s.StageObject(ctx, strings.NewReader("contents"), testKey)
	require.NoError(t, err)
	require.Equal(t, `\\old-nas\exports`, upload.Location)

	// The endpoint now points to another share that has neither file.
	require.NoError(t, os.Remove(share.local(smb.Path(testKey+".partial"))))
	s.location = `\\new-nas\exports`
	require.NoError(t, s.CompleteObject(ctx, upload))
	entry := hook.LastEntry()
	require.NotNil(t, entry)
	require.Equal(t, `\\old-nas\exports`, entry.Data["stagedLocation"])
	require.Contains(t, entry.Message, ".partial")

	// State from earlier versions has no location and is completed as before.
	upload.Location = ""
	hook.Reset()
	require.NoError(t, s.CompleteObject(ctx, upload))
	require.NotContains(t, hook.LastEntry().Data, "stagedLocation")
}

func TestStageReconnect(t *testing.T) {
	for _, operation := range []string{"mkdir", "open", "write", "sync", "close"} {
		t.Run(operation, func(t *testing.T) {
			s, share := newTestStore(t)
			share.failures[operation] = []error{&smb2.TransportError{Err: io.ErrUnexpectedEOF}}
			contents := bytes.Repeat([]byte("abcdefg"), 300000)
			upload, err := s.StageObject(context.Background(), bytes.NewReader(contents), testKey)
			require.NoError(t, err)
			require.Equal(t, 2, share.connections)
			require.NoError(t, s.CompleteObject(context.Background(), upload))
			final, err := os.ReadFile(share.local(smb.Path(testKey)))
			require.NoError(t, err)
			require.Equal(t, contents, final)
			if operation == "write" || operation == "sync" || operation == "close" {
				require.Equal(t, []int{os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.O_WRONLY}, share.opens)
			}
		})
	}
}

func TestStageReconnectLostWrites(t *testing.T) {
	s, share := newTestStore(t)
	conn := s.connection.(*testConnection)
	conn.reconnect = func() {
		// The server comes back without writes it had acknowledged.
		require.NoError(t, os.Truncate(share.local(smb.Path(testKey+".partial")), 0))
	}
	share.failures["write"] = []error{nil, &smb2.TransportError{Err: io.ErrUnexpectedEOF}}
	_, err := s.StageObject(context.Background(), bytes.NewReader(bytes.Repeat([]byte("x"), 3*1024*1024)), testKey)
	require.ErrorContains(t, err, "after reconnecting")
	require.Equal(t, 2, share.connections)
	require.NoFileExists(t, share.local(smb.Path(testKey)))
}

func TestCompleteReconnect(t *testing.T) {
	for _, committed := range []bool{false, true} {
		s, share := newTestStore(t)
		upload, err := s.StageObject(context.Background(), strings.NewReader("contents"), testKey)
		require.NoError(t, err)
		share.renameCommitted = committed
		share.failures["rename"] = []error{&smb2.TransportError{Err: io.EOF}}
		require.NoError(t, s.CompleteObject(context.Background(), upload))
		require.Equal(t, 2, share.connections)
		require.NoError(t, s.CompleteObject(context.Background(), upload))
		contents, err := os.ReadFile(share.local(smb.Path(testKey)))
		require.NoError(t, err)
		require.Equal(t, "contents", string(contents))
	}
}

func TestRetryLimit(t *testing.T) {
	for _, failure := range []error{
		os.ErrPermission,
		&os.PathError{Op: "write", Path: testKey, Err: syscall.ENOTDIR},
		&smb2.TransportError{Err: io.EOF},
	} {
		s, share := newTestStore(t)
		share.failures["write"] = []error{failure, failure}
		_, err := s.StageObject(context.Background(), strings.NewReader("contents"), testKey)
		require.Error(t, err)
		if _, ok := failure.(*smb2.TransportError); ok {
			require.Equal(t, 2, share.connections)
		} else {
			require.Equal(t, 1, share.connections)
		}
		require.NoFileExists(t, share.local(smb.Path(testKey)))
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestStageReadError(t *testing.T) {
	s, share := newTestStore(t)
	_, err := s.StageObject(context.Background(), failingReader{}, testKey)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, 1, share.connections)
	require.NoFileExists(t, share.local(smb.Path(testKey)))
}

func TestStoreCanceledAndClosed(t *testing.T) {
	s, share := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.StageObject(ctx, strings.NewReader("contents"), testKey)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, share.opens)
	require.NoError(t, s.Close())
	_, err = s.StageObject(context.Background(), strings.NewReader("contents"), testKey)
	require.ErrorContains(t, err, "closed")
	require.ErrorContains(t, s.CompleteObject(context.Background(), &smbUpload{Key: testKey}), "closed")
	require.Equal(t, 1, share.closes)
}

// stallingShare never answers, like a server that keeps the connection open
// but stops responding.
type stallingShare struct {
	*testShare
	ctx context.Context
}

func (s stallingShare) WithContext(ctx context.Context) smbShare {
	return stallingShare{s.testShare, ctx}
}

func (s stallingShare) stall() error {
	<-s.ctx.Done()
	return s.ctx.Err()
}

func (s stallingShare) MkdirAll(string, os.FileMode) error { return s.stall() }
func (s stallingShare) Stat(string) (os.FileInfo, error)   { return nil, s.stall() }

func TestStoreStallTimesOut(t *testing.T) {
	defer func(op time.Duration) { opTimeout = op }(opTimeout)
	opTimeout = 50 * time.Millisecond

	s, share := newTestStore(t)
	s.connection.(*testConnection).share = stallingShare{testShare: share, ctx: context.Background()}

	_, err := s.StageObject(context.Background(), strings.NewReader("contents"), testKey)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, s.CompleteObject(context.Background(), &smbUpload{Key: testKey}), context.DeadlineExceeded)
	// A timeout is not a lost connection, so the store does not reconnect.
	require.Equal(t, 1, share.connections)
}

// slowReader yields its chunks with a pause before each, like rows arriving
// slowly during a long backfill transaction.
type slowReader struct {
	chunks []string
	pause  time.Duration
}

func (r *slowReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	time.Sleep(r.pause)
	n := copy(p, r.chunks[0])
	r.chunks = r.chunks[1:]
	return n, nil
}

func TestStageSlowStreamDoesNotTimeOut(t *testing.T) {
	defer func(op time.Duration) { opTimeout = op }(opTimeout)
	opTimeout = 50 * time.Millisecond

	// Each wait for rows, and the upload as a whole, outlasts opTimeout.
	s, share := newTestStore(t)
	upload, err := s.StageObject(context.Background(), &slowReader{chunks: []string{"a,1\n", "b,2\n", "c,3\n"}, pause: 80 * time.Millisecond}, testKey)
	require.NoError(t, err)
	require.NoError(t, s.CompleteObject(context.Background(), upload))
	contents, err := os.ReadFile(share.local(smb.Path(testKey)))
	require.NoError(t, err)
	require.Equal(t, "a,1\nb,2\nc,3\n", string(contents))
}

func TestCompleteMissingStagedPartial(t *testing.T) {
	s, share := newTestStore(t)
	ctx := context.Background()
	upload, err := s.StageObject(ctx, strings.NewReader("contents"), testKey)
	require.NoError(t, err)

	// Something removes the partial file before this process completes it.
	require.NoError(t, os.Remove(share.local(smb.Path(testKey+".partial"))))
	require.ErrorContains(t, s.CompleteObject(ctx, upload), "staged by this task is missing")

	// After a restart the upload comes from state, and the file may have been
	// completed and picked up before the restart.
	s.staged = nil
	require.NoError(t, s.CompleteObject(ctx, upload))
}

func TestCheckStaged(t *testing.T) {
	s, share := newTestStore(t)
	ctx := context.Background()
	upload, err := s.StageObject(ctx, strings.NewReader("contents"), testKey)
	require.NoError(t, err)
	require.NoError(t, s.checkStaged(ctx))

	// The server fails over before the commit and comes back without the partial.
	share.failures["stat"] = []error{&smb2.TransportError{Err: io.EOF}}
	conn := s.connection.(*testConnection)
	conn.reconnect = func() {
		require.NoError(t, os.Remove(share.local(smb.Path(testKey+".partial"))))
	}
	require.ErrorContains(t, s.checkStaged(ctx), "staged by this task is missing before commit")
	require.Equal(t, 2, share.connections)

	// The server comes back with a shorter copy of the partial.
	conn.reconnect = nil
	require.NoError(t, os.WriteFile(share.local(smb.Path(testKey+".partial")), []byte("cont"), 0600))
	require.ErrorContains(t, s.checkStaged(ctx), "has 4 bytes before commit, expected 8")
	require.NoError(t, os.WriteFile(share.local(smb.Path(testKey+".partial")), []byte("contents"), 0600))
	require.NoError(t, s.checkStaged(ctx))

	// Completed files are no longer checked.
	delete(s.staged, upload.Key)
	require.NoError(t, s.checkStaged(ctx))
}

func TestCompleteReconnectLostPartial(t *testing.T) {
	s, share := newTestStore(t)
	ctx := context.Background()
	upload, err := s.StageObject(ctx, strings.NewReader("contents"), testKey)
	require.NoError(t, err)

	// The rename never applies and the server comes back without the partial.
	share.failures["rename"] = []error{&smb2.TransportError{Err: io.EOF}}
	conn := s.connection.(*testConnection)
	conn.reconnect = func() {
		require.NoError(t, os.Remove(share.local(smb.Path(testKey+".partial"))))
	}
	require.ErrorContains(t, s.CompleteObject(ctx, upload), "staged by this task is missing")
	require.Equal(t, 2, share.connections)
}

// closeStallingShare opens files whose Close never answers.
type closeStallingShare struct {
	*testShare
	ctx context.Context
}

func (s closeStallingShare) WithContext(ctx context.Context) smbShare {
	return closeStallingShare{s.testShare, ctx}
}

func (s closeStallingShare) OpenFile(name string, flag int, mode os.FileMode) (smbFile, error) {
	f, err := s.testShare.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	return closeStallingFile{f, s.ctx}, nil
}

type closeStallingFile struct {
	smbFile
	ctx context.Context
}

func (f closeStallingFile) Close() error {
	<-f.ctx.Done()
	return errors.Join(f.smbFile.Close(), f.ctx.Err())
}

func TestStageErrorCloseTimesOut(t *testing.T) {
	defer func(op time.Duration) { opTimeout = op }(opTimeout)
	opTimeout = 50 * time.Millisecond

	s, share := newTestStore(t)
	s.connection.(*testConnection).share = closeStallingShare{testShare: share, ctx: context.Background()}
	done := make(chan error, 1)
	go func() {
		_, err := s.StageObject(context.Background(), failingReader{}, testKey)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	case <-time.After(5 * time.Second):
		t.Fatal("closing the partial file after an error did not time out")
	}
}
