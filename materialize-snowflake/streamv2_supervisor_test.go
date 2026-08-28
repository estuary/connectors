package connector

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeSidecarArgv runs the stdlib-only protocol fake; these tests need only a
// system python3, not the snowpipe-streaming SDK.
func fakeSidecarArgv(t *testing.T) []string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	return []string{python, "testdata/fake_sidecar.py"}
}

func startFakeSidecar(t *testing.T, mode string) (*sidecarSupervisor, *sidecarClient, error) {
	t.Helper()
	if mode != "" {
		t.Setenv("FAKE_SIDECAR_MODE", mode)
	}
	sup, client, err := startSidecar(context.Background(), fakeSidecarArgv(t))
	if err == nil {
		t.Cleanup(func() { sup.stop(client) })
	}
	return sup, client, err
}

func TestSidecarHappyPath(t *testing.T) {
	sup, client, err := startFakeSidecar(t, "")
	require.NoError(t, err)

	var ctx = context.Background()
	require.NoError(t, client.Configure(ctx, sidecarProfile{Account: "acct", User: "u", URL: "https://x", PrivateKey: "k"}, sup.authToken))

	opened, err := client.OpenChannel(ctx, "DB", "SCHEMA", "TBL", "chan_0")
	require.NoError(t, err)
	require.Nil(t, opened.CommittedToken)
	require.Nil(t, opened.committedToken())

	require.NoError(t, client.Append(ctx, "chan_0", "1", "2", []byte(`[{"ID":1},{"ID":2}]`), 2))

	// The commit wait reports the channel's status, not just its token.
	committed, err := client.WaitCommit(ctx, "chan_0", "2")
	require.NoError(t, err)
	require.Equal(t, "2", *committed.CommittedToken)
	require.Zero(t, committed.RowsErrorCount)

	status, err := client.ChannelStatus(ctx, "chan_0")
	require.NoError(t, err)
	require.NotNil(t, status.CommittedToken)
	require.Equal(t, "2", *status.CommittedToken)
	// The log field carries the token itself, not the address of the pointer
	// holding it.
	require.Equal(t, "2", status.committedToken())

	// Reopen reports the committed token.
	opened, err = client.OpenChannel(ctx, "DB", "SCHEMA", "TBL", "chan_0")
	require.NoError(t, err)
	require.NotNil(t, opened.CommittedToken)
	require.Equal(t, "2", *opened.CommittedToken)

	sup.stop(client)
	select {
	case <-sup.died:
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not exit after stop")
	}
}

// TestSidecarAppendFraming pins the wire shape of an append against the sidecar
// reader which consumes it: a header line declaring the batch, then the row
// bytes verbatim, delimited by the length the header gave and nothing else. The
// rows appear nowhere in the header, which is what keeps them off the sidecar's
// JSON parsing path.
func TestSidecarAppendFraming(t *testing.T) {
	conn, sidecar := net.Pipe()
	var client = newSidecarClient(conn, make(chan struct{}), func() error { return nil })

	var rows = []json.RawMessage{
		json.RawMessage(`{"KEY":1,"DOC":{"nested":["a",1,null]}}`),
		json.RawMessage(`{"KEY":2,"S":"你好 🌟"}`),
	}
	var framed = []byte(`[` + string(rows[0]) + `,` + string(rows[1]) + `]`)
	var appended = make(chan error, 1)
	go func() {
		appended <- client.Append(context.Background(), "ch_0", "10", "11", framed, len(rows))
		// A second request must be found where the payload ended, so the reply
		// to this one is what proves the reader kept its place.
		appended <- client.CloseChannel(context.Background(), "ch_0", false)
	}()

	var reader = bufio.NewReader(sidecar)
	var header = readSidecarRequest(t, reader)
	require.Equal(t, "append", header.Op)
	require.Equal(t, "ch_0", header.Params.Channel)
	require.Equal(t, "10", header.Params.StartToken)
	require.Equal(t, "11", header.Params.EndToken)
	require.Equal(t, 2, header.Params.RowCount)
	require.NotContains(t, header.raw, "KEY")

	var payload = make([]byte, header.Params.PayloadLen)
	_, err := io.ReadFull(reader, payload)
	require.NoError(t, err)
	require.JSONEq(t, `[{"KEY":1,"DOC":{"nested":["a",1,null]}},{"KEY":2,"S":"你好 🌟"}]`, string(payload))
	require.Equal(t, `[`+string(rows[0])+`,`+string(rows[1])+`]`, string(payload))

	writeSidecarReply(t, sidecar, header.ID)
	require.NoError(t, <-appended)

	// The next header begins immediately after the payload's last byte.
	var next = readSidecarRequest(t, reader)
	require.Equal(t, "close_channel", next.Op)
	writeSidecarReply(t, sidecar, next.ID)
	require.NoError(t, <-appended)
}

// TestSidecarAppendFramingOfNoRows covers the empty batch, which the connector
// does not send but which must still frame as a payload the reader can consume
// rather than as no payload at all.
func TestSidecarAppendFramingOfNoRows(t *testing.T) {
	conn, sidecar := net.Pipe()
	var client = newSidecarClient(conn, make(chan struct{}), func() error { return nil })

	var appended = make(chan error, 1)
	go func() { appended <- client.Append(context.Background(), "ch_0", "1", "1", []byte("[]"), 0) }()

	var reader = bufio.NewReader(sidecar)
	var header = readSidecarRequest(t, reader)
	require.Equal(t, 0, header.Params.RowCount)
	require.Equal(t, 2, header.Params.PayloadLen)

	var payload = make([]byte, header.Params.PayloadLen)
	_, err := io.ReadFull(reader, payload)
	require.NoError(t, err)
	require.Equal(t, "[]", string(payload))

	writeSidecarReply(t, sidecar, header.ID)
	require.NoError(t, <-appended)
}

type sidecarRequestHeader struct {
	ID     uint64 `json:"id"`
	Op     string `json:"op"`
	Params struct {
		Channel    string `json:"channel"`
		StartToken string `json:"start_token"`
		EndToken   string `json:"end_token"`
		RowCount   int    `json:"row_count"`
		PayloadLen int    `json:"payload_len"`
	} `json:"params"`

	raw string
}

func readSidecarRequest(t *testing.T, reader *bufio.Reader) sidecarRequestHeader {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	require.NoError(t, err)

	var header sidecarRequestHeader
	require.NoError(t, json.Unmarshal(line, &header))
	header.raw = string(line)
	return header
}

func writeSidecarReply(t *testing.T, conn net.Conn, id uint64) {
	t.Helper()
	_, err := fmt.Fprintf(conn, "{\"id\":%d,\"ok\":true}\n", id)
	require.NoError(t, err)
}

func TestSidecarBadAuthRejected(t *testing.T) {
	_, client, err := startFakeSidecar(t, "")
	require.NoError(t, err)

	err = client.Configure(context.Background(), sidecarProfile{}, "wrong-token")
	var scErr *sidecarError
	require.ErrorAs(t, err, &scErr)
	require.Equal(t, "auth", scErr.Code)
}

// TestSidecarStructuredErrorCode verifies that a structured error code returned
// by the sidecar round-trips into sidecarError.Code so callers can log and
// classify it.
func TestSidecarStructuredErrorCode(t *testing.T) {
	sup, client, err := startFakeSidecar(t, "")
	require.NoError(t, err)

	var ctx = context.Background()
	require.NoError(t, client.Configure(ctx, sidecarProfile{}, sup.authToken))

	_, err = client.OpenChannel(ctx, "DB", "SCHEMA", "UNSUPPORTED_TBL", "chan_0")
	var scErr *sidecarError
	require.ErrorAs(t, err, &scErr)
	require.Equal(t, "unsupported_table", scErr.Code)
}

func TestSidecarNeverReady(t *testing.T) {
	defer func(d time.Duration) { sidecarReadyTimeout = d }(sidecarReadyTimeout)
	sidecarReadyTimeout = 2 * time.Second

	_, _, err := startFakeSidecar(t, "never_ready")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not ready after")
}

func TestSidecarExitsBeforeReady(t *testing.T) {
	_, _, err := startFakeSidecar(t, "exit_before_ready")
	require.Error(t, err)
	// The error must carry both the exit status and the stderr tail.
	require.Contains(t, err.Error(), "exit status 7")
	require.Contains(t, err.Error(), "boom: fake sidecar failed to initialize")
}

func TestSidecarCrashMidRPC(t *testing.T) {
	sup, client, err := startFakeSidecar(t, "crash_after_configure")
	require.NoError(t, err)

	var ctx = context.Background()
	require.NoError(t, client.Configure(ctx, sidecarProfile{}, sup.authToken))

	select {
	case <-sup.died:
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not crash as instructed")
	}

	_, err = client.OpenChannel(ctx, "DB", "SCHEMA", "TBL", "chan_0")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exit status 9")
	require.Contains(t, err.Error(), "fake native crash")
}

func TestSidecarHangSurfacesTimeout(t *testing.T) {
	sup, client, err := startFakeSidecar(t, "hang_on_append")
	require.NoError(t, err)

	var ctx = context.Background()
	require.NoError(t, client.Configure(ctx, sidecarProfile{}, sup.authToken))

	shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = client.Append(shortCtx, "chan_0", "1", "1", []byte("[]"), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out or cancelled")

	// A timed-out RPC poisons the client; subsequent calls fail fast.
	err = client.Configure(ctx, sidecarProfile{}, sup.authToken)
	require.Error(t, err)
}

func TestSidecarStopEscalatesToKill(t *testing.T) {
	defer func(d time.Duration) { sidecarStopTimeout = d }(sidecarStopTimeout)
	sidecarStopTimeout = 2 * time.Second

	sup, client, err := startFakeSidecar(t, "ignore_sigterm")
	require.NoError(t, err)

	// Poison the RPC path so stop's shutdown RPC can't succeed. The fake
	// ignores SIGTERM, so only the SIGKILL escalation can end it.
	sup.conn.Close()
	var done = make(chan struct{})
	go func() {
		sup.stop(client)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2*sidecarStopTimeout + 5*time.Second):
		t.Fatal("stop did not complete")
	}
	select {
	case <-sup.died:
	default:
		t.Fatal("sidecar still running after stop")
	}
	require.Contains(t, sup.waitErr.Error(), "killed")
}

// TestSidecarKillReportsSignalFailure covers a supervisor whose process cannot
// be signalled and whose exit has therefore never been observed: kill must
// report why rather than wait for an exit which can never come.
func TestSidecarKillReportsSignalFailure(t *testing.T) {
	var sup = &sidecarSupervisor{
		// A pid far above any this system has allocated, so that signalling its
		// process group fails with ESRCH.
		cmd:  &exec.Cmd{Process: &os.Process{Pid: 1 << 30}},
		died: make(chan struct{}),
		tail: newTailBuffer(stderrTailLines, stderrTailBytes),
	}

	var done = make(chan error, 1)
	go func() { done <- sup.kill() }()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "killing sidecar process group")
	case <-time.After(10 * time.Second):
		t.Fatal("kill blocked on an exit which cannot happen")
	}
}

// TestSidecarKillTimesOutAwaitingExit covers the signal succeeding while the
// exit is never observed: kill must give up rather than block indefinitely.
func TestSidecarKillTimesOutAwaitingExit(t *testing.T) {
	defer func(d time.Duration) { sidecarKillTimeout = d }(sidecarKillTimeout)
	sidecarKillTimeout = 500 * time.Millisecond

	var sup = &sidecarSupervisor{
		// Signalling our own process group would succeed but kill the test
		// binary, so signal nothing at all and let the wait do the work.
		cmd:  &exec.Cmd{},
		died: make(chan struct{}),
		tail: newTailBuffer(stderrTailLines, stderrTailBytes),
	}

	var done = make(chan error, 1)
	go func() { done <- sup.kill() }()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "did not exit within")
	case <-time.After(10 * time.Second):
		t.Fatal("kill blocked past its own timeout")
	}
}

func TestSidecarProcessGroupSignal(t *testing.T) {
	sup, client, err := startFakeSidecar(t, "")
	require.NoError(t, err)

	// The child must be in its own process group, distinct from ours.
	pgid, err := syscall.Getpgid(sup.cmd.Process.Pid)
	require.NoError(t, err)
	require.Equal(t, sup.cmd.Process.Pid, pgid)
	require.NotEqual(t, syscall.Getpgrp(), pgid)

	sup.stop(client)
}

func TestTailBuffer(t *testing.T) {
	var tb = newTailBuffer(3, 1024)
	for i := 0; i < 5; i++ {
		tb.add(fmt.Sprintf("line-%d", i))
	}
	require.Equal(t, "line-2\nline-3\nline-4", tb.contents())

	tb = newTailBuffer(100, 10)
	tb.add(strings.Repeat("a", 8))
	tb.add(strings.Repeat("b", 8))
	require.Equal(t, strings.Repeat("b", 8), tb.contents())
}

func TestIsOpsLogLine(t *testing.T) {
	require.True(t, isOpsLogLine(`{"level":"info","msg":"hello","fields":{}}`))
	require.False(t, isOpsLogLine(`2026-07-17T00:00:00Z | INFO | core (1) | not json`))
	require.False(t, isOpsLogLine(`{"other":"json"}`))
	require.False(t, isOpsLogLine(`Traceback (most recent call last):`))
}
