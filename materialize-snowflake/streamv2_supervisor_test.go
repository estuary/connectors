package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	tok, err := client.OpenChannel(ctx, "DB", "SCHEMA", "TBL", "chan_0")
	require.NoError(t, err)
	require.Nil(t, tok)

	var rows = []json.RawMessage{json.RawMessage(`{"ID":1}`), json.RawMessage(`{"ID":2}`)}
	require.NoError(t, client.Append(ctx, "chan_0", "base:0", rows))
	require.NoError(t, client.WaitCommit(ctx, "chan_0", "base:0"))

	status, err := client.ChannelStatus(ctx, "chan_0")
	require.NoError(t, err)
	require.NotNil(t, status.CommittedToken)
	require.Equal(t, "base:0", *status.CommittedToken)

	// Reopen reports the committed token.
	tok, err = client.OpenChannel(ctx, "DB", "SCHEMA", "TBL", "chan_0")
	require.NoError(t, err)
	require.NotNil(t, tok)
	require.Equal(t, "base:0", *tok)

	sup.stop(client)
	select {
	case <-sup.died:
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not exit after stop")
	}
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
	err = client.Append(shortCtx, "chan_0", "base:0", nil)
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
