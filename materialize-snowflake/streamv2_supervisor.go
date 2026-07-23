package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// sidecarReadyTimeout is a var so tests can shorten it.
var sidecarReadyTimeout = 30 * time.Second

// sidecarStopTimeout is a var so tests can shorten it.
var sidecarStopTimeout = 10 * time.Second

const (

	// stderrTailLines/Bytes bound the ring buffer of recent sidecar stderr
	// retained for inclusion in fatal errors.
	stderrTailLines = 64
	stderrTailBytes = 16 * 1024
)

// defaultSidecarArgv locates the sidecar as installed in the connector image.
// Tests override via SNOWPIPE_SIDECAR_PYTHON or by passing their own argv.
func defaultSidecarArgv() []string {
	var python = "/opt/venv/bin/python"
	if p := os.Getenv("SNOWPIPE_SIDECAR_PYTHON"); p != "" {
		python = p
	}
	return []string{python, "-m", "snowpipe_sidecar"}
}

// sidecarSupervisor owns the lifecycle of the Python sidecar process: spawn,
// readiness, transport establishment, failure detection, and teardown. The
// failure policy is crash-only: the supervisor never restarts the sidecar;
// callers surface its errors up the transactor, exiting the connector so that
// the runtime restarts it and recovery replays via offset tokens.
type sidecarSupervisor struct {
	cmd    *exec.Cmd
	conn   net.Conn
	tmpDir string

	authToken string

	tail *tailBuffer

	died    chan struct{} // closed when the process has exited
	waitErr error         // cmd.Wait result, valid once died is closed

	stopOnce sync.Once
}

type sidecarReadyMsg struct {
	Ready     bool   `json:"ready"`
	Transport string `json:"transport"`
	Port      int    `json:"port"`
}

// startSidecar launches argv (the sidecar command), performs the readiness
// handshake, and connects the RPC transport. A unix domain socket is used
// whenever one can be bound; otherwise the sidecar listens on an ephemeral
// localhost TCP port which it reports in its ready message.
func startSidecar(ctx context.Context, argv []string) (*sidecarSupervisor, *sidecarClient, error) {
	tmpDir, err := os.MkdirTemp("", "sfss")
	if err != nil {
		return nil, nil, fmt.Errorf("creating sidecar temp dir: %w", err)
	}

	var s = &sidecarSupervisor{
		tmpDir: tmpDir,
		tail:   newTailBuffer(stderrTailLines, stderrTailBytes),
		died:   make(chan struct{}),
	}

	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, nil, fmt.Errorf("generating sidecar auth token: %w", err)
	}
	s.authToken = hex.EncodeToString(token[:])

	// Prefer a unix domain socket; fall back to TCP over localhost only if
	// this system can't bind one (e.g. an exotic tmpdir filesystem).
	var sockPath = filepath.Join(tmpDir, "rpc.sock")
	var transportArgs []string
	if probe, err := net.Listen("unix", filepath.Join(tmpDir, "probe.sock")); err != nil {
		log.WithError(err).Warn("cannot bind unix domain socket; sidecar will use TCP over localhost")
		transportArgs = []string{"--tcp"}
	} else {
		probe.Close()
		os.Remove(filepath.Join(tmpDir, "probe.sock"))
		transportArgs = []string{"--uds", sockPath}
	}

	s.cmd = exec.CommandContext(ctx, argv[0], append(argv[1:], transportArgs...)...)
	s.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	// On ctx cancellation ask politely first; WaitDelay bounds how long we
	// wait before the runtime force-kills and closes the pipes.
	s.cmd.Cancel = func() error { return s.signal(syscall.SIGTERM) }
	s.cmd.WaitDelay = sidecarStopTimeout

	s.cmd.Env = append(os.Environ(),
		"SS_LOG_TARGET=stderr", // the SDK's native core logs to stdout by default, which would corrupt the ready handshake
		"SS_LOG_LEVEL="+sidecarCoreLogLevel(),
	)

	stdin, err := s.cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("sidecar stdin: %w", err)
	}
	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("sidecar stdout: %w", err)
	}
	stderr, err := s.cmd.StderrPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("sidecar stderr: %w", err)
	}

	if err := s.cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("starting sidecar %q: %w", argv[0], err)
	}
	log.WithFields(log.Fields{"pid": s.cmd.Process.Pid, "argv": argv}).Info("started snowpipe streaming sidecar")

	var pipesDone sync.WaitGroup
	pipesDone.Add(1)
	go func() {
		defer pipesDone.Done()
		s.relayStderr(stderr)
	}()

	// The auth token travels over stdin so it appears in no argv or environ;
	// the sidecar must echo it in the configure RPC.
	go func() {
		io.WriteString(stdin, s.authToken+"\n")
	}()

	// readyCh receives the single ready line the sidecar prints to stdout.
	var readyCh = make(chan sidecarReadyMsg, 1)
	var readyErrCh = make(chan error, 1)
	pipesDone.Add(1)
	go func() {
		defer pipesDone.Done()
		var reader = bufio.NewReader(stdout)
		line, err := reader.ReadString('\n')
		if err != nil {
			readyErrCh <- fmt.Errorf("sidecar exited before becoming ready: %w", err)
			return
		}
		var msg sidecarReadyMsg
		if err := json.Unmarshal([]byte(line), &msg); err != nil || !msg.Ready {
			readyErrCh <- fmt.Errorf("unexpected sidecar ready line %q", strings.TrimSpace(line))
			return
		}
		readyCh <- msg
		// Nothing further is expected on stdout, but drain it so the child
		// can never block writing there.
		io.Copy(io.Discard, reader)
	}()

	go func() {
		pipesDone.Wait()
		s.waitErr = s.cmd.Wait()
		close(s.died)
	}()

	var ready sidecarReadyMsg
	select {
	case ready = <-readyCh:
	case err := <-readyErrCh:
		s.kill()
		return nil, nil, s.exitError(err)
	case <-s.died:
		return nil, nil, s.exitError(fmt.Errorf("sidecar exited before becoming ready"))
	case <-time.After(sidecarReadyTimeout):
		s.kill()
		return nil, nil, s.exitError(fmt.Errorf("sidecar not ready after %s", sidecarReadyTimeout))
	case <-ctx.Done():
		s.kill()
		return nil, nil, ctx.Err()
	}

	switch ready.Transport {
	case "uds":
		s.conn, err = net.Dial("unix", sockPath)
	case "tcp":
		s.conn, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", ready.Port))
	default:
		err = fmt.Errorf("sidecar reported unknown transport %q", ready.Transport)
	}
	if err != nil {
		s.kill()
		return nil, nil, s.exitError(fmt.Errorf("connecting to sidecar: %w", err))
	}
	log.WithField("transport", ready.Transport).Debug("connected to snowpipe streaming sidecar")

	return s, newSidecarClient(s.conn, s.died, func() error { return s.exitError(nil) }), nil
}

func sidecarCoreLogLevel() string {
	if log.IsLevelEnabled(log.DebugLevel) {
		return "debug"
	}
	return "warn"
}

// relayStderr forwards the sidecar's stderr to the connector's stderr, which
// the runtime collects as ops logs. Lines that are already structured ops-log
// JSON pass through verbatim; anything else (native-core log lines, stray
// tracebacks, partial lines) is wrapped in a structured log so that raw text
// can never corrupt the ops-log stream. Every line is also retained in a
// bounded tail for inclusion in fatal errors.
func (s *sidecarSupervisor) relayStderr(stderr io.Reader) {
	var scanner = bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		var line = scanner.Text()
		if line == "" {
			continue
		}
		s.tail.add(line)

		if isOpsLogLine(line) {
			fmt.Fprintln(os.Stderr, line)
		} else {
			log.WithField("sidecarOutput", line).Info("sidecar output")
		}
	}
	if err := scanner.Err(); err != nil {
		log.WithError(err).Warn("reading sidecar stderr")
	}
}

func isOpsLogLine(line string) bool {
	if !strings.HasPrefix(line, "{") {
		return false
	}
	var probe struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
	}
	return json.Unmarshal([]byte(line), &probe) == nil && probe.Level != "" && probe.Msg != ""
}

// exitError describes the sidecar's demise, folding in the exit status and
// recent stderr so the terminal connector error is actionable even when the
// child's own logs were lost.
func (s *sidecarSupervisor) exitError(cause error) error {
	var parts []string
	if cause != nil {
		parts = append(parts, cause.Error())
	}

	select {
	case <-s.died:
		if s.waitErr != nil {
			parts = append(parts, fmt.Sprintf("sidecar process: %s", s.waitErr))
		} else if cause == nil {
			parts = append(parts, "sidecar process exited unexpectedly")
		}
	default:
		if cause == nil {
			parts = append(parts, "sidecar process failed")
		}
	}

	if tail := s.tail.contents(); tail != "" {
		parts = append(parts, fmt.Sprintf("recent sidecar output:\n%s", tail))
	}
	return fmt.Errorf("snowpipe streaming sidecar: %s", strings.Join(parts, "; "))
}

func (s *sidecarSupervisor) signal(sig syscall.Signal) error {
	if s.cmd.Process == nil {
		return nil
	}
	// Negative pid signals the whole process group, catching any children the
	// sidecar may have forked.
	return syscall.Kill(-s.cmd.Process.Pid, sig)
}

func (s *sidecarSupervisor) kill() {
	s.signal(syscall.SIGKILL)
	<-s.died
}

// stop tears the sidecar down gracefully: shutdown RPC, then SIGTERM, then
// SIGKILL. It is safe to call multiple times and with a nil client.
func (s *sidecarSupervisor) stop(client *sidecarClient) {
	s.stopOnce.Do(func() {
		defer os.RemoveAll(s.tmpDir)

		select {
		case <-s.died:
			return // already gone
		default:
		}

		if client != nil {
			var ctx, cancel = context.WithTimeout(context.Background(), rpcTimeoutShutdown)
			client.Shutdown(ctx)
			cancel()
		}
		if s.conn != nil {
			s.conn.Close()
		}

		s.signal(syscall.SIGTERM)
		select {
		case <-s.died:
		case <-time.After(sidecarStopTimeout):
			log.Warn("sidecar did not exit after SIGTERM; killing")
			s.kill()
		}
	})
}

// tailBuffer retains the most recent lines within line-count and byte bounds.
type tailBuffer struct {
	mu       sync.Mutex
	lines    []string
	maxLines int
	maxBytes int
	bytes    int
}

func newTailBuffer(maxLines, maxBytes int) *tailBuffer {
	return &tailBuffer{maxLines: maxLines, maxBytes: maxBytes}
}

func (t *tailBuffer) add(line string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, line)
	t.bytes += len(line)
	for len(t.lines) > t.maxLines || (t.bytes > t.maxBytes && len(t.lines) > 1) {
		t.bytes -= len(t.lines[0])
		t.lines = t.lines[1:]
	}
}

func (t *tailBuffer) contents() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.lines, "\n")
}
