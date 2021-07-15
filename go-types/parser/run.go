package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"

	log "github.com/sirupsen/logrus"
)

const ProgramName = "parser"

// ParseStream invokes the parser using the given config file and Reader of data to parse.
// The callback is called as JSON documents are emitted by the parser, and receives
// batches of documents in JSON format. Each record includes a single trailing newline.
// The data provided to the callback is from a shared buffer, and must not be retained after the
// callback returns. You need to copy the data if you need it longer than that.
//
// If the callback returns an error, or if the context is cancelled,
// the parser is sent a SIGTERM and the error is returned.
//
// Or if the parser exits with a non-zero status, an error is returned containing
// a bounded prefix of the container's stderr output.
func ParseStream(
	ctx context.Context,
	configPath string,
	input io.Reader,
	callback func(lines []json.RawMessage) error,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var cmd = exec.Command(ProgramName, "parse", "--config-file", configPath)
	var fe = new(firstError)

	if log.IsLevelEnabled(log.DebugLevel) {
		cmd.Args = append(cmd.Args, "--log", "parser=debug")
	}

	cmd.Stdin = input
	cmd.Stdout = &parserStdout{
		onLines: callback,
		onError: func(err error) { fe.SetIfNil(err) },
	}
	cmd.Stderr = &parserStderr{}

	if err := cmd.Start(); err != nil {
		fe.SetIfNil(fmt.Errorf("starting connector: %w", err))
	}

	// Arrange for the parser to be signaled if |ctx| is cancelled.
	go func(signal func(os.Signal) error) {
		<-ctx.Done()
		_ = signal(syscall.SIGTERM)
	}(cmd.Process.Signal)

	if err := cmd.Wait(); err != nil {
		fe.SetIfNil(fmt.Errorf("%w with stderr:\n\n%s",
			err, cmd.Stderr.(*parserStderr).err.String()))
	}

	if len(cmd.Stdout.(*parserStdout).rem) != 0 {
		fe.SetIfNil(fmt.Errorf("connector exited without a final newline"))
	}

	return fe.First()
}

// GetSpec invokes the parser to get the configuration json schema. The returned schema can then be
// included directly in a connector configuration schema if desired.
func GetSpec() (json.RawMessage, error) {
	var spec, err = exec.Command(ProgramName, "spec").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute 'parser spec': %w", err)
	}
	return json.RawMessage(spec), nil
}

// parserStdout collects lines of parser output and invokes its callback.
type parserStdout struct {
	rem     []byte
	scratch []json.RawMessage

	onLines func([]json.RawMessage) error
	onError func(error)
}

func (r *parserStdout) Write(p []byte) (int, error) {
	var n = len(p)

	// If there was an unconsumed remainder, prefix it into |p|.
	if len(r.rem) != 0 {
		p = append(r.rem, p...)
		r.rem = nil
	}

	// Accumulate linebreaks of |p| into |lines|.
	var lines = r.scratch[:0]
	for {
		var pivot = bytes.IndexByte(p, '\n')
		if pivot == -1 {
			break
		}

		lines = append(lines, p[:pivot+1])
		p = p[pivot+1:]
	}

	if err := r.onLines(lines); err != nil {
		r.onError(err)
	}

	// Clone unconsumed remainder of |p| for next Write invocation.
	if len(p) != 0 {
		r.rem = append([]byte(nil), p...)
	}
	r.scratch = lines[:0]

	return n, nil
}

// parserStderr is an io.Writer that collects a bounded amount of stderr from the parser.
type parserStderr struct {
	err bytes.Buffer
}

func (r *parserStderr) Write(p []byte) (int, error) {
	var n = len(p)
	var rem = maxStderrBytes - r.err.Len()

	if rem < n {
		p = p[:rem]
	}
	r.err.Write(p)
	return n, nil
}

const maxStderrBytes = 8192
