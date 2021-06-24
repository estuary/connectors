package parser

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"os/exec"
)

const ParserExecutable = "parser"

// Result represents asynchronous results from the parser. Each Result will include either an Error
// or a Document, but not both.
type Result struct {
	// Error represents a terminal error from the parser. If this is not nill, then no more results
	// will be sent.
	Error error
	// Successfully parsed document, formatted as a JSON object on a single line.
	Document json.RawMessage
}

// ParseStream invokes the parser using the given config file and inputStream representing the data
// to parse. The results from the parser are provided asynchronously on the returned channel. An
// error is returned directly only if there's a problem spawning the child process. Parsing errors
// are always reported on the returned channel.
func ParseStream(ctx context.Context, configPath string, inputStream io.Reader) (<-chan Result, error) {
	var cmd = exec.Command(ParserExecutable, "parse", "--config-file", configPath)
	cmd.Stdin = inputStream

	var stdout, err = cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start parser: %w", err)
	}
	go forwardStderr(stderr)

	var resultCh = make(chan Result)
	go handleParseResults(ctx, stdout, cmd, resultCh)
	return resultCh, nil
}

// GetSpec invokes the parser to get the configuration json schema. The returned schema can then be
// included directly in a connector configuration schema if desired.
func GetSpec() (json.RawMessage, error) {
	var spec, err = exec.Command(ParserExecutable, "spec").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute 'parser spec': %w", err)
	}
	return json.RawMessage(spec), nil
}

// TODO: This function is messy as hell. Give it some love
func handleParseResults(ctx context.Context, stdout io.Reader, cmd *exec.Cmd, resultCh chan<- Result) {
	defer func() {
		if isRunning(cmd) {
			e := cmd.Process.Kill()
			log.WithField("error", e).Warnf("killed parser proces")
		}
	}()
	var scanner = bufio.NewScanner(stdout)
	var err error
	for scanner.Scan() && err == nil {
		select {
		case <-ctx.Done():
			return
		case resultCh <- Result{Document: json.RawMessage(scanner.Bytes())}:
		}
	}
	if err == nil {
		err = scanner.Err()
	}
	if err == nil {
		log.Debug("waiting for parser process to exit")
		err = cmd.Wait()
	}
	if err != nil {
		log.WithField("error", err).Errorf("parser failed")
		select {
		case <-ctx.Done():
			return
		case resultCh <- Result{Error: err}:
		}
	}
	close(resultCh)
}

// TODO: consider logging parser stderr instead of writing directly to stderr
func forwardStderr(stderr io.Reader) {
	var scanner = bufio.NewScanner(stderr)
	for scanner.Scan() {
		fmt.Fprintln(os.Stderr, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.WithField("error", err).Errorf("reading parser stderr pipe failed")
	}
}

func isRunning(cmd *exec.Cmd) bool {
	return cmd != nil &&
		cmd.Process != nil &&
		(cmd.ProcessState == nil || !cmd.ProcessState.Exited())
}
