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
// to parse. The `onRecord` function is called for each JSON document emitted by the parser. The
// data provided to the callback is from a shared buffer, and must not be retained after the
// callback returns. You need to copy the data if you need it longer than that.
func ParseStream(ctx context.Context, configPath string, inputStream io.Reader, onRecord func(json.RawMessage) error) error {
	var cmd = exec.Command(ParserExecutable, "parse", "--config-file", configPath)
	if log.IsLevelEnabled(log.DebugLevel) {
		cmd.Args = append(cmd.Args, "--log", "parser=debug")
	}
	cmd.Stdin = inputStream

	var stdout, err = cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("failed to start parser: %w", err)
	}
	go forwardStderr(stderr)

	return handleParseResults(ctx, stdout, cmd, onRecord)
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

func handleParseResults(ctx context.Context, stdout io.Reader, cmd *exec.Cmd, onRecord func(json.RawMessage) error) error {
	defer func() {
		if isRunning(cmd) {
			e := cmd.Process.Kill()
			log.WithField("error", e).Warnf("killed parser proces")
		}
	}()
	var scanner = bufio.NewScanner(stdout)
	var err error
	for scanner.Scan() {
		if err = onRecord(json.RawMessage(scanner.Bytes())); err != nil {
			return err
		}
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("reading next output: %w", err)
	}
	log.Debug("waiting for parser process to exit")
	return cmd.Wait()
}

func forwardStderr(stderr io.Reader) {
	var scanner = bufio.NewScanner(stderr)
	for scanner.Scan() {
		fmt.Fprintln(os.Stderr, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		// It's unlikely that an error here would be anything more than a red herring. If the
		// scanner does return an error, it's mostly likely due to the stderr pipe getting closed
		// when the Cmd is done, which is racey.
		log.WithField("error", err).Debug("reading parser stderr pipe failed")
	}
}

func isRunning(cmd *exec.Cmd) bool {
	return cmd != nil &&
		cmd.Process != nil &&
		(cmd.ProcessState == nil || !cmd.ProcessState.Exited())
}
