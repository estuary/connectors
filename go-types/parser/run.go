package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"syscall"

	"github.com/estuary/connectors/go-types/firsterror"
	"github.com/estuary/connectors/go-types/jsonl"
	log "github.com/sirupsen/logrus"
)

const ProgramName = "parser"

// ParseStream invokes the parser using the given config file and inputStream representing the data
// to parse. The `onRecord` function is called for each JSON document emitted by the parser. The
// data provided to the callback is from a shared buffer, and must not be retained after the
// callback returns. You need to copy the data if you need it longer than that.
func ParseStream(ctx context.Context, configPath string, inputStream io.Reader, onRecord func(json.RawMessage) error) error {
	var cmd = exec.Command(ProgramName, "parse", "--config-file", configPath)
	defer func() {
		if isRunning(cmd) {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
	}()
	if log.IsLevelEnabled(log.DebugLevel) {
		cmd.Args = append(cmd.Args, "--log", "parser=debug")
	}
	cmd.Stdin = inputStream

	var firstError = &firsterror.Collector{}
	var stdoutJson, stderrJson json.RawMessage
	cmd.Stdout = &jsonl.Stream{
		OnNew: func() interface{} { return &stdoutJson },
		OnDecode: func(val interface{}) error {
			return onRecord(stdoutJson)
		},
		OnError: func(err error) {
			firstError.OnError(err)
		},
	}
	cmd.Stderr = &jsonl.Stream{
		OnNew: func() interface{} { return &stderrJson },
		OnDecode: func(val interface{}) error {
			return onRecord(stdoutJson)
		},
		OnError: func(err error) {
			firstError.OnError(err)
		},
	}

	firstError.OnError(cmd.Run())
	return firstError.First()
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

func isRunning(cmd *exec.Cmd) bool {
	return cmd != nil &&
		cmd.Process != nil &&
		(cmd.ProcessState == nil || !cmd.ProcessState.Exited())
}
