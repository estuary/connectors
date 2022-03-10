// The schemabuilder package is a Go wrapper around the `flow-schemalate` binary for building
// elasticsearch schemas using the `elasticsearch-schema` subcommand. For now, this package is
// specific to the `elasticsearch-schema` subcommand, and doesn't wrap any other subcommands
// provided by flow-schemalate.
package schemabuilder

import (
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os/exec"
)

// ProgramName is the name of schemalate binary built from rust.
const ProgramName = "flow-schemalate"

// RunSchemaBuilder is a wrapper in GO around the flow-schemalate CLI
func RunSchemaBuilder(
	schemaJSON json.RawMessage,
) ([]byte, error) {
	var args = []string{"firebolt-schema"}

	log.WithFields(log.Fields{
		"args": args,
	}).Debug("resolved flow-schemalate args")
	var cmd = exec.Command(ProgramName, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("getting stdin pipeline: %w", err)
	}

	go func() {
		defer stdin.Close()
		var n, writeErr = stdin.Write([]byte(schemaJSON))
		var entry = log.WithFields(log.Fields{
			"nBytes": n,
			"error":  writeErr,
		})
		if writeErr == nil {
			entry.Debug("finished writing json schema to schemalate")
		} else {
			entry.Error("failed to write json schema to schemalate")
		}
	}()

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("fetching output: %w. With stderr: %s", err, stderr.String())
	}
	return out, nil
}
