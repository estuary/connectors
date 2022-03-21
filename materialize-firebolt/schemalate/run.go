// The schemalate package is a Go wrapper around the `flow-schemalate` binary for building
// firebolt queries and validating projections using the `firebolt-schema` subcommand.
package schemalate

import (
	"bytes"
	"encoding/json"
	"fmt"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	proto "github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"os/exec"
)

// ProgramName is the name of schemalate binary built from rust.
const ProgramName = "flow-schemalate"

type BindingBundle struct {
	CreateTable         string `json:"create_table"`
	CreateExternalTable string `json:"create_external_table"`
	InsertFromTable     string `json:"insert_from_table"`
}

type QueriesBundle struct {
	Bindings []BindingBundle
}

func GetQueriesBundle(
	spec *pf.MaterializationSpec,
) (*QueriesBundle, error) {
	var args = []string{"firebolt-schema", "--query-bundle"}

	specBytes, err := proto.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshalling materialization spec: %w", err)
	}

	out, err := Run(args, specBytes)
	if err != nil {
		return nil, fmt.Errorf("error running command %w", err)
	}

	var bundle QueriesBundle
	err = json.Unmarshal(out, &bundle)
	if err != nil {
		return nil, fmt.Errorf("parsing queries bundle %w", err)
	}

	return &bundle, nil
}

func ValidateNewProjection(
	spec *pm.ValidateRequest_Binding,
) (map[string]*pm.Constraint, error) {
	var args = []string{"firebolt-schema", "--validate-new-projection"}

	specBytes, err := proto.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshalling materialization spec: %w", err)
	}

	out, err := Run(args, specBytes)
	if err != nil {
		return nil, fmt.Errorf("error running command %w", err)
	}

	var constraints map[string]*pm.Constraint
	err = json.Unmarshal(out, &constraints)
	if err != nil {
		return nil, fmt.Errorf("parsing queries bundle %w", err)
	}

	return constraints, nil
}

func Run(
	args []string,
	input []byte,
) ([]byte, error) {
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
		var n, writeErr = stdin.Write(input)
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
