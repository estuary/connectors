// The schemalate package is a Go wrapper around the `flow-schemalate` binary for building
// firebolt queries and validating projections using the `firebolt-schema` subcommand.
package schemalate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/jsonpb"
	proto "github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// ProgramName is the name of schemalate binary built from rust.
const ProgramName = "flow-schemalate"

type BindingBundle struct {
	CreateTable         string `json:"create_table"`
	CreateExternalTable string `json:"create_external_table"`
	InsertFromTable     string `json:"insert_from_table"`
	DropTable           string `json:"drop_table"`
	DropExternalTable   string `json:"drop_external_table"`
}

type QueriesBundle struct {
	Bindings []BindingBundle
}

func GetQueriesBundle(
	spec *pf.MaterializationSpec,
) (*QueriesBundle, error) {
	var args = []string{"firebolt-schema", "query-bundle"}

	var specBytes, err = proto.Marshal(spec)
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
		return nil, fmt.Errorf("parsing queries bundle %w with stdout %s", err, out)
	}

	return &bundle, nil
}

func GetDropQuery(
	table string,
) (string, error) {
	var args = []string{"firebolt-schema", "drop-query"}

	out, err := Run(args, []byte(table))
	if err != nil {
		return "", fmt.Errorf("error running command %w", err)
	}

	return string(out), nil
}

func ValidateNewProjection(
	spec *pm.Request_Validate_Binding,
) (map[string]*pm.Response_Validated_Constraint, error) {
	var args = []string{"firebolt-schema", "validate-new-projection"}

	var specBytes, err = proto.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshalling materialization spec: %w", err)
	}

	out, err := Run(args, specBytes)
	if err != nil {
		return nil, fmt.Errorf("error running command %w", err)
	}

	var constraints map[string]json.RawMessage
	err = json.Unmarshal(out, &constraints)
	if err != nil {
		return nil, fmt.Errorf("parsing constraints map %w with stdout %s", err, out)
	}

	var finalConstraints = map[string]*pm.Response_Validated_Constraint{}
	for key, val := range constraints {
		var finalConstraint pm.Response_Validated_Constraint
		err = jsonpb.Unmarshal(bytes.NewReader(val), &finalConstraint)
		if err != nil {
			return nil, fmt.Errorf("parsing constraints map %w with stdout %s", err, out)
		}
		finalConstraints[key] = &finalConstraint
	}

	return finalConstraints, nil
}

func ValidateExistingProjection(
	existing *pf.MaterializationSpec_Binding,
	proposed *pm.Request_Validate_Binding,
) (map[string]*pm.Response_Validated_Constraint, error) {
	var args = []string{"firebolt-schema", "validate-existing-projection"}

	var req = pm.Extra_ValidateExistingProjectionRequest{
		ExistingBinding: existing,
		ProposedBinding: proposed,
	}

	var reqBytes, err = proto.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshalling validate existing projection request: %w", err)
	}

	out, err := Run(args, reqBytes)
	if err != nil {
		return nil, fmt.Errorf("error running command %w", err)
	}

	var constraints map[string]json.RawMessage
	err = json.Unmarshal(out, &constraints)
	if err != nil {
		return nil, fmt.Errorf("parsing constraints map %w with stdout %s", err, out)
	}

	var finalConstraints = map[string]*pm.Response_Validated_Constraint{}
	for key, val := range constraints {
		var finalConstraint pm.Response_Validated_Constraint
		err = jsonpb.Unmarshal(bytes.NewReader(val), &finalConstraint)
		if err != nil {
			return nil, fmt.Errorf("parsing constraints map %w with stdout %s", err, out)
		}
		finalConstraints[key] = &finalConstraint
	}

	return finalConstraints, nil
}

func ValidateBindingAgainstConstraints(
	binding *pf.MaterializationSpec_Binding,
	constraints map[string]*pm.Response_Validated_Constraint,
) error {
	var args = []string{"firebolt-schema", "validate-binding-against-constraints"}

	var req = pm.Extra_ValidateBindingAgainstConstraints{
		Binding:     binding,
		Constraints: constraints,
	}

	var reqBytes, err = proto.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshalling validate binding against constraints request: %w", err)
	}

	out, err := Run(args, reqBytes)
	if err != nil {
		return fmt.Errorf("error running command %w", err)
	}

	outString := string(out)
	if outString != "" {
		return fmt.Errorf("validation failed %s", outString)
	}

	return nil
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
	cmd.Stdin = bytes.NewReader(input)

	var out, err = cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("fetching output: %w. With stdout %s and stderr: %s", err, out, stderr.String())
	}
	os.Stderr.WriteString(stderr.String())

	return out, nil
}
