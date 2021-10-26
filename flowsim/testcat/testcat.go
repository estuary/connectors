package testcat

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/estuary/protocols/catalog"
	"github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// TestCatalog is the flow.yaml format of a catalog.
type TestCatalog struct {
	Collections      map[string]TestCollection      `yaml:"collections"`
	Materializations map[string]TestMaterialization `yaml:"materializations"`
}

// Collection is a Collection representation.
type TestCollection struct {
	Keys   []string `yaml:"key,omitempty"`
	Schema TestSchema
}

// TestType represents one or more types
type TestType []string

// MarshalYAML returns a single value if there is only one TestType.
func (tt TestType) MarshalYAML() (interface{}, error) {
	if len(tt) == 1 {
		return tt[0], nil
	}
	return tt, nil
}

// MarshalJSON returns a single value if there is only one TestType.
func (tt TestType) MarshalJSON() ([]byte, error) {
	if len(tt) == 1 {
		return json.Marshal(tt[0])
	}
	// Otherwise marshal it as a normal string slice.
	return json.Marshal([]string(tt))
}

// Schema represents a simple json schema.
type TestSchema struct {
	Type       TestType              `yaml:"type"`
	Format     string                `yaml:"format,omitempty"`
	Properties map[string]TestSchema `yaml:"properties,omitempty"`
	Required   []string              `yaml:"required,omitempty"`
}

// Materialization representation.
type TestMaterialization struct {
	Endpoint TestEndpoint
	Bindings []Binding
}

// Endpoint is an interface that allows custom marshaling depending on the Endpoint type.
type TestEndpoint interface {
	EndpointType() flow.EndpointType
	MarshalYAML() (interface{}, error)
}

// Binding binds the source to a materialization.
type Binding struct {
	Source   string    `yaml:"source"`
	Resource ConfigMap `yaml:"resource"`
}

// ConfigMap is a simple map of configuration and resource options for a connector.
type ConfigMap map[string]interface{}

// EndpointSQLite is the SQLite representation to allow for catalog builds.
type EndpointSQLite struct {
	Path string
}

// EndpointType returns the protobuf enum type for SQLite.
func (ep EndpointSQLite) EndpointType() flow.EndpointType {
	return flow.EndpointType_SQLITE
}

// MarshalYAML returns the SQLite materialization endpoint config.
func (ep EndpointSQLite) MarshalYAML() (interface{}, error) {
	var out struct {
		Sqlite struct {
			Path string `yaml:"path"`
		} `yaml:"sqlite"`
	}
	out.Sqlite.Path = ep.Path
	return out, nil
}

// EndpointFlowSync is the FLowSync representation for the materialization for running tests.
type TestEndpointFlowSync struct {
	Image  string    `json:"image" yaml:"image"`
	Config ConfigMap `json:"config" yaml:"config"`
}

// EndpointType returns the proto enum endpoint type for a FlowSync materialization.
func (ep TestEndpointFlowSync) EndpointType() flow.EndpointType {
	return flow.EndpointType_FLOW_SINK
}

// MarshalYAML returns the yaml materialization config for a FlowSync connector.
func (ep TestEndpointFlowSync) MarshalYAML() (interface{}, error) {
	type Alias TestEndpointFlowSync
	return struct {
		FlowSink Alias `yaml:"flowSink"`
	}{
		FlowSink: Alias(ep),
	}, nil
}

// BuildCatalog invokes `flowctl check` to build the named |source| catalog and returns it.
// It converts the materialization to SQLite to not require docker container to exist.
// Once complete it puts back the original materialization without the connector config and
// resource config as the output of the Validate request on the real connector returns those.
// The config and resource config must be put back after making the real Validate request.
func BuildCatalog(ctx context.Context, tc *TestCatalog) (*catalog.BuiltCatalog, error) {

	// Store the original materializations.
	var materializations = tc.Materializations

	// Replace the materializations with an SQLite compatible binding so we can compile cleanly
	tc.Materializations = make(map[string]TestMaterialization)
	for name, materialization := range materializations {
		var bindings []Binding
		for i, binding := range materialization.Bindings {
			bindings = append(bindings, Binding{
				Source:   binding.Source,
				Resource: map[string]interface{}{"table": fmt.Sprintf("placeholder_%d", i)},
			})
		}
		tc.Materializations[name] = TestMaterialization{
			Endpoint: EndpointSQLite{Path: ":memory:"},
			Bindings: bindings,
		}
	}

	// Compile the catalog
	var workdir, err = ioutil.TempDir("", "catalog")
	if err != nil {
		return nil, fmt.Errorf("creating tempdir: %w", err)
	}
	defer os.RemoveAll(workdir)

	file, err := os.CreateTemp(workdir, "test_catalog")
	if err != nil {
		return nil, fmt.Errorf("create catalog: %w", err)
	}

	var encoder = yaml.NewEncoder(file)
	if err = encoder.Encode(tc); err != nil {
		return nil, fmt.Errorf("write catalog: %w", err)
	}

	if err = encoder.Close(); err != nil {
		return nil, fmt.Errorf("close catalog: %w", err)
	}

	if err = file.Close(); err != nil {
		return nil, fmt.Errorf("close file: %w", err)
	}

	// Invoke flowctl to compile the catalog.
	var cmd = exec.Command("flowctl", "check", "--directory", workdir, "--source", file.Name())
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err = cmd.Run(); err != nil {
		logrus.Error("dumping catalog")
		dump, _ := os.ReadFile(file.Name())
		fmt.Print(string(dump))
		return nil, fmt.Errorf("running flowctl check: %w", err)
	}

	built, err := catalog.LoadFromSQLite(filepath.Join(workdir, "catalog.db"))
	if err != nil {
		return nil, fmt.Errorf("load catalog: %w", err)
	}

	// Put the original materialization components back in the built catalog.
	for i, builtMaterialization := range built.Materializations {
		var materialization = materializations[string(builtMaterialization.Materialization)]

		// Replace the EndpointType type and EndpointSpecJson with the original.
		builtMaterialization.EndpointType = materialization.Endpoint.EndpointType()
		builtMaterialization.EndpointSpecJson, err = json.Marshal(materialization.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("replace endpoint spec json: %w", err)
		}

		// Replace the bindings with the originals
	builtBindingLoop:
		for _, builtBinding := range builtMaterialization.Bindings {
			for _, binding := range materialization.Bindings {
				if builtBinding.Collection.Collection == flow.Collection(binding.Source) {
					if builtBinding.ResourceSpecJson, err = json.Marshal(binding.Resource); err != nil {
						return nil, fmt.Errorf("replace resource spec json: %w", err)
					}
					// NOTE: The values for ResourcePath and DeltaUpdates will be wrong at this point
					// they are for SQLite and there is no way to determine this until you actually
					// call Validate on the connector. The TestCode must update these values before
					// calling Apply with the Real DeltaUpdate and ResourcePath values from the
					// Validate call.
					continue builtBindingLoop
				}
			}
			// This shouldn't happen, but throw an error if it could not match up the built binding with the original.
			return nil, fmt.Errorf("could not find original binding for binding %#vs", builtBinding)
		}
		built.Materializations[i] = builtMaterialization
	}

	return built, nil

}
