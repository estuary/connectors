package testcat

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/estuary/protocols/catalog"
	"github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// TestCatalog is the flow.yaml format of a catalog.
type TestCatalog struct {
	Collections      map[string]Collection      `yaml:"collections"`
	Materializations map[string]Materialization `yaml:"materializations"`
}

// Collection is a Collection representation.
type Collection struct {
	Keys   []string `yaml:"key,omitempty"`
	Schema Schema
}

// Schema represents a simple json schema.
type Schema struct {
	Type       string            `yaml:"type"`
	Properties map[string]Schema `yaml:"properties,omitempty"`
	Required   []string          `yaml:"required,omitempty"`
}

// Materialization representation.
type Materialization struct {
	Endpoint Endpoint
	Bindings []Binding
}

// Endpoint is an interface that allows custom marshaling depending on the Endpoint type.
type Endpoint interface {
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
type EndpointFlowSync struct {
	Image  string    `json:"image" yaml:"image"`
	Config ConfigMap `json:"config" yaml:"config"`
}

// EndpointType returns the proto enum endpoint type for a FlowSync materialization.
func (ep EndpointFlowSync) EndpointType() flow.EndpointType {
	return flow.EndpointType_FLOW_SINK
}

// MarshalYAML returns the yaml materialization config for a FlowSync connector.
func (ep EndpointFlowSync) MarshalYAML() (interface{}, error) {
	type Alias EndpointFlowSync
	return struct {
		FlowSink Alias `yaml:"flowSink"`
	}{
		FlowSink: Alias(ep),
	}, nil
}

// BuildCollection is a helper to automatically build a schema, parse keys and return a Collection.
func BuildCollection(in interface{}) Collection {
	var c Collection
	c.Schema, c.Keys = BuildSchema(in)
	return c
}

// BuildSchema parses a struct and builds the flow.yaml schema representation along with it's keys.
// It can handle just about any struct. To determine the name of the fields in the schema, you can use the
// flowsim tag to set the name. You can also use the key tag option to set the struct field as a key and
// required to set it as a required field. (keys are automatically required) See testdata.Basic for
// and example struct.
func BuildSchema(in interface{}) (Schema, []string) {

	var schema Schema
	var keys []string

	value := reflect.ValueOf(in)
	valueKind := value.Kind()

	// Dereference if a pointer.
	if (valueKind == reflect.Ptr && !value.IsNil()) || valueKind == reflect.Interface {
		value = value.Elem()
		valueKind = value.Kind()
	}

	switch valueKind {
	// Support maps, but it won't have any keys.
	case reflect.Map:
		schema.Type = "object"
		schema.Properties = make(map[string]Schema)
		for _, mapKey := range value.MapKeys() {
			schema.Properties[mapKey.String()], _ = BuildSchema(value.MapIndex(mapKey).Interface())
		}
	// For structs, parse all the fields.
	case reflect.Struct:
		schema.Type = "object"
		schema.Properties = make(map[string]Schema)
		for i := 0; i < value.NumField(); i++ {
			structKey := value.Type().Field(i)
			// Parse the flowsim tags to find required fields and keys.
			fieldName := structKey.Tag.Get("flowsim")
			tagOptions := []string{}
			if fieldName == "" {
				fieldName = structKey.Name
			} else {
				// Get the field name and tagOptions.
				if idx := strings.Index(fieldName, ","); idx != -1 {
					tagOptions = strings.Split(fieldName[idx+1:], ",")
					fieldName = fieldName[:idx]
				}
				// Skipped field.
				if fieldName == "-" {
					continue
				}
			}
			// If it's a key, mark it as key and as required.
			if optionsHas(tagOptions, "key") {
				keys = append(keys, "/"+fieldName)
				schema.Required = append(schema.Required, fieldName)
			} else if optionsHas(tagOptions, "required") {
				// Mark as required
				schema.Required = append(schema.Required, fieldName)
			}
			// Parse the field properties (as well as child objects).
			var fieldKeys []string
			schema.Properties[fieldName], fieldKeys = BuildSchema(value.Field(i).Interface())
			// Any child keys need to be appended to the keys slice.
			for _, key := range fieldKeys {
				keys = append(keys, "/"+fieldName+key)
			}
		}
	default:
		// All other types.
		schema.Type = SchemaType(valueKind)
	}

	return schema, keys

}

// optionHas is a helper to parse struct tags and see if it has an option.
func optionsHas(options []string, value string) bool {
	for _, s := range options {
		if s == value {
			return true
		}
	}
	return false
}

// SchemaType returns the json type for a field.
func SchemaType(t reflect.Kind) string {

	switch t {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Array, reflect.Slice:
		return "array"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.String:
		return "string"
	}
	return "unknown"

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
	tc.Materializations = make(map[string]Materialization)
	for name, materialization := range materializations {
		var bindings []Binding
		for i, binding := range materialization.Bindings {
			bindings = append(bindings, Binding{
				Source:   binding.Source,
				Resource: map[string]interface{}{"table": fmt.Sprintf("placeholder_%d", i)},
			})
		}
		tc.Materializations[name] = Materialization{
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
