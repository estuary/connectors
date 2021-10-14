package mattest

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/estuary/protocols/catalog"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

/*
collections:
  coltest:
    schema:
      type: object
      properties:
        key1: { type: integer }
        key2: { type: boolean }
        boolean: { type: boolean }
        integer: { type: integer }
        number: { type: number }
        string: { type: string }
      required: [key1, key2]
    key: [/key1, /key2]

materializations:
  mattest:
    endpoint:
      #sqlite:
      #  path: ":memory:"
      flowSink:
        image: materialize-postgres:local
        config:
          host: 127.0.0.1
          user: flow
          password: flow
    bindings:
      - source: test
        resource:
          table: key_value

}
*/

type TestCatalog struct {
	Collections      map[string]Collection      `yaml:"collections"`
	Materializations map[string]Materialization `yaml:"materializations"`
}

type Collection struct {
	Keys   []string `yaml:"key,omitempty"`
	Schema Schema
}

type Schema struct {
	Type       string            `yaml:"type"`
	Properties map[string]Schema `yaml:"properties,omitempty"`
	Required   []string          `yaml:"required,omitempty"`
}

type Materialization struct {
	Endpoint Endpoint
	Bindings []Binding
}

type Endpoint interface {
	MarshalYAML() (interface{}, error)
}

type Binding struct {
	Source   string    `yaml:"source"`
	Resource ConfigMap `yaml:"resource"`
}

type EndpointFlowSync struct {
	Image  string `yaml:"image"`
	Config ConfigMap
}

type ConfigMap map[string]interface{}

func (ep EndpointFlowSync) MarshalYAML() (interface{}, error) {
	type Alias EndpointFlowSync
	return struct {
		FlowSink Alias `yaml:"flowSink"`
	}{
		FlowSink: Alias(ep),
	}, nil
}

func BuildSchema(in interface{}) Schema {

	var schema Schema

	value := reflect.ValueOf(in)
	valueKind := value.Kind()

	// Dereference if a pointer.
	if (valueKind == reflect.Ptr && !value.IsNil()) || valueKind == reflect.Interface {
		value = value.Elem()
		valueKind = value.Kind()
	}

	switch valueKind {
	case reflect.Map:
		schema.Type = "object"
		schema.Properties = make(map[string]Schema)
		for _, mapKey := range value.MapKeys() {
			schema.Properties[mapKey.String()] = BuildSchema(value.MapIndex(mapKey).Interface())
		}
	case reflect.Struct:
		schema.Type = "object"
		schema.Properties = make(map[string]Schema)
		for i := 0; i < value.NumField(); i++ {
			structKey := value.Type().Field(i)
			// Utilize any yaml tags if they are available
			fieldName := structKey.Tag.Get("yaml")
			tagOptions := []string{}
			if fieldName == "" {
				fieldName = structKey.Name
			} else {
				// Omit any tag options.
				if idx := strings.Index(fieldName, ","); idx != -1 {
					tagOptions = strings.Split(fieldName[idx+1:], ",")
					fieldName = fieldName[:idx]
				}
				// Don't encode
				if fieldName == "-" {
					continue
				}
			}
			schema.Properties[fieldName] = BuildSchema(value.Field(i).Interface())
			if optionsHas(tagOptions, "required") {
				schema.Required = append(schema.Required, fieldName)
			}
		}
	default:
		schema.Type = SchemaType(valueKind)
	}

	return schema

}

func optionsHas(options []string, value string) bool {
	for _, s := range options {
		if s == value {
			return true
		}
	}
	return false
}

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

// BuildCatalog invokes `flowctl check` to build the named |source|
// catalog and returns it.
func BuildCatalog(ctx context.Context, tc *TestCatalog, network string) (*catalog.BuiltCatalog, error) {

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

	var cmd = exec.Command("flowctl", "check", "--directory", workdir, "--source", file.Name(), "--network", network)
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

	return built, nil

}
