package schemabuilder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
)

// ProgramName is the name of schema builder binary built from rust.
const ProgramName = "schema-builder"

// DateSpec configures a date field in elastic search schema.
// https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
type DateSpec struct {
	Format string `json:"format"`
}

// KeywordSpec configures a keyword field for elastic search schema.
type KeywordSpec struct {
	//https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html
	IgnoreAbove int `json:"ignore_above"`
	// Whether or not to specify the field as text/keyword dual field.
	DualText bool `json:"dual_text"`
}

// ElasticFieldType specifies the type to override the field with.
type ElasticFieldType struct {
	// A snake_case string corresponding to a enum type of ESBasicType
	// defined in src/elastic_search_data_types.rs
	FieldType string
	// Effective if FieldType is "date"
	DateSpec DateSpec
	// Effective if FieldType is "keyword"
	KeywordSpec KeywordSpec
}

func (e *ElasticFieldType) toMap() map[string]interface{} {
	var m = make(map[string]interface{})
	var spec interface{}
	switch e.FieldType {
	case "date":
		spec = e.DateSpec
	case "keyword":
		spec = e.KeywordSpec
	default:
		spec = nil
	}

	m[e.FieldType] = spec
	return m
}

// FieldOverride specifies which field in the resulting elastic search schema
// and how it is overridden.
type FieldOverride struct {
	// A '/'-delimitated json pointer to the location of the overridden field.
	Pointer string
	// The overriding type.
	EsType ElasticFieldType
}

func (f *FieldOverride) toMap() map[string]interface{} {
	return map[string]interface{}{
		"pointer": f.Pointer,
		"es_type": f.EsType.toMap(),
	}
}

// RunSchemaBuilder is a wrapper in GO around rust schema-builder.
func RunSchemaBuilder(
	schemaURI string,
	schemaJSON string,
	overrides []*FieldOverride,
) ([]byte, error) {
	var cmd = exec.Command(ProgramName)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("getting stdin pipeline: %w", err)
	}

	var overrideMap = make([](map[string]interface{}), 0, len(overrides))
	for _, override := range overrides {
		overrideMap = append(overrideMap, override.toMap())
	}
	input, err := json.Marshal(map[string]interface{}{
		"schema_uri":  schemaURI,
		"schema_json": schemaJSON,
		"overrides":   overrideMap,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	go func() {
		defer stdin.Close()
		stdin.Write(input)
	}()

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("fetching output: %w. With stderr: %s", err, stderr.String())
	}
	return out, nil
}
