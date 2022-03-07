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

// DateSpec configures a date field in elastic search schema.
type DateSpec struct {
	Format string `json:"format"`
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (DateSpec) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Format":
		return "Format of the date. " +
			"See https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html."
	default:
		return ""
	}
}

// KeywordSpec configures a keyword field for elastic search schema.
type KeywordSpec struct {
	IgnoreAbove int `json:"ignore_above"`
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (KeywordSpec) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "IgnoreAbove":
		return "Strings longer than the ignore_above setting will not be indexed or stored. " +
			"See https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html"
	default:
		return ""
	}
}

// TextSpec configures a text field for elastic search schema.
type TextSpec struct {
	DualKeyword        bool `json:"dual_keyword"`
	KeywordIgnoreAbove int  `json:"keyword_ignore_above"`
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (TextSpec) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "DualKeyword":
		return "Whether or not to specify the field as text/keyword dual field."

	case "KeywordIgnoreAbove":
		return "Effective only if DualKeyword is enabled. Strings longer than the ignore_above setting will not be indexed or stored. " +
			"See https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html"
	default:
		return ""
	}
}

// ElasticFieldType specifies the type to override the field with.
type ElasticFieldType struct {
	// A snake_case string corresponding to a enum type of ESBasicType
	// defined in src/elastic_search_data_types.rs
	FieldType string `json:"field_type"`

	DateSpec    DateSpec    `json:"date_spec,omitempty"`
	KeywordSpec KeywordSpec `json:"keyword_spec,omitempty"`
	TextSpec    TextSpec    `json:"text_spec,omitempty"`
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (ElasticFieldType) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "FieldType":
		return "The elastic search field data types. " +
			"Supported types include: boolean, date, double, geo_point, geo_shape, keyword, long, null, text."
	case "DateSpec":
		return "Spec of the date field, effective if field_type is 'date'. " +
			"See https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html"
	case "KeywordSpec":
		return "Spec of the keyword field, effective if field_type is 'keyword'. " +
			"See https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html"
	case "TextSpec":
		return "Spec of the text field, effective if field_type is 'text'."
	default:
		return ""
	}
}

// MarshalJSON provides customized marshalJSON of ElasticFieldType
func (e ElasticFieldType) MarshalJSON() ([]byte, error) {
	var m = make(map[string]interface{})
	var spec interface{}
	switch e.FieldType {
	case "date":
		spec = e.DateSpec
	case "keyword":
		spec = e.KeywordSpec
	case "text":
		spec = e.TextSpec
	default:
		spec = nil
	}

	m["type"] = e.FieldType
	if spec != nil {
		if specJson, err := json.Marshal(spec); err != nil {
			return nil, err
		} else if err = json.Unmarshal(specJson, &m); err != nil {
			return nil, err
		}
	}

	return json.Marshal(m)
}

// FieldOverride specifies which field in the resulting elastic search schema
// and how it is overridden. This structure matches the JSON that's expected by the `--es-type`
// argument of the `elasticsearch-scheama` subcommand.
type FieldOverride struct {
	Pointer string           `json:"pointer"`
	EsType  ElasticFieldType `json:"es_type"`
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (FieldOverride) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Pointer":
		return "A '/'-delimitated json pointer to the location of the overridden field."
	case "EsType":
		return "The overriding elastic search data type of the field."
	default:
		return ""
	}
}

// RunSchemaBuilder is a wrapper in GO around the flow-schemalate CLI
func RunSchemaBuilder(
	schemaJSON json.RawMessage,
	overrides []FieldOverride,
) ([]byte, error) {
	var args = []string{"elasticsearch-schema"}
	for _, fo := range overrides {
		if typeOverride, err := json.Marshal(&fo); err != nil {
			return nil, fmt.Errorf("marshalling field override: %w", err)
		} else {
			args = append(args, "--es-type", string(typeOverride))
		}
	}

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
