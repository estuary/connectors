package mattest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildSchema(t *testing.T) {

	type schemaTest struct {
		Key1    int     `yaml:"key1"`
		Key2    bool    `yaml:"key2"`
		Boolean bool    `yaml:"boolean"`
		Integer int     `yaml:"integer"`
		Number  float32 `yaml:"number"`
		String  string  `yaml:"string"`
	}

	schema := BuildSchema(schemaTest{})

	expected := Schema{
		Type: "object",
		Properties: map[string]Schema{
			"key1":    {Type: "integer"},
			"key2":    {Type: "boolean"},
			"boolean": {Type: "boolean"},
			"integer": {Type: "integer"},
			"number":  {Type: "number"},
			"string":  {Type: "string"},
		},
	}

	require.Equal(t, expected, schema)

}
