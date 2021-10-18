package testcat

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildSchema(t *testing.T) {

	type schemaTest struct {
		Key1    int     `flowsim:"key1,key"`
		Key2    bool    `flowsim:"key2,key"`
		Boolean bool    `flowsim:"boolean"`
		Integer int     `flowsim:"integer"`
		Number  float32 `flowsim:"number"`
		String  string  `flowsim:"string"`
	}

	schema, keys := BuildSchema(schemaTest{})

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
		Required: []string{"key1", "key2"},
	}
	require.Equal(t, expected, schema)
	require.Equal(t, []string{"/key1", "/key2"}, keys)
}

func TestBuildNestedSchema(t *testing.T) {

	type schemaTest struct {
		Key1    int     `flowsim:"key1,key"`
		Key2    bool    `flowsim:"key2,key"`
		Boolean bool    `flowsim:"boolean"`
		Integer int     `flowsim:"integer"`
		Number  float32 `flowsim:"number"`
		String  string  `flowsim:"string"`
		Object  struct {
			NestedKey1 string `flowsim:"nested_key1,required"`
		} `flowsim:"object"`
	}

	schema, keys := BuildSchema(schemaTest{})

	expected := Schema{
		Type: "object",
		Properties: map[string]Schema{
			"key1":    {Type: "integer"},
			"key2":    {Type: "boolean"},
			"boolean": {Type: "boolean"},
			"integer": {Type: "integer"},
			"number":  {Type: "number"},
			"string":  {Type: "string"},
			"object": {
				Type: "object",
				Properties: map[string]Schema{
					"nested_key1": {Type: "string"},
				},
				Required: []string{"nested_key1"},
			},
		},
		Required: []string{"key1", "key2"},
	}

	require.Equal(t, expected, schema)
	require.Equal(t, []string{"/key1", "/key2"}, keys)

}
