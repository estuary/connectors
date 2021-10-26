package testcat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildTestSchema(t *testing.T) {

	type schemaTest struct {
		Key1     int       `flowsim:"key1,key"`
		Key2     bool      `flowsim:"key2,key"`
		Boolean  bool      `flowsim:"boolean"`
		Integer  int       `flowsim:"integer"`
		Number   float32   `flowsim:"number"`
		String   string    `flowsim:"string"`
		DateTime time.Time `flowsim:"dateTime,required"`
	}

	schema, keys, err := BuildSchema(schemaTest{})
	require.NoError(t, err)

	expected := TestSchema{
		Type: []string{"object"},
		Properties: map[string]TestSchema{
			"key1":     {Type: []string{"integer"}},
			"key2":     {Type: []string{"boolean"}},
			"boolean":  {Type: []string{"boolean"}},
			"integer":  {Type: []string{"integer"}},
			"number":   {Type: []string{"number"}},
			"string":   {Type: []string{"string"}},
			"dateTime": {Type: []string{"string"}, Format: "date-time"},
		},
		Required: []string{"key1", "key2", "dateTime"},
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

	schema, keys, err := BuildSchema(schemaTest{})
	require.NoError(t, err)

	expected := TestSchema{
		Type: []string{"object"},
		Properties: map[string]TestSchema{
			"key1":    {Type: []string{"integer"}},
			"key2":    {Type: []string{"boolean"}},
			"boolean": {Type: []string{"boolean"}},
			"integer": {Type: []string{"integer"}},
			"number":  {Type: []string{"number"}},
			"string":  {Type: []string{"string"}},
			"object": {
				Type: []string{"object"},
				Properties: map[string]TestSchema{
					"nested_key1": {Type: []string{"string"}},
				},
				Required: []string{"nested_key1"},
			},
		},
		Required: []string{"key1", "key2"},
	}

	require.Equal(t, expected, schema)
	require.Equal(t, []string{"/key1", "/key2"}, keys)

}

type schemaCustom struct {
	Key1    int     `flowsim:"key1,key"`
	Key2    bool    `flowsim:"key2,key"`
	Boolean bool    `flowsim:"boolean"`
	Integer int     `flowsim:"integer"`
	Number  float32 `flowsim:"number"`
	String  string  `flowsim:"string"`
}

func (sc schemaCustom) BuildSchema() (TestSchema, []string, error) {
	return TestSchema{
		Type: []string{"object"},
		Properties: map[string]TestSchema{
			"keyA":   {Type: []string{"string"}},
			"keyB":   {Type: []string{"integer"}},
			"Field1": {Type: []string{"string"}},
		},
		Required: []string{"abc", "123"},
	}, []string{"/abc", "/123"}, nil
}

func TestBuildCustomSchema(t *testing.T) {

	schema, keys, err := BuildSchema(schemaCustom{})
	require.NoError(t, err)

	expected := TestSchema{
		Type: []string{"object"},
		Properties: map[string]TestSchema{
			"keyA":   {Type: []string{"string"}},
			"keyB":   {Type: []string{"integer"}},
			"Field1": {Type: []string{"string"}},
		},
		Required: []string{"abc", "123"},
	}

	require.Equal(t, expected, schema)
	require.Equal(t, []string{"/abc", "/123"}, keys)

}
