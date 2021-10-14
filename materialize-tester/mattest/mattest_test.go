package mattest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestTestCatalogMarshalYAML(t *testing.T) {

	type schemaTest struct {
		Key1    int     `yaml:"key1,required"`
		Key2    bool    `yaml:"key2,required"`
		Boolean bool    `yaml:"boolean"`
		Integer int     `yaml:"integer"`
		Number  float32 `yaml:"number"`
		String  string  `yaml:"string"`
	}
	schema := BuildSchema(schemaTest{})

	c := TestCatalog{
		Collections: map[string]Collection{
			"coltest": {
				Schema: schema,
				Keys:   []string{"/key1", "/key2"},
			},
		},
		Materializations: map[string]Materialization{
			"mattest": {
				Endpoint: EndpointFlowSync{
					Image: "materialize-postgres:local",
					Config: map[string]interface{}{
						"host":     "127.0.0.1",
						"user":     "flow",
						"password": "flow",
					},
				},
				Bindings: []Binding{
					{
						Source: "coltest",
						Resource: map[string]interface{}{
							"table": "key_value",
						},
					},
				},
			},
		},
	}
	composed, err := yaml.Marshal(c)
	require.Nil(t, err)
	composedMap := make(map[string]interface{})
	err = yaml.Unmarshal(composed, &composedMap)
	require.Nil(t, err)

	parsed := make(map[string]interface{})
	err = yaml.Unmarshal([]byte(`
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
   flowSink:
    image: materialize-postgres:local
    config:
     host: 127.0.0.1
     user: flow
     password: flow
  bindings:
  - source: coltest
    resource:
     table: key_value
`), &parsed)
	require.Nil(t, err)

	require.Equal(t, parsed, composedMap)

}
