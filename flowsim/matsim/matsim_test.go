package matsim

import (
	"testing"

	"github.com/estuary/connectors/flowsim/testcat"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestTestCatalogMarshalYAML(t *testing.T) {

	type schemaTest struct {
		Key1        int     `flowsim:"key1,key"`
		Key2        bool    `flowsim:"key2,key"`
		Boolean     bool    `flowsim:"boolean"`
		Integer     int     `flowsim:"integer"`
		NumberField float32 `flowsim:"numberField"`
		String      string  `flowsim:"string"`
	}

	c := testcat.TestCatalog{
		Collections: map[string]testcat.Collection{
			"coltest": testcat.BuildCollection(schemaTest{}),
		},
		Materializations: map[string]testcat.Materialization{
			"mattest": {
				Endpoint: testcat.EndpointFlowSync{
					Image: "materialize-postgres:local",
					Config: map[string]interface{}{
						"host":     "127.0.0.1",
						"user":     "flow",
						"password": "flow",
					},
				},
				Bindings: []testcat.Binding{
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
     numberField: { type: number }
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
