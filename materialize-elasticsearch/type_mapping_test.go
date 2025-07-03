package main

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPropForProjection(t *testing.T) {
	tests := []struct {
		name string
		in   *pf.Projection
		want property
	}{
		{
			name: "array with string items - nullable",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array", "null"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"string", "null"},
					},
				},
			},
			want: property{Type: elasticTypeText},
		},
		{
			name: "array with string items - not nullable",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"string"},
					},
				},
			},
			want: property{Type: elasticTypeText},
		},
		{
			name: "array with object items",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"object"},
					},
				},
			},
			want: objProp(),
		},
		{
			name: "array with array items",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"array"},
					},
				},
			},
			want: objProp(),
		},
		{
			name: "array with multiple item types",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"object", "string"},
					},
				},
			},
			want: objProp(),
		},
		{
			name: "array with unknown item types",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
				},
			},
			want: objProp(),
		},
		{
			name: "multiple types",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array", "string", "object"},
				},
			},
			want: objProp(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, propForProjection(tt.in, tt.in.Inference.Types, fieldConfig{}))
		})
	}
}
