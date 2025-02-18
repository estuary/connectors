package main

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPropForProjection(t *testing.T) {
	tests := []struct {
		name string
		in   pf.Projection
		want mappedProperty
	}{
		{
			name: "array with string items - nullable",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array", "null"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"string", "null"},
					},
				},
				Ptr: "/foo",
			},
			want: mappedProperty{Type: elasticTypeText},
		},
		{
			name: "array with string items - not nullable",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"string"},
					},
				},
				Ptr: "/foo",
			},
			want: mappedProperty{Type: elasticTypeText},
		},
		{
			name: "array with object items",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"object"},
					},
				},
				Ptr: "/foo",
			},
			want: objProp(),
		},
		{
			name: "array with array items",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"array"},
					},
				},
				Ptr: "/foo",
			},
			want: objProp(),
		},
		{
			name: "array with multiple item types",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
					Array: &pf.Inference_Array{
						ItemTypes: []string{"object", "string"},
					},
				},
				Ptr: "/foo",
			},
			want: objProp(),
		},
		{
			name: "array with unknown item types",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array"},
				},
				Ptr: "/foo",
			},
			want: objProp(),
		},
		{
			name: "multiple types",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"array", "string", "object"},
				},
				Ptr: "/foo",
			},
			want: objProp(),
		},
		{
			name: "root document",
			in: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"object"},
				},
				Ptr: "",
			},
			want: func() mappedProperty {
				p := objProp()
				p.Index = boolPtr(false)
				return p
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := propForProjection(tt.in, tt.in.Inference.Types, fieldConfig{})
			require.Equal(t, tt.want, got)
		})
	}
}
