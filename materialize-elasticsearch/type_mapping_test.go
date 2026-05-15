package main

import (
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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
		{
			name: "string date-time",
			in: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"string"},
					String_: &pf.Inference_String{
						Format: "date-time",
					},
				},
			},
			want: property{
				Type:   elasticTypeDate,
				Format: "strict_date_optional_time_nanos||strict_date_optional_time||epoch_millis",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, propForProjection(tt.in, tt.in.Inference.Types, fieldConfig{}))
		})
	}
}

func TestPropertyCompatible(t *testing.T) {
	tests := []struct {
		name       string
		prop       property
		existing   boilerplate.ExistingField
		compatible bool
	}{
		{
			name: "date compatible",
			prop: property{
				Type:   elasticTypeDate,
				Format: "",
			},
			existing: boilerplate.ExistingField{
				Type:   "date",
				Format: "",
			},
			compatible: true,
		},
		{
			name: "date with incompatible format",
			prop: property{
				Type:   elasticTypeDate,
				Format: "strict_date_optional_time_nanos||strict_date_optional_time||epoch_millis",
			},
			existing: boilerplate.ExistingField{
				Type:   "date",
				Format: "",
			},
			compatible: false,
		},
		{
			name: "date compatible when ignore format",
			prop: property{
				Type:         elasticTypeDate,
				Format:       "strict_date_optional_time_nanos||strict_date_optional_time||epoch_millis",
				IgnoreFormat: true,
			},
			existing: boilerplate.ExistingField{
				Type:   "date",
				Format: "",
			},
			compatible: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.compatible {
				require.True(t, tt.prop.Compatible(tt.existing))
			} else {
				require.False(t, tt.prop.Compatible(tt.existing))
			}
		})
	}
}
