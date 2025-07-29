package materialize

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestAsFormattedNumeric(t *testing.T) {
	tests := []struct {
		name         string
		inference    pf.Inference
		isPrimaryKey bool
		want         StringWithNumericFormat
	}{
		{
			name: "integer formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "number formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "nullable integer formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"null", "string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "nullable number formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"null", "string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "integer formatted string with integer",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"integer", "string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "number formatted string with number",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"number", "string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "nullable integer formatted string with integer",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"integer", "null", "string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "nullable number formatted string with number",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"null", "number", "string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "doesn't apply to collection keys",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: true,
			want:         "",
		},
		{
			name: "doesn't apply to other types",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"object"},
			},
			isPrimaryKey: false,
			want:         "",
		},
		{
			name: "doesn't apply to strings with other formats",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"null", "number", "string"},
				String_: &pf.Inference_String{
					Format: "base64",
				},
			},
			isPrimaryKey: false,
			want:         "",
		},
		{
			name: "doesn't apply if there are other types",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"string", "object"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatted, ok := AsFormattedNumeric(&pf.Projection{
				Inference:    tt.inference,
				IsPrimaryKey: tt.isPrimaryKey,
			})

			require.Equal(t, tt.want, formatted)
			if tt.want == "" {
				require.False(t, ok)
			} else {
				require.True(t, ok)
			}
		})
	}
}
