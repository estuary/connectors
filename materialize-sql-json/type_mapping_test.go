package sql

import (
	"testing"

	"github.com/estuary/connectors/go/protocol"
	"github.com/stretchr/testify/require"
)

func TestAsFlatType(t *testing.T) {
	tests := []struct {
		name      string
		inference protocol.Inference
		flatType  FlatType
		mustExist bool
	}{
		{
			name: "integer formatted string with integer",
			inference: protocol.Inference{
				Exists: protocol.MustExist,
				Types:  []string{"integer", "string"},
				String_: &protocol.StringInference{
					Format: "integer",
				},
			},
			flatType:  INTEGER,
			mustExist: true,
		},
		{
			name: "number formatted string with number",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"number", "string"},
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  NUMBER,
			mustExist: false,
		},
		{
			name: "integer formatted string with number",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"number", "string"},
				String_: &protocol.StringInference{
					Format: "integer",
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "single number type",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"number"},
			},
			flatType:  NUMBER,
			mustExist: false,
		},
		{
			name: "number formatted string with number and other field",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"number", "string", "array"},
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "number formatted string with integer",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"integer", "string"},
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "multiple types with null",
			inference: protocol.Inference{
				Exists: protocol.MustExist,
				Types:  []string{"integer", "string", "null"},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "no types",
			inference: protocol.Inference{
				Exists: protocol.MustExist,
				Types:  nil,
			},
			flatType:  NEVER,
			mustExist: false,
		},
		{
			name: "no types with format",
			inference: protocol.Inference{
				Exists: protocol.MustExist,
				Types:  nil,
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  NEVER,
			mustExist: false,
		},
		{
			name: "other formatted string with integer",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"integer", "string"},
				String_: &protocol.StringInference{
					Format: "array",
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "format with two non-string fields",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"integer", "number"},
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "format with two string fields",
			inference: protocol.Inference{
				Exists: protocol.MayExist,
				Types:  []string{"string", "string"},
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "allowable format with a null is not mustExist",
			inference: protocol.Inference{
				Exists: protocol.MustExist,
				Types:  []string{"string", "null"},
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  STRING,
			mustExist: false,
		},
		{
			name: "single string formatted as numeric",
			inference: protocol.Inference{
				Exists: protocol.MustExist,
				Types:  []string{"string"},
				String_: &protocol.StringInference{
					Format: "number",
				},
			},
			flatType:  NUMBER,
			mustExist: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projection := &Projection{
				Projection: &protocol.Projection{
					Inference: tt.inference,
				},
			}

			flatType, mustExist := projection.AsFlatType()
			require.Equal(t, tt.flatType, flatType)
			require.Equal(t, tt.mustExist, mustExist)
		})
	}
}
