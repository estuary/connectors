package sql

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestAsFlatType(t *testing.T) {
	tests := []struct {
		name      string
		inference pf.Inference
		flatType  FlatType
		mustExist bool
	}{
		{
			name: "integer formatted string with integer",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{pf.JsonTypeInteger, pf.JsonTypeString},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeInteger,
				},
			},
			flatType:  INTEGER,
			mustExist: true,
		},
		{
			name: "number formatted string with number",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeNumber, pf.JsonTypeString},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeNumber,
				},
			},
			flatType:  NUMBER,
			mustExist: false,
		},
		{
			name: "integer formatted string with number",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeNumber, pf.JsonTypeString},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeInteger,
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "single number type",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeNumber},
			},
			flatType:  NUMBER,
			mustExist: false,
		},
		{
			name: "number formatted string with number and other field",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeNumber, pf.JsonTypeString, pf.JsonTypeArray},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeNumber,
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "number formatted string with integer",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeInteger, pf.JsonTypeString},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeNumber,
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "multiple types with null",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{pf.JsonTypeInteger, pf.JsonTypeString, pf.JsonTypeNull},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "no types",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  nil,
			},
			flatType:  NEVER,
			mustExist: false,
		},
		{
			name: "no types with format",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  nil,
				String_: &pf.Inference_String{
					Format: pf.JsonTypeNumber,
				},
			},
			flatType:  NEVER,
			mustExist: false,
		},
		{
			name: "other formatted string with integer",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeInteger, pf.JsonTypeString},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeArray,
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "format with two non-string fields",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeInteger, pf.JsonTypeNumber},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeNumber,
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "format with two string fields",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{pf.JsonTypeString, pf.JsonTypeString},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeNumber,
				},
			},
			flatType:  MULTIPLE,
			mustExist: false,
		},
		{
			name: "allowable format with a null is not mustExist",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{pf.JsonTypeString, pf.JsonTypeNull},
				String_: &pf.Inference_String{
					Format: pf.JsonTypeNumber,
				},
			},
			flatType:  STRING,
			mustExist: false,
		},
	}

	for _, tt := range tests {
		projection := &Projection{
			Projection: pf.Projection{
				Inference: tt.inference,
			},
		}

		flatType, mustExist := projection.AsFlatType()
		require.Equal(t, tt.flatType, flatType)
		require.Equal(t, tt.mustExist, mustExist)
	}
}
