package sql

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// TestMapType_NullabilityDDL covers the nullability branch in DDLMapper.MapType
//
// In the resulting MappedType: DDL is the createDDL (notNull form when
// mustExist, nullable form otherwise), NullableDDL is always nullableFn(bare),
// and BareDDL is the unmodified mapped DDL. When no WithNotNullFn/WithNullableFn
// options are supplied, both fns default to identity so all three fields equal
// the bare DDL.
func TestMapType_NullabilityDDL(t *testing.T) {
	const baseDDL = "TEXT"

	notNullFn := func(ddl string) string { return ddl + " NOT NULL" }
	nullableFn := func(ddl string) string { return ddl + " NULL" }

	mustExistProjection := Projection{
		Projection: pf.Projection{
			Field: "f",
			Inference: pf.Inference{
				Types:   []string{"string"},
				Exists:  pf.Inference_MUST,
				String_: &pf.Inference_String{},
			},
		},
	}
	mayExistProjection := Projection{
		Projection: pf.Projection{
			Field: "f",
			Inference: pf.Inference{
				Types:   []string{"string"},
				Exists:  pf.Inference_MAY,
				String_: &pf.Inference_String{},
			},
		},
	}
	// A projection whose types include "null" has mustExist=false even when
	// Exists is MUST.
	mustExistWithNullProjection := Projection{
		Projection: pf.Projection{
			Field: "f",
			Inference: pf.Inference{
				Types:   []string{"string", "null"},
				Exists:  pf.Inference_MUST,
				String_: &pf.Inference_String{},
			},
		},
	}

	for _, tc := range []struct {
		name            string
		projection      Projection
		notNullFn       func(string) string
		nullableFn      func(string) string
		wantDDL         string
		wantNullableDDL string
	}{
		{
			name:            "mustExist with both fns applies NOT NULL to DDL, NULL to NullableDDL",
			projection:      mustExistProjection,
			notNullFn:       notNullFn,
			nullableFn:      nullableFn,
			wantDDL:         "TEXT NOT NULL",
			wantNullableDDL: "TEXT NULL",
		},
		{
			name:            "mayExist with both fns applies NULL to DDL and NullableDDL",
			projection:      mayExistProjection,
			notNullFn:       notNullFn,
			nullableFn:      nullableFn,
			wantDDL:         "TEXT NULL",
			wantNullableDDL: "TEXT NULL",
		},
		{
			name:            "mustExist with default identity fns leaves all DDLs bare",
			projection:      mustExistProjection,
			notNullFn:       nil,
			nullableFn:      nil,
			wantDDL:         "TEXT",
			wantNullableDDL: "TEXT",
		},
		{
			name:            "mayExist with default identity fns leaves all DDLs bare",
			projection:      mayExistProjection,
			notNullFn:       nil,
			nullableFn:      nil,
			wantDDL:         "TEXT",
			wantNullableDDL: "TEXT",
		},
		{
			name:            "mustExist with only notNullFn set",
			projection:      mustExistProjection,
			notNullFn:       notNullFn,
			nullableFn:      nil,
			wantDDL:         "TEXT NOT NULL",
			wantNullableDDL: "TEXT",
		},
		{
			name:            "mustExist with only nullableFn set leaves createDDL bare",
			projection:      mustExistProjection,
			notNullFn:       nil,
			nullableFn:      nullableFn,
			wantDDL:         "TEXT",
			wantNullableDDL: "TEXT NULL",
		},
		{
			name:            "mayExist with only nullableFn set",
			projection:      mayExistProjection,
			notNullFn:       nil,
			nullableFn:      nullableFn,
			wantDDL:         "TEXT NULL",
			wantNullableDDL: "TEXT NULL",
		},
		{
			name:            "mayExist with only notNullFn set leaves all DDLs bare",
			projection:      mayExistProjection,
			notNullFn:       notNullFn,
			nullableFn:      nil,
			wantDDL:         "TEXT",
			wantNullableDDL: "TEXT",
		},
		{
			name:            "types contain null forces mustExist=false and applies nullableFn",
			projection:      mustExistWithNullProjection,
			notNullFn:       notNullFn,
			nullableFn:      nullableFn,
			wantDDL:         "TEXT NULL",
			wantNullableDDL: "TEXT NULL",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts []DDLMapperOption
			if tc.notNullFn != nil {
				opts = append(opts, WithNotNullFn(tc.notNullFn))
			}
			if tc.nullableFn != nil {
				opts = append(opts, WithNullableFn(tc.nullableFn))
			}

			mapper := NewDDLMapper(
				FlatTypeMappings{
					STRING: MapStatic(baseDDL),
				},
				opts...,
			)

			got := mapper.MapType(&tc.projection, FieldConfig{})

			require.Equal(t, tc.wantDDL, got.DDL, "DDL")
			require.Equal(t, tc.wantNullableDDL, got.NullableDDL, "NullableDDL")
			// BareDDL must always be the unmodified mapped DDL, regardless of
			// the configured nullability fns.
			require.Equal(t, baseDDL, got.BareDDL, "BareDDL")
		})
	}
}

func TestAsFlatType_Binary(t *testing.T) {
	cases := []struct {
		name            string
		contentEncoding string
		contentMediaTyp string
		want            FlatType
	}{
		{
			name:            "base64 without contentMediaType",
			contentEncoding: "base64",
			contentMediaTyp: "",
			want:            BINARY,
		},
		{
			name:            "base64 + application/octet-stream",
			contentEncoding: "base64",
			contentMediaTyp: "application/octet-stream",
			want:            BINARY,
		},
		{
			name:            "base64 + unrelated contentMediaType routes to string",
			contentEncoding: "base64",
			contentMediaTyp: "application/x-protobuf; proto=flow.MaterializationSpec",
			want:            STRING,
		},
		{
			name:            "no encoding",
			contentEncoding: "",
			contentMediaTyp: "",
			want:            STRING,
		},
		{
			name:            "octet-stream without base64 stays string",
			contentEncoding: "",
			contentMediaTyp: "application/octet-stream",
			want:            STRING,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &Projection{Projection: pf.Projection{
				Inference: pf.Inference{
					Types: []string{"string"},
					String_: &pf.Inference_String{
						ContentEncoding: tc.contentEncoding,
						ContentType:     tc.contentMediaTyp,
					},
					Exists: pf.Inference_MAY,
				},
			}}
			ft, _ := p.AsFlatType()
			require.Equal(t, tc.want, ft)
		})
	}
}
