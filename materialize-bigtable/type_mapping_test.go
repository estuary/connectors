package main

import (
	"encoding/binary"
	"math"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestMapType(t *testing.T) {
	type encodeCase struct {
		name    string
		in      any
		want    []byte
		wantErr bool
	}

	float64BE := func(f float64) []byte {
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, math.Float64bits(f))
		return out
	}

	uint64BE := func(n uint64) []byte {
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, n)
		return out
	}

	nan := float64BE(math.NaN())
	posInf := float64BE(math.Inf(1))
	negInf := float64BE(math.Inf(-1))

	for _, tc := range []struct {
		name     string
		flatType boilerplate.FlatType
		encodes  []encodeCase
	}{
		{
			name:     "bool",
			flatType: boilerplate.FlatTypeBoolean{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "true", in: true, want: []byte{0x01}},
				{name: "false", in: false, want: []byte{0x00}},
				{name: "wrong type", in: "yes", wantErr: true},
			},
		},
		{
			name:     "integer",
			flatType: boilerplate.FlatTypeInteger{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "int64 positive", in: int64(0x0102030405060708), want: uint64BE(0x0102030405060708)},
				{name: "int64 negative", in: int64(-1), want: uint64BE(math.MaxUint64)},
				{name: "int", in: int(42), want: uint64BE(42)},
				{name: "uint64 max", in: uint64(math.MaxUint64), want: uint64BE(math.MaxUint64)},
				{name: "float64", in: float64(3.0), want: uint64BE(3)},
				{name: "wrong type", in: "abc", wantErr: true},
			},
		},
		{
			name:     "number",
			flatType: boilerplate.FlatTypeNumber{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "float64", in: 1.5, want: float64BE(1.5)},
				{name: "int", in: 7, want: float64BE(7)},
				{name: "int64", in: int64(7), want: float64BE(7)},
				{name: "uint64", in: uint64(7), want: float64BE(7)},
				{name: "NaN string", in: "NaN", want: nan},
				{name: "+Inf string", in: "Infinity", want: posInf},
				{name: "-Inf string", in: "-Infinity", want: negInf},
				{name: "float64 NaN", in: math.NaN(), want: nan},
				{name: "float64 +Inf", in: math.Inf(1), want: posInf},
				{name: "float64 -Inf", in: math.Inf(-1), want: negInf},
				{name: "unknown string", in: "abc", wantErr: true},
				{name: "wrong type", in: true, wantErr: true},
			},
		},
		{
			name:     "string",
			flatType: boilerplate.FlatTypeString{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "string", in: "hello", want: []byte("hello")},
				{name: "wrong type", in: 42, wantErr: true},
			},
		},
		{
			name:     "binary",
			flatType: boilerplate.FlatTypeBinary{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "base64 string decoded", in: "aGVsbG8=", want: []byte("hello")},
				{name: "invalid base64", in: "@@@", wantErr: true},
				{name: "wrong type", in: 42, wantErr: true},
			},
		},
		{
			name:     "string-format-integer",
			flatType: boilerplate.FlatTypeStringFormatInteger{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "string positive", in: "42", want: uint64BE(42)},
				{name: "string negative", in: "-1", want: uint64BE(math.MaxUint64)},
				{name: "string at uint64 max", in: "18446744073709551615", want: uint64BE(math.MaxUint64)},
				{name: "string beyond uint64", in: "18446744073709551616", wantErr: true},
				{name: "string fractional zero", in: "1.0", want: uint64BE(1)},
				{name: "int64", in: int64(42), want: uint64BE(42)},
				{name: "uint64", in: uint64(42), want: uint64BE(42)},
				{name: "float64", in: float64(3.0), want: uint64BE(3)},
				{name: "unparseable", in: "abc", wantErr: true},
				{name: "wrong type", in: true, wantErr: true},
			},
		},
		{
			name: "integer (wide via numeric inference)",
			flatType: boilerplate.FlatTypeInteger{
				InferenceNumeric: pf.Inference_Numeric{Maximum: 1e20},
			},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "int", in: int(42), want: []byte("42")},
				{name: "int64 negative", in: int64(-1), want: []byte("-1")},
				{name: "uint64", in: uint64(123), want: []byte("123")},
				{name: "float64", in: float64(7), want: []byte("7")},
				{name: "wrong type", in: true, wantErr: true},
			},
		},
		{
			name: "string-format-integer (wide via string inference)",
			flatType: boilerplate.FlatTypeStringFormatInteger{
				InferenceString: pf.Inference_String{MaxLength: 30},
			},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "small string", in: "42", want: []byte("42")},
				{name: "negative string", in: "-1", want: []byte("-1")},
				{name: "beyond uint64", in: "99999999999999999999999",
					want: []byte("99999999999999999999999")},
				{name: "fractional zero", in: "1.0", want: []byte("1")},
				{name: "wrong type", in: true, wantErr: true},
			},
		},
		{
			name: "string-format-number (wide via string inference)",
			flatType: boilerplate.FlatTypeStringFormatNumber{
				InferenceString: pf.Inference_String{MaxLength: 40},
			},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "short string", in: "1.5", want: []byte("1.5")},
				{name: "high precision string",
					in:   "3.14159265358979323846264338327950288",
					want: []byte("3.14159265358979323846264338327950288")},
				{name: "NaN string", in: "NaN", want: []byte("NaN")},
				{name: "+Inf string", in: "Infinity", want: []byte("Infinity")},
				{name: "-Inf string", in: "-Infinity", want: []byte("-Infinity")},
				{name: "float64", in: 1.5, want: []byte("1.5")},
				{name: "float64 NaN", in: math.NaN(), want: []byte("NaN")},
				{name: "float64 +Inf", in: math.Inf(1), want: []byte("Infinity")},
				{name: "float64 -Inf", in: math.Inf(-1), want: []byte("-Infinity")},
				{name: "int", in: 7, want: []byte("7")},
				{name: "int64", in: int64(7), want: []byte("7")},
				{name: "uint64", in: uint64(7), want: []byte("7")},
				{name: "wrong type", in: true, wantErr: true},
			},
		},
		{
			name:     "string-format-number",
			flatType: boilerplate.FlatTypeStringFormatNumber{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "string", in: "1.5", want: float64BE(1.5)},
				{name: "NaN", in: "NaN", want: nan},
				{name: "+Inf", in: "Infinity", want: posInf},
				{name: "-Inf", in: "-Infinity", want: negInf},
				{name: "float64", in: 1.5, want: float64BE(1.5)},
				{name: "int", in: 7, want: float64BE(7)},
				{name: "unparseable", in: "abc", wantErr: true},
				{name: "wrong type", in: true, wantErr: true},
			},
		},
		{
			name:     "array",
			flatType: boilerplate.FlatTypeArray{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "bytes pass-through", in: []byte(`[1,2]`), want: []byte(`[1,2]`)},
			},
		},
		{
			name:     "object",
			flatType: boilerplate.FlatTypeObject{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "bytes pass-through", in: []byte(`{"a":1}`), want: []byte(`{"a":1}`)},
			},
		},
		{
			name:     "multiple",
			flatType: boilerplate.FlatTypeMultiple{},
			encodes: []encodeCase{
				{name: "nil", in: nil, want: []byte{}},
				{name: "bytes pass-through", in: []byte(`{"a":1}`), want: []byte(`{"a":1}`)},
				{name: "string", in: "hi", want: []byte(`"hi"`)},
				{name: "int", in: 7, want: []byte("7")},
				{name: "int64", in: int64(7), want: []byte("7")},
				{name: "uint64", in: uint64(7), want: []byte("7")},
				{name: "float64", in: 1.5, want: []byte("1.5")},
				{name: "bool true", in: true, want: []byte("true")},
				{name: "bool false", in: false, want: []byte("false")},
				{name: "map", in: map[string]int{"a": 1}, want: []byte(`{"a":1}`)},
			},
		},
		{name: "never", flatType: boilerplate.FlatTypeNever{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mt := mapType(boilerplate.Projection{
				Projection: pf.Projection{Field: "f"},
				FlatType:   tc.flatType,
			})
			require.Equal(t, "f", mt.field)

			for _, ec := range tc.encodes {
				t.Run(ec.name, func(t *testing.T) {
					got, err := mt.encode(ec.in)
					if ec.wantErr {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, ec.want, got)
				})
			}
		})
	}
}
