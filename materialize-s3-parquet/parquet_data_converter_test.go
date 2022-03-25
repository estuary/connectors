package main

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

type test struct {
	input    tuple.TupleElement
	expected interface{}
}

var stringTests = []test{
	{input: tuple.TupleElement("test_value"), expected: "test_value"},
	{input: tuple.TupleElement(""), expected: ""},
}

func TestStringField(t *testing.T) {
	var fld = newStringField("teststr_name", false)
	require.Equal(t, `{"Tag": "name=teststr_name, inname=IORSXG5DTORZF63TBNVSQ____, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"}`, fld.Tag())
	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5DTORZF63TBNVSQ____", structField.Name)
	require.Equal(t, "string", structField.Type.Name())

	for _, test := range stringTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		require.Equal(t, test.expected, v.Interface())
	}

	var v = reflect.New(structField.Type).Elem()
	var err1 = fld.Set(nil, v)
	require.EqualError(t, err1, "unexpected nil value to a non-optional field")

	var err2 = fld.Set(tuple.TupleElement(1), v)
	require.EqualError(t, err2, "invalid string type (int)")
}

func TestOptionalStringField(t *testing.T) {
	var fld = newStringField("test/str_name", true)
	require.Equal(t, `{"Tag": "name=test/str_name, inname=IORSXG5BPON2HEX3OMFWWK___, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"}`, fld.Tag())
	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5BPON2HEX3OMFWWK___", structField.Name)
	require.Equal(t, "string", structField.Type.Elem().Name())

	for _, test := range stringTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		require.Equal(t, test.expected, v.Elem().Interface())
	}

	var v = reflect.New(structField.Type).Elem()
	fld.Set(nil, v)
	require.True(t, v.IsNil())

	var actualError = fld.Set(tuple.TupleElement(1), v)
	require.EqualError(t, actualError, "invalid string type (int)")
}

var intTests = []test{
	{input: tuple.TupleElement(int(-11)), expected: int64(-11)},
	{input: tuple.TupleElement(uint(12)), expected: int64(12)},
	{input: tuple.TupleElement(int64(-13)), expected: int64(-13)},
	{input: tuple.TupleElement(uint64(14)), expected: int64(14)},
}

func TestIntField(t *testing.T) {
	var fld = newIntField("test/int_name", false)
	require.Equal(t, `{"Tag": "name=test/int_name, inname=IORSXG5BPNFXHIX3OMFWWK___, type=INT64, repetitiontype=REQUIRED"}`, fld.Tag())

	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5BPNFXHIX3OMFWWK___", structField.Name)
	require.Equal(t, "int64", structField.Type.Name())

	for _, test := range intTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		require.Equal(t, test.expected, v.Interface())
	}

	var v = reflect.New(structField.Type).Elem()
	var err1 = fld.Set(nil, v)
	require.EqualError(t, err1, "unexpected nil value to a non-optional field")

	var err2 = fld.Set(tuple.TupleElement("bad input"), v)
	require.EqualError(t, err2, "invalid integer type (string)")
}

func TestOptionalIntField(t *testing.T) {
	var fld = newIntField("testint_name", true)
	require.Equal(t, `{"Tag": "name=testint_name, inname=IORSXG5DJNZ2F63TBNVSQ____, type=INT64, repetitiontype=OPTIONAL"}`, fld.Tag())

	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5DJNZ2F63TBNVSQ____", structField.Name)
	require.Equal(t, "int64", structField.Type.Elem().Name())

	for _, test := range intTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		require.Equal(t, test.expected, v.Elem().Interface())
	}

	var v = reflect.New(structField.Type).Elem()
	fld.Set(nil, v)
	require.True(t, v.IsNil())

	var actualError = fld.Set(tuple.TupleElement("bad input"), reflect.New(structField.Type).Elem())
	require.EqualError(t, actualError, "invalid integer type (string)")
}

var floatTests = []test{
	{input: tuple.TupleElement(float32(-11.1234)), expected: float64(-11.1234)},
	{input: tuple.TupleElement(float64(12.12345678)), expected: float64(12.12345678)},
}

func TestFloatField(t *testing.T) {
	var fld = newFloatField("testfloat_name", false)
	require.Equal(t, `{"Tag": "name=testfloat_name, inname=IORSXG5DGNRXWC5C7NZQW2ZI_, type=DOUBLE, repetitiontype=REQUIRED"}`, fld.Tag())

	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5DGNRXWC5C7NZQW2ZI_", structField.Name)
	require.Equal(t, "float64", structField.Type.Name())

	for _, test := range floatTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		var expected = reflect.ValueOf(test.input)
		if expected.Type().Name() == "float32" {
			require.Greater(t, math.Pow(0.1, 6), math.Abs(reflect.ValueOf(test.expected).Float()-v.Float()))
		} else {
			require.Greater(t, math.Pow(0.1, 12), math.Abs(reflect.ValueOf(test.expected).Float()-v.Float()))
		}
	}

	var v = reflect.New(structField.Type).Elem()
	var err1 = fld.Set(nil, v)
	require.EqualError(t, err1, "unexpected nil value to a non-optional field")

	var err2 = fld.Set(tuple.TupleElement(int(0)), v)
	require.EqualError(t, err2, "invalid float type (int)")
}

func TestOptionalFloatField(t *testing.T) {
	var fld = newFloatField("testfloat_name", true)
	require.Equal(t, `{"Tag": "name=testfloat_name, inname=IORSXG5DGNRXWC5C7NZQW2ZI_, type=DOUBLE, repetitiontype=OPTIONAL"}`, fld.Tag())

	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5DGNRXWC5C7NZQW2ZI_", structField.Name)
	require.Equal(t, "float64", structField.Type.Elem().Name())

	for _, test := range floatTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		var expected = reflect.ValueOf(test.input)
		if expected.Type().Name() == "float32" {
			require.Greater(t, math.Pow(0.1, 6), math.Abs(reflect.ValueOf(test.expected).Float()-v.Elem().Float()))
		} else {
			require.Greater(t, math.Pow(0.1, 12), math.Abs(reflect.ValueOf(test.expected).Float()-v.Elem().Float()))
		}
	}

	var v = reflect.New(structField.Type).Elem()
	fld.Set(nil, v)
	require.True(t, v.IsNil())

	var actualError = fld.Set(tuple.TupleElement(int(0)), reflect.New(structField.Type).Elem())
	require.EqualError(t, actualError, "invalid float type (int)")
}

var boolTests = []test{
	{input: tuple.TupleElement(true), expected: true},
	{input: tuple.TupleElement(false), expected: false},
}

func TestBoolField(t *testing.T) {
	var fld = newBoolField("testbool_name", false)
	require.Equal(t, `{"Tag": "name=testbool_name, inname=IORSXG5DCN5XWYX3OMFWWK___, type=BOOLEAN, repetitiontype=REQUIRED"}`, fld.Tag())

	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5DCN5XWYX3OMFWWK___", structField.Name)
	require.Equal(t, "bool", structField.Type.Name())

	for _, test := range boolTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		require.Equal(t, test.expected, v.Interface())
	}

	var v = reflect.New(structField.Type).Elem()
	var err1 = fld.Set(nil, v)
	require.EqualError(t, err1, "unexpected nil value to a non-optional field")

	var err2 = fld.Set(tuple.TupleElement(int(0)), v)
	require.EqualError(t, err2, "invalid bool type (int)")
}

func TestOptionalBoolField(t *testing.T) {
	var fld = newBoolField("testbool_name", true)
	require.Equal(t, `{"Tag": "name=testbool_name, inname=IORSXG5DCN5XWYX3OMFWWK___, type=BOOLEAN, repetitiontype=OPTIONAL"}`, fld.Tag())

	var structField = fld.ToStructField()
	require.Equal(t, "IORSXG5DCN5XWYX3OMFWWK___", structField.Name)
	require.Equal(t, "bool", structField.Type.Elem().Name())

	for _, test := range boolTests {
		var v = reflect.New(structField.Type).Elem()
		fld.Set(test.input, v)
		require.Equal(t, test.expected, v.Elem().Interface())
	}

	var v = reflect.New(structField.Type).Elem()
	fld.Set(nil, v)
	require.True(t, v.IsNil())

	var actualError = fld.Set(tuple.TupleElement(int(0)), reflect.New(structField.Type).Elem())
	require.EqualError(t, actualError, "invalid bool type (int)")
}

func TestNewParquetDataConverter_empty(t *testing.T) {
	var cvrt, err1 = NewParquetDataConverter(&pf.MaterializationSpec_Binding{})
	require.NoError(t, err1)
	require.Equal(t, `{"Tag": "name=parquet-go-root", "Fields": []}`, cvrt.JSONFileSchema())

	var actual, err2 = cvrt.Convert(tuple.Tuple{}, tuple.Tuple{})
	require.NoError(t, err2)
	require.Equal(t, struct{}{}, actual)
}

func testConverterInput(mustExist bool) *pf.MaterializationSpec_Binding {
	var exists pf.Inference_Exists
	if mustExist {
		exists = pf.Inference_MUST
	} else {
		exists = pf.Inference_MAY
	}
	return &pf.MaterializationSpec_Binding{
		FieldSelection: pf.FieldSelection{
			Keys:   []string{"str", "bool"},
			Values: []string{"int64", "uint64", "int", "uint", "float32", "float64"},
		},
		Collection: pf.CollectionSpec{
			Projections: []pf.Projection{
				{Field: "str", Inference: pf.Inference{Types: []string{"string"}, Exists: exists}},
				{Field: "bool", Inference: pf.Inference{Types: []string{"boolean"}, Exists: exists}},
				{Field: "int64", Inference: pf.Inference{Types: []string{"integer"}, Exists: exists}},
				{Field: "uint64", Inference: pf.Inference{Types: []string{"integer"}, Exists: exists}},
				{Field: "int", Inference: pf.Inference{Types: []string{"integer"}, Exists: exists}},
				{Field: "uint", Inference: pf.Inference{Types: []string{"integer"}, Exists: exists}},
				{Field: "float32", Inference: pf.Inference{Types: []string{"number"}, Exists: exists}},
				{Field: "float64", Inference: pf.Inference{Types: []string{"number"}, Exists: exists}},
			},
		},
	}
}

func expectedSchema(repetitiontype string) string {
	return fmt.Sprintf(`{"Tag": "name=parquet-go-root", `+
		`"Fields": [{"Tag": "name=str, inname=ION2HE___, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=%[1]s"}, `+
		`{"Tag": "name=bool, inname=IMJXW63A_, type=BOOLEAN, repetitiontype=%[1]s"}, `+
		`{"Tag": "name=int64, inname=INFXHINRU, type=INT64, repetitiontype=%[1]s"}, `+
		`{"Tag": "name=uint64, inname=IOVUW45BWGQ______, type=INT64, repetitiontype=%[1]s"}, `+
		`{"Tag": "name=int, inname=INFXHI___, type=INT64, repetitiontype=%[1]s"}, `+
		`{"Tag": "name=uint, inname=IOVUW45A_, type=INT64, repetitiontype=%[1]s"}, `+
		`{"Tag": "name=float32, inname=IMZWG6YLUGMZA____, type=DOUBLE, repetitiontype=%[1]s"}, `+
		`{"Tag": "name=float64, inname=IMZWG6YLUGY2A____, type=DOUBLE, repetitiontype=%[1]s"}]}`,
		repetitiontype)

}

var multiTypeInputKey = []tuple.TupleElement{"test_str", true}
var multiTypeInputValues = []tuple.TupleElement{int64(-64), uint64(64), int(-1), uint(1), float32(32.3232), float64(-64.64646464)}

var emptyInputKey = []tuple.TupleElement{nil, nil}
var emptyInputValues = []tuple.TupleElement{nil, nil, nil, nil, nil, nil}

func TestNewParquetDataConverter_allRequiredTypes(t *testing.T) {
	var cvrt, err1 = NewParquetDataConverter(testConverterInput(true))
	require.NoError(t, err1)
	require.Equal(t, expectedSchema("REQUIRED"), cvrt.JSONFileSchema())

	var actual, err2 = cvrt.Convert(multiTypeInputKey, multiTypeInputValues)
	require.NoError(t, err2)
	var actualValues = reflect.ValueOf(actual)

	require.Equal(t, "test_str", actualValues.Field(0).String())
	require.Equal(t, true, actualValues.Field(1).Bool())
	require.Equal(t, int64(-64), actualValues.Field(2).Int())
	require.Equal(t, int64(64), actualValues.Field(3).Int())
	require.Equal(t, int64(-1), actualValues.Field(4).Int())
	require.Equal(t, int64(1), actualValues.Field(5).Int())
	require.Greater(t, math.Pow(0.1, 6), math.Abs(float64(32.3232)-actualValues.Field(6).Float()))
	require.Greater(t, math.Pow(0.1, 12), math.Abs(float64(-64.64646464)-actualValues.Field(7).Float()))
}

func TestNewParquetDataConverter_allOptionalTypes(t *testing.T) {
	var cvrt, err1 = NewParquetDataConverter(testConverterInput(false))
	require.NoError(t, err1)
	require.Equal(t, expectedSchema("OPTIONAL"), cvrt.JSONFileSchema())

	var actual, err2 = cvrt.Convert(multiTypeInputKey, multiTypeInputValues)
	require.NoError(t, err2)
	var actualValues = reflect.ValueOf(actual)

	require.Equal(t, "test_str", actualValues.Field(0).Elem().String())
	require.Equal(t, true, actualValues.Field(1).Elem().Bool())
	require.Equal(t, int64(-64), actualValues.Field(2).Elem().Int())
	require.Equal(t, int64(64), actualValues.Field(3).Elem().Int())
	require.Equal(t, int64(-1), actualValues.Field(4).Elem().Int())
	require.Equal(t, int64(1), actualValues.Field(5).Elem().Int())
	require.Greater(t, math.Pow(0.1, 6), math.Abs(float64(32.3232)-actualValues.Field(6).Elem().Float()))
	require.Greater(t, math.Pow(0.1, 12), math.Abs(float64(-64.64646464)-actualValues.Field(7).Elem().Float()))
}

func TestNewParquetDataConverter_allOptionalTypesWithNullValues(t *testing.T) {
	var cvrt, err1 = NewParquetDataConverter(testConverterInput(false))
	require.NoError(t, err1)
	require.Equal(t, expectedSchema("OPTIONAL"), cvrt.JSONFileSchema())

	var actual, err2 = cvrt.Convert(emptyInputKey, emptyInputValues)
	require.NoError(t, err2)
	actualValues := reflect.ValueOf(actual)

	for i := 0; i < actualValues.NumField(); i++ {
		require.True(t, actualValues.Field(i).IsNil())
	}
}

func TestNewParquetDataConverter_scalarValueWithNull(t *testing.T) {
	var cvrt, err1 = NewParquetDataConverter(
		&pf.MaterializationSpec_Binding{
			FieldSelection: pf.FieldSelection{
				Keys: []string{"test_field"},
			},
			Collection: pf.CollectionSpec{
				Projections: []pf.Projection{
					{Field: "test_field", Inference: pf.Inference{Types: []string{"string", "null"}}},
				},
			},
		})
	require.NoError(t, err1)
	require.Equal(t,
		`{"Tag": "name=parquet-go-root", `+
			`"Fields": [{"Tag": "name=test_field, inname=IORSXG5C7MZUWK3DE, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"}]}`,
		cvrt.JSONFileSchema())

	var actualStr, err2 = cvrt.Convert(tuple.Tuple{"a_str"}, tuple.Tuple{})
	require.NoError(t, err2)
	require.Equal(t, "a_str", reflect.ValueOf(actualStr).Field(0).Elem().String())

	var actualNull, err3 = cvrt.Convert(tuple.Tuple{nil}, tuple.Tuple{})
	require.NoError(t, err3)
	require.True(t, reflect.ValueOf(actualNull).Field(0).IsNil())
}

func TestNewParquetDataConverter_invalid(t *testing.T) {
	var _, err1 = NewParquetDataConverter(
		&pf.MaterializationSpec_Binding{
			FieldSelection: pf.FieldSelection{
				Keys: []string{"test_field"},
			},
			Collection: pf.CollectionSpec{
				Projections: []pf.Projection{
					{Field: "test_field", Inference: pf.Inference{Types: []string{"string", "int"}}},
				},
			},
		})
	require.EqualError(t, err1, "columns of multi-scalar types are not supported: [string int]")

	var _, err2 = NewParquetDataConverter(
		&pf.MaterializationSpec_Binding{
			FieldSelection: pf.FieldSelection{
				Keys: []string{"test_field"},
			},
			Collection: pf.CollectionSpec{
				Projections: []pf.Projection{
					{Field: "test_field", Inference: pf.Inference{Types: []string{"array"}}},
				},
			},
		})
	require.EqualError(t, err2, "field of unexpected type (array)")
}
