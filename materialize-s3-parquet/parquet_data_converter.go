package main

import (
	"encoding/base32"
	"fmt"
	"reflect"
	"strings"

	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
)

// A pqField represents a column in the parquet file that stores data in a projected field
// from Flow data stream.
type pqField interface {
	// Returns a Tag string used for constructing the json representation of the parquet file schema.
	// This is used when creating and intializing a parquet file.
	// Refer to this for an example of Tag and schema.
	// https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go#L36
	Tag() string

	// Returns the reflect description of the field in a go Struct.
	// This is used for creating an object that holds the data to populate a row in parquet file.
	ToStructField() reflect.StructField

	// Converts the flow value `t` into the correct type in Go, and populates the corresponding field
	// in a Go struct specified by `fldToSet`.
	Set(t tuple.TupleElement, fldToSet reflect.Value) error
}

func toInternalFieldName(s string) string {
	return "I" + strings.ReplaceAll(base32.StdEncoding.EncodeToString([]byte(s)), "=", "_")
}

// stringField implements the pqField interface for processing strings.
type stringField struct{ name string }

func newStringField(name string) *stringField {
	return &stringField{name: name}
}
func (s *stringField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"}`, s.name, toInternalFieldName(s.name))
}
func (s *stringField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(s.name),
		Type: reflect.TypeOf(""),
	}
}
func (s *stringField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if v := reflect.ValueOf(t); v.Kind() == reflect.String {
		fldToSet.SetString(v.String())
	} else {
		return fmt.Errorf("invalid string type (%s)", v.Kind().String())
	}
	return nil
}

// optionalStringField implements the pqField interface for processing optional strings declared as pointers.
type optionalStringField struct{ name string }

func newOptionalStringField(name string) *optionalStringField {
	return &optionalStringField{name: name}
}
func (os *optionalStringField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"}`, os.name, toInternalFieldName(os.name))
}
func (os *optionalStringField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(os.name),
		Type: reflect.PtrTo(reflect.TypeOf("")),
	}
}
func (os *optionalStringField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if t != nil {
		if v := reflect.ValueOf(t); v.Kind() == reflect.String {
			pv := reflect.New(fldToSet.Type().Elem())
			pv.Elem().SetString(v.String())
			fldToSet.Set(pv)
			return nil
		} else {
			return fmt.Errorf("invalid string type (%s)", v.Kind().String())
		}
	}
	return nil
}

// intField implements the pqField interface for processing integers.
type intField struct{ name string }

func newIntField(name string) *intField {
	return &intField{name: name}
}
func (i *intField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=INT64, repetitiontype=REQUIRED"}`, i.name, toInternalFieldName(i.name))
}
func (i *intField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(i.name),
		Type: reflect.TypeOf(int64(0)),
	}
}
func (i *intField) Set(t tuple.TupleElement, fldToSet reflect.Value) (err error) {
	err = nil
	switch v := reflect.ValueOf(t); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fldToSet.SetInt(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fldToSet.SetInt(int64(v.Uint()))
	default:
		err = fmt.Errorf("invalid integer type (%s)", v.Kind().String())
	}
	return
}

// optionalIntField implements the pqField interface for processing optional integers declared as pointers.
type optionalIntField struct{ name string }

func newOptionalIntField(name string) *optionalIntField {
	return &optionalIntField{name: name}
}
func (oi *optionalIntField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=INT64, repetitiontype=OPTIONAL"}`, oi.name, toInternalFieldName(oi.name))
}
func (oi *optionalIntField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(oi.name),
		Type: reflect.PtrTo(reflect.TypeOf(int64(0))),
	}
}
func (oi *optionalIntField) Set(t tuple.TupleElement, fldToSet reflect.Value) (err error) {
	err = nil
	if t != nil {
		pv := reflect.New(fldToSet.Type().Elem())
		switch v := reflect.ValueOf(t); v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			pv.Elem().SetInt(v.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			pv.Elem().SetInt(int64(v.Uint()))
		default:
			err = fmt.Errorf("invalid integer type (%s)", v.Kind().String())
		}

		fldToSet.Set(pv)
	}
	return
}

// floatField implements the pqField interface for processing floats/doubles.
type floatField struct{ name string }

func newFloatField(name string) *floatField {
	return &floatField{name: name}
}
func (f *floatField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=DOUBLE, repetitiontype=REQUIRED"}`, f.name, toInternalFieldName(f.name))
}
func (f *floatField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(f.name),
		Type: reflect.TypeOf(float64(0)),
	}
}
func (f *floatField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	switch v := reflect.ValueOf(t); v.Kind() {
	case reflect.Float32, reflect.Float64:
		fldToSet.SetFloat(v.Float())
		return nil
	default:
		return fmt.Errorf("invalid float type (%s)", v.Kind().String())
	}
}

// optionalFloatField implements the pqField interface for processing optional floats/doubles declared as pointers.
type optionalFloatField struct{ name string }

func newOptionalFloatField(name string) *optionalFloatField {
	return &optionalFloatField{name: name}
}
func (of *optionalFloatField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=DOUBLE, repetitiontype=OPTIONAL"}`, of.name, toInternalFieldName(of.name))
}
func (of *optionalFloatField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(of.name),
		Type: reflect.PtrTo(reflect.TypeOf(float64(0))),
	}
}
func (of *optionalFloatField) Set(t tuple.TupleElement, fldToSet reflect.Value) (err error) {
	err = nil
	if t != nil {
		pv := reflect.New(fldToSet.Type().Elem())

		switch v := reflect.ValueOf(t); v.Kind() {
		case reflect.Float32, reflect.Float64:
			pv.Elem().SetFloat(v.Float())
		default:
			err = fmt.Errorf("invalid float type (%s)", v.Kind().String())
		}

		fldToSet.Set(pv)
	}
	return
}

// boolField implements the pqField interface for processing booleans.
type boolField struct{ name string }

func newBoolField(name string) *boolField {
	return &boolField{name: name}
}
func (b *boolField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BOOLEAN, repetitiontype=REQUIRED"}`, b.name, toInternalFieldName(b.name))
}
func (b *boolField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(b.name),
		Type: reflect.TypeOf(false),
	}
}
func (b *boolField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if v := reflect.ValueOf(t); v.Kind() == reflect.Bool {
		fldToSet.SetBool(v.Bool())
		return nil
	} else {
		return fmt.Errorf("invalid bool type (%s)", v.Kind().String())
	}
}

// optionalBoolField implements the pqField interface for processing optional booleans declared as pointers.
type optionalBoolField struct{ name string }

func newOptionalBoolField(name string) *optionalBoolField {
	return &optionalBoolField{name: name}
}
func (ob *optionalBoolField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BOOLEAN, repetitiontype=OPTIONAL"}`, ob.name, toInternalFieldName(ob.name))
}
func (ob *optionalBoolField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: toInternalFieldName(ob.name),
		Type: reflect.PtrTo(reflect.TypeOf(false)),
	}
}
func (ob *optionalBoolField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if t != nil {
		if v := reflect.ValueOf(t); v.Kind() == reflect.Bool {
			pv := reflect.New(fldToSet.Type().Elem())
			pv.Elem().SetBool(v.Bool())
			fldToSet.Set(pv)
		} else {
			return fmt.Errorf("invalid bool type (%s)", v.Kind().String())
		}
	}
	return nil
}

func newPqField(fieldType string, name string, optional bool) (pqField, error) {
	switch {
	case fieldType == "string" && optional:
		return newOptionalStringField(name), nil
	case fieldType == "string" && !optional:
		return newStringField(name), nil
	case fieldType == "integer" && optional:
		return newOptionalIntField(name), nil
	case fieldType == "integer" && !optional:
		return newIntField(name), nil
	case fieldType == "number" && optional:
		return newOptionalFloatField(name), nil
	case fieldType == "number" && !optional:
		return newFloatField(name), nil
	case fieldType == "boolean" && optional:
		return newOptionalBoolField(name), nil
	case fieldType == "boolean" && !optional:
		return newBoolField(name), nil
	default:
		return nil, fmt.Errorf("field of unexpected type (%s)", fieldType)
	}
}

// ParquetDataConverter converts the schema/data from Flow into formats accepted by the parquet file processor.
type ParquetDataConverter struct {
	pqFields     []pqField
	rowObjSchema reflect.Type
}

// NewParquetDataConverter creates a ParquetDataConverter.
func NewParquetDataConverter(binding *pf.MaterializationSpec_Binding) (*ParquetDataConverter, error) {
	var fieldSelections = binding.FieldSelection
	var allFields = make([]string, 0, len(fieldSelections.Keys)+len(fieldSelections.Values))
	allFields = append(append(allFields, fieldSelections.Keys...), fieldSelections.Values...)
	var pqFields = make([]pqField, 0, len(allFields))
	var structFields = make([]reflect.StructField, 0, len(allFields))

	for _, field := range allFields {

		var projection = binding.Collection.GetProjection(field)
		if !projection.Inference.IsSingleType() {
			return nil, fmt.Errorf("columns of multi-scalar types are not supported: %+v", projection.Inference.Types)
		}

		var optional = !projection.Inference.MustExist || !projection.Inference.IsSingleScalarType()
		var fieldType string
		for _, tp := range projection.Inference.Types {
			if tp != pf.JsonTypeNull {
				fieldType = tp
				break
			}
		}

		newFld, err := newPqField(fieldType, field, optional)
		if err != nil {
			return nil, err
		}
		pqFields = append(pqFields, newFld)
		structFields = append(structFields, newFld.ToStructField())
	}

	return &ParquetDataConverter{pqFields: pqFields, rowObjSchema: reflect.StructOf(structFields)}, nil
}

// JSONFileSchema returns the parquet file schema represented in a JSON string.
func (pd *ParquetDataConverter) JSONFileSchema() string {
	var tags = make([]string, 0, len(pd.pqFields))
	for _, field := range pd.pqFields {
		tags = append(tags, field.Tag())
	}
	return fmt.Sprintf(`{"Tag": "name=parquet-go-root", "Fields": [%s]}`, strings.Join(tags, ", "))
}

// Convert converts the key/values into a Go object for populating a row in a parquet file.
func (pd *ParquetDataConverter) Convert(key tuple.Tuple, values tuple.Tuple) (interface{}, error) {
	row := reflect.New(pd.rowObjSchema).Elem()

	for i, t := range key {
		if err := pd.pqFields[i].Set(t, row.Field(i)); err != nil {
			return "", err
		}
	}
	for i, t := range values {
		idx := i + len(key)
		if err := pd.pqFields[idx].Set(t, row.Field(idx)); err != nil {
			return "", err
		}
	}
	return row.Interface(), nil
}
