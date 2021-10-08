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
// from a Flow data stream.
type pqField interface {
	// Returns a Tag string used for constructing the json representation of the parquet file schema.
	// This is used when creating and intializing a parquet file.
	// Refer to this for an example of Tag and schema.
	// https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go#L36
	Tag() string

	// Returns the reflect description of the field in a go Struct.
	// This is used for creating an object that holds the data to populate a row in parquet file.
	ToStructField() reflect.StructField
	// Converts a tupleElement from Flow into the correct type in Go, and populates the corresponding field
	// in a Go struct specified by `fldToSet`.
	Set(t tuple.TupleElement, fldToSet reflect.Value) error
}

// pqFiledBase provides the base functions to all pqFields.
type pqFieldBase struct {
	name        string
	optional    bool
	reflectType reflect.Type
}

func (p *pqFieldBase) getInternalFieldName() string {
	return "I" + strings.ReplaceAll(base32.StdEncoding.EncodeToString([]byte(p.name)), "=", "_")
}

func (p *pqFieldBase) getRepetitionType() string {
	if p.optional {
		return "OPTIONAL"
	}

	return "REQUIRED"
}

func (p *pqFieldBase) getReflectType() reflect.Type {
	if p.optional {
		return reflect.PtrTo(p.reflectType)
	}
	return p.reflectType
}

func (p *pqFieldBase) getFieldToSet(fldToSet reflect.Value) reflect.Value {
	if p.optional {
		var pv = reflect.New(p.getReflectType().Elem())
		fldToSet.Set(pv)
		return pv.Elem()
	}
	return fldToSet
}

func (p *pqFieldBase) toStructField() reflect.StructField {
	return reflect.StructField{
		Name: p.getInternalFieldName(),
		Type: p.getReflectType(),
	}
}

func newPqFieldBase(name string, optional bool, reflectType reflect.Type) *pqFieldBase {
	return &pqFieldBase{name: name, optional: optional, reflectType: reflectType}
}

// stringField implements the pqField interface for processing strings.
type stringField struct {
	pqField
	*pqFieldBase
}

func newStringField(name string, optional bool) *stringField {
	return &stringField{
		pqFieldBase: newPqFieldBase(name, optional, reflect.TypeOf("")),
	}
}
func (s *stringField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=%s"}`,
		s.name, s.pqFieldBase.getInternalFieldName(), s.pqFieldBase.getRepetitionType())
}
func (s *stringField) ToStructField() reflect.StructField {
	return s.pqFieldBase.toStructField()
}
func (s *stringField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if t != nil {
		fldToSet = s.pqFieldBase.getFieldToSet(fldToSet)
		if v := reflect.ValueOf(t); v.Kind() != reflect.String {
			return fmt.Errorf("invalid string type (%s)", v.Kind().String())
		} else {
			fldToSet.SetString(v.String())
		}
	} else if !s.optional {
		return fmt.Errorf("unexpected nil value to a non-optional string field")
	}
	return nil
}

// intField implements the pqField interface for processing integers.
type intField struct {
	pqField
	*pqFieldBase
}

func newIntField(name string, optional bool) *intField {
	return &intField{
		pqFieldBase: newPqFieldBase(name, optional, reflect.TypeOf(int64(0))),
	}
}
func (i *intField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=INT64, repetitiontype=%s"}`,
		i.name, i.pqFieldBase.getInternalFieldName(), i.pqFieldBase.getRepetitionType())
}
func (i *intField) ToStructField() reflect.StructField {
	return i.pqFieldBase.toStructField()
}
func (i *intField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if t != nil {
		fldToSet = i.pqFieldBase.getFieldToSet(fldToSet)
		switch v := reflect.ValueOf(t); v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fldToSet.SetInt(v.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fldToSet.SetInt(int64(v.Uint()))
		default:
			return fmt.Errorf("invalid integer type (%s)", v.Kind().String())
		}
	} else if !i.optional {
		return fmt.Errorf("unexpected nil value to a non-optional int field")
	}
	return nil
}

// floatField implements the pqField interface for processing floats/doubles.
type floatField struct {
	pqField
	*pqFieldBase
}

func newFloatField(name string, optional bool) *floatField {
	return &floatField{
		pqFieldBase: newPqFieldBase(name, optional, reflect.TypeOf(float64(0))),
	}
}
func (f *floatField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=DOUBLE, repetitiontype=%s"}`,
		f.name, f.pqFieldBase.getInternalFieldName(), f.pqFieldBase.getRepetitionType())
}
func (f *floatField) ToStructField() reflect.StructField {
	return f.pqFieldBase.toStructField()
}
func (f *floatField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if t != nil {
		fldToSet = f.pqFieldBase.getFieldToSet(fldToSet)
		switch v := reflect.ValueOf(t); v.Kind() {
		case reflect.Float32, reflect.Float64:
			fldToSet.SetFloat(v.Float())
			return nil
		default:
			return fmt.Errorf("invalid float type (%s)", v.Kind().String())
		}
	} else if !f.optional {
		return fmt.Errorf("unexpected nil value to a non-optional float field")
	}
	return nil
}

type boolField struct {
	pqField
	*pqFieldBase
}

func newBoolField(name string, optional bool) *boolField {
	return &boolField{
		pqFieldBase: newPqFieldBase(name, optional, reflect.TypeOf(false)),
	}
}
func (b *boolField) Tag() string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BOOLEAN, repetitiontype=%s"}`,
		b.name, b.pqFieldBase.getInternalFieldName(), b.pqFieldBase.getRepetitionType())
}
func (b *boolField) ToStructField() reflect.StructField {
	return b.pqFieldBase.toStructField()
}
func (b *boolField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if t != nil {
		fldToSet = b.pqFieldBase.getFieldToSet(fldToSet)
		if v := reflect.ValueOf(t); v.Kind() == reflect.Bool {
			fldToSet.SetBool(v.Bool())
			return nil
		} else {
			return fmt.Errorf("invalid bool type (%s)", v.Kind().String())
		}
	} else if !b.optional {
		return fmt.Errorf("unexpected nil value to a non-optional bool field")
	}
	return nil
}

func newPqField(fieldType string, name string, optional bool) (pqField, error) {
	switch {
	case fieldType == "string":
		return newStringField(name, optional), nil
	case fieldType == "integer":
		return newIntField(name, optional), nil
	case fieldType == "number":
		return newFloatField(name, optional), nil
	case fieldType == "boolean":
		return newBoolField(name, optional), nil
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
