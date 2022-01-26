package main

import (
	"encoding/base32"
	"fmt"
	"reflect"
	"strings"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// typeStrategy is an interface that provides functions with datatype-specific logic to process a column of a parquet file.
type typeStrategy interface {
	// Returns a Tag string used for constructing the json representation of the parquet file schema.
	// This is used when creating and intializing a parquet file.
	// Refer to this for an example of Tag and schema.
	// https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go#L36
	tag(name, internalName, repetitionType string) string

	// Returns the Go reflect type of this field.
	reflectType() reflect.Type

	// Converts a tupleElement from Flow into the correct type in Go, and populates the corresponding field
	// in a Go struct specified by `fldToSet`.
	set(t tuple.TupleElement, fldToSet reflect.Value) error
}

// A pqField represents a column in the parquet file that stores data in a projected field from a Flow data stream.
type pqField struct {
	name     string
	optional bool
	// datatype-specific logics.
	typeStrategy typeStrategy
}

// Returns the reflect description of the field in a Go struct.
// This is used for creating an object that holds the data to populate a row in parquet file.
func (p *pqField) ToStructField() reflect.StructField {
	return reflect.StructField{
		Name: p.getInternalFieldName(),
		Type: p.getReflectType(),
	}
}

// Proxy to the strategy.
func (p *pqField) Tag() string {
	var repetitionType = "REQUIRED"
	if p.optional {
		repetitionType = "OPTIONAL"
	}
	return p.typeStrategy.tag(p.name, p.getInternalFieldName(), repetitionType)
}

// Proxy to the strategy.
func (p *pqField) Set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if t != nil {
		return p.typeStrategy.set(t, p.getFieldToSet(fldToSet))
	} else if !p.optional {
		return fmt.Errorf("unexpected nil value to a non-optional field")
	}
	return nil
}

func (p *pqField) getInternalFieldName() string {
	return "I" + strings.ReplaceAll(base32.StdEncoding.EncodeToString([]byte(p.name)), "=", "_")
}

func (p *pqField) getReflectType() reflect.Type {
	if p.optional {
		return reflect.PtrTo(p.typeStrategy.reflectType())
	}
	return p.typeStrategy.reflectType()
}

func (p *pqField) getFieldToSet(fldToSet reflect.Value) reflect.Value {
	if p.optional {
		var pv = reflect.New(p.getReflectType().Elem())
		fldToSet.Set(pv)
		return pv.Elem()
	}
	return fldToSet
}

// stringStrategy implements the typeStrategy interface for strings.
type stringStrategy struct{}

func (s *stringStrategy) tag(name, internalName, repetitionType string) string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=%s"}`,
		name, internalName, repetitionType)
}
func (s *stringStrategy) reflectType() reflect.Type {
	return reflect.TypeOf("")
}
func (s *stringStrategy) set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if v := reflect.ValueOf(t); v.Kind() != reflect.String {
		return fmt.Errorf("invalid string type (%s)", v.Kind().String())
	} else {
		fldToSet.SetString(v.String())
	}
	return nil
}

func newStringField(name string, optional bool) *pqField {
	return &pqField{
		name:         name,
		optional:     optional,
		typeStrategy: &stringStrategy{},
	}
}

// intStrategy implements the typeStrategy interface for integers.
type intStrategy struct{}

func (i *intStrategy) tag(name, internalName, repetitionType string) string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=INT64, repetitiontype=%s"}`,
		name, internalName, repetitionType)
}
func (i *intStrategy) reflectType() reflect.Type {
	return reflect.TypeOf(int64(0))
}
func (i *intStrategy) set(t tuple.TupleElement, fldToSet reflect.Value) error {
	switch v := reflect.ValueOf(t); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fldToSet.SetInt(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fldToSet.SetInt(int64(v.Uint()))
	default:
		return fmt.Errorf("invalid integer type (%s)", v.Kind().String())
	}
	return nil
}
func newIntField(name string, optional bool) *pqField {
	return &pqField{
		name:         name,
		optional:     optional,
		typeStrategy: &intStrategy{},
	}
}

// floatStrategy implements the typeStrategy interface for floats/doubles.
type floatStrategy struct{}

func (f *floatStrategy) tag(name, internalName, repetitionType string) string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=DOUBLE, repetitiontype=%s"}`,
		name, internalName, repetitionType)
}
func (f *floatStrategy) reflectType() reflect.Type {
	return reflect.TypeOf(float64(0))
}
func (f *floatStrategy) set(t tuple.TupleElement, fldToSet reflect.Value) error {
	switch v := reflect.ValueOf(t); v.Kind() {
	case reflect.Float32, reflect.Float64:
		fldToSet.SetFloat(v.Float())
		return nil
	default:
		return fmt.Errorf("invalid float type (%s)", v.Kind().String())
	}
}
func newFloatField(name string, optional bool) *pqField {
	return &pqField{
		name:         name,
		optional:     optional,
		typeStrategy: &floatStrategy{},
	}
}

// boolStrategy implements the typeStrategy interface for booleans.
type boolStrategy struct{}

func (b *boolStrategy) tag(name, internalName, repetitionType string) string {
	return fmt.Sprintf(`{"Tag": "name=%s, inname=%s, type=BOOLEAN, repetitiontype=%s"}`,
		name, internalName, repetitionType)
}
func (b *boolStrategy) reflectType() reflect.Type {
	return reflect.TypeOf(false)
}
func (b *boolStrategy) set(t tuple.TupleElement, fldToSet reflect.Value) error {
	if v := reflect.ValueOf(t); v.Kind() == reflect.Bool {
		fldToSet.SetBool(v.Bool())
		return nil
	} else {
		return fmt.Errorf("invalid bool type (%s)", v.Kind().String())
	}
}
func newBoolField(name string, optional bool) *pqField {
	return &pqField{
		name:         name,
		optional:     optional,
		typeStrategy: &boolStrategy{},
	}
}

func newPqField(fieldType string, name string, optional bool) (*pqField, error) {
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
	pqFields     []*pqField
	rowObjSchema reflect.Type
}

// NewParquetDataConverter creates a ParquetDataConverter.
func NewParquetDataConverter(binding *pf.MaterializationSpec_Binding) (*ParquetDataConverter, error) {
	var fieldSelections = binding.FieldSelection
	var allFields = make([]string, 0, len(fieldSelections.Keys)+len(fieldSelections.Values))
	allFields = append(append(allFields, fieldSelections.Keys...), fieldSelections.Values...)
	var pqFields = make([]*pqField, 0, len(allFields))
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
