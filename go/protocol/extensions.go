package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"

	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// GetProjectionByField finds the projection with the given field name, or nil if one does not exist
func GetProjectionByField(field string, projections []Projection) *Projection {
	for p := range projections {
		if projections[p].Field == field {
			return &projections[p]
		}
	}
	return nil
}

// GetProjection finds the projection with the given field name, or nil if one does not exist
func (m CollectionSpec) GetProjection(field string) *Projection {
	return GetProjectionByField(field, m.Projections)
}

// AllFields returns the complete set of all the fields as a single string slice. All the keys
// fields will be ordered first, in the same order as they appear in Keys, followed by all the
// Values fields in the same order, with the root document field coming last.
func (fields FieldSelection) AllFields() []string {
	var all = make([]string, 0, len(fields.Keys)+len(fields.Values)+1)
	all = append(all, fields.Keys...)
	all = append(all, fields.Values...)
	if fields.Document != nil {
		all = append(all, *fields.Document)
	}
	return all
}

// IsRootDocumentProjection returns true only if this is a projection of the entire document,
// meaning that the json pointer is the empty string.
func (projection Projection) IsRootDocumentProjection() bool {
	return len(projection.Ptr) == 0
}

// IsSingleType returns true if this inference may only hold a single type besides null For example,
// if the types are ["string", "null"] or just ["string"], then this would return true.
func (i Inference) IsSingleType() bool {
	var nTypes = 0
	for _, ty := range i.Types {
		if ty != JsonTypeNull {
			nTypes++
		}
	}
	return nTypes == 1
}

// IsSingleScalarType returns true if this inference may hold a single scalar type besides null.
func (i Inference) IsSingleScalarType() bool {
	var isScalar = false
	var nTypes = 0
	for _, ty := range i.Types {
		switch ty {
		case JsonTypeNull:
		case JsonTypeInteger, JsonTypeNumber, JsonTypeBoolean, JsonTypeString:
			isScalar = true
			nTypes++
		default:
			nTypes++
		}
	}
	return isScalar && nTypes == 1
}

// Type_ constants for each type name used in JSON schemas.
const (
	JsonTypeNull    = "null"
	JsonTypeInteger = "integer"
	JsonTypeNumber  = "number"
	JsonTypeBoolean = "boolean"
	JsonTypeString  = "string"
	JsonTypeObject  = "object"
	JsonTypeArray   = "array"
)

// IsForbidden returns true if the constraint type forbids inclusion in a materialization. This will
// return true for FIELD_FORBIDDEN and UNSATISFIABLE, and false for any other constraint type.
func (m *ConstraintType) IsForbidden() bool {
	switch *m {
	case FieldForbidden, Unsatisfiable:
		return true
	default:
		return false
	}
}

// Equal returns true if this FieldSelection is deeply equal to the other.
func (fields *FieldSelection) Equal(other *FieldSelection) bool {
	if other == nil {
		return fields == nil
	}

	if len(fields.Keys) != len(other.Keys) {
		return false
	}
	for i := range fields.Keys {
		if fields.Keys[i] != other.Keys[i] {
			return false
		}
	}
	if len(fields.Values) != len(other.Values) {
		return false
	}
	for i := range fields.Values {
		if fields.Values[i] != other.Values[i] {
			return false
		}
	}

	if fields.Document == nil {
		if other.Document != nil {
			return false
		}
	} else {
		if *fields.Document != *other.Document {
			return false
		}
	}
	if len(fields.FieldConfig) != len(other.FieldConfig) {
		return false
	}
	for key := range fields.FieldConfig {
		if string(fields.FieldConfig[key]) != string(other.FieldConfig[key]) {
			return false
		}
	}

	return true
}

type Validator interface {
	Validate() error
}

// UnmarshalStrict unmarshals |doc| into |m|, using a strict decoding of the document which
// prohibits unknown fields. If decoding is successful, then |m| is also validated.
func UnmarshalStrict(doc json.RawMessage, into Validator) error {
	var d = json.NewDecoder(bytes.NewReader(doc))
	d.DisallowUnknownFields()

	if err := d.Decode(into); err != nil {
		return err
	}
	return into.Validate()
}

// ExplicitZeroCheckpoint is a zero-valued message encoding, implemented as a trivial encoding of
// the max-value 2^29-1 protobuf tag with boolean true.
var ExplicitZeroCheckpoint = base64.StdEncoding.EncodeToString([]byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1})

// MaterializationSpecPbToBindings exists to extract the equivalent ApplyBinding's from a protobuf
// pf.MaterializationSpec. This is needed as a compatibility layer for materializations that convert
// to the JSON protocol and need to parsed stored specs from the protobuf protocol. It is also used
// for testing in the SQL package. Ideally it could be removed when the compatibility layer is no
// longer required.
func MaterializationSpecPbToBindings(pbSpec *pf.MaterializationSpec) []ApplyBinding {
	out := []ApplyBinding{}

	for _, b := range pbSpec.Bindings {
		out = append(out, ApplyBinding{
			Collection:     collectionSpecPbToBindings(b.Collection),
			ResourceConfig: b.ResourceSpecJson,
			ResourcePath:   b.ResourcePath,
			DeltaUpdates:   b.DeltaUpdates,
			FieldSelection: fieldSelectionPbToBindings(b.FieldSelection),
		})
	}

	return out
}

func collectionSpecPbToBindings(in pf.CollectionSpec) CollectionSpec {
	return CollectionSpec{
		Name:            in.Collection.String(),
		Key:             in.KeyPtrs,
		PartitionFields: in.PartitionFields,
		Projections:     projectionsPbToBindings(in.Projections),
		Schema:          in.GetReadSchemaJson(),
		ReadSchema:      in.ReadSchemaJson,
		WriteSchema:     in.WriteSchemaJson,
	}
}

func fieldSelectionPbToBindings(in pf.FieldSelection) FieldSelection {
	return FieldSelection{
		Keys:        in.Keys,
		Values:      in.Values,
		Document:    &in.Document,
		FieldConfig: in.FieldConfigJson,
	}
}

func projectionsPbToBindings(in []pf.Projection) []Projection {
	out := []Projection{}

	for _, p := range in {
		out = append(out, Projection{
			Ptr:            p.Ptr,
			Field:          p.Field,
			Explicit:       p.Explicit,
			IsPartitionKey: p.IsPartitionKey,
			IsPrimaryKey:   p.IsPrimaryKey,
			Inference:      inferencePbToBindings(p.Inference),
		})
	}

	return out
}

func inferencePbToBindings(in pf.Inference) Inference {
	return Inference{
		Types:       in.Types,
		String_:     inferenceStringPbToBindings(in.String_),
		Title:       in.Title,
		Description: in.Description,
		Default_:    in.DefaultJson,
		Secret:      in.Secret,
		Exists:      Exists(cases.Title(language.English).String(in.Exists.String())),
	}
}

func inferenceStringPbToBindings(in *pf.Inference_String) *StringInference {
	if in == nil {
		return nil
	}

	return &StringInference{
		ContentType:     in.ContentType,
		Format:          in.Format,
		ContentEncoding: in.ContentEncoding,
		IsBase64:        in.IsBase64,
		MaxLength:       int(in.MaxLength),
	}
}
