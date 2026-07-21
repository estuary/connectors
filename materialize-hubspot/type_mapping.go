package hubspot

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// MappedField is a field name and a property it could be materialized to.
//
// If we created properties on demand this is the property that would be
// create, but it does not necessarily represent the actual property the field
// will be using.
type MappedField struct {
	Name string
	Ptr  string

	// The Property we would create if one does not exist.
	Property *Property
}

func NewMappedField(projection pf.Projection, fc FieldConfig) (*MappedField, error) {
	types := append([]string(nil), projection.Inference.Types...)

	// All properties can be set to null but doing so is a no-op; the property
	// will not be modified.
	//
	// To clear a property set it to "", this will be allowed for any property
	// type.
	types = slices.DeleteFunc(types, func(s string) bool { return s == "null" })
	slices.Sort(types)

	propertyName, err := PropertyName(projection.Field)
	if err != nil {
		return nil, err
	}
	property := &Property{
		Name:           propertyName,
		Label:          projection.Inference.Title,
		Description:    projection.Inference.Description,
		HasUniqueValue: projection.IsPrimaryKey,
	}

	switch strings.Join(types, ",") {
	case "integer":
		property.Type = NumberPropertyType
		property.FieldType = NumberPropertyFieldType
	case "integer,numeric":
		property.Type = NumberPropertyType
		property.FieldType = NumberPropertyFieldType
	case "numeric":
		property.Type = NumberPropertyType
		property.FieldType = NumberPropertyFieldType
	case "boolean":
		property.Type = BoolPropertyType
		property.FieldType = BooleanCheckboxPropertyFieldType
		property.Options = []PropertyOption{
			{
				Label:        "True",
				Value:        "true",
				DisplayOrder: 0,
			},
			{
				Label:        "False",
				Value:        "false",
				DisplayOrder: 1,
			},
		}
	case "string":
		switch projection.Inference.String_.Format {
		case "date-time":
			property.Type = DatetimePropertyType
			property.FieldType = DatePropertyFieldType
		case "date":
			property.Type = DatePropertyType
			property.FieldType = DatePropertyFieldType
		case "number":
			property.Type = NumberPropertyType
			property.FieldType = NumberPropertyFieldType
		default:
			if len(projection.Inference.EnumJsonVec) > 0 {
				var options []PropertyOption
				for _, data := range projection.Inference.EnumJsonVec {
					var valueStr string
					if err := json.Unmarshal(data, &valueStr); err != nil {
						return nil, err
					}
					options = append(options, PropertyOption{
						Label:        valueStr,
						Value:        valueStr,
						DisplayOrder: DisplayOrderLast,
					})
				}
				property.Type = EnumPropertyType
				property.FieldType = RadioPropertyFieldType
				property.Options = options
			} else {
				property.Type = StringPropertyType
				property.FieldType = TextPropertyFieldType
			}
		}
	default:
		// Other types (object, array, null) and other combinations will be
		// serialized as a JSON string.  This can be up to 65,536 characters.
		property.Type = StringPropertyType
		property.FieldType = TextPropertyFieldType
	}

	return &MappedField{
		Name:     projection.Field,
		Ptr:      projection.Ptr,
		Property: property,
	}, nil
}

func (m *MappedField) String() string {
	return m.Name
}

// Compatible is true if the MappedField can be written to the existing Property.
func (m *MappedField) Compatible(existing *Property) bool {
	if m.Property.Type == existing.Type {
		return true
	}

	// If not an exact match, maybe we can widen.
	switch m.Property.Type {
	case BoolPropertyType:
		return existing.Type == StringPropertyType || existing.Type == NumberPropertyType
	case EnumPropertyType:
		return existing.Type == StringPropertyType
	case DatePropertyType:
		return existing.Type == StringPropertyType
	case DatetimePropertyType:
		return existing.Type == StringPropertyType
	case StringPropertyType:
		// Strings are allowed to be stored in enumeration properties.  This
		// can fail during Store if the string does not match one of the
		// defined options.
		return existing.Type == EnumPropertyType
	case NumberPropertyType:
		return existing.Type == StringPropertyType
	}
	return false
}

func (m *MappedField) CanMigrate(existing *Property) bool {
	// We never migrate fields in HubSpot.
	//
	// While, with exceptions it is generally possible to change the type of a
	// property, it doesn't change any of the existing data.
	return false
}

// Convert a value to be stored into the property.
//
// Properties will be either a bool, string, integer or float, or nil.
func (m *MappedField) Convert(elem tuple.TupleElement, property *Property) (any, error) {
	switch elem.(type) {
	case nil:
		return nil, nil
	}

	switch property.Type {
	case BoolPropertyType:
		switch v := elem.(type) {
		case bool:
			return v, nil
		}
	case EnumPropertyType:
		switch v := elem.(type) {
		case string:
			return v, nil
		}
	case DatePropertyType:
		switch v := elem.(type) {
		case string:
			return v, nil
		}
	case DatetimePropertyType:
		switch v := elem.(type) {
		case string:
			return v, nil
		}
	case StringPropertyType:
		switch v := elem.(type) {
		case string:
			return v, nil
		case json.RawMessage:
			return string(v), nil
		}
	case NumberPropertyType:
		switch v := elem.(type) {
		case int, int64, uint, uint64, float64:
			return v, nil
		case string:
			return strconv.ParseFloat(v, 64)
		}
	}
	return nil, fmt.Errorf("unable to convert %T to %q for property %q", elem, property.Type, property.Name)
}

// ConvertString produces a string version of the field.  This is used for
// comparing the match property to the value returned from the API.
func (m *MappedField) ConvertString(elem tuple.TupleElement, property *Property) (string, error) {
	v, err := m.Convert(elem, property)
	if err != nil {
		return "", err
	}

	switch v := v.(type) {
	case nil:
		return "", nil
	case bool:
		return strconv.FormatBool(v), nil
	case string:
		return v, nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	}
	return "", fmt.Errorf(`unable to convert %T to "string" for property %q`, elem, property.Name)
}

var (
	validPropertyChars = regexp.MustCompile("[^a-z0-9_]+")

	startsNumber = regexp.MustCompile("^[0-9]+")
)

// PropertyName converts a projection field name to its property name.
//
// > Property names must contain only lowercase letters, numbers, and
// > underscores. They must start with a letter.
//
// Based on the conversion that is made in the HubSpot webapp from the label
// name to the property name.
func PropertyName(fieldName string) (string, error) {
	name := strings.TrimPrefix(fieldName, "properties/")
	name = strings.ToLower(name)
	// Underscore is allowed but not as a leading character.
	name = strings.TrimPrefix(name, "_")
	name = validPropertyChars.ReplaceAllString(name, "")
	name = startsNumber.ReplaceAllString(name, "n$0")

	if len(name) > MaxPropertyNameLength {
		name = name[:MaxPropertyNameLength]
	}

	if name == "" {
		return "", fmt.Errorf("unable to convert to a valid property name: %q", fieldName)
	}
	return name, nil
}
