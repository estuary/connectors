// Package encrow provides a JSON encoder optimized for SQL table rows.
package encrow

import (
	"fmt"
	"sort"

	"github.com/segmentio/encoding/json"
)

// A Shape represents the structure of a particular document.
type Shape struct {
	Arity    int            // Number of values which should be provided to Encode. May not match the length of prefix/swizzle arrays.
	Names    []string       // Names of the fields in the order they will be serialized.
	Prefixes []string       // Prefixes for each field, in the order they will be serialized.
	Swizzle  []int          // Indices into the values list which correspond to the fields in the prefixes. May not include all values.
	Encoders []ValueEncoder // Encoders for each field, in the order they will be used. A nil value indicates that normal JSON reflection should be used

	SkipNulls bool // When true, fields with a nil value will not be serialized.
}

// ValueEncoder represents strategies for encoding and appending JSON serialized values into a buffer.
type ValueEncoder interface {
	MarshalTo(buf []byte, v any) ([]byte, error)
}

// DefaultEncoder is a ValueEncoder which uses normal JSON reflection. It is flexible but inefficient.
type DefaultEncoder struct {
	Flags json.AppendFlags // Flags to pass to the JSON encoder.
}

func (e *DefaultEncoder) MarshalTo(buf []byte, v any) ([]byte, error) {
	return json.Append(buf, v, e.Flags)
}

// NewShape constructs a new Shape corresponding to the provided field names
// with DefaultEncoder as the ValueEncoder for each field.
func NewShape(fields []string) *Shape {
	var encoders = make([]ValueEncoder, len(fields))
	for i := range encoders {
		encoders[i] = &DefaultEncoder{Flags: json.EscapeHTML | json.SortMapKeys}
	}
	return NewShapeWithEncoders(fields, encoders)
}

// NewShapeWithEncoders constructs a new Shape corresponding to the provided field
// names and encoders. A field name of "" represents a value which is present in
// the values list but which should not be serialized.
func NewShapeWithEncoders(fields []string, encoders []ValueEncoder) *Shape {
	// Construct a list of swizzle indices which order the fields by name.
	var swizzle = make([]int, len(fields))
	for i := range swizzle {
		swizzle[i] = i
	}
	sort.Slice(swizzle, func(i, j int) bool {
		return fields[swizzle[i]] < fields[swizzle[j]]
	})

	// Remove any empty-name fields so that they will not be serialized.
	// Since the swizzle indices are sorted, "" will always be at the start.
	for len(swizzle) > 0 && fields[swizzle[0]] == "" {
		swizzle = swizzle[1:]
	}

	// Reorder the names and encoders according to the swizzle indices.
	var orderedNames = make([]string, len(swizzle))
	var orderedEncoders = make([]ValueEncoder, len(swizzle))
	for i, j := range swizzle {
		orderedNames[i] = fields[j]
		orderedEncoders[i] = encoders[j]
	}

	return &Shape{
		Arity:    len(fields),
		Names:    orderedNames,
		Prefixes: GeneratePrefixes(orderedNames),
		Swizzle:  swizzle,
		Encoders: orderedEncoders,
	}
}

// SkipNulls will cause serialized results to omit fields with a `nil` value.
func (s *Shape) SetSkipNulls(skip bool) {
	s.SkipNulls = skip
}

// GeneratePrefixes creates a list of prefixes for the named object fields,
// in the order they are provided.
func GeneratePrefixes(fields []string) []string {
	var prefixes []string
	for _, fieldName := range fields {
		var quotedFieldName, err = json.Marshal(fieldName)
		if err != nil {
			panic(fmt.Errorf("error escaping field name %q: %w", fieldName, err))
		}
		prefixes = append(prefixes, fmt.Sprintf("%s:", quotedFieldName))
	}
	return prefixes
}

// Encode serializes a list of values into the specified shape, appending to the provided buffer.
func (s *Shape) Encode(buf []byte, values []any) ([]byte, error) {
	var err error
	if len(values) != s.Arity {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", s.Arity, len(values))
	}
	if s.Arity == 0 {
		return append(buf, '{', '}'), nil
	}

	var firstValue = true
	buf = append(buf, '{')
	for idx, vidx := range s.Swizzle {
		var v = values[vidx]
		if s.SkipNulls && v == nil {
			continue
		}

		if !firstValue {
			buf = append(buf, ',')
		}
		firstValue = false

		buf = append(buf, s.Prefixes[idx]...)
		buf, err = s.Encoders[idx].MarshalTo(buf, v)
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", s.Names[idx], err)
		}
	}
	buf = append(buf, '}')
	return buf, nil
}
