// Package encrow provides a JSON encoder optimized for SQL table rows.
package encrow

import (
	"fmt"
	"sort"

	"github.com/segmentio/encoding/json"
)

// A Shape represents the structure of a particular document.
type Shape struct {
	arity     int            // Number of values which should be provided to Encode. May not match the length of prefix/swizzle arrays.
	names     []string       // Names of the fields in the order they will be serialized.
	prefixes  []string       // Prefixes for each field, in the order they will be serialized.
	swizzle   []int          // Indices into the values list which correspond to the fields in the prefixes. May not include all values.
	encoders  []ValueEncoder // Encoders for each field, in the order they will be used. A nil value indicates that normal JSON reflection should be used
	skipNulls bool
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
	for fields[swizzle[0]] == "" {
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
		arity:    len(fields),
		names:    orderedNames,
		prefixes: generatePrefixes(orderedNames),
		swizzle:  swizzle,
		encoders: orderedEncoders,
	}
}

// SkipNulls will cause serialized results to omit fields with a `nil` value.
func (s *Shape) SkipNulls() {
	s.skipNulls = true
}

func generatePrefixes(fields []string) []string {
	var prefixes []string
	for idx, fieldName := range fields {
		var quotedFieldName, err = json.Marshal(fieldName)
		if err != nil {
			panic(fmt.Errorf("error escaping field name %q: %w", fieldName, err))
		}
		if idx == 0 {
			prefixes = append(prefixes, fmt.Sprintf("{%s:", quotedFieldName))
		} else {
			prefixes = append(prefixes, fmt.Sprintf(",%s:", quotedFieldName))
		}
	}
	return prefixes
}

// Encode serializes a list of values into the specified shape. If a buffer slice
// is provided it will be truncated and reused.
func (s *Shape) Encode(buf []byte, values []any) ([]byte, error) {
	var err error
	if len(values) != s.arity {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", s.arity, len(values))
	}
	if s.arity == 0 {
		return []byte("{}"), nil
	}

	buf = buf[:0]
	for idx, vidx := range s.swizzle {
		var v = values[vidx]
		if s.skipNulls && v == nil {
			continue
		}

		buf = append(buf, s.prefixes[idx]...)
		buf, err = s.encoders[idx].MarshalTo(buf, v)
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", s.names[idx], err)
		}
	}
	buf = append(buf, '}')
	return buf, nil
}
