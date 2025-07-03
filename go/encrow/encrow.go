// Package encrow provides a JSON encoder optimized for SQL table rows.
package encrow

import (
	"fmt"
	"sort"

	"github.com/segmentio/encoding/json"
)

// A Shape represents the structure of a particular document.
type Shape struct {
	Arity    int      // Number of values which should be provided to Encode. May not match the length of prefix/swizzle arrays.
	Names    []string // Names of the fields in the order they will be serialized.
	Prefixes []string // Prefixes for each field, in the order they will be serialized.
	Swizzle  []int    // Indices into the values list which correspond to the fields in the prefixes. May not include all values.

	Flags     json.AppendFlags // JSON serialization flags to use when encoding values.
	SkipNulls bool             // When true, fields with a nil value will not be serialized.
}

// NewShape constructs a new Shape corresponding to the provided field names.
func NewShape(fields []string) *Shape {
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
	for i, j := range swizzle {
		orderedNames[i] = fields[j]
	}

	return &Shape{
		Arity:    len(fields),
		Names:    orderedNames,
		Prefixes: generatePrefixes(orderedNames),
		Swizzle:  swizzle,
		// Default flags, unless overridden via SetFlags.
		Flags: json.EscapeHTML | json.SortMapKeys,
	}
}

// SetFlags overrides the default flags, if alternate behavior is desired.
func (s *Shape) SetFlags(flags json.AppendFlags) {
	s.Flags = flags
}

// SkipNulls will cause serialized results to omit fields with a `nil` value.
func (s *Shape) SetSkipNulls(skip bool) {
	s.SkipNulls = skip
}

// generatePrefixes creates a list of prefixes for the named object fields,
// in the order they are provided.
func generatePrefixes(fields []string) []string {
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
		buf, err = json.Append(buf, v, s.Flags)
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", s.Names[idx], err)
		}
	}
	buf = append(buf, '}')
	return buf, nil
}
