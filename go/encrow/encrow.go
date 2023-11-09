// Package encrow provides a JSON encoder optimized for SQL table rows.
package encrow

import (
	"fmt"
	"sort"

	"github.com/segmentio/encoding/json"
)

// A Shape represents the structure of a particular document.
type Shape struct {
	arity    int
	prefixes []string
	swizzle  []int
	flags    json.AppendFlags
}

// NewShape constructs a new Shape corresponding to the provided field names.
func NewShape(fields []string) *Shape {
	// Construct a list of swizzle indices which order the fields by name.
	var swizzle = make([]int, len(fields))
	for i := 0; i < len(fields); i++ {
		swizzle[i] = i
	}
	sort.Slice(swizzle, func(i, j int) bool {
		return fields[swizzle[i]] < fields[swizzle[j]]
	})

	// Sort names
	var sortedNames = make([]string, len(fields))
	for i, j := range swizzle {
		sortedNames[i] = fields[j]
	}

	return &Shape{
		arity:    len(sortedNames),
		prefixes: generatePrefixes(sortedNames),
		swizzle:  swizzle,
		// Default flags, unless overridden via SetFlags.
		flags: json.EscapeHTML | json.SortMapKeys,
	}
}

// SetFlags overrides the default flags, if alternate behavior is desired.
func (s *Shape) SetFlags(flags json.AppendFlags) {
	s.flags = flags
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
		buf = append(buf, s.prefixes[idx]...)
		buf, err = json.Append(buf, values[vidx], s.flags)
		if err != nil {
			return nil, err
		}
	}
	buf = append(buf, '}')
	return buf, nil
}
