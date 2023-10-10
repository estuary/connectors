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
	}
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

// Encode serializes a list of values into the specified shape.
func (s *Shape) Encode(values []any) ([]byte, error) {
	var err error
	if len(values) != s.arity {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", s.arity, len(values))
	}
	if s.arity == 0 {
		return []byte("{}"), nil
	}

	var bs []byte
	for idx, vidx := range s.swizzle {
		bs = append(bs, s.prefixes[idx]...)
		bs, err = json.Append(bs, values[vidx], json.EscapeHTML|json.SortMapKeys)
		if err != nil {
			return nil, err
		}
	}
	bs = append(bs, '}')
	return bs, nil
}
