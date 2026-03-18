package sql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripNullFields(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		fields   []string
		expected string
	}{
		{
			name:     "strips only specified fields' null values",
			input:    `{"a":1,"b":null,"c":"hello","d":null}`,
			fields:   []string{"b", "d"},
			expected: `{"a":1,"c":"hello"}`,
		},
		{
			name:     "preserves null for fields not in strip set",
			input:    `{"a":null,"b":null,"c":null}`,
			fields:   []string{"a"},
			expected: `{"b":null,"c":null}`,
		},
		{
			name:     "preserves non-null values for fields in strip set",
			input:    `{"a":"value","b":42,"c":null}`,
			fields:   []string{"a", "b", "c"},
			expected: `{"a":"value","b":42}`,
		},
		{
			name:     "does not strip nulls inside nested objects",
			input:    `{"a":{"nested":null},"b":[null,1],"c":null}`,
			fields:   []string{"a", "b", "c", "nested"},
			expected: `{"a":{"nested":null},"b":[null,1]}`,
		},
		{
			name:     "empty fields list is a no-op",
			input:    `{"a":null,"b":null}`,
			fields:   []string{},
			expected: `{"a":null,"b":null}`,
		},
		{
			name:     "handles empty object",
			input:    `{}`,
			fields:   []string{"a"},
			expected: `{}`,
		},
		{
			name:     "returns non-object JSON unchanged",
			input:    `[1,2,3]`,
			fields:   []string{"a"},
			expected: `[1,2,3]`,
		},
		{
			name:     "returns scalar JSON unchanged",
			input:    `"hello"`,
			fields:   []string{"a"},
			expected: `"hello"`,
		},
		{
			name:     "handles malformed JSON gracefully",
			input:    `{not valid json`,
			fields:   []string{"a"},
			expected: `{not valid json`,
		},
		{
			name:     "strips all specified nulls leaving empty object",
			input:    `{"a":null,"b":null}`,
			fields:   []string{"a", "b"},
			expected: `{}`,
		},
		{
			name:     "nil fields list is a no-op",
			input:    `{"a":null,"b":null}`,
			fields:   nil,
			expected: `{"a":null,"b":null}`,
		},
		{
			name:     "mixed: strip some nulls, preserve others",
			input:    `{"opt_str":null,"opt_int":null,"nullable_str":null,"nullable_int":null,"id":1}`,
			fields:   []string{"opt_str", "opt_int"},
			expected: `{"nullable_str":null,"nullable_int":null,"id":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StripNullFields(json.RawMessage(tt.input), tt.fields)
			var expectedMap, resultMap map[string]json.RawMessage
			if json.Unmarshal([]byte(tt.expected), &expectedMap) == nil && json.Unmarshal(result, &resultMap) == nil {
				require.Equal(t, expectedMap, resultMap)
			} else {
				require.Equal(t, tt.expected, string(result))
			}
		})
	}
}
