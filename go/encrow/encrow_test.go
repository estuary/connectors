package encrow

import (
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	stdjson "encoding/json"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const benchmarkDatasetSize = 1000

func BenchmarkStandardSerialization(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var names, values = benchmarkDataset(benchmarkDatasetSize)
		b.StartTimer()

		for _, row := range values {
			var fields = make(map[string]any)
			for idx, val := range row {
				fields[names[idx]] = val
			}
			var bs, err = stdjson.Marshal(fields)
			require.NoError(b, err)
			io.Discard.Write(bs)
		}
	}
}

func BenchmarkShapeSerialization(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var names, values = benchmarkDataset(benchmarkDatasetSize)
		b.StartTimer()

		var bs []byte
		var err error
		var s *Shape
		for _, row := range values {
			if s == nil {
				s = NewShape(names)
			}
			bs, err = s.Encode(bs, row)
			require.NoError(b, err)
			io.Discard.Write(bs)
		}
	}
}

func TestSerializationEquivalence(t *testing.T) {
	var names, values = benchmarkDataset(benchmarkDatasetSize)
	checkEquivalence(t, names, values)
}

func FuzzSerialization(f *testing.F) {
	f.Add(`{}`)
	f.Add(`{"id": 123}`)
	f.Add(`{"data": "foobar", "moredata": "xyzzy"}`)
	f.Add(`{"boolval": true}`)
	f.Add(`{"val": null}`)
	f.Add(`{"val": "2023-10-10T21:16:45Z"}`)
	f.Add(`{"val": "2023-10-10T21:16:45-06:00"}`)
	f.Add(`{"nested": {"object": "value"}}`)

	f.Fuzz(func(t *testing.T, input string) {
		// Decode input string as a valid object
		var obj map[string]any
		if err := stdjson.Unmarshal([]byte(input), &obj); err != nil {
			t.SkipNow()
		}

		// Decompose object into separate lists of names and values
		var names []string
		var vals []any
		for name, val := range obj {
			names = append(names, name)
			vals = append(vals, val)
		}

		// Check equivalence of standard JSON serialization and optimized serialization.
		// Repeat the values list three times to exercise buffer reuse behavior.
		checkEquivalence(t, names, [][]any{vals, vals, vals})
	})
}

func TestOmitField(t *testing.T) {
	var shape = NewShape([]string{"a", "b", "c", "d"})
	for _, tc := range []struct {
		name   string
		values []any
		want   string
	}{
		{
			name:   "no omissions",
			values: []any{1, 2, nil, 4},
			want:   `{"a":1,"b":2,"c":null,"d":4}`,
		},
		{
			name:   "omit middle field",
			values: []any{1, OmitField, 3, 4},
			want:   `{"a":1,"c":3,"d":4}`,
		},
		{
			name:   "omit first and last",
			values: []any{OmitField, 2, 3, OmitField},
			want:   `{"b":2,"c":3}`,
		},
		{
			name:   "omit all fields",
			values: []any{OmitField, OmitField, OmitField, OmitField},
			want:   `{}`,
		},
		{
			name:   "omit distinct from nil",
			values: []any{nil, OmitField, nil, OmitField},
			want:   `{"a":null,"c":null}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := shape.Encode(nil, tc.values)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got))
		})
	}
}

func checkEquivalence(t *testing.T, names []string, values [][]any) {
	var shape = NewShape(names)
	var shapeBytes []byte
	for _, row := range values {
		var fields = make(map[string]any)
		for idx, val := range row {
			fields[names[idx]] = val
		}
		standardBytes, err := stdjson.Marshal(fields)
		require.NoError(t, err)

		shapeBytes, err = shape.Encode(shapeBytes[:0], row)
		require.NoError(t, err)

		require.Equal(t, string(standardBytes), string(shapeBytes))
	}
}

func benchmarkDataset(size int) ([]string, [][]any) {
	var names = []string{"id", "type", "ctime", "mtime", "name", "description", "sequenceNumber", "state", "version", "intParamA", "intParamB", "intParamC", "intParamD"}
	var values [][]any
	for i := 0; i < size; i++ {
		var row = []any{
			uuid.New().String(),
			[]string{"event", "action", "item", "unknown"}[rand.Intn(4)],
			time.Now().Add(-time.Duration(rand.Intn(10000000)+50000000) * time.Second),
			time.Now().Add(-time.Duration(rand.Intn(10000000)+10000000) * time.Second),
			fmt.Sprintf("Row Number %d", i),
			fmt.Sprintf("An object representing some row in a synthetic dataset. This one is row number %d.", i),
			i,
			[]string{"new", "in-progress", "completed"}[rand.Intn(3)],
			rand.Intn(3),
			rand.Intn(1000000000),
			rand.Intn(1000000000),
			rand.Intn(1000000000),
			rand.Intn(1000000000),
		}
		values = append(values, row)
	}
	return names, values
}
