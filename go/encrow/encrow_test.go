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

		var s *Shape
		for _, row := range values {
			if s == nil {
				s = NewShape(names)
			}
			var bs, err = s.Encode(row)
			require.NoError(b, err)
			io.Discard.Write(bs)
		}
	}
}

func TestSerializationEquivalence(t *testing.T) {
	var names, values = benchmarkDataset(benchmarkDatasetSize)
	for _, row := range values {
		checkEquivalence(t, names, row)
	}
}

func FuzzSerialization(f *testing.F) {
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

		// Check equivalence of standard JSON serialization and optimized serialization
		checkEquivalence(t, names, vals)
	})
}

func checkEquivalence(t *testing.T, names []string, values []any) {
	var fields = make(map[string]any)
	for idx, val := range values {
		fields[names[idx]] = val
	}
	standardBytes, err := stdjson.Marshal(fields)
	require.NoError(t, err)

	var shape = NewShape(names)
	shapeBytes, err := shape.Encode(values)
	require.NoError(t, err)

	require.Equal(t, string(standardBytes), string(shapeBytes))
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
