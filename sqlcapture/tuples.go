package sqlcapture

import (
	"bytes"
	"fmt"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/jackc/pgtype"
)

const TimeKey = "T"

// encodeRowKey extracts the appropriate key-fields by name from a map and encodes
// them as a FoundationDB serialized tuple.
func encodeRowKey(key []string, fields map[string]interface{}) ([]byte, error) {
	var xs = make([]interface{}, len(key))
	for i, elem := range key {
		xs[i] = fields[elem]
	}
	return packTuple(xs)
}

// We translate a list of column values (representing the primary key of a
// database row) into a list of bytes using the FoundationDB tuple encoding
// package. This tuple encoding has the useful property that lexicographic
// comparison of these bytes is equivalent to lexicographic comparison of
// the equivalent values, and thus we don't have to write code to do that.
//
// Encoding primary keys like this also makes `state.json` round-tripping
// across restarts trivial.
func packTuple(xs []interface{}) (bs []byte, err error) {
	var t []tuple.TupleElement
	for _, x := range xs {
		// Values not natively supported by the FoundationDB tuple encoding code
		// must be converted into ones that are.
		switch x := x.(type) {
		case int32:
			t = append(t, int(x))
		case time.Time:
			// This is a temp workaround tat enables the capture to work with
			// tables that specifies a field of "timestamp with time zone"
			// to be part of the key.
			t = append(t, tuple.Tuple{TimeKey, x.UnixNano()})

		case pgtype.Numeric:
			// This is a temp workaround that enables the capture to work with
			// tables that specifies a Numberic field to be part of the key.
			var n int
			if err := x.AssignTo(&n); err != nil {
				return nil, fmt.Errorf("decoding pgtype.Numeric, %w", err)
			}
			t = append(t, n)
		default:
			t = append(t, x)
		}
	}

	// The `Pack()` function doesn't have an error return value, and instead
	// will just panic with a string value if given an unsupported value type.
	// This defer will catch and translate these panics into proper error returns.
	defer func() {
		if r := recover(); r != nil {
			if errStr, ok := r.(string); ok {
				err = fmt.Errorf("error serializing FDB tuple: %s", errStr)
			} else {
				panic(r)
			}
		}
	}()
	return tuple.Tuple(t).Pack(), nil
}

func unpackTuple(bs []byte) ([]interface{}, error) {
	var t, err = tuple.Unpack(bs)
	if err != nil {
		return nil, err
	}
	var xs []interface{}
	for _, elem := range t {
		switch elem := elem.(type) {
		case tuple.Tuple:
			if len(elem) == 2 {
				if key, ok := elem[0].(string); ok && key == TimeKey {
					if unixNano, ok := elem[1].(int64); ok {
						xs = append(xs, time.Unix(0, unixNano))
						continue
					}
				}
			}
		}

		xs = append(xs, elem)

	}
	return xs, nil
}

func compareTuples(xs, ys []byte) int {
	return bytes.Compare(xs, ys)
}
