package sqlcapture

import (
	"bytes"
	"fmt"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// encodeRowKey extracts the appropriate key-fields by name from a map and encodes
// them as a FoundationDB serialized tuple.
func encodeRowKey(key []string, fields map[string]interface{}, db Database) ([]byte, error) {
	var xs = make([]interface{}, len(key))
	var err error
	for i, elem := range key {
		if xs[i], err = db.EncodeKeyFDB(fields[elem]); err != nil {
			return nil, fmt.Errorf("encode row key: %w", err)
		}
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
		case uint16:
			t = append(t, uint(x))
		case uint32:
			t = append(t, uint(x))
		case int16:
			t = append(t, int(x))
		case int32:
			t = append(t, int(x))
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

func unpackTuple(bs []byte, db Database) ([]interface{}, error) {
	var t, err = tuple.Unpack(bs)
	if err != nil {
		return nil, err
	}
	var xs []interface{}
	for _, elem := range t {
		if decoded, err := db.DecodeKeyFDB(elem); err != nil {
			return nil, fmt.Errorf("unpack tuple: %w", err)
		} else {
			xs = append(xs, decoded)
		}
	}

	return xs, nil
}

func compareTuples(xs, ys []byte) int {
	return bytes.Compare(xs, ys)
}
