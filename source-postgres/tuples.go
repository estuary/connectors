package main

import (
	"bytes"

	"github.com/estuary/protocols/fdb/tuple"
	"github.com/pkg/errors"
)

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
				err = errors.New(errStr)
			} else {
				panic(r)
			}
		}
	}()
	return tuple.Tuple(t).Pack(), nil
}

func unpackTuple(bs []byte) ([]interface{}, error) {
	t, err := tuple.Unpack(bs)
	if err != nil {
		return nil, err
	}
	var xs []interface{}
	for _, elem := range t {
		xs = append(xs, elem)
	}
	return xs, nil
}

func compareTuples(xs, ys []byte) int {
	return bytes.Compare(xs, ys)
}
