package sqlcapture

import (
	"bytes"
	"fmt"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// EncodeRowKey extracts the appropriate key-fields by name from a map and encodes
// them as a FoundationDB serialized tuple. It applies the `translate` callback to
// each field value so that complex database-specific types can be "lowered" into
// FDB-serializable values.
func EncodeRowKey[T any](key []string, fields map[string]interface{}, fieldTypes map[string]T, translate func(key interface{}, ktype T) (tuple.TupleElement, error)) ([]byte, error) {
	var xs = make([]tuple.TupleElement, len(key))
	var err error
	for i, elem := range key {
		var ktype = fieldTypes[elem]
		if xs[i], err = translate(fields[elem], ktype); err != nil {
			return nil, fmt.Errorf("error encoding column %q: %w", elem, err)
		}
	}
	return PackTuple(xs)
}

// EncodeRowKeyIndices extracts the appropriate key fields by index from a list and
// encodes them as a FoundationDB serialized tuple. It applies the `translate` callback
// to each field value so that complex database-specific types can be "lowered" into
// FDB-serializable values.
func EncodeRowKeyIndices[T any](keyIndices []int, vals []any, keyTypes []T, translate func(key any, ktype T) (tuple.TupleElement, error)) ([]byte, error) {
	var xs = make([]tuple.TupleElement, len(keyIndices))
	var err error
	for i, n := range keyIndices {
		if xs[i], err = translate(vals[n], keyTypes[i]); err != nil {
			return nil, fmt.Errorf("error encoding column at index %d: %w", n, err)
		}
	}
	return PackTuple(xs)
}

// We translate a list of column values (representing the primary key of a
// database row) into a list of bytes using the FoundationDB tuple encoding
// package. This tuple encoding has the useful property that lexicographic
// comparison of these bytes is equivalent to lexicographic comparison of
// the equivalent values, and thus we don't have to write code to do that.
//
// Encoding primary keys like this also makes `state.json` round-tripping
// across restarts trivial.
func PackTuple(xs []tuple.TupleElement) (bs []byte, err error) {
	for i := range xs {
		// Values not natively supported by the FoundationDB tuple encoding code
		// must be converted into ones that are.
		switch x := xs[i].(type) {
		case uint8:
			xs[i] = uint(x)
		case uint16:
			xs[i] = uint(x)
		case uint32:
			xs[i] = uint(x)
		case int8:
			xs[i] = int(x)
		case int16:
			xs[i] = int(x)
		case int32:
			xs[i] = int(x)
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
	return tuple.Tuple(xs).Pack(), nil
}

// UnpackTuple decodes a FoundationDB-serialized sequence of bytes into a list of
// tuple elements, and then applies the `translate` callback to each one in order
// to reverse any lowering of complex database-specific types done in EncodeRowKey.
func UnpackTuple(bs []byte, translate func(t tuple.TupleElement) (interface{}, error)) ([]interface{}, error) {
	var t, err = tuple.Unpack(bs)
	if err != nil {
		return nil, err
	}
	var xs []interface{}
	for _, elem := range t {
		decoded, err := translate(elem)
		if err != nil {
			return nil, fmt.Errorf("unpack tuple: %w", err)
		}
		xs = append(xs, decoded)
	}

	return xs, nil
}

func compareTuples(xs, ys []byte) int {
	return bytes.Compare(xs, ys)
}
