package sqlcapture

import (
	"bytes"
	"encoding/binary"
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

// AppendFDB encodes a single value as a FoundationDB serialized tuple element and
// appends it to the provided byte slice. Since the FDB tuple encoding concatenates
// elements at the top level, repeated calls can be used to build up a tuple.
func AppendFDB(buf []byte, v tuple.TupleElement) (bs []byte, err error) {
	// Directly dispatch common values to an optimized encoding function.
	switch x := v.(type) {
	case uint8:
		return AppendUintFDB(buf, uint64(x))
	case uint16:
		return AppendUintFDB(buf, uint64(x))
	case uint32:
		return AppendUintFDB(buf, uint64(x))
	case uint64:
		return AppendUintFDB(buf, x)
	case int8:
		return AppendIntFDB(buf, int64(x))
	case int16:
		return AppendIntFDB(buf, int64(x))
	case int32:
		return AppendIntFDB(buf, int64(x))
	case int64:
		return AppendIntFDB(buf, x)
	case string:
		return AppendBytesFDB(buf, FDBStringCode, []byte(x))
	case []byte:
		return AppendBytesFDB(buf, FDBBytesCode, x)
	}

	// Everything else uses the (slow) FoundationDB implementation with slice
	// allocation and conversion to `any` and reflection and deferred recovers
	// as a sort of exception handling mechanism.
	//
	// The `Pack()` function doesn't have an error return value, and instead
	// will just panic with a string value if given an unsupported value type.
	// This defer will catch and translate these panics into proper error returns.
	defer func() {
		if r := recover(); r != nil {
			if errStr, ok := r.(string); ok {
				err = fmt.Errorf("error serializing FDB value: %s", errStr)
			} else {
				panic(r)
			}
		}
	}()
	var packed = tuple.Tuple([]tuple.TupleElement{v}).Pack()
	return append(buf, packed...), nil
}

const intZeroCode = 0x14

var sizeLimits = []uint64{
	1<<(0*8) - 1,
	1<<(1*8) - 1,
	1<<(2*8) - 1,
	1<<(3*8) - 1,
	1<<(4*8) - 1,
	1<<(5*8) - 1,
	1<<(6*8) - 1,
	1<<(7*8) - 1,
	1<<(8*8) - 1,
}

func bisectLeft(u uint64) int {
	var n int
	for sizeLimits[n] < u {
		n++
	}
	return n
}

func AppendIntFDB(buf []byte, i int64) (bs []byte, err error) {
	if i >= 0 {
		return AppendUintFDB(buf, uint64(i))
	}

	n := bisectLeft(uint64(-i))
	var scratch [8]byte

	buf = append(buf, byte(intZeroCode-n))
	offsetEncoded := int64(sizeLimits[n]) + i
	binary.BigEndian.PutUint64(scratch[:], uint64(offsetEncoded))
	return append(buf, scratch[8-n:]...), nil
}

func AppendUintFDB(buf []byte, i uint64) (bs []byte, err error) {
	if i == 0 {
		return append(buf, byte(intZeroCode)), nil
	}

	n := bisectLeft(i)
	var scratch [8]byte

	buf = append(buf, byte(intZeroCode+n))
	binary.BigEndian.PutUint64(scratch[:], i)
	return append(buf, scratch[8-n:]...), nil
}

const (
	FDBBytesCode  = 0x01
	FDBStringCode = 0x02
)

func AppendBytesFDB(buf []byte, code byte, b []byte) ([]byte, error) {
	buf = append(buf, code)
	if i := bytes.IndexByte(b, 0x00); i >= 0 {
		for i >= 0 {
			buf = append(buf, b[:i+1]...) // Append the bytes up to and including the 0x00 byte.
			buf = append(buf, 0xFF)       // Append the 0xFF byte to indicate that we're not done yet.
			b = b[i+1:]                   // Move past the 0x00 byte.
			i = bytes.IndexByte(b, 0x00)  // Find the next 0x00 byte.
		}
	}
	buf = append(buf, b...)
	return append(buf, 0), nil
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
