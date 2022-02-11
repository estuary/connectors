package sqlcapture

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/jackc/pgtype"
)

const numericKey = "N"

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
		case pgtype.Numeric:
			if encodedKey, err := EncodePgNumeric(x); err != nil {
				return nil, fmt.Errorf("encode pgtype.Numeric: %w", err)
			} else if encodedBinary, err := x.EncodeBinary(nil, nil); err != nil {
				return nil, fmt.Errorf("encode binary pgtype Numeric: %w", err)
			} else {
				t = append(t, tuple.Tuple{numericKey, encodedKey, encodedBinary})
			}
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
			if len(elem) == 3 {
				if key, ok := elem[0].(string); ok && key == numericKey {
					xs = append(xs, elem[2])
					continue
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

func reverseNegfraction(digits string) []byte {
	// The input digits string represents a negative integer, and starts with the `-` sign.
	// The function performs the following operations:
	// 1. remove the `-` sign, which is redundant.
	// 2. reverse each digits to make sure the order between the encoded byte slices is consistent with
	//    the order between the negative values.
	//    This is done by replacing each digit x with (9 - x). (replace '0' with '9', '1' with '8', ..., '9' with '0').
	// 3. Add an sentinel value `0x3a` (ASCII of ":") at the end of the encoded byte slice, to make sure the order of
	//    encoded byte slices are consistent between a negative value and its prefixes. E.g.
	//    "-123" (encoded as "876:") is greater than "-1234" (encoded as "8765:").

	const sentinel = 0x3a
	var b = []byte(digits)
	for i := 1; i < len(b); i++ {
		b[i-1] = 0x69 - b[i] // '9' - b[i] + '0'
	}

	if len(b) > 0 {
		b[len(b)-1] = sentinel
	}

	return b
}

func EncodePgNumeric(n pgtype.Numeric) (tuple.Tuple, error) {
	if n.Status == pgtype.Null {
		return nil, errors.New("Null value in Numeric Key Field")
	} else if n.Status == pgtype.Undefined {
		return nil, errors.New("Undefined value in Numeric Key Field")
	} else if n.NaN {
		return nil, errors.New("NaN value in a Numeric Key Field ")
	}

	// Conceptually, a pgtype.Numeric is encoded into a tuple of (exponents, fraction),
	// in which exponents is an int, and fraction is a slice of bytes.
	// It is ensured the encoded tuple preserves the order of the original numeric value
	// based on `int` and `byte slice` ordering on both components, respectively.

	// For positive numbers, the fraction is converted from the string of digits in the input without
	// leading or trailing zeros. and the exponents is set properly to make sure
	// the represented value = 10 ^ (exponents) * 0.fraction.
	//   E.g. 0.0041 is represetned by (exp = -2, fraction="41"),
	//    and 3.14 is represented by (exp = 1, fraction="314")

	// For negative numbers, suppose its absolute value is encoded as (expAbs, fracAbs),
	// and encoding result of the number is (-expAbs, reverseNegfraction("-fracAbs")),
	// see func reverseNegfraction for details.

	// In order for the encoding to preserve orders properly, we need to make sure the
	// exponents of positive numbers are always positive. and this is done by adding a
	// bias to the encoded exponents, and use the biasedExponents in ordering.

	// According to https://www.postgresql.org/docs/current/datatype-numeric.html,
	// the lowest value of exponent is -16382. Set the bias to be 20000,
	// to make sure the biasedExponent of positive n is always positive.
	const exponentBias = 20000

	// Encoding for 0.
	var biasedExponent int = 0
	var fractionBytes []byte = nil

	if n.Int.Sign() == 1 {
		var str = n.Int.String()
		biasedExponent = len(str) + int(n.Exp) + exponentBias
		fractionBytes = []byte(strings.TrimRight(str, "0"))
	} else if n.Int.Sign() == -1 {
		var str = n.Int.String()
		biasedExponent = -(len(str) - 1 + int(n.Exp) + exponentBias)
		fractionBytes = reverseNegfraction(strings.TrimRight(str, "0"))
	}

	return []tuple.TupleElement{biasedExponent, fractionBytes}, nil
}
