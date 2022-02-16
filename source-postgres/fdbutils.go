package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/jackc/pgtype"
)

const numericKey = "N"

// encodePgNumericKeyFDB encodes a pgtype.Numeric value to an FDB tuple.
func encodePgNumericKeyFDB(key pgtype.Numeric) (tuple.Tuple, error) {
	if encodedKey, err := encodePgNumeric(key); err != nil {
		return nil, fmt.Errorf("encode pgtype.Numeric: %w", err)
	} else if encodedBinary, err := key.EncodeBinary(nil, nil); err != nil {
		return nil, fmt.Errorf("encode binary pgtype Numeric: %w", err)
	} else {
		return tuple.Tuple{numericKey, encodedKey, encodedBinary}, nil
	}
}

// maybeDecodePgNumericTuple tries to decode the input tuple as a pgNumeric value.
// returns the decoded value if succeeds, and nil if not.
func maybeDecodePgNumericTuple(t tuple.Tuple) interface{} {
	if len(t) == 3 && t[0] == numericKey {
		if b, ok := t[2].([]byte); ok {
			var n pgtype.Numeric
			if err := n.DecodeBinary(nil, b); err == nil {
				return n
			}
		}
	}
	return nil
}

func encodePgNumeric(n pgtype.Numeric) (tuple.Tuple, error) {
	if n.Status == pgtype.Null {
		return nil, errors.New("Null value in Numeric Key Field")
	} else if n.Status == pgtype.Undefined {
		return nil, errors.New("Undefined value in Numeric Key Field")
	} else if n.NaN {
		return nil, errors.New("NaN value in a Numeric Key Field ")
	}

	// Conceptually, a pgtype.Numeric is encoded into a tuple of (exponents, fraction),
	// in which exponents is an int, and fraction is a slice of bytes.
	// It is ensured the encoded tuple preserves the order of the original numeric values
	// based on `int` and `byte slice` ordering on both components, respectively.

	// For positive numbers, the fraction is converted from the string of digits in the input without
	// leading or trailing zeros, and the `exponents`` is set properly to make sure
	// the represented value = 10 ^ (exponents) * 0.fraction.
	//   E.g. 0.0041 is represetned by (exp = -2, fraction="41"),
	//    and 3.14 is represented by (exp = 1, fraction="314")

	// For negative numbers, suppose its absolute value is encoded as (expAbs, fracAbs),
	// and the encoding result of the number is (-expAbs, invertNegfraction("-fracAbs")),
	// see func invertNegfraction for details.

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
		fractionBytes = invertNegfraction(strings.TrimRight(str, "0"))
	}

	return []tuple.TupleElement{biasedExponent, fractionBytes}, nil
}

func invertNegfraction(digits string) []byte {
	// The input digits string represents a negative integer, and starts with the `-` sign.
	// The function performs the following operations:
	// 1. remove the `-` sign, which is redundant.
	// 2. invert each digits to make sure the order between the encoded byte slices is consistent with
	//    the order between the negative values.
	//    This is done by replacing each digit x with (9 - x). (replace '0' with '9', '1' with '8', ..., '9' with '0').
	// 3. add an sentinel value `0x3a` (ASCII of ":") at the end of the encoded byte slice, to make sure the order of
	//    the encoded byte slices are consistent between a negative value and its prefixes. E.g.
	//    "-123" (encoded as "876:") is greater than "-1234" (encoded as "8765:").
	const sentinel = 0x3a
	var b = []byte(digits)
	for i := 1; i < len(b); i++ {
		b[i-1] = '9' - b[i] + '0'
	}

	if len(b) > 0 {
		b[len(b)-1] = sentinel
	}

	return b
}
