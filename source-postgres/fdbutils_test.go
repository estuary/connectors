package main

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
)

func TestEncodePgNumeric(t *testing.T) {
	var testCases = []struct {
		input    pgtype.Numeric
		expected tuple.Tuple
	}{
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(1), Exp: 0},
			[]tuple.TupleElement{20001, []byte("1")}, // "1e0"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(1), Exp: -16382},
			[]tuple.TupleElement{3619, []byte("1")}, // "1e-16382"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(12309), Exp: -6},
			[]tuple.TupleElement{19999, []byte("12309")}, // "12309e-6"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(123400), Exp: 3},
			[]tuple.TupleElement{20009, []byte("1234")}, // "123400e3"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(0), Exp: 0},
			[]tuple.TupleElement{0, []byte(nil)}, // "0e0"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(-1), Exp: 0},
			[]tuple.TupleElement{-20001, []byte("8:")}, // "-1e0"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(-1), Exp: -16382},
			[]tuple.TupleElement{-3619, []byte("8:")}, // "-1e-1"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(-12309), Exp: -6},
			[]tuple.TupleElement{-19999, []byte("87690:")}, // "-12309e-6"
		},
		{
			pgtype.Numeric{Status: pgtype.Present, Int: big.NewInt(-123400), Exp: 3},
			[]tuple.TupleElement{-20009, []byte("8765:")}, // "-123400e3"
		},
	}

	for _, test := range testCases {
		var actual, err = encodePgNumeric(test.input)
		require.NoError(t, err)
		require.Equal(t, test.expected, actual)
	}
}

func TestEncodeAndDecodePgNumericKeyFDB(t *testing.T) {
	var k = 20000
	var resumeKeyIndex = 100
	var ctx, tableName, testCases = buildNumericKeyTestCases(t, k)
	var tb = postgresTestBackend(t)

	var preEncoded []byte
	for i := 0; i < len(testCases); i++ {
		tp, err := encodePgNumericKeyFDB(testCases[i])
		require.NoError(t, err)

		// Make sure the encoded preserves the order.
		var encoded = tp.Pack()
		if i > 0 {
			var message = fmt.Sprintf("Comparing %+v, %+v", testCases[i-1], testCases[i])
			require.Equal(t, -1, bytes.Compare(preEncoded, encoded), message)
		}
		preEncoded = encoded

		// Make sure decode returns the original value.
		var decoded = maybeDecodePgNumericTuple(tp)
		require.Equal(t, testCases[i], decoded, fmt.Sprintf("Testing: %+v", testCases[i]))

		// Make sure the decoded element can be used in resume query.
		if i == resumeKeyIndex {
			var query = fmt.Sprintf("SELECT count(1) FROM %s WHERE id < $1;", tableName)
			var n int
			require.NoError(t, tb.control.QueryRow(ctx, query, decoded).Scan(&n))
			require.Equal(t, n, resumeKeyIndex)
		}
	}
}

// populates a database table with random numeric ids, and
// returns the db tableName, and a list of pgtype.Numeric numbers in ascending order extracted from the table.
func buildNumericKeyTestCases(t *testing.T, k int) (ctx context.Context, tableName string, testCases []pgtype.Numeric) {
	ctx = context.Background()
	var tb = postgresTestBackend(t)
	tableName = tb.CreateTable(ctx, t, "", "(id NUMERIC PRIMARY KEY)")

	tb.Query(ctx, t, fmt.Sprintf(
		`INSERT INTO %[1]s(id) 
		 (SELECT (random() * 2 * %[2]d) - %[2]d
		   FROM generate_series(1, %[2]d))
		 ON CONFLICT (id) DO NOTHING;`,
		tableName, k),
	)

	var rows, err = tb.control.Query(ctx, fmt.Sprintf("SELECT id FROM %s ORDER BY id;", tableName))
	require.NoError(t, err)
	defer rows.Close()

	testCases = make([]pgtype.Numeric, 0, k)
	for rows.Next() {
		var id pgtype.Numeric
		err = rows.Scan(&id)
		require.NoError(t, err)

		testCases = append(testCases, id)
	}

	return
}
