package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"unicode"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestVarcharKeyDiscovery(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "42325208"
	tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(id VARCHAR(32) PRIMARY KEY, data TEXT)"))
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

// TestCollatedCapture runs a complete capture from a table with an `xCHAR
// COLLATE SQL_Latin1_General_CP1_CI_AS` primary key, using key values which
// are known to exhibit interesting sort behavior.
//
// For the CHAR and VARCHAR column types we know that the capture will use
// our special order-preserving key encoding, but the NCHAR/NVARCHAR cases
// are considered "unpredictable" and we fall back to trusting the database
// ordering of keys and avoid doing any key sorting ourselves.
func TestCollatedCapture(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()
	for idx, columnType := range []string{"Char", "VarChar", "NChar", "NVarChar"} {
		t.Run(columnType, func(t *testing.T) {
			var uniqueID = fmt.Sprintf("00913034%02d", idx)
			var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(id %s(32) COLLATE SQL_Latin1_General_CP1_CI_AS PRIMARY KEY, data TEXT)", columnType))

			var testRows [][]any
			var testRunes = []rune("?abcdef€‚ƒ„…†‡ˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞß÷ÿ") // 100 distinct runes
			for idx, r := range testRunes {
				testRows = append(testRows, []any{fmt.Sprintf("a%sa %03d", string(r), idx), fmt.Sprintf("Row A%03d", idx)})
				testRows = append(testRows, []any{fmt.Sprintf("a%sb %03d", string(r), idx), fmt.Sprintf("Row B%03d", idx)})
			}
			tb.Insert(ctx, t, tableName, testRows)

			var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
			cs.Validator = &st.OrderedCaptureValidator{}
			tests.VerifiedCapture(ctx, t, cs)
		})
	}
}

// TestColumnCollations verifies that our encoding of text column keys can
// be round-tripped through FDB serialization/deserialization without loss,
// and that for collations which are considered "predictable" the serialized
// key ordering matches the server result ordering.
func TestColumnCollations(t *testing.T) {
	var tb, ctx = sqlserverTestBackend(t), context.Background()

	// Generate a set of unique test strings which we expect to exhibit
	// interesting sort behavior. Many of these runes have been selected
	// deliberately to exercise code points which various SQL Server code
	// pages sort differently from Unicode code point order. This list
	// should only ever be added to.
	var testRunes = "abc?@-_,·°€丠両░▒▓█αßσµ☆★"
	var testStrings []string
	for idx, r := range testRunes {
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'a', r, 'a'}), idx*3+0))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'b', r, 'b'}), idx*3+1))
		testStrings = append(testStrings, fmt.Sprintf("%s %06d", string([]rune{'c', r, 'c'}), idx*3+2))
	}

	for idx, tc := range []struct {
		ColumnType    string
		CollationName string
	}{
		{"VARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"VARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"VARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"VARCHAR", "Chinese_PRC_BIN2"},
		{"NVARCHAR", "SQL_Latin1_General_CP1_CI_AS"}, // SQL Server Default
		{"NVARCHAR", "SQL_Latin1_General_CP437_BIN"},
		{"NVARCHAR", "Latin1_General_100_BIN2_UTF8"},
		{"NVARCHAR", "Chinese_PRC_BIN2"},
	} {
		var subtestName = fmt.Sprintf("%s_%s", tc.ColumnType, tc.CollationName)
		t.Run(subtestName, func(t *testing.T) {
			var uniqueID = fmt.Sprintf("2553_%04d", idx)
			var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(id %s(32) COLLATE %s PRIMARY KEY, data TEXT)", tc.ColumnType, tc.CollationName))

			var insertRows [][]any
			for idx, str := range testStrings {
				insertRows = append(insertRows, []any{str, fmt.Sprintf("This is test row %d with contents %q", idx, str)})
			}
			tb.Insert(ctx, t, tableName, insertRows)

			var resultRows, err = tb.control.QueryContext(ctx, fmt.Sprintf(`SELECT id, data FROM %s ORDER BY id`, tableName))
			require.NoError(t, err)
			defer resultRows.Close()

			// Simulate what we'd end up with after all the discovery->backfill plumbing.
			var keyColumns = []string{"id"}
			var idCollation = &sqlserverTextColumnType{strings.ToLower(tc.ColumnType), tc.CollationName, tc.ColumnType + "(32)"}
			var columnTypes = map[string]any{
				"id":   idCollation,
				"data": &sqlserverTextColumnType{"text", "SQL_Latin1_General_CP1_CI_AS", "TEXT"},
			}

			var prevID string
			var prevKey []byte
			for resultRows.Next() {
				var id, data string
				require.NoError(t, resultRows.Scan(&id, &data))
				var rowKey, err = sqlcapture.EncodeRowKey(keyColumns, map[string]any{"id": id, "data": data}, columnTypes, encodeKeyFDB)
				require.NoError(t, err)

				roundTrippedKey, err := sqlcapture.UnpackTuple(rowKey, decodeKeyFDB)
				require.NoError(t, err)
				if len(roundTrippedKey) != 1 || id != roundTrippedKey[0] {
					t.Fatalf("key round-trip failure: originally %#v but now %#v", id, roundTrippedKey)
				}

				if predictableCollation(idCollation) && bytes.Compare(prevKey, rowKey) >= 0 {
					t.Fatalf("key ordering failure: we think %q >= %q (in `%s(32) COLLATE %s` column) but the server disagrees", prevID, id, tc.ColumnType, tc.CollationName)
				}
				prevID, prevKey = id, rowKey
			}
		})
	}
}

func TestGenerateCollationTable(t *testing.T) {
	// This test is not really intended to be run as an automated test at all, it's
	// more a tool meant to be used by a human to add support for a new collation
	// by autogenerating a mapping table. But it was easiest to write it as a test,
	// and leaving it in the test suite makes it less likely to bitrot.
	//
	// To use this tool, set the `GENERATE_COLLATION_ID` environment variable to the
	// name of a collation we would like to be able to predict and then run the test.
	var collationID = os.Getenv("GENERATE_COLLATION_ID") // Example: "varchar/SQL_Latin1_General_CP1_CI_AS"
	if collationID == "" {
		t.Skipf("skipping %q because GENERATE_COLLATION_ID is unset", t.Name())
	}

	var tb, ctx = sqlserverTestBackend(t), context.Background()
	var uniqueID = "27623054"

	var collationIDBits = strings.Split(collationID, "/")
	var columnType = collationIDBits[0]
	var collationName = collationIDBits[1]
	var tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(id %s(32) COLLATE %s PRIMARY KEY, tiebreaker INTEGER, codepoint INTEGER)", columnType, collationName))

	// Make a list of every valid Unicode code point that has been assigned.
	// Note that this range is still significantly smaller than all theoretically
	// valid code points.
	var generateRunes []rune
	var validateRunes []rune
	const maxAssignedRune = 0x3134A
	for i := rune(0); i <= maxAssignedRune; i++ {
		// Ignore the "surrogate characters" and "private use" ranges
		if (0xD800 <= i && i <= 0xF8FF) || (0xF0000 <= i && i <= 0xFFFFD) || (0x100000 <= i && i <= 0x10FFFD) {
			continue
		}
		generateRunes = append(generateRunes, i)
		if unicode.IsPrint(i) {
			validateRunes = append(validateRunes, i)
		}
	}

	// Insert two rows for each Unicode code point. For a given character we generate
	// the two row keys "${CHAR} 0 ${CODEPOINT}" and "${CHAR} 1 ${CODEPOINT}". Each
	// part of this key is chosen deliberately:
	//   - The character itself comes first, so that the table will be sorted primarily
	//     by the collation ordering of each character.
	//   - After the character is a "tiebreaker" digit of 0 or 1. This can be used to
	//     detect when multiple characters are considered equivalent in a collation.
	//   - Finally the codepoint is included, to ensure that every row key is distinct
	//     regardless of how the character itself is collated.
	var tableRows [][]any
	for _, r := range generateRunes {
		tableRows = append(tableRows, []any{fmt.Sprintf("%s 1 %06d", string(r), int(r)), 1, r})
		tableRows = append(tableRows, []any{fmt.Sprintf("%s 0 %06d", string(r), int(r)), 0, r})
	}
	tb.Insert(ctx, t, tableName, tableRows)

	// Read the rows back out of the database and build a result table.
	//
	// We collect successive rows with a tiebreaker value of zero, as this indicates
	// that they are all considered equal. Then when a tiebreaker of 1 is reached we
	// consider the previous run of equal characters to be ended.
	var currentSortRunes []rune // The current list of runes which are considered equal under collation ordering.
	var collationTable [][]rune // A list of such equivalent-rune-lists, in collation order.
	var query = fmt.Sprintf(`SELECT id, tiebreaker, codepoint FROM %s ORDER BY id`, tableName)
	var rows, err = tb.control.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var key string
		var tiebreaker int
		var originalCodepoint int
		require.NoError(t, rows.Scan(&key, &tiebreaker, &originalCodepoint))
		log.WithField("key", key).Trace("sorted test row")

		var keyRunes = []rune(key)
		if keyRunes[1] != ' ' {
			// Sometimes input characters will be transcoded into multiple. For instance,
			// U+10000 when stored into a SQL_Latin1_General_CP1_CI_AS results in the two-
			// character string "??". We don't care about this, because our mapping tables
			// only need to cover the individual characters we might receive *out* of a table
			// with a particular storage format.
			log.WithField("key", key).Debug("ignoring row with multiple-character transcoding")
			continue
		}

		// We only want to build a collation table over characters that it's
		// actually possible to retrieve from the database in a given collation.
		// Note that due to normalization this means we might insert the same
		// code point multiple times. Later on when we build the mapping table
		// this will get deduplicated, and also sanity-checked to make sure that
		// the same codepoint doesn't occur at multiple distinct sort positions.
		var codepoint = keyRunes[0]
		if tiebreaker == 0 {
			if codepoint == 0xFFFD && originalCodepoint != 0xFFFD {
				log.WithField("originally", originalCodepoint).Warn("ignoring U+FFFD REPLACEMENT CHARACTER normalization")
			} else {
				log.WithField("codepoint", codepoint).WithField("index", len(collationTable)).Trace("assigned codepoint to sort index")
				currentSortRunes = append(currentSortRunes, codepoint)
			}
		} else {
			// If we encounter a nonzero tiebreaker value as the first row then this
			// collation is too crazy for the current table-building logic.
			if len(collationTable) == 0 && len(currentSortRunes) == 0 {
				log.Fatalf("tiebreaker zero must be sorted first: got %d for %d", tiebreaker, codepoint)
			}
			if len(currentSortRunes) > 0 {
				collationTable = append(collationTable, currentSortRunes)
				currentSortRunes = nil
			}
		}
	}
	if len(currentSortRunes) > 0 {
		log.Fatalf("tiebreaker one must be sorted last")
	}

	// Identify the sort index to which most characters map. Typically vast chunks of
	// the Unicode code-point space are mapped to '?' or something along those lines,
	// and the mapping table becomes much more concise once we remove all those entries
	// in favor of a single default value.
	var mostCommonIndex int
	for idx := range collationTable {
		if len(collationTable[idx]) > len(collationTable[mostCommonIndex]) {
			mostCommonIndex = idx
		}
	}
	log.WithField("index", mostCommonIndex).WithField("runes", len(collationTable[mostCommonIndex])).WithField("first", collationTable[mostCommonIndex][0]).Info("most common sort index")

	// Build a table mapping runes to sort indexes, omitting the most common mapping
	// because that shrinks the table by a significant factor. Then add the "default"
	// mapping for 'rune' -1 since that cannot exist and it simplifies things to have
	// all the collation info in this one table.
	var collationMapping = make(map[rune]int)
	for sortIndex, runes := range collationTable {
		if sortIndex != mostCommonIndex {
			for _, r := range runes {
				if existingIndex, ok := collationMapping[r]; ok {
					if existingIndex != sortIndex {
						log.WithField("rune", r).WithField("firstidx", existingIndex).WithField("nextidx", sortIndex).Warn("same rune occurs at multiple sort positions")
					}
				} else {
					collationMapping[r] = sortIndex
				}
			}
		}
	}
	collationMapping[-1] = mostCommonIndex

	// Insert a bunch more randomly generated test strings into the table so that
	// we can validate that the collation-key encoding works. We only use printable
	// runes here because SQL Server's handling of nonprintable characters can be
	// a bit wonky (multiple characters will be transcoded into U+FFFD REPLACEMENT CHARACTER
	// upon retrieval, but their comparison order is somehow distinct inside the database).
	var validationRows [][]any
	for _, r := range validateRunes {
		validationRows = append(validationRows, []any{fmt.Sprintf("%s a (U+%04X) %06d", string([]rune{r}), r, len(validationRows)), 2, r})
		validationRows = append(validationRows, []any{fmt.Sprintf("%s b (U+%04X) %06d", string([]rune{r}), r, len(validationRows)), 2, r})
		validationRows = append(validationRows, []any{fmt.Sprintf("%s c (U+%04X) %06d", string([]rune{r}), r, len(validationRows)), 2, r})
	}
	tb.Insert(ctx, t, tableName, validationRows)

	// Verify that the collated representation is properly ordered.
	validationResults, err := tb.control.QueryContext(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE tiebreaker = 2 ORDER BY id`, tableName))
	require.NoError(t, err)
	defer validationResults.Close()
	var prevEncodedKey []byte
	var prevKey string
	for validationResults.Next() {
		var keyText string
		require.NoError(t, validationResults.Scan(&keyText))
		log.WithField("key", keyText).Trace("sorted validation row")
		var nextEncodedKey = encodeCollationSortKeyUsingTable(collationMapping, keyText)
		require.NoError(t, err)
		if prevEncodedKey != nil {
			if bytes.Compare(prevEncodedKey, nextEncodedKey) != -1 {
				log.WithField("prev", prevKey).WithField("next", keyText).Fatal("ordering validation error")
			}
		}
		prevEncodedKey, prevKey = nextEncodedKey, keyText
	}

	// Print the success output
	log.WithField("collation", collationID).Info("validation complete")
	fmt.Printf("%#v\n", collationMapping)
}
