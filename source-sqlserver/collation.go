package main

import (
	"fmt"
	"strings"
)

// encodeCollationSortKey receives a UTF-8 string and a description of the SQL Server
// column type and the collation name by which it is ordered, and returns a sequence
// of encoded bytes with the property that the *bytewise lexicographic* ordering of
// such bytes matches the *collated text* ordering applied by SQL Server, for a given
// combination of column type and collation name.
//
// It does this using a set of fixed data tables which map Unicode code points which
// may appear in an input string to integer "sort values", and then encoding them
// using a UTF8-esque multibyte varint encoding which preserves *integer* lexicographic
// ordering.
//
// If a particular collation is not currently implemented as a data table, an error
// will be returned instead. TODO(wgd): Document where the tool lives for autogenerating
// new collation data tables.
func encodeCollationSortKey(info *sqlserverTextColumnType, text string) ([]byte, error) {
	var collationID = strings.ToLower(info.Type) + "/" + info.Collation
	if collationTable, ok := supportedTextCollations[collationID]; ok {
		return encodeCollationSortKeyUsingTable(collationTable, text), nil
	}
	return nil, fmt.Errorf("collation %q is not currently supported, please file a bug report", collationID)
}

func encodeCollationSortKeyUsingTable(collation collationMapping, text string) []byte {
	var sortKey []byte
	for _, r := range text {
		// Fetch the sort-index value corresponding to this character. Every
		// collation data table has a "default value" which is stored at integer
		// index -1. This is safe since the Unicode text input has no concept of
		// negative code points, and keeping everything in a single int->int
		// mapping table makes things a lot simpler.
		var x, ok = collation[r]
		if !ok {
			x = collation[-1]
		}

		// A trivial varint encoding which happens to just be a knockoff of UTF-8 with the
		// serial numbers filed off. Since this reimplementation lacks any concept of "illegal
		// values" it supports any integer [0, 0x200000) which should be plenty of range.
		// And since it's a knockoff of UTF-8 it has the same "bytewise lexicographic
		// ordering matches integer lexicographic ordering" property that we need for
		// sortable row keys.
		switch {
		case x <= 0x7F: // 7 bits
			sortKey = append(sortKey, byte(x))
		case x <= 0x7FF: // 11 bits (5+6)
			sortKey = append(sortKey, 0b11000000+byte(x>>6), 0x80+byte(x&0x3F))
		case x <= 0xFFFF: // 16 bits (4+6+6)
			sortKey = append(sortKey, 0b11100000+byte(x>>12), 0x80+byte((x>>6)&0x3F), 0x80+byte(x&0x3F))
		default: // 21 bits (3+6+6+6)
			sortKey = append(sortKey, 0b11110000+byte(x>>18), 0x80+byte((x>>12)&0x3F), 0x80+byte((x>>6)&0x3F), 0x80+byte(x&0x3F))
		}
	}
	return sortKey
}

type collationMapping = map[rune]int

var supportedTextCollations = map[string]collationMapping{}
