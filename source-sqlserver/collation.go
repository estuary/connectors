package main

import (
	"fmt"
	"strings"
)

// predictableCollation returns true if and only if the connector's logic for
// "encodeCollationSortKey" will be able to accurately reproduce the database
// key ordering behavior.
func predictableCollation(info *sqlserverTextColumnType) bool {
	var collationID = strings.ToLower(info.Type) + "/" + info.Collation
	var _, ok = supportedTextCollations[collationID]
	return ok
}

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

var (
	supportedTextCollations = map[string]collationMapping{
		"char/SQL_Latin1_General_CP1_CI_AS":    collationCharSQLLatin1GeneralCP1CIAS,
		"varchar/SQL_Latin1_General_CP1_CI_AS": collationCharSQLLatin1GeneralCP1CIAS,
	}
	collationCharSQLLatin1GeneralCP1CIAS = map[int32]int{-1: 53, 0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, 11: 11, 12: 12, 13: 13, 14: 14, 15: 15, 16: 16, 17: 17, 18: 18, 19: 19, 20: 20, 21: 21, 22: 22, 23: 23, 24: 24, 25: 25, 26: 26, 27: 27, 28: 28, 29: 29, 30: 30, 31: 31, 32: 32, 33: 33, 34: 34, 35: 35, 36: 36, 37: 37, 38: 38, 39: 39, 40: 40, 41: 41, 42: 42, 43: 43, 44: 44, 45: 45, 46: 46, 47: 47, 48: 127, 49: 128, 50: 129, 51: 130, 52: 131, 53: 132, 54: 133, 55: 134, 56: 135, 57: 136, 58: 48, 59: 49, 60: 50, 61: 51, 62: 52, 64: 54, 65: 137, 66: 139, 67: 140, 68: 141, 69: 142, 70: 143, 71: 144, 72: 145, 73: 146, 74: 147, 75: 148, 76: 149, 77: 150, 78: 151, 79: 152, 80: 153, 81: 154, 82: 155, 83: 156, 84: 158, 85: 159, 86: 160, 87: 161, 88: 162, 89: 163, 90: 164, 91: 55, 92: 56, 93: 57, 94: 58, 95: 59, 96: 60, 97: 137, 98: 139, 99: 140, 100: 141, 101: 142, 102: 143, 103: 144, 104: 145, 105: 146, 106: 147, 107: 148, 108: 149, 109: 150, 110: 151, 111: 152, 112: 153, 113: 154, 114: 155, 115: 156, 116: 158, 117: 159, 118: 160, 119: 161, 120: 162, 121: 163, 122: 164, 123: 61, 124: 62, 125: 63, 126: 64, 127: 65, 160: 93, 161: 94, 162: 95, 163: 96, 164: 97, 165: 98, 166: 99, 167: 100, 168: 101, 169: 102, 170: 103, 171: 104, 172: 105, 173: 106, 174: 107, 175: 108, 176: 109, 177: 110, 178: 111, 179: 112, 180: 113, 181: 114, 182: 115, 183: 116, 184: 117, 185: 118, 186: 119, 187: 120, 188: 121, 189: 122, 190: 123, 191: 124, 192: 137, 193: 137, 194: 137, 195: 137, 196: 137, 197: 137, 198: 138, 199: 140, 200: 142, 201: 142, 202: 142, 203: 142, 204: 146, 205: 146, 206: 146, 207: 146, 208: 165, 209: 151, 210: 152, 211: 152, 212: 152, 213: 152, 214: 152, 215: 125, 216: 152, 217: 159, 218: 159, 219: 159, 220: 159, 221: 163, 222: 166, 223: 157, 224: 137, 225: 137, 226: 137, 227: 137, 228: 137, 229: 137, 230: 138, 231: 140, 232: 142, 233: 142, 234: 142, 235: 142, 236: 146, 237: 146, 238: 146, 239: 146, 240: 165, 241: 151, 242: 152, 243: 152, 244: 152, 245: 152, 246: 152, 247: 126, 248: 152, 249: 159, 250: 159, 251: 159, 252: 159, 253: 163, 254: 166, 255: 163, 338: 77, 339: 90, 352: 75, 353: 88, 376: 92, 381: 78, 382: 91, 402: 68, 710: 73, 732: 86, 8211: 84, 8212: 85, 8216: 79, 8217: 80, 8218: 67, 8220: 81, 8221: 82, 8222: 69, 8224: 71, 8225: 72, 8226: 83, 8230: 70, 8240: 74, 8249: 76, 8250: 89, 8364: 66, 8482: 87}
)
