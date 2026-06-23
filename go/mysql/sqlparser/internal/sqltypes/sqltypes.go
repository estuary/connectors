// Copyright 2019 The Vitess Authors.
// Licensed under the Apache License, Version 2.0 (see the LICENSE file in the
// parent sqlparser directory).
//
// This file contains code and data derived from vitess.io/vitess/go/sqltypes
// (v0.21.3), reduced by Estuary Technologies, Inc. to a proto-free subset.

// Package sqltypes is a trimmed, protobuf-free shim of the upstream
// vitess.io/vitess/go/sqltypes package. It exposes only the pieces the
// extracted SQL parser references: the MySQL type-tag enum, the SQL
// string-literal escaping helpers, and the tokenizer's decode table.
//
// The upstream package aliases its Type to a protobuf enum
// (querypb.Type), which is the single dependency that drags the entire
// protobuf module closure into the parser. Here Type is a plain int32 whose
// constant values are copied verbatim from query.proto so the tags stay
// faithful, even though the parser only ever reads ColumnType.Type as a
// string and never depends on these integer values.
package sqltypes

import (
	"strconv"
	"strings"
)

// Type is a MySQL value type tag. Upstream this is `= querypb.Type`; here it
// is a standalone int32 with identical constant values.
type Type int32

// Type-tag constants, with integer values copied from query.proto. Unknown is
// the parser's sentinel for "no type"; the rest mirror querypb.Type_*.
const (
	Unknown   = Type(-1)
	Null      = Type(0)
	Int8      = Type(257)
	Uint8     = Type(770)
	Int16     = Type(259)
	Uint16    = Type(772)
	Int24     = Type(261)
	Uint24    = Type(774)
	Int32     = Type(263)
	Uint32    = Type(776)
	Int64     = Type(265)
	Uint64    = Type(778)
	Float32   = Type(1035)
	Float64   = Type(1036)
	Timestamp = Type(2061)
	Date      = Type(2062)
	Time      = Type(2063)
	Datetime  = Type(2064)
	Year      = Type(785)
	Decimal   = Type(18)
	Text      = Type(6163)
	Blob      = Type(10260)
	VarChar   = Type(6165)
	VarBinary = Type(10262)
	Char      = Type(6167)
	Binary    = Type(10264)
	Bit       = Type(2073)
	Enum      = Type(2074)
	Set       = Type(2075)
	Tuple     = Type(28)
	Geometry  = Type(2077)
	TypeJSON  = Type(2078)
	HexNum    = Type(4128)
	HexVal    = Type(4129)
	BitNum    = Type(4130)
	Vector    = Type(2083)
)

// Type flag bits, copied from querypb.Flag. Only the integral/unsigned bits
// are needed (by IsUnsigned).
const (
	flagIsIntegral = 256
	flagIsUnsigned = 512
)

// typeName maps a type tag to its MySQL/proto enum name, copied from
// querypb.Type_name. Used by String() when formatting statically-typed bind
// variables.
var typeName = map[Type]string{
	Null:      "NULL_TYPE",
	Int8:      "INT8",
	Uint8:     "UINT8",
	Int16:     "INT16",
	Uint16:    "UINT16",
	Int24:     "INT24",
	Uint24:    "UINT24",
	Int32:     "INT32",
	Uint32:    "UINT32",
	Int64:     "INT64",
	Uint64:    "UINT64",
	Float32:   "FLOAT32",
	Float64:   "FLOAT64",
	Timestamp: "TIMESTAMP",
	Date:      "DATE",
	Time:      "TIME",
	Datetime:  "DATETIME",
	Year:      "YEAR",
	Decimal:   "DECIMAL",
	Text:      "TEXT",
	Blob:      "BLOB",
	VarChar:   "VARCHAR",
	VarBinary: "VARBINARY",
	Char:      "CHAR",
	Binary:    "BINARY",
	Bit:       "BIT",
	Enum:      "ENUM",
	Set:       "SET",
	Tuple:     "TUPLE",
	Geometry:  "GEOMETRY",
	TypeJSON:  "JSON",
	HexNum:    "HEXNUM",
	HexVal:    "HEXVAL",
	BitNum:    "BITNUM",
	Vector:    "VECTOR",
}

// String returns the MySQL/proto enum name for the type, mirroring the
// upstream protobuf enum's String method.
func (t Type) String() string {
	if name, ok := typeName[t]; ok {
		return name
	}
	return "Type(" + strconv.Itoa(int(t)) + ")"
}

// IsUnsigned returns true if the type is an unsigned integral type.
func IsUnsigned(t Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral|flagIsUnsigned
}

// IsDecimal returns true if the type is the DECIMAL type.
func IsDecimal(t Type) bool {
	return t == Decimal
}

// IsDate returns true if the type is a date-like type (excluding TIME).
func IsDate(t Type) bool {
	return t == Datetime || t == Date || t == Timestamp
}

// DontEscape signals that a byte requires no escaping in a SQL string literal.
const DontEscape = byte(255)

// SQLEncodeMap maps a byte to its escape character, or DontEscape if it needs
// no escaping. SQLDecodeMap is its inverse, used by the tokenizer to unescape
// string literals.
var (
	SQLEncodeMap [256]byte
	SQLDecodeMap [256]byte
)

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

var decodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

func init() {
	for i := range SQLEncodeMap {
		SQLEncodeMap[i] = DontEscape
		SQLDecodeMap[i] = DontEscape
	}
	for i := range SQLEncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			SQLEncodeMap[byte(i)] = to
		}
	}
	for i := range SQLDecodeMap {
		if to, ok := decodeRef[byte(i)]; ok {
			SQLDecodeMap[to] = byte(i)
		}
	}
}

// BufEncodeStringSQL encodes a string as a single-quoted SQL string literal
// into the given builder.
func BufEncodeStringSQL(buf *strings.Builder, val string) {
	buf.WriteByte('\'')
	for idx := 0; idx < len(val); idx++ {
		ch := val[idx]
		// Preserve \% and \_ as-is so LIKE escape sequences are not double-escaped.
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteByte(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}

// EncodeStringSQL encodes a string as a single-quoted SQL string literal.
func EncodeStringSQL(val string) string {
	var buf strings.Builder
	BufEncodeStringSQL(&buf, val)
	return buf.String()
}

// BufEncodeBytesSQL encodes raw bytes as a single-quoted SQL string literal.
// Upstream this is reached via MakeTrusted(VarBinary, val).EncodeSQL(buf); the
// quoted path collapses to this byte-escaping routine.
func BufEncodeBytesSQL(buf *strings.Builder, val []byte) {
	buf.WriteByte('\'')
	for idx := 0; idx < len(val); idx++ {
		ch := val[idx]
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteByte(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}
