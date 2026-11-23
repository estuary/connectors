package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

func TestTranslateDBToJSONType(t *testing.T) {
	var cases = []struct {
		name     string
		column   db2ColumnType
		nullable bool
		schema   string
	}{
		{"IntegerKey", db2ColumnType{Name: "INTEGER"}, false, `{"type":"integer"}`},
		{"NullableSmallint", db2ColumnType{Name: "SMALLINT"}, true, `{"type":["integer","null"]}`},
		{"Bigint", db2ColumnType{Name: "BIGINT"}, false, `{"type":"integer"}`},
		{"Boolean", db2ColumnType{Name: "BOOLEAN"}, true, `{"type":["boolean","null"]}`},
		{"Real", db2ColumnType{Name: "REAL"}, false, `{"type":"number"}`},
		{"Double", db2ColumnType{Name: "DOUBLE"}, false, `{"type":"number"}`},
		{"Decimal", db2ColumnType{Name: "DECIMAL", Length: 10, Scale: 2}, true, `{"type":["string","null"],"format":"number"}`},
		{"Decfloat", db2ColumnType{Name: "DECFLOAT"}, false, `{"type":"string","format":"number"}`},
		{"Char", db2ColumnType{Name: "CHARACTER", Length: 8, Codepage: 1208}, true, `{"type":["string","null"],"maxLength":8}`},
		{"Varchar", db2ColumnType{Name: "VARCHAR", Length: 50, Codepage: 1208}, false, `{"type":"string","maxLength":50}`},
		{"LongVarchar", db2ColumnType{Name: "LONG VARCHAR", Length: 32700, Codepage: 1208}, false, `{"type":"string","maxLength":32700}`},
		{"CharBitData", db2ColumnType{Name: "CHARACTER", Length: 8}, false, `{"type":"string","contentEncoding":"base64"}`},
		{"VarcharBitData", db2ColumnType{Name: "VARCHAR", Length: 50}, true, `{"type":["string","null"],"contentEncoding":"base64"}`},
		{"LongVarcharBitData", db2ColumnType{Name: "LONG VARCHAR", Length: 32700}, false, `{"type":"string","contentEncoding":"base64"}`},
		{"Blob", db2ColumnType{Name: "BLOB"}, false, `{"type":"string","contentEncoding":"base64"}`},
		{"Varbinary", db2ColumnType{Name: "VARBINARY"}, true, `{"type":["string","null"],"contentEncoding":"base64"}`},
		{"Clob", db2ColumnType{Name: "CLOB"}, true, `{"type":["string","null"]}`},
		{"Graphic", db2ColumnType{Name: "GRAPHIC"}, false, `{"type":"string"}`},
		{"Vargraphic", db2ColumnType{Name: "VARGRAPHIC"}, true, `{"type":["string","null"]}`},
		{"LongVargraphic", db2ColumnType{Name: "LONG VARGRAPHIC", Codepage: 1200}, true, `{"type":["string","null"]}`},
		{"Dbclob", db2ColumnType{Name: "DBCLOB"}, false, `{"type":"string"}`},
		{"XML", db2ColumnType{Name: "XML"}, true, `{"type":["string","null"]}`},
		{"Date", db2ColumnType{Name: "DATE"}, false, `{"type":"string","format":"date"}`},
		{"Time", db2ColumnType{Name: "TIME"}, false, `{"type":"string"}`},
		{"Timestamp", db2ColumnType{Name: "TIMESTAMP"}, true, `{"type":["string","null"],"format":"date-time"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var db db2Database
			var schema, err = db.TranslateDBToJSONType(sqlcapture.ColumnInfo{DataType: tc.column, IsNullable: tc.nullable}, false)
			require.NoError(t, err)
			var encoded, marshalErr = json.Marshal(schema)
			require.NoError(t, marshalErr)
			require.JSONEq(t, tc.schema, string(encoded))
		})
	}
	t.Run("UnknownType", func(t *testing.T) {
		var db db2Database
		var _, err = db.TranslateDBToJSONType(sqlcapture.ColumnInfo{DataType: db2ColumnType{Name: "CUSTOM_TYPE"}}, false)
		require.ErrorContains(t, err, "unhandled Db2 type")
	})
}

func TestTranslateDB2Values(t *testing.T) {
	var db, _ = testDatabase(t)
	db.datetimeLocation = time.FixedZone("test", -7*60*60)
	var timestamp = time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	for _, tc := range []struct {
		name     string
		column   db2ColumnType
		value    any
		expected any
	}{
		{"Integer", db2ColumnType{Name: "INTEGER"}, int32(123), int32(123)},
		{"Smallint", db2ColumnType{Name: "SMALLINT"}, int32(-2), int32(-2)},
		{"Text", db2ColumnType{Name: "VARCHAR", Codepage: 1208}, []byte("apple"), "apple"},
		{"LongVarchar", db2ColumnType{Name: "LONG VARCHAR", Codepage: 1208}, []byte("a@b.c"), "a@b.c"},
		{"LongVargraphic", db2ColumnType{Name: "LONG VARGRAPHIC", Codepage: 1200}, []byte("é漢🌍"), "é漢🌍"},
		{"UnknownText", db2ColumnType{Name: "CUSTOM_TYPE", Codepage: 1208}, []byte("a@b.c"), "a@b.c"},
		{"UnknownBinary", db2ColumnType{Name: "CUSTOM_TYPE"}, []byte{0, 1, 255}, []byte{0, 1, 255}},
		{"CharPadding", db2ColumnType{Name: "CHARACTER", Codepage: 1208}, []byte("SKU-A   "), "SKU-A   "},
		{"BitData", db2ColumnType{Name: "VARCHAR"}, []byte{0, 1, 255}, []byte{0, 1, 255}},
		{"LongVarcharBitData", db2ColumnType{Name: "LONG VARCHAR"}, []byte{0, 1, 255}, []byte{0, 1, 255}},
		{"Decimal", db2ColumnType{Name: "DECIMAL"}, []byte("12345678901234567890.12"), "12345678901234567890.12"},
		{"DecfloatNaN", db2ColumnType{Name: "DECFLOAT"}, []byte("sNaN"), "NaN"},
		{"Date", db2ColumnType{Name: "DATE"}, timestamp, "2026-01-02"},
		{"Time", db2ColumnType{Name: "TIME"}, timestamp, "03:04:05"},
		{"TimeText", db2ColumnType{Name: "TIME"}, "03.04.05", "03:04:05"},
		{"TimeBytes", db2ColumnType{Name: "TIME"}, []byte("03:04:05"), "03:04:05"},
		{"TimeMidnight", db2ColumnType{Name: "TIME"}, []byte("00.00.00"), "00:00:00"},
		{"TimeEndOfDay", db2ColumnType{Name: "TIME"}, []byte("24.00.00"), "24:00:00"},
		{"TimeEndOfDayText", db2ColumnType{Name: "TIME"}, "24:00:00", "24:00:00"},
		{"TimeNull", db2ColumnType{Name: "TIME"}, nil, nil},
		{"Timestamp", db2ColumnType{Name: "TIMESTAMP"}, timestamp, "2026-01-02T03:04:05.123456-07:00"},
		{"Timestamp12", db2ColumnType{Name: "TIMESTAMP", Scale: 12}, []byte("2026-01-02-03.04.05.123456789001"), "2026-01-02T03:04:05.123456789001-07:00"},
		{"Timestamp10", db2ColumnType{Name: "TIMESTAMP", Scale: 10}, "2026-01-02 03:04:05.1234567890", "2026-01-02T03:04:05.1234567890-07:00"},
		{"TimestampNull", db2ColumnType{Name: "TIMESTAMP", Scale: 12}, nil, nil},
		{"Null", db2ColumnType{Name: "INTEGER"}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var value, err = db.translateDB2Value(tc.value, tc.column)
			require.NoError(t, err)
			require.Equal(t, tc.expected, value)
		})
	}
}

func TestSourceMetadataSchema(t *testing.T) {
	var db, _ = testDatabase(t)
	for _, name := range []string{"schema", "table", "cursor", "intent", "ts_ms"} {
		var _, found = db.SourceMetadataSchema(true).Properties.Get(name)
		require.True(t, found, name)
	}
	var _, found = db.SourceMetadataSchema(true).Properties.Get("tag")
	require.False(t, found)
	db.config.Advanced.SourceTag = "fixture"
	_, found = db.SourceMetadataSchema(true).Properties.Get("tag")
	require.True(t, found)
}
