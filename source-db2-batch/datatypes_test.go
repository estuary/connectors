package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

// TestKeyDiscovery exercises the connector's ability to discover types of primary key columns.
func TestKeyDiscovery(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
		k_smallint SMALLINT NOT NULL,
		k_integer INTEGER NOT NULL,
		k_bigint BIGINT NOT NULL,
		k_bool BOOLEAN NOT NULL,
		k_str VARCHAR(8) NOT NULL,
		data CLOB,
		PRIMARY KEY (k_smallint, k_integer, k_bigint, k_bool, k_str)
	)`)
	cupaloy.SnapshotT(t, summarizeBindings(t, discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))))
}

// TestIntegerTypes exercises discovery and capture of the integer types
// SMALLINT, INTEGER, BIGINT, and BOOLEAN.
func TestIntegerTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        smallint_col SMALLINT,
        integer_col INTEGER,
        bigint_col BIGINT,
        boolean_col BOOLEAN
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, 42, 42, 42, true}, // Regular row with normal values
			{1, -32768, -2147483648, -9223372036854775808, false}, // Minimum values
			{2, 32767, 2147483647, 9223372036854775807, true},     // Maximum values
			{3, 0, 0, 0, false},     // Zero/false values
			{4, nil, nil, nil, nil}, // Null values
		} {
			executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, smallint_col, integer_col, bigint_col, boolean_col) VALUES (?, ?, ?, ?, ?)", tableName), row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestNumericTypes exercises discovery and capture of the numeric types
// DECIMAL, REAL, DOUBLE, and DECFLOAT.
func TestNumericTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        decimal_col DECIMAL(10,2),
        decimal_precise_col DECIMAL(31,8),
        real_col REAL,
        double_col DOUBLE,
        decfloat16_col DECFLOAT(16),
        decfloat34_col DECFLOAT(34)
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		// Use SQL literals for all columns for consistency
		for _, row := range [][]string{
			// Regular values
			{"0", "123.45", "123.45678901", "123.456", "123.456", "123.456", "123.456"},

			// Zero and null
			{"1", "0.0", "0.0", "0.0", "0.0", "0.0", "0.0"},
			{"2", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"},

			// Minimum/maximum values (most negative / most positive finite)
			{"3", "-99999999.99", "-99999999999999999999999.99999999", "-3.4e38", "-1.7e308", "DECFLOAT('-9.9E+384')", "DECFLOAT('-9.9E+6144')"},
			{"4", "99999999.99", "99999999999999999999999.99999999", "3.4e38", "1.7e308", "DECFLOAT('9.9E+384')", "DECFLOAT('9.9E+6144')"},

			// Smallest positive values
			{"5", "0.01", "0.00000001", "1.1754943508222875e-38", "2.2250738585072014e-308", "DECFLOAT('1E-383')", "DECFLOAT('1E-6143')"},

			// Infinity and NaN
			{"6", "0", "0", "0", "0", "DECFLOAT('Infinity')", "DECFLOAT('-Infinity')"},
			{"7", "0", "0", "0", "0", "DECFLOAT('NaN')", "DECFLOAT('sNaN')"},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, decimal_col, decimal_precise_col, real_col, double_col,
                    decfloat16_col, decfloat34_col
                ) VALUES (%s)`, tableName, strings.Join(row, ", ")))
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestStringTypes exercises discovery and capture of the string types
// CHAR, VARCHAR, and CLOB.
func TestStringTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        char_col CHAR(16),
        varchar_col VARCHAR(100),
        clob_col CLOB
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "Hello", "Hello", "Hello"},
			{1, "A", "A", "A"},
			{2, "1234567890123456", "1234567890123456", "1234567890123456"},
			{3, "Hello world", "Hello world", "Hello world with more text"},
			{4, "Tab\tNewline\n", "Tab\tNewline\n", "Tab\tNewline\n"},
			{5, "", "", ""},
			{6, nil, nil, nil},
			{7, "  trim test  ", "  trim test  ", "  trim test  "},
			{8, "Short", "Short string", strings.Repeat("Long text ", 1000)},
			{9, `Special "quotes"`, `Special "quotes"`, `Special "quotes" and \backslashes\`},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, char_col, varchar_col, clob_col
                ) VALUES (?, ?, ?, ?)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestCharForBitDataTypes exercises discovery and capture of CHAR and VARCHAR columns
// both with and without the FOR BIT DATA attribute. Columns with FOR BIT DATA should
// be treated as binary data (base64 encoded) rather than text strings.
func TestCharForBitDataTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        char_col CHAR(16),
        varchar_col VARCHAR(100),
        char_bit_col CHAR(16) FOR BIT DATA,
        varchar_bit_col VARCHAR(100) FOR BIT DATA
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Regular text values - same data in all columns
			{0, "Hello", "Hello", []byte("Hello"), []byte("Hello")},
			{1, "A", "A", []byte("A"), []byte("A")},

			// Binary data that wouldn't be valid text
			{2, "text only", "text only", []byte{0x00, 0xFF, 0x7F, 0x80}, []byte{0x00, 0xFF, 0x7F, 0x80}},
			{3, "more text", "more text", []byte{0xDE, 0xAD, 0xBE, 0xEF}, []byte{0xDE, 0xAD, 0xBE, 0xEF}},

			// Empty and null values
			{4, "", "", []byte{}, []byte{}},
			{5, nil, nil, nil, nil},

			// Long values
			{6, "1234567890123456", "1234567890123456789012345678901234567890",
				[]byte("1234567890123456"), []byte("1234567890123456789012345678901234567890")},

			// Whitespace handling
			{7, "  padded  ", "  padded  ", []byte("  padded  "), []byte("  padded  ")},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, char_col, varchar_col, char_bit_col, varchar_bit_col
                ) VALUES (?, ?, ?, ?, ?)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestDBCSTypes exercises discovery and capture of the double-byte character set types
// GRAPHIC, VARGRAPHIC, and DBCLOB.
func TestDBCSTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        graphic_col GRAPHIC(16),
        vargraphic_col VARGRAPHIC(100),
        dbclob_col DBCLOB
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, "Hello", "Hello", "Hello"},
			{1, "世界", "世界", "世界"},
			{2, "Hello 世界", "Hello 世界", "Hello 世界"},
			{3, "こんにちは", "こんにちは世界", "こんにちは世界"},
			{4, "안녕하세요", "안녕하세요 세계", "안녕하세요 세계"},
			{5, "你好", "你好世界", "你好世界"},
			{6, "🌍🌎🌏", "🌍🌎🌏 Emojis", "🌍🌎🌏 Emojis in DBCLOB"},
			{7, "", "", ""},
			{8, nil, nil, nil},
			{9, "Mixed 混合 text", "Mixed 混合 text", strings.Repeat("Unicode 世界 ", 10)},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, graphic_col, vargraphic_col, dbclob_col
                ) VALUES (?, ?, ?, ?)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestBinaryTypes exercises discovery and capture of the binary types
// BINARY, VARBINARY, and BLOB.
func TestBinaryTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        binary_col BINARY(5),
        varbinary_col VARBINARY(10),
        blob_col BLOB
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			{0, []byte("Hello"), []byte("Hello"), []byte("Hello")},
			{1, []byte{0, 0, 0, 0, 0}, []byte{0, 0, 0}, []byte{0, 0, 0}},
			{2, []byte{255, 255, 255, 255, 255}, []byte{255, 255, 255}, []byte{255, 255, 255}},
			{3, []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}, []byte{0xAA, 0xBB, 0xCC}, []byte{0xAA, 0xBB, 0xCC}},
			{4, []byte{}, []byte{}, []byte{}},
			{5, []byte(nil), []byte(nil), []byte(nil)},
			{6, []byte{0, 1, 2, 3, 4}, []byte{0, 1, 2, 3, 4, 5, 0xFF}, []byte{0, 1, 2, 3, 4, 5, 0xFF}},
			{7, []byte("Hello"), []byte("Hello 世"), []byte("Hello 世界")},
			{8, []byte{0x00, 0xFF, 0x7F, 0x80, 0x01},
				[]byte{0x00, 0xFF, 0x7F, 0x80, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB},
				[]byte{0x00, 0xFF, 0x7F, 0x80, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, binary_col, varbinary_col, blob_col
                ) VALUES (?, ?, ?, ?)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestTemporalTypes exercises discovery and capture of the temporal types
// DATE, TIME, and TIMESTAMP.
func TestTemporalTypes(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        date_col DATE,
        time_col TIME,
        timestamp_col TIMESTAMP
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		// Note: DB2 TIME type does not support fractional seconds - only TIMESTAMP does
		for _, row := range [][]any{
			{0, "2025-02-14", "14:44:29", "2025-02-14 14:44:29.123456"},
			{1, "1969-07-20", "20:17:00", "1969-07-20 20:17:00"},
			{2, "2077-07-20", "15:30:00", "2077-07-20 15:30:00"},
			{3, "0001-01-01", "00:00:00", "0001-01-01 00:00:00"},
			{4, "9999-12-31", "23:59:59", "9999-12-31 23:59:59.999999"},
			{5, "2024-02-14", "15:30:45", "2024-02-14 15:30:45"},
			{6, "2024-02-14", "15:30:45", "2024-02-14 15:30:45.123"},
			{7, "2024-02-14", "15:30:45", "2024-02-14 15:30:45.123456"},
			{8, "1900-01-01", "12:00:00", "1900-01-01 12:00:00"},
			{9, nil, nil, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (
                    id, date_col, time_col, timestamp_col
                ) VALUES (?, ?, ?, ?)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestDatetimeLocation exercises capture of the TIMESTAMP type with different time zones.
func TestDatetimeLocation(t *testing.T) {
	var ctx, control = context.Background(), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))

	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY NOT NULL, timestamp_col TIMESTAMP)`)
	setShutdownAfterQuery(t, true)

	for _, row := range [][]any{
		{0, nil},
		{1, "2025-02-27 14:30:00"},        // Regular date and time
		{2, "0001-01-01 00:00:00"},        // Minimum value. Often reveals interesting offsets for dates very long ago.
		{3, "9999-12-31 23:59:59.999999"}, // Near maximum value
		{4, "2025-02-27 14:30:45.123456"}, // With fractional seconds
		{5, "2000-01-01 00:00:00"},        // Y2K date
		{6, "2025-12-31 23:59:59"},        // End of year
		{7, "2025-03-09 03:30:00"},        // During DST start
		{8, "1883-11-18 12:00:00"},        // Historical date with interesting timezone behavior
	} {
		executeControlQuery(t, control, fmt.Sprintf(`INSERT INTO %s (id, timestamp_col) VALUES (?, ?)`, tableName), row...)
	}

	for _, tc := range []struct{ name, timezone string }{
		{"UTC", "UTC"},
		{"Chicago", "America/Chicago"}, // Exercises DST and also historical "Local Mean Time" for years before 1883.
		{"Tokyo", "Asia/Tokyo"},        // Also has an interesting local time for historical dates.
		{"ExplicitPositiveOffset", "+04:00"},
		{"ExplicitNegativeOffset", "-05:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = testCaptureSpec(t)
			cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
			cs.EndpointSpec.(*Config).Advanced.Timezone = tc.timezone
			cs.Capture(ctx, t, nil)
			cupaloy.SnapshotT(t, cs.Summary())
		})
	}
}

// TestUUIDType exercises discovery and capture of the CHAR(36) type.
func TestUUIDType(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        uuid_col CHAR(36)
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Standard UUID v4 format (random)
			{0, "d36af820-6ac3-4052-ad19-7eba916a020b"},

			// UUID with all characters as same value
			{1, "11111111-1111-1111-1111-111111111111"},
			{2, "00000000-0000-0000-0000-000000000000"},
			{3, "ffffffff-ffff-ffff-ffff-ffffffffffff"},

			// Different versions of UUID
			{4, "a0eebc99-9c0b-11eb-a8b3-0242ac130003"}, // v1 time-based
			{5, "a0eebc99-9c0b-21eb-a8b3-0242ac130003"}, // v2 DCE security
			{6, "a0eebc99-9c0b-31eb-a8b3-0242ac130003"}, // v3 name-based MD5
			{7, "a0eebc99-9c0b-41eb-a8b3-0242ac130003"}, // v4 random
			{8, "a0eebc99-9c0b-51eb-a8b3-0242ac130003"}, // v5 name-based SHA-1
			{9, "018df60a-0040-7000-a000-0242ac130003"}, // v7 time-ordered

			// Null value
			{10, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, uuid_col) VALUES (?, ?)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}

// TestXMLType exercises discovery and capture of the XML type.
func TestXMLType(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(
        id INTEGER PRIMARY KEY NOT NULL,
        xml_col XML
    )`)

	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))
	setResourceCursor(t, cs.Bindings[0], "ID")
	t.Run("Discovery", func(t *testing.T) { cupaloy.SnapshotT(t, summarizeBindings(t, cs.Bindings)) })

	t.Run("Capture", func(t *testing.T) {
		setShutdownAfterQuery(t, true)
		for _, row := range [][]any{
			// Simple XML
			{0, "<root><element>Hello</element></root>"},

			// Complex nested structure
			{1, `<library>
                <book id="1">
                    <title>Sample Book</title>
                    <author>John Doe</author>
                    <published>2024</published>
                    <genres>
                        <genre>Fiction</genre>
                        <genre>Adventure</genre>
                    </genres>
                </book>
            </library>`},

			// XML with attributes
			{2, `<user id="123" active="true">
                <name>Alice</name>
                <email>alice@example.com</email>
            </user>`},

			// XML with special characters
			{3, `<text><![CDATA[Special & <characters> " ' in CDATA]]></text>`},

			// XML with namespaces
			{4, `<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
                <soap:Header><auth>token123</auth></soap:Header>
                <soap:Body><message>Hello World</message></soap:Body>
            </soap:Envelope>`},

			// XML with unicode characters
			{5, `<greeting language="multi">Hello 世界 🌍</greeting>`},

			// Empty elements
			{6, `<empty/>`},

			// Self-closing tags
			{7, `<document><header/><body/><footer/></document>`},

			// Large XML document
			{8, fmt.Sprintf(`<root><content>%s</content></root>`,
				strings.Repeat("This is a long text content. ", 100))},

			// Well-formed XML with mixed content
			{9, `<article>
                <title>Mixed Content</title>
                <para>This is <emphasis>important</emphasis> text with <link href="#ref">links</link>.</para>
            </article>`},

			// Null value
			{10, nil},
		} {
			executeControlQuery(t, control, fmt.Sprintf(`
                INSERT INTO %s (id, xml_col) VALUES (?, ?)`, tableName),
				row...)
		}
		cs.Capture(ctx, t, nil)
		cupaloy.SnapshotT(t, cs.Summary())
	})
}
