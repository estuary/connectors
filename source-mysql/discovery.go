package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"strings"
	"unsafe"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/charmap"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	truncateColumnThreshold = 8 * 1024 * 1024 // Arbitrarily selected value
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *mysqlDatabase) DiscoverTables(ctx context.Context) (map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, error) {
	var tableMap = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo)
	var tables, err = getTables(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)

		// Depending on feature flag settings, we may normalize multiple table names
		// to the same StreamID. This is a problem and other parts of discovery won't
		// be able to handle it gracefully, so it's a fatal error.
		if other, ok := tableMap[streamID]; ok {
			return nil, fmt.Errorf("table name collision between %q and %q",
				fmt.Sprintf("%s.%s", table.Schema, table.Name),
				fmt.Sprintf("%s.%s", other.Schema, other.Name),
			)
		}

		// The connector used to require a watermarks table as part of its operation, and so we
		// automatically excluded it from the discovered bindings as an implementation detail. Now
		// the connector no longer uses watermarks, but some number of users will still have the
		// table lingering around and we don't want to suddenly start capturing it, so for now
		// we're keeping this logic to filter it out of discovery.
		//
		// We filter out both the name from the configuration and the hard-coded default name, so
		// that even if configuration updates cause the deprecated property to be lost we'll still
		// keep filtering out the table in the common cases.
		if streamID.String() == db.config.Advanced.WatermarksTable || streamID.String() == "flow.watermarks" {
			table.OmitBinding = true
		}
		table.UseSchemaInference = db.featureFlags["use_schema_inference"]
		table.EmitSourcedSchemas = db.featureFlags["emit_sourced_schemas"]
		tableMap[streamID] = table
	}

	// Enumerate every column of every table, and then aggregate into a
	// map from StreamID to TableInfo structs.
	columns, err := db.getColumns(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error discovering columns: %w", err)
	}
	for _, column := range columns {
		// Create or look up the appropriate TableInfo struct for a given schema+name
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			continue // Ignore information about excluded tables
		}

		// Finally we can add to the column info map and column-name-ordering list
		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column
		info.ColumnNames = append(info.ColumnNames, column.Name)
		tableMap[streamID] = info
	}

	// Enumerate the primary key of every table and add that information
	// into the table map.
	primaryKeys, err := getPrimaryKeys(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}
	for id, key := range primaryKeys {
		var info, ok = tableMap[id]
		if !ok {
			continue // Ignore information about excluded tables
		}
		info.PrimaryKey = key
		tableMap[id] = info
	}

	// Enumerate secondary indexes and use those as a fallback where present
	// for tables whose primary key is unset.
	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("unable to list database secondary indexes: %w", err)
	}
	for streamID, indexColumns := range secondaryIndexes {
		var info, ok = tableMap[streamID]
		if !ok || info.PrimaryKey != nil {
			continue
		}

		// Make a list of all usable indexes.
		logrus.WithFields(logrus.Fields{
			"table":   streamID,
			"indices": len(indexColumns),
		}).Debug("checking for suitable secondary indexes")
		var suitableIndexes []string
		for indexName, columns := range indexColumns {
			if columnsNonNullable(info.Columns, columns) {
				logrus.WithFields(logrus.Fields{
					"table":   streamID,
					"index":   indexName,
					"columns": columns,
				}).Debug("secondary index could be used as primary key")
				suitableIndexes = append(suitableIndexes, indexName)
			}
		}

		// Sort the list by index name and pick the first one, if there are multiple.
		// This helps ensure stable selection, although it could still change due to
		// the creation of a new secondary index.
		sort.Strings(suitableIndexes)
		if len(suitableIndexes) > 0 {
			var selectedIndex = suitableIndexes[0]
			logrus.WithFields(logrus.Fields{
				"table": streamID,
				"index": selectedIndex,
			}).Debug("selected secondary index as table key")
			info.PrimaryKey = indexColumns[selectedIndex]
			info.FallbackKey = true
		} else {
			logrus.WithField("table", streamID).Debug("no secondary index is suitable")
		}
	}

	// Determine whether the database sorts the keys of a each table in a
	// predictable order or not. The term "predictable" here specifically
	// means "able to be reproduced using bytewise lexicographic ordering of
	// the serialized row keys generated by this connector".
	for _, info := range tableMap {
		for _, colName := range info.PrimaryKey {
			if !predictableColumnOrder(info.Columns[colName].DataType) {
				info.UnpredictableKeyOrdering = true
			}
		}
	}

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		for id, info := range tableMap {
			logrus.WithFields(logrus.Fields{
				"stream":     id,
				"keyColumns": info.PrimaryKey,
			}).Debug("discovered table")
		}
	}

	return tableMap, nil
}

// Returns true if the bytewise lexicographic ordering of serialized row keys for
// this column type matches the database backfill ordering.
func predictableColumnOrder(colType any) bool {
	// Currently all textual primary key columns are considered to be 'unpredictable' so that backfills
	// will default to using the 'imprecise' ordering semantics which avoids full-table sorts. Refer to
	// https://github.com/estuary/connectors/issues/1343 for more details.
	if t, ok := colType.(*mysqlColumnType); ok {
		return !slices.Contains([]string{"char", "varchar", "text", "tinytext", "mediumtext", "longtext"}, t.Type)
	}
	// Consider the MariaDB UUID column type as unpredictable since they're not ordered in
	// the obvious way which would allow lexicographic comparison of the string representation
	// to work and this is simpler than implementing the necessary FDB tuple encoding logic.
	if colType == "uuid" {
		return false
	}
	return true
}

func columnsNonNullable(columnsInfo map[string]sqlcapture.ColumnInfo, columnNames []string) bool {
	for _, columnName := range columnNames {
		if columnsInfo[columnName].IsNullable {
			return false
		}
	}
	return true
}

func (db *mysqlDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo, isPrimaryKey bool) (*jsonschema.Schema, error) {
	if !db.featureFlags["date_schema_format"] {
		// NOTE(2025-02-06): As part of github.com/estuary/connectors/issues/1994 we fixed
		// a long-standing complaint, that MySQL discovered date columns as a bare `type: string`
		// rather than `type: string, format: date`.
		//
		// A long time ago MySQL date values were captured as the raw strings coming from MySQL.
		// Unfortunately, MySQL supports date values which illegal under the `format: date` rules,
		// for example '0000-00-00'. This value may exist in an unknown number of collections and
		// materialization targets, so we can't unconditionally add `format: date` to our discovery
		// for all captures.
		//
		// This feature flag maintains backwards-compatibility for preexisting captures while allowing
		// us to change the default for new captures going forward.
		mysqlTypeToJSON["date"] = columnSchema{jsonType: "string"}
	}

	var schema columnSchema
	if typeName, ok := column.DataType.(string); ok {
		schema, ok = mysqlTypeToJSON[typeName]
		if !ok {
			return nil, fmt.Errorf("unhandled MySQL type %q", typeName)
		}
		schema.nullable = column.IsNullable
	} else if columnType, ok := column.DataType.(*mysqlColumnType); ok {
		schema, ok = mysqlTypeToJSON[columnType.Type]
		if !ok {
			return nil, fmt.Errorf("unhandled MySQL type %q", typeName)
		}
		schema.nullable = column.IsNullable
		schema.extras = make(map[string]interface{})
		switch columnType.Type {
		case "enum":
			var options []interface{}
			for _, val := range columnType.EnumValues {
				options = append(options, val)
			}
			if column.IsNullable {
				options = append(options, nil)
			}
			schema.extras["enum"] = options
		case "tinyint", "smallint", "mediumint", "int", "bigint":
			if columnType.Unsigned {
				// FIXME(wgd)(2024-05-08): We should be specifying `minimum: 0` for unsigned integer
				// columns but currently there are collections and captures in production
				// which this breaks. Re-enable this after we've fixed those.
				// schema.extras["minimum"] = 0
			}
		case "char":
			// CHAR(n) - fixed-length, space-padded
			if columnType.MaxLength > 0 {
				var length = uint64(columnType.MaxLength)
				schema.minLength = &length
				schema.maxLength = &length
			}
		case "varchar":
			// VARCHAR(n) - variable-length with limit
			if columnType.MaxLength > 0 {
				var length = uint64(columnType.MaxLength)
				schema.maxLength = &length
			}
		case "binary":
			// BINARY(n) - fixed-length binary, base64 encoded
			// Binary data is base64 encoded: every 3 bytes becomes 4 characters
			if columnType.MaxLength > 0 {
				var base64Length = uint64((columnType.MaxLength + 2) / 3 * 4)
				schema.minLength = &base64Length
				schema.maxLength = &base64Length
			}
		case "varbinary":
			// VARBINARY(n) - variable-length binary, base64 encoded
			// Binary data is base64 encoded: every 3 bytes becomes 4 characters
			if columnType.MaxLength > 0 {
				var base64Length = uint64((columnType.MaxLength + 2) / 3 * 4)
				schema.maxLength = &base64Length
			}
		}
		// TODO(wgd): Is there a good way to describe possible SET values
		// as a JSON schema? Currently discovery just says 'string'.
	} else {
		return nil, fmt.Errorf("unhandled MySQL type %#v", column.DataType)
	}

	// Pass-through the column description.
	if column.Description != nil {
		schema.description = *column.Description
	}
	return schema.toType(), nil
}

func getTables(_ context.Context, conn mysqlClient, selectedSchemas []string) ([]*sqlcapture.DiscoveryInfo, error) {
	var query = new(strings.Builder)
	var args []any
	fmt.Fprintf(query, `SELECT table_schema, table_name, table_type, engine, table_collation`)
	fmt.Fprintf(query, `  FROM information_schema.tables`)
	if len(selectedSchemas) > 0 {
		fmt.Fprintf(query, `  WHERE table_schema IN (%s)`, strings.Join(slices.Repeat([]string{"?"}, len(selectedSchemas)), ", "))
		for _, schema := range selectedSchemas {
			args = append(args, schema)
		}
	} else {
		fmt.Fprintf(query, `  WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')`)
	}
	fmt.Fprintf(query, ";")

	var results, err = conn.Execute(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	defer results.Close()

	var tables []*sqlcapture.DiscoveryInfo
	for _, row := range results.Values {
		var tableSchema = string(row[0].AsString())
		var tableName = string(row[1].AsString())
		var collation = string(row[4].AsString())
		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:    tableSchema,
			Name:      tableName,
			BaseTable: strings.EqualFold(string(row[2].AsString()), "BASE TABLE"),
			ExtraDetails: &mysqlTableDiscoveryDetails{
				StorageEngine:  string(row[3].AsString()),
				DefaultCharset: charsetFromCollation(collation),
			},
		})
	}
	return tables, nil
}

func charsetFromCollation(name string) string {
	// TODO(wgd): The only way we can end up with an empty collation name here is if the
	// TABLE_COLLATION column of INFORMATION_SCHEMA.TABLES is empty. For now we can just
	// assume it's UTF-8 compatible, but for perfect correctness we need to keep track of
	// the server's default collation setting and use that here.
	if name == "" {
		logrus.Debug("assuming UTF-8 for unspecified collation(s)")
		return mysqlDefaultCharset
	}

	// According to https://dev.mysql.com/doc/refman/8.4/en/information-schema-tables-table.html:
	//
	//     The output does not explicitly list the table default character set, but the collation
	//     name begins with the character set name.
	//
	// We rely on this assumption to identify known charsets based on the decoders table here.
	for charset := range mysqlStringDecoders {
		if strings.HasPrefix(name, charset+"_") {
			return charset
		}
	}
	logrus.WithField("collation", name).Error("unknown charset for collation, assuming UTF-8")
	return mysqlDefaultCharset
}

type mysqlTableDiscoveryDetails struct {
	StorageEngine  string
	DefaultCharset string
}

func (db *mysqlDatabase) getColumns(_ context.Context, conn mysqlClient, selectedSchemas []string) ([]sqlcapture.ColumnInfo, error) {
	var query = new(strings.Builder)
	var args []any
	fmt.Fprintf(query, `SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type, column_type, character_set_name, character_maximum_length`)
	fmt.Fprintf(query, `  FROM information_schema.columns`)
	if len(selectedSchemas) > 0 {
		fmt.Fprintf(query, `  WHERE table_schema IN (%s)`, strings.Join(slices.Repeat([]string{"?"}, len(selectedSchemas)), ", "))
		for _, schema := range selectedSchemas {
			args = append(args, schema)
		}
	} else {
		fmt.Fprintf(query, `  WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')`)
	}
	fmt.Fprintf(query, `  ORDER BY table_schema, table_name, ordinal_position;`)

	var results, err = conn.Execute(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer results.Close()

	var columns []sqlcapture.ColumnInfo
	for _, row := range results.Values {
		var tableSchema, tableName = string(row[0].AsString()), string(row[1].AsString())
		var columnName = string(row[3].AsString())
		var dataType, fullColumnType = string(row[5].AsString()), string(row[6].AsString())
		var charsetName = string(row[7].AsString())
		var maxLength = int(row[8].AsInt64())
		logrus.WithFields(logrus.Fields{
			"schema":     tableSchema,
			"table":      tableName,
			"column":     columnName,
			"dataType":   dataType,
			"columnType": fullColumnType,
			"charset":    charsetName,
			"maxLength":  maxLength,
		}).Debug("discovered column")
		var parsedDataType, err = db.parseDataType(dataType, fullColumnType, charsetName, maxLength)
		if err != nil {
			return nil, fmt.Errorf("error parsing column %q.%q.%q type: %w", tableSchema, tableName, columnName, err)
		}
		columns = append(columns, sqlcapture.ColumnInfo{
			TableSchema: tableSchema,
			TableName:   tableName,
			Index:       int(row[2].AsInt64()),
			Name:        columnName,
			IsNullable:  string(row[4].AsString()) != "NO",
			DataType:    parsedDataType,
		})
	}
	return columns, err
}

func (db *mysqlDatabase) parseDataType(typeName, fullColumnType, charset string, maxLength int) (any, error) {
	switch typeName {
	case "enum":
		// Illegal values are represented internally by MySQL as the integer 0. Adding
		// this to the list as the zero-th element allows everything else to flow naturally.
		var enumValues, err = parseEnumValues(fullColumnType)
		if err != nil {
			return nil, fmt.Errorf("error parsing enum %q values: %w", fullColumnType, err)
		}
		return &mysqlColumnType{Type: "enum", EnumValues: append([]string{""}, enumValues...)}, nil
	case "set":
		var enumValues, err = parseEnumValues(fullColumnType)
		if err != nil {
			return nil, fmt.Errorf("error parsing enum %q values: %w", fullColumnType, err)
		}
		return &mysqlColumnType{Type: "set", EnumValues: enumValues}, nil
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		if typeName == "tinyint" && fullColumnType == "tinyint(1)" && db.featureFlags["tinyint1_as_bool"] {
			// Booleans in MySQL are annoying because 'BOOLEAN' is just an alias for 'TINYINT(1)'
			// and 'TRUE/FALSE' for 1/0, but since it's 'TINYINT(1)' rather than just 'TINYINT'
			// and nobody uses TINYINT(1) for other purposes we can reliably assume that it means
			// the user wants us to treat it as a boolean anyway, so translate this as "boolean".
			return &mysqlColumnType{Type: "boolean"}, nil
		}
		return &mysqlColumnType{Type: typeName, Unsigned: strings.Contains(fullColumnType, "unsigned")}, nil
	case "tinytext", "text", "mediumtext", "longtext":
		return &mysqlColumnType{Type: typeName, Charset: charset}, nil
	case "char", "varchar":
		return &mysqlColumnType{Type: typeName, Charset: charset, MaxLength: maxLength}, nil
	case "binary", "varbinary":
		return &mysqlColumnType{Type: typeName, MaxLength: maxLength}, nil
	}
	return typeName, nil
}

type mysqlColumnType struct {
	Type       string   `json:"type" mapstructure:"type"`                   // The basic name of the column type.
	EnumValues []string `json:"enum,omitempty" mapstructure:"enum"`         // The list of values which an enum (or set) column can contain.
	Unsigned   bool     `json:"unsigned,omitempty" mapstructure:"unsigned"` // True IFF an integer type is unsigned
	Charset    string   `json:"charset,omitempty" mapstructure:"charset"`   // The character set of a text column.
	MaxLength  int      `json:"maxlen,omitempty" mapstructure:"maxlen"`     // The maximum length of a binary column
}

func (t *mysqlColumnType) String() string {
	if t.Unsigned {
		return t.Type + " unsigned"
	}
	if t.Charset != "" && t.Charset != mysqlDefaultCharset {
		return t.Type + " with charset " + t.Charset
	}
	if t.Type == "binary" {
		return fmt.Sprintf("%s(%d)", t.Type, t.MaxLength)
	}
	return t.Type
}

// The default character set in modern MySQL / MariaDB releases.
const mysqlDefaultCharset = "utf8mb4"

func decodeBytesToString(charset string, bs []byte) (string, error) {
	if charset == "" {
		// Assume an unknown charset is UTF-8 so it can be omitted from serialized metadata.
		charset = mysqlDefaultCharset
	}

	var decodeFn, ok = mysqlStringDecoders[charset]
	if !ok {
		// If the charset of a column is unknown, we assume it's UTF-8. This means that,
		// hopefully, no captures will suddenly begin failing when the text decoding fix
		// goes to production. Since before this logic existed we assumed all text would
		// be UTF-8 compatible this doesn't hurt any previously-working captures.
		//
		// Instead of erroring out, the connector will log an error which we can go check
		// for after this reaches production, which will tell us if there are any charsets
		// we still need to add. But this function is called for every text column of every
		// replicated change event so it would be much too spammy if we logged it every time,
		// so we also register the unknown charset as UTF-8 so this only triggers once per
		// unknown charset (per task restart).
		logrus.WithField("charset", charset).Error("unknown charset, assuming UTF-8/ASCII compatible")
		mysqlStringDecoders[charset] = decodeUTF8
		decodeFn = decodeUTF8
	}
	var str, err = decodeFn(bs)
	if err != nil {
		return "", fmt.Errorf("internal error: failed to decode bytes to charset %q: %w", charset, err)
	}

	// If the string is short enough then we're done, otherwise we need to apply
	// a Unicode-aware truncation to the string contents.
	if len(str) <= truncateColumnThreshold {
		return str, nil
	}
	var buf = new(strings.Builder)
	for _, r := range str {
		buf.WriteRune(r)
		if buf.Len() > truncateColumnThreshold {
			break
		}
	}
	return buf.String(), nil
}

var mysqlStringDecoders = map[string]func([]byte) (string, error){
	"utf8":    decodeUTF8, // MariaDB alias for utf8mb3 or utf8mb4 depending on config. We don't care, it's all UTF-8 text to us.
	"ascii":   decodeUTF8, // Guaranteed only ASCII characters (8-bit clean), meaning we can still treat it as UTF-8.
	"utf8mb3": decodeUTF8,
	"utf8mb4": decodeUTF8,
	"latin1":  decodeLatin1,
	"ucs2":    decodeUCS2,
}

func decodeUTF8(bs []byte) (string, error) {
	// We know that in our connector, string decoders are only ever applied to have
	// their result immediately serialized into a JSON output document, so it's safe
	// to directly cast []byte -> string here for efficiency.
	return *(*string)(unsafe.Pointer(&bs)), nil
}

func decodeLatin1(bs []byte) (string, error) {
	var decoder = charmap.ISO8859_1.NewDecoder()
	var decodedBytes, err = decoder.Bytes(bs)
	if err != nil {
		return "", nil
	}
	return string(decodedBytes), nil
}

func decodeUCS2(bs []byte) (string, error) {
	if len(bs)%2 == 1 {
		return "", fmt.Errorf("string length must be a multiple of two: got %d bytes", len(bs))
	}
	var runes = make([]rune, 0, len(bs)/2)
	for i := 0; i < len(bs); i += 2 {
		// Per https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-ucs2.html
		// MySQL's UCS-2 charset only supports BMP characters, so we ignore here
		// the possibility of surrogate pairs.
		//
		// If this proves incorrect, we could instead use package 'unicode/utf16'
		// to decode []uint16 -> string
		runes = append(runes, rune(binary.BigEndian.Uint16(bs[i:])))
	}
	return string(runes), nil
}

func parseEnumValues(details string) ([]string, error) {
	// Construct a simple dummy query containing the full enum column type.
	var query = fmt.Sprintf(`CREATE TABLE dummy (x %s)`, details)

	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, fmt.Errorf("error constructing SQL parser: %w", err)
	}
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// We're making several unchecked assumptions here, but since we know the exact
	// structure of the input query we can safely assume these things.
	var enumValues = stmt.(*sqlparser.CreateTable).TableSpec.Columns[0].Type.EnumValues
	return unquoteEnumValues(enumValues), nil
}

// unquoteEnumValues applies MySQL single-quote-unescaping to a list of single-quoted
// escaped string values such as the EnumValues list returned by the Vitess SQL Parser
// package when parsing a DDL query involving enum/set values.
//
// The single-quote wrapping and escaping of these strings is clearly deliberate, as
// under the hood the package actually tokenizes the strings to raw values and then
// explicitly calls `encodeSQLString()` to re-wrap them when building the AST. The
// actual reason for doing this is unknown however, and it makes very little sense.
//
// So whatever, here's a helper function to undo that escaping and get back down to
// the raw strings again.
func unquoteEnumValues(values []string) []string {
	var unquoted []string
	for _, qval := range values {
		unquoted = append(unquoted, unquoteMySQLString(qval))
	}
	return unquoted
}

// unquoteStringMySQL unquotes a MySQL-format single-quoted string (and unescapes
// any backslash escapes) and returns it in unquoted, unescaped form.
func unquoteMySQLString(qstr string) string {
	if strings.HasPrefix(qstr, "'") && strings.HasSuffix(qstr, "'") {
		qstr = strings.TrimPrefix(qstr, "'")
		qstr = strings.TrimSuffix(qstr, "'")
		for old, new := range mysqlStringEscapeReplacements {
			qstr = strings.ReplaceAll(qstr, old, new)
		}
	}
	return qstr
}

// mysqlStringEscapeReplacements contains the complete list of MySQL string escapes from
// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
// plus the `'​'` repeated-single-quote mechanism.
var mysqlStringEscapeReplacements = map[string]string{
	`''`: "'",
	`\0`: "\x00",
	`\'`: "'",
	`\"`: `"`,
	`\b`: "\b",
	`\n`: "\n",
	`\r`: "\r",
	`\t`: "\t",
	`\Z`: "\x1A",
	`\\`: "\\",
	`\%`: "%",
	`\_`: "_",
}

// getPrimaryKeys queries the database to produce a map from table names to
// primary keys. Table names are fully qualified as "<schema>.<name>", and
// primary keys are represented as a list of column names, in the order that
// they form the table's primary key.
func getPrimaryKeys(_ context.Context, conn mysqlClient, selectedSchemas []string) (map[sqlcapture.StreamID][]string, error) {
	var query = new(strings.Builder)
	var args []any
	fmt.Fprintf(query, `SELECT table_schema, table_name, column_name, seq_in_index`)
	fmt.Fprintf(query, `  FROM information_schema.statistics`)
	fmt.Fprintf(query, `  WHERE index_name = 'primary'`)
	if len(selectedSchemas) > 0 {
		fmt.Fprintf(query, `    AND table_schema IN (%s)`, strings.Join(slices.Repeat([]string{"?"}, len(selectedSchemas)), ", "))
		for _, schema := range selectedSchemas {
			args = append(args, schema)
		}
	} else {
		fmt.Fprintf(query, `    AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')`)
	}
	fmt.Fprintf(query, `  ORDER BY table_schema, table_name, seq_in_index;`)

	var results, err = conn.Execute(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying primary keys: %w", err)
	}
	defer results.Close()

	var keys = make(map[sqlcapture.StreamID][]string)
	for _, row := range results.Values {
		var streamID = sqlcapture.JoinStreamID(string(row[0].AsString()), string(row[1].AsString()))
		var columnName, index = string(row[2].AsString()), int(row[3].AsInt64())
		logrus.WithFields(logrus.Fields{
			"stream": streamID,
			"column": columnName,
			"index":  index,
		}).Trace("discovered primary-key column")
		keys[streamID] = append(keys[streamID], columnName)
		if index != len(keys[streamID]) {
			return nil, fmt.Errorf("primary key column %q (of table %q) appears out of order", columnName, streamID)
		}
	}
	return keys, nil
}

func getSecondaryIndexes(_ context.Context, conn mysqlClient, selectedSchemas []string) (map[sqlcapture.StreamID]map[string][]string, error) {
	var query = new(strings.Builder)
	var args []any
	fmt.Fprintf(query, `SELECT stat.table_schema, stat.table_name, stat.index_name, stat.column_name, stat.seq_in_index`)
	fmt.Fprintf(query, `  FROM information_schema.statistics stat`)
	fmt.Fprintf(query, `  WHERE stat.non_unique = 0`)
	fmt.Fprintf(query, `    AND stat.index_name != 'PRIMARY'`)
	if len(selectedSchemas) > 0 {
		fmt.Fprintf(query, `    AND stat.table_schema IN (%s)`, strings.Join(slices.Repeat([]string{"?"}, len(selectedSchemas)), ", "))
		for _, schema := range selectedSchemas {
			args = append(args, schema)
		}
	} else {
		fmt.Fprintf(query, `    AND stat.table_schema NOT IN ('information_schema', 'sys', 'performance_schema', 'mysql')`)
	}
	fmt.Fprintf(query, `  ORDER BY stat.table_schema, stat.table_name, stat.index_name, stat.seq_in_index`)

	var results, err = conn.Execute(query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying secondary indexes: %w", err)
	}
	defer results.Close()

	// Run the 'list secondary indexes' query and aggregate results into
	// a `map[StreamID]map[IndexName][]ColumnName`
	var streamIndexColumns = make(map[sqlcapture.StreamID]map[string][]string)
	for _, row := range results.Values {
		var streamID = sqlcapture.JoinStreamID(string(row[0].AsString()), string(row[1].AsString()))
		var indexName, columnName = string(row[2].AsString()), string(row[3].AsString())
		var keySequence = int(row[4].AsInt64())
		var indexColumns = streamIndexColumns[streamID]
		if indexColumns == nil {
			indexColumns = make(map[string][]string)
			streamIndexColumns[streamID] = indexColumns
		}
		indexColumns[indexName] = append(indexColumns[indexName], columnName)
		if len(indexColumns[indexName]) != keySequence {
			return nil, fmt.Errorf("internal error: secondary index key ordering failure: index %q on stream %q: column %q appears out of order", indexName, streamID, columnName)
		}
	}
	return streamIndexColumns, err
}

type columnSchema struct {
	contentEncoding string
	description     string
	format          string
	nullable        bool
	extras          map[string]interface{}
	jsonType        string
	minLength       *uint64
	maxLength       *uint64
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format:      s.format,
		Description: s.description,
		Extras:      make(map[string]interface{}),
		MinLength:   s.minLength,
		MaxLength:   s.maxLength,
	}
	for k, v := range s.extras {
		out.Extras[k] = v
	}

	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding // New in 2019-09.
	}

	if s.jsonType == "" {
		// No type constraint.
	} else if s.nullable {
		out.Extras["type"] = []string{s.jsonType, "null"} // Use variadic form.
	} else {
		out.Type = s.jsonType
	}
	return out
}

var mysqlTypeToJSON = map[string]columnSchema{
	"tinyint":   {jsonType: "integer"},
	"smallint":  {jsonType: "integer"},
	"mediumint": {jsonType: "integer"},
	"int":       {jsonType: "integer"},
	"bigint":    {jsonType: "integer"},
	"bit":       {jsonType: "integer"},

	// BOOLEAN is a pseudo-type that doesn't entirely exist in MySQL except as an alias for TINYINT(1)
	"boolean": {jsonType: "boolean"},

	"float":   {jsonType: "number"},
	"double":  {jsonType: "number"},
	"decimal": {jsonType: "string", format: "number"},

	"char":    {jsonType: "string"},
	"varchar": {jsonType: "string"},

	"tinytext":   {jsonType: "string"},
	"text":       {jsonType: "string"},
	"mediumtext": {jsonType: "string"},
	"longtext":   {jsonType: "string"},

	"binary":     {jsonType: "string", contentEncoding: "base64"},
	"varbinary":  {jsonType: "string", contentEncoding: "base64"},
	"tinyblob":   {jsonType: "string", contentEncoding: "base64"},
	"blob":       {jsonType: "string", contentEncoding: "base64"},
	"mediumblob": {jsonType: "string", contentEncoding: "base64"},
	"longblob":   {jsonType: "string", contentEncoding: "base64"},

	"enum": {jsonType: "string"},
	"set":  {jsonType: "string"},

	"date": {jsonType: "string", format: "date"},

	"datetime":  {jsonType: "string", format: "date-time"},
	"timestamp": {jsonType: "string", format: "date-time"},
	"time":      {jsonType: "string"},
	"year":      {jsonType: "integer"},

	"json": {},

	"uuid": {jsonType: "string", format: "uuid"}, // Only present in MariaDB
}
