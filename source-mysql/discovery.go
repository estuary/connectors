package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/google/uuid"
	"github.com/invopop/jsonschema"
	"github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/charmap"
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
	columns, err := db.getColumns(ctx, db.conn)
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
	primaryKeys, err := getPrimaryKeys(ctx, db.conn)
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
	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn)
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

func (db *mysqlDatabase) translateRecordFields(isBackfill bool, columnTypes map[string]interface{}, f map[string]interface{}) error {
	if columnTypes == nil {
		return fmt.Errorf("unknown column types")
	}
	if f == nil {
		return nil
	}
	for id, val := range f {
		// MariaDB versions 10.4 and up include a synthetic `DB_ROW_HASH_1` column in the
		// binlog row change events for certain tables with unique hash indices (see issue
		// https://github.com/estuary/connectors/issues/1344). In such cases we won't have
		// any type information for the column, but we also don't want it anyway, so as a
		// special case we just delete the 'DB_ROW_HASH_1' property from the document.
		if id == "DB_ROW_HASH_1" && columnTypes[id] == nil {
			delete(f, id)
			continue
		}
		var translated, err = db.translateRecordField(isBackfill, columnTypes[id], val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

const mysqlTimestampLayout = "2006-01-02 15:04:05"

var errDatabaseTimezoneUnknown = errors.New("system variable 'time_zone' or timezone from capture configuration must contain a valid IANA time zone name or +HH:MM offset (go.estuary.dev/80J6rX)")

func (db *mysqlDatabase) translateRecordField(isBackfill bool, columnType interface{}, val interface{}) (interface{}, error) {
	if columnType == nil {
		return nil, fmt.Errorf("unknown column type")
	}
	if columnType, ok := columnType.(*mysqlColumnType); ok {
		return columnType.translateRecordField(isBackfill, val)
	}
	if str, ok := val.(string); ok {
		val = []byte(str)
	}
	switch val := val.(type) {
	case float64:
		switch columnType {
		case "float":
			// Converting floats to strings requires accurate knowledge of the float
			// precision to not be like `123.45600128173828` so a 'float' column must
			// be truncated back down to float32 here. Note that MySQL translates a
			// column type like FLOAT(53) where N>23 into a 'double' column type, so
			// we can trust that the type name here reflects the desired precision.
			return float32(val), nil
		}
	case []byte:
		// Make a solely-owned copy of any byte data. The MySQL client library does
		// some dirty memory-reuse hackery which is only safe so long as byte data
		// returned from a query is fully consumed before `results.Close()` is called.
		//
		// We don't currently guarantee that, so this copy is necessary to avoid any
		// chance of memory corruption.
		//
		// This can be removed after the backfill buffering changes of August 2023
		// are complete, since once that's done results should be fully processed
		// as soon as they're received.
		val = append(make([]byte, 0, len(val)), val...)
		if typeName, ok := columnType.(string); ok {
			switch typeName {
			case "bit":
				var acc uint64
				for _, x := range val {
					acc = (acc << 8) | uint64(x)
				}
				return acc, nil
			case "binary", "varbinary":
				// Unlike BINARY(N) columns, VARBINARY(N) doesn't play around with adding or removing
				// trailing null bytes so we don't have to do anything to correct for that here. The
				// "binary" case here is a fallback for captures whose metadata predates the change
				// to discover binary columns as a complex type with length property.
				if len(val) > truncateColumnThreshold {
					val = val[:truncateColumnThreshold]
				}
				return val, nil
			case "blob", "tinyblob", "mediumblob", "longblob":
				if len(val) > truncateColumnThreshold {
					val = val[:truncateColumnThreshold]
				}
				return val, nil
			case "time":
				// The MySQL client library parsing logic for TIME columns is
				// kind of dumb and inserts either '-' or '\x00' as the first
				// byte of a time value. We want to strip off a leading null
				// if present.
				if len(val) > 0 && val[0] == 0 {
					val = val[1:]
				}
				return string(val), nil
			case "json":
				if len(val) == 0 {
					// The empty string is technically invalid JSON but null should be
					// a reasonable translation.
					return nil, nil
				}
				if len(val) > truncateColumnThreshold {
					val = oversizePlaceholderJSON(val)
				}
				if !json.Valid(val) {
					// If the contents of a JSON column are malformed and non-empty we
					// don't really have any option other than stringifying it. But we
					// can wrap it in an object with an 'invalidJSON' property so that
					// there's at least some hope of identifying such values later on.
					return map[string]any{"invalidJSON": string(val)}, nil
				}
				return json.RawMessage(val), nil
			case "timestamp":
				// Per the MySQL docs:
				//
				//  > Invalid DATE, DATETIME, or TIMESTAMP values are converted to the “zero” value
				//  > of the appropriate type ('0000-00-00' or '0000-00-00 00:00:00')"
				//
				// But month 0 and day 0 don't exist so this can't be parsed and even if
				// it could it wouldn't be a valid RFC3339 timestamp. Since this is the
				// "your data is junk" sentinel value we replace it with a similar one
				// that actually is a valid RFC3339 timestamp.
				//
				// MySQL doesn't allow timestamp values with a zero YYYY-MM-DD to have
				// nonzero fractional seconds, so a simple prefix match can be used.
				if strings.HasPrefix(string(val), "0000-00-00 00:00:00") {
					return "0001-01-01T00:00:00Z", nil
				}
				var inputTimestamp = normalizeMySQLTimestamp(string(val))
				t, err := time.Parse(mysqlTimestampLayout, inputTimestamp)
				if err != nil {
					return nil, fmt.Errorf("error parsing timestamp %q: %w", inputTimestamp, err)
				}
				return t.Format(time.RFC3339Nano), nil
			case "datetime":
				// See note above in the "timestamp" case about replacing this default sentinel
				// value with a valid RFC3339 timestamp sentinel value. The same reasoning applies
				// here for "datetime".
				if strings.HasPrefix(string(val), "0000-00-00 00:00:00") {
					return "0001-01-01T00:00:00Z", nil
				}
				if db.datetimeLocation == nil {
					return nil, fmt.Errorf("unable to translate DATETIME values: %w", errDatabaseTimezoneUnknown)
				}
				var inputTimestamp = normalizeMySQLTimestamp(string(val))
				t, err := time.ParseInLocation(mysqlTimestampLayout, inputTimestamp, db.datetimeLocation)
				if err != nil {
					return nil, fmt.Errorf("error parsing datetime %q: %w", inputTimestamp, err)
				}
				return t.UTC().Format(time.RFC3339Nano), nil
			case "date":
				return normalizeMySQLDate(string(val)), nil
			case "uuid": // The 'UUID' column type is only in MariaDB
				if parsed, err := uuid.Parse(string(val)); err == nil {
					return parsed.String(), nil
				} else if parsed, err := uuid.FromBytes(val); err == nil {
					return parsed.String(), nil
				} else {
					return nil, fmt.Errorf("error parsing UUID: %w", err)
				}
			}
		}
		if len(val) > truncateColumnThreshold {
			val = val[:truncateColumnThreshold]
		}
		return string(val), nil
	}
	return val, nil
}

func oversizePlaceholderJSON(orig []byte) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"flow_truncated":true,"original_size":%d}`, len(orig)))
}

func normalizeMySQLTimestamp(ts string) string {
	// Split timestamp into "YYYY-MM-DD" and "HH:MM:SS ..." portions
	var tsBits = strings.SplitN(ts, " ", 2)
	if len(tsBits) != 2 {
		return ts
	}
	tsBits[0] = normalizeMySQLDate(tsBits[0])    // Normalize the date
	var normalized = tsBits[0] + " " + tsBits[1] // Reassemble date + time
	if normalized != ts {
		logrus.WithFields(logrus.Fields{
			"input":  ts,
			"output": normalized,
		}).Debug("normalized illegal timestamp")
	}
	return normalized
}

func normalizeMySQLDate(str string) string {
	// Split "YYYY-MM-DD" into "YYYY" "MM" and "DD" portions
	var bits = strings.Split(str, "-")
	if len(bits) != 3 {
		return str
	}
	// Replace zero-valued year/month/day with ones instead
	if bits[0] == "0000" {
		bits[0] = "0001"
	}
	if bits[1] == "00" {
		bits[1] = "01"
	}
	if bits[2] == "00" {
		bits[2] = "01"
	}
	return bits[0] + "-" + bits[1] + "-" + bits[2]
}

const queryDiscoverTables = `
  SELECT table_schema, table_name, table_type, engine, table_collation
  FROM information_schema.tables
  WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys');`

func getTables(_ context.Context, conn mysqlClient, selectedSchemas []string) ([]*sqlcapture.DiscoveryInfo, error) {
	var results, err = conn.Execute(queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	defer results.Close()

	var tables []*sqlcapture.DiscoveryInfo
	for _, row := range results.Values {
		var tableSchema = string(row[0].AsString())
		var tableName = string(row[1].AsString())
		var collation = string(row[4].AsString())
		var omitBinding = false
		if len(selectedSchemas) > 0 && !slices.Contains(selectedSchemas, tableSchema) {
			logrus.WithFields(logrus.Fields{
				"schema": tableSchema,
				"table":  tableName,
			}).Debug("table in filtered schema")
			omitBinding = true
		}
		tables = append(tables, &sqlcapture.DiscoveryInfo{
			Schema:      tableSchema,
			Name:        tableName,
			BaseTable:   strings.EqualFold(string(row[2].AsString()), "BASE TABLE"),
			OmitBinding: omitBinding,
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

const queryDiscoverColumns = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable, data_type, column_type, character_set_name, character_maximum_length
  FROM information_schema.columns
  WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
  ORDER BY table_schema, table_name, ordinal_position;`

func (db *mysqlDatabase) getColumns(_ context.Context, conn mysqlClient) ([]sqlcapture.ColumnInfo, error) {
	var results, err = conn.Execute(queryDiscoverColumns)
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
		columns = append(columns, sqlcapture.ColumnInfo{
			TableSchema: tableSchema,
			TableName:   tableName,
			Index:       int(row[2].AsInt64()),
			Name:        columnName,
			IsNullable:  string(row[4].AsString()) != "NO",
			DataType:    db.parseDataType(dataType, fullColumnType, charsetName, maxLength),
		})
	}
	return columns, err
}

func (db *mysqlDatabase) parseDataType(typeName, fullColumnType, charset string, maxLength int) any {
	switch typeName {
	case "enum":
		// Illegal values are represented internally by MySQL as the integer 0. Adding
		// this to the list as the zero-th element allows everything else to flow naturally.
		return &mysqlColumnType{Type: "enum", EnumValues: append([]string{""}, parseEnumValues(fullColumnType)...)}
	case "set":
		return &mysqlColumnType{Type: "set", EnumValues: parseEnumValues(fullColumnType)}
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		if typeName == "tinyint" && fullColumnType == "tinyint(1)" && db.featureFlags["tinyint1_as_bool"] {
			// Booleans in MySQL are annoying because 'BOOLEAN' is just an alias for 'TINYINT(1)'
			// and 'TRUE/FALSE' for 1/0, but since it's 'TINYINT(1)' rather than just 'TINYINT'
			// and nobody uses TINYINT(1) for other purposes we can reliably assume that it means
			// the user wants us to treat it as a boolean anyway, so translate this as "boolean".
			return &mysqlColumnType{Type: "boolean"}
		}
		return &mysqlColumnType{Type: typeName, Unsigned: strings.Contains(fullColumnType, "unsigned")}
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return &mysqlColumnType{Type: typeName, Charset: charset}
	case "binary":
		return &mysqlColumnType{Type: typeName, MaxLength: maxLength}
	}
	return typeName
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

func (t *mysqlColumnType) translateRecordField(isBackfill bool, val interface{}) (interface{}, error) {
	switch t.Type {
	case "enum":
		if index, ok := val.(int64); ok {
			if 0 <= index && int(index) < len(t.EnumValues) {
				return t.EnumValues[index], nil
			}
			return "", fmt.Errorf("enum value out of range: index %d does not match known options %q, backfill the table to reinitialize the inconsistent table metadata", index, t.EnumValues)
		} else if bs, ok := val.([]byte); ok {
			return string(bs), nil
		}
		return val, nil
	case "set":
		if bitfield, ok := val.(int64); ok {
			var acc strings.Builder
			for idx := 0; idx < len(t.EnumValues); idx++ {
				if bitfield&(1<<idx) != 0 {
					if acc.Len() > 0 {
						acc.WriteByte(',')
					}
					acc.WriteString(t.EnumValues[idx])
				}
			}
			return acc.String(), nil
		} else if bs, ok := val.([]byte); ok {
			return string(bs), nil
		}
		return val, nil
	case "boolean":
		if val == nil {
			return nil, nil
		}
		// This is an ugly hack, but it works without requiring a bunch of annoying type
		// assertions to make sure we handle the possible value types coming from backfills
		// and from replication, and it's significantly less likely to break in the future.
		return fmt.Sprintf("%d", val) != "0", nil
	case "tinyint":
		if sval, ok := val.(int8); ok && t.Unsigned {
			return uint8(sval), nil
		}
		return val, nil
	case "smallint":
		if sval, ok := val.(int16); ok && t.Unsigned {
			return uint16(sval), nil
		}
		return val, nil
	case "mediumint":
		if sval, ok := val.(int32); ok && t.Unsigned {
			// A MySQL 'MEDIUMINT' is a 24-bit integer value which is stored into an int32 by the client library,
			// so we convert to a uint32 and mask off any sign-extended upper bits.
			return uint32(sval) & 0x00FFFFFF, nil
		}
		return val, nil
	case "int":
		if sval, ok := val.(int32); ok && t.Unsigned {
			return uint32(sval), nil
		}
		return val, nil
	case "bigint":
		if sval, ok := val.(int64); ok && t.Unsigned {
			return uint64(sval), nil
		}
		return val, nil
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		if str, ok := val.(string); ok {
			return str, nil
		} else if bs, ok := val.([]byte); ok {
			if isBackfill {
				// Backfills always return string results as UTF-8
				return string(bs), nil
			}
			return decodeBytesToString(t.Charset, bs)
		} else if val == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("internal error: text column value must be bytes or nil: got %v", val)
	case "binary":
		if str, ok := val.(string); ok {
			// Binary column values arriving via replication are decoded into a string (which
			// is not valid UTF-8, it's just an immutable sequence of bytes), so we cast that
			// back to []byte so we can handle backfill / replication values consistently.
			val = []byte(str)
		}
		if bs, ok := val.([]byte); ok {
			if len(bs) < t.MaxLength {
				// Binary column values are stored in the binlog with trailing nulls removed,
				// and returned from backfill queries with the trailing nulls. We have to either
				// strip or pad the values to make these match, and since BINARY(N) is a fixed-
				// length column type the obvious most-correct answer is probably to zero-pad
				// the replicated values back up to the column size.
				bs = append(bs, make([]byte, t.MaxLength-len(bs))...)
			}
			if len(bs) > truncateColumnThreshold {
				bs = bs[:truncateColumnThreshold]
			}
			return bs, nil
		}
		return val, nil
	}
	return val, fmt.Errorf("error translating value of complex column type %q", t.Type)
}

func (t *mysqlColumnType) encodeKeyFDB(val any) (tuple.TupleElement, error) {
	switch t.Type {
	case "enum":
		if bs, ok := val.([]byte); ok {
			if idx := slices.Index(t.EnumValues, string(bs)); idx >= 0 {
				return idx, nil
			}
			return val, fmt.Errorf("internal error: failed to translate enum value %q to integer index", string(bs))
		}
		return val, nil
	case "boolean", "tinyint", "smallint", "mediumint", "int", "bigint":
		return val, nil
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		// Backfill text keys are serialized as the raw bytes or string we receive, which is generally
		// fine because we always receive backfill results in UTF-8.
		return val, nil
	case "binary", "varbinary":
		// Binary keys are serialized as the raw bytes, which provides correct
		// lexicographic ordering in FDB tuples
		return val, nil
	}
	return val, fmt.Errorf("internal error: failed to encode column of type %q as backfill key", t.Type)
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
	return string(bs), nil
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

// enumValuesRegexp matches a MySQL-format single-quoted string followed by
// a comma or EOL. It uses non-capturing groups for the alternations on string
// body characters and terminator so that submatch #1 is the full string body.
// The options for string body characters are, in order, two successive quotes,
// anything backslash-escaped, and anything that isn't a single-quote.
var enumValuesRegexp = regexp.MustCompile(`'((?:''|\\.|[^'])*)'(?:,|$)`)

// enumValueReplacements contains the complete list of MySQL string escapes from
// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html#character-escape-sequences
// plus the `'​'` repeated-single-quote mechanism.
var enumValueReplacements = map[string]string{
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

func parseEnumValues(details string) []string {
	// The detailed type description looks something like `enum('foo', 'bar,baz', 'asdf')`
	// so we start by extracting the parenthesized portion.
	if i := strings.Index(details, "("); i >= 0 {
		if j := strings.Index(details, ")"); j > i {
			details = details[i+1 : j]
		}
	}

	// Apply a regex which matches each `'foo',` clause in the enum/set description,
	// and take submatch #1 which is the body of each string.
	var opts []string
	for _, match := range enumValuesRegexp.FindAllStringSubmatch(details, -1) {
		opts = append(opts, unquoteMySQLString(match[1]))
	}
	return opts
}

// unquoteStringMySQL unquotes a MySQL-format single-quoted string (and unescapes
// any backslash escapes) and returns it in unquoted, unescaped form.
func unquoteMySQLString(qstr string) string {
	if strings.HasPrefix(qstr, "'") && strings.HasSuffix(qstr, "'") {
		qstr = strings.TrimPrefix(qstr, "'")
		qstr = strings.TrimSuffix(qstr, "'")
		for old, new := range enumValueReplacements {
			qstr = strings.ReplaceAll(qstr, old, new)
		}
	}
	return qstr
}

const queryDiscoverPrimaryKeys = `
SELECT table_schema, table_name, column_name, seq_in_index
  FROM information_schema.statistics
  WHERE index_name = 'primary'
    AND table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
  ORDER BY table_schema, table_name, seq_in_index;
`

// getPrimaryKeys queries the database to produce a map from table names to
// primary keys. Table names are fully qualified as "<schema>.<name>", and
// primary keys are represented as a list of column names, in the order that
// they form the table's primary key.
func getPrimaryKeys(_ context.Context, conn mysqlClient) (map[sqlcapture.StreamID][]string, error) {
	var results, err = conn.Execute(queryDiscoverPrimaryKeys)
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

const queryDiscoverSecondaryIndices = `
SELECT stat.table_schema, stat.table_name, stat.index_name, stat.column_name, stat.seq_in_index
  FROM information_schema.statistics stat
  WHERE stat.non_unique = 0
    AND stat.index_name != 'PRIMARY'
    AND stat.table_schema NOT IN ('information_schema', 'sys', 'performance_schema', 'mysql')
  ORDER BY stat.table_schema, stat.table_name, stat.index_name, stat.seq_in_index
`

func getSecondaryIndexes(_ context.Context, conn mysqlClient) (map[sqlcapture.StreamID]map[string][]string, error) {
	var results, err = conn.Execute(queryDiscoverSecondaryIndices)
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
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format:      s.format,
		Description: s.description,
		Extras:      make(map[string]interface{}),
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
