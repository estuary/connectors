package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

const (
	infinityTimestamp         = "9999-12-31T23:59:59Z"
	negativeInfinityTimestamp = "0000-01-01T00:00:00Z"
	rfc3339TimeFormat         = "15:04:05.999999999Z07:00"
	truncateColumnThreshold   = 8 * 1024 * 1024 // Arbitrarily selected value
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *postgresDatabase) DiscoverTables(ctx context.Context) (map[string]*sqlcapture.DiscoveryInfo, error) {
	// Get lists of all tables, columns and primary keys in the database
	var tables, err = getTables(ctx, db.conn, db.config.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("unable to list database tables: %w", err)
	}
	columns, err := getColumns(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database columns: %w", err)
	}
	primaryKeys, err := getPrimaryKeys(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}
	secondaryIndexes, err := getSecondaryIndexes(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database secondary indexes: %w", err)
	}

	// Column descriptions just add a bit of user-friendliness. They're so unimportant
	// that failure to list them shouldn't even be a fatal error.
	columnDescriptions, err := getColumnDescriptions(ctx, db.conn)
	if err != nil {
		logrus.WithField("err", err).Warn("error fetching column descriptions")
	}

	// Aggregate column and primary key information into DiscoveryInfo structs
	// using a map from fully-qualified "<schema>.<name>" table names to
	// the corresponding DiscoveryInfo.
	var tableMap = make(map[string]*sqlcapture.DiscoveryInfo)
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Name)
		tableMap[streamID] = table
	}
	for _, column := range columns {
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			continue
		}

		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column
		info.ColumnNames = append(info.ColumnNames, column.Name)
		tableMap[streamID] = info
	}
	for _, desc := range columnDescriptions {
		var streamID = sqlcapture.JoinStreamID(desc.TableSchema, desc.TableName)
		if info, ok := tableMap[streamID]; ok {
			if column, ok := info.Columns[desc.ColumnName]; ok {
				logrus.WithFields(logrus.Fields{
					"table":  streamID,
					"column": desc.ColumnName,
					"desc":   desc.Description,
				}).Trace("got column description")
				var description = desc.Description
				column.Description = &description
				info.Columns[desc.ColumnName] = column
			}
			tableMap[streamID] = info
		}
	}

	for streamID, key := range primaryKeys {
		// The `getColumns()` query implements the "exclude system schemas" logic,
		// so here we ignore primary key information for tables we don't care about.
		var info, ok = tableMap[streamID]
		if !ok {
			continue
		}
		logrus.WithFields(logrus.Fields{
			"table": streamID,
			"key":   key,
		}).Trace("queried primary key")
		info.PrimaryKey = key
		tableMap[streamID] = info
	}

	// For tables which have no primary key but have a valid secondary index,
	// fill in that secondary index as the 'primary key' for our purposes.
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
	if colType == "varchar" || colType == "bpchar" || colType == "text" {
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

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *postgresDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo) (*jsonschema.Schema, error) {
	var arrayColumn = false
	var colSchema columnSchema

	if columnType, ok := column.DataType.(string); ok {
		// If the column type looks like `_foo` then it's an array of elements of type `foo`.
		if strings.HasPrefix(columnType, "_") {
			columnType = strings.TrimPrefix(columnType, "_")
			arrayColumn = true
		}

		// Translate the basic value/element type into a JSON Schema type
		colSchema, ok = postgresTypeToJSON[columnType]
		if !ok {
			return nil, fmt.Errorf("unable to translate PostgreSQL type %q into JSON schema", columnType)
		}
	} else if colSchema, ok = column.DataType.(columnSchema); ok {
		// Nothing else to do since the columnSchema was already determined.
	} else {
		return nil, fmt.Errorf("unable to translate PostgreSQL type %q into JSON schema", column.DataType)
	}

	var jsonType *jsonschema.Schema
	// If the column is an array, wrap the element type in a multidimensional array structure.
	if arrayColumn {
		// Nullability applies to the array itself, not the items. Items are always allowed to be
		// null unless additional checks are imposed on the column which we currently can't look
		// for.
		colSchema.nullable = true

		jsonType = &jsonschema.Schema{
			Type: "object",
			Extras: map[string]interface{}{
				"properties": map[string]*jsonschema.Schema{
					"dimensions": {
						Type:  "array",
						Items: &jsonschema.Schema{Type: "integer"},
					},
					"elements": {
						Type:  "array",
						Items: colSchema.toType(),
					},
				},
			},
			Required: []string{"dimensions", "elements"},
		}

		// The column value itself may be null if the column is nullable.
		if column.IsNullable {
			jsonType.Extras["type"] = []string{"object", "null"}
		}
	} else {
		colSchema.nullable = column.IsNullable
		jsonType = colSchema.toType()
	}

	// Pass-through a postgres column description.
	if column.Description != nil {
		jsonType.Description = *column.Description
	}
	return jsonType, nil
}

func translateRecordFields(table *sqlcapture.DiscoveryInfo, f map[string]interface{}) error {
	if f == nil {
		return nil
	}
	for id, val := range f {
		var columnInfo *sqlcapture.ColumnInfo
		if table != nil {
			if info, ok := table.Columns[id]; ok {
				columnInfo = &info
			}
		}

		var translated, err = translateRecordField(columnInfo, val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

func oversizePlaceholderJSON(orig []byte) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"flow_truncated":true,"original_size":%d}`, len(orig)))
}

// translateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodeable output format. As a concrete example, the
// PostgreSQL `cidr` type becomes a `*net.IPNet`, but the default JSON
// marshalling of a `net.IPNet` isn't a great fit and we'd prefer to use
// the `String()` method to get the usual "192.168.100.0/24" notation.
func translateRecordField(column *sqlcapture.ColumnInfo, val interface{}) (interface{}, error) {
	var columnName = "<unknown>"
	var dataType interface{}
	if column != nil {
		columnName = column.Name
		dataType = column.DataType
	}

	switch dataType {
	case "timetz":
		if x, ok := val.(string); ok {
			var formats = []string{
				"15:04:05.999999999Z07:00",
				"15:04:05.999999999Z07",
				"15:04:05Z07:00",
				"15:04:05Z07",
			}
			for _, format := range formats {
				if t, err := time.Parse(format, x); err == nil {
					return t.Format(rfc3339TimeFormat), nil
				}
			}
		}
	}
	switch x := val.(type) {
	case net.HardwareAddr: // column types 'macaddr' and 'macaddr8'
		return x.String(), nil
	case [16]uint8: // column type 'uuid'
		var s = new(strings.Builder)
		for i := range x {
			if i == 4 || i == 6 || i == 8 || i == 10 {
				s.WriteString("-")
			}
			fmt.Fprintf(s, "%02x", x[i])
		}
		return s.String(), nil
	case string:
		if len(x) > truncateColumnThreshold {
			return x[:truncateColumnThreshold], nil
		}
		return x, nil
	case []byte:
		if len(x) > truncateColumnThreshold {
			return x[:truncateColumnThreshold], nil
		}
		return x, nil
	case pgtype.Array[any]:
		return translateArray(column, x)
	case pgtype.Range[any]:
		return stringifyRange(x)
	case map[string]any: // TODO(wgd): Is this needed for anything after we fix JSON columns?
		var bs, err = json.Marshal(x)
		if err != nil {
			return nil, fmt.Errorf("error reserializing json column %q: %w", columnName, err)
		}
		if len(bs) > truncateColumnThreshold {
			return oversizePlaceholderJSON(bs), nil
		}
		return json.RawMessage(bs), nil
	case pgtype.Text:
		if len(x.String) > truncateColumnThreshold {
			return x.String[:truncateColumnThreshold], nil
		}
		return x.String, nil
	case pgtype.InfinityModifier:
		// Postgres has special timestamp values for "infinity" and "-infinity" which it handles
		// internally for performing comparisions. We do our best to represent these as an RFC3339
		// timestamp string here, as the smallest possible timestamp in the case of negative
		// infinity and the largest possible for infinity. There is also a pgtype.None infinity
		// modifier which is being left unhandled currently as I don't know that it has any meaning
		// for a captured value.
		if x == pgtype.Infinity {
			return infinityTimestamp, nil
		} else if x == pgtype.NegativeInfinity {
			return negativeInfinityTimestamp, nil
		}
	case time.Time:
		return formatRFC3339(x)
	case pgtype.Time:
		return x.Microseconds, nil // For historical reasons, times (note: not timestamps) without time zone are serialized as Unix microseconds
	case pgtype.Numeric:
		// By default a Numeric value is marshalled to a JSON number, but most JSON parsers and
		// encoders suffer from precision issues so we really want them to be turned into a JSON
		// string containing the formatted number instead.
		var bs, err = json.Marshal(x)
		if err != nil {
			return nil, err
		}
		return string(bs), nil
	case pgtype.Bits:
		return x.Value() // The Value() method encodes to the desired "0101" text string
	case pgtype.Interval:
		return x.Value()
	case pgtype.Line:
		return x.Value()
	case pgtype.Lseg:
		return x.Value()
	case pgtype.Box:
		return x.Value()
	case pgtype.Path:
		return x.Value()
	case pgtype.Polygon:
		return x.Value()
	case pgtype.Circle:
		return x.Value()
	}

	return val, nil
}

func stringifyRange(r pgtype.Range[any]) (string, error) {
	if r.LowerType == pgtype.Empty || r.UpperType == pgtype.Empty {
		return "empty", nil
	}

	var buf = new(strings.Builder)
	switch r.LowerType {
	case pgtype.Inclusive:
		buf.WriteString("[")
	case pgtype.Exclusive, pgtype.Unbounded:
		buf.WriteString("(")
	}
	if r.LowerType == pgtype.Inclusive || r.LowerType == pgtype.Exclusive {
		if translated, err := translateRecordField(nil, r.Lower); err != nil {
			fmt.Fprintf(buf, "%v", r.Lower)
		} else {
			fmt.Fprintf(buf, "%v", translated)
		}
	}
	buf.WriteString(",")
	if r.UpperType == pgtype.Inclusive || r.UpperType == pgtype.Exclusive {
		if translated, err := translateRecordField(nil, r.Upper); err != nil {
			fmt.Fprintf(buf, "%v", r.Upper)
		} else {
			fmt.Fprintf(buf, "%v", translated)
		}
	}
	switch r.UpperType {
	case pgtype.Inclusive:
		buf.WriteString("]")
	case pgtype.Exclusive, pgtype.Unbounded:
		buf.WriteString(")")
	}
	return buf.String(), nil
}

func formatRFC3339(t time.Time) (any, error) {
	if t.Year() < 0 || t.Year() > 9999 {
		// We could in theory clamp excessively large years to positive infinity, but this
		// is of limited usefulness since these are never real dates, they're mostly just
		// dumb typos like `20221` and so we might as well normalize errors consistently.
		return negativeInfinityTimestamp, nil
	}
	return t.Format(time.RFC3339Nano), nil
}

func translateArray(_ *sqlcapture.ColumnInfo, x pgtype.Array[any]) (any, error) {
	var dims []int
	for _, dim := range x.Dims {
		dims = append(dims, int(dim.Length))
	}
	for idx := range x.Elements {
		var translated, err = translateRecordField(nil, x.Elements[idx])
		if err != nil {
			return nil, err
		}
		x.Elements[idx] = translated
	}
	return map[string]any{
		"dimensions": dims,
		"elements":   x.Elements,
	}, nil
}

type columnSchema struct {
	contentEncoding string
	format          string
	nullable        bool
	jsonType        string
}

func (s columnSchema) toType() *jsonschema.Schema {
	var out = &jsonschema.Schema{
		Format: s.format,
		Extras: make(map[string]interface{}),
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

var postgresTypeToJSON = map[string]columnSchema{
	"bool": {jsonType: "boolean"},

	"int2": {jsonType: "integer"},
	"int4": {jsonType: "integer"},
	"int8": {jsonType: "integer"},

	"numeric": {jsonType: "string", format: "number"},
	"float4":  {jsonType: "number"},
	"float8":  {jsonType: "number"},

	"varchar": {jsonType: "string"},
	"bpchar":  {jsonType: "string"},
	"text":    {jsonType: "string"},
	"bytea":   {jsonType: "string", contentEncoding: "base64"},
	"xml":     {jsonType: "string"},
	"bit":     {jsonType: "string"},
	"varbit":  {jsonType: "string"},

	"json":     {},
	"jsonb":    {},
	"jsonpath": {jsonType: "string"},

	// Domain-Specific Types
	"date":        {jsonType: "string", format: "date-time"},
	"timestamp":   {jsonType: "string", format: "date-time"},
	"timestamptz": {jsonType: "string", format: "date-time"},
	"time":        {jsonType: "integer"},
	"timetz":      {jsonType: "string", format: "time"},
	"interval":    {jsonType: "string"},
	"money":       {jsonType: "string"},
	"point":       {jsonType: "string"},
	"line":        {jsonType: "string"},
	"lseg":        {jsonType: "string"},
	"box":         {jsonType: "string"},
	"path":        {jsonType: "string"},
	"polygon":     {jsonType: "string"},
	"circle":      {jsonType: "string"},
	"inet":        {jsonType: "string"},
	"cidr":        {jsonType: "string"},
	"macaddr":     {jsonType: "string"},
	"macaddr8":    {jsonType: "string"},
	"tsvector":    {jsonType: "string"},
	"tsquery":     {jsonType: "string"},
	"uuid":        {jsonType: "string", format: "uuid"},
}

const queryDiscoverTables = `
  SELECT n.nspname, c.relname
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
  WHERE n.nspname NOT IN ('pg_catalog', 'pg_internal', 'information_schema', 'catalog_history', 'cron')
    AND NOT c.relispartition
    AND c.relkind IN ('r', 'p');` // 'r' means "Ordinary Table" and 'p' means "Partitioned Table"

func getTables(ctx context.Context, conn *pgx.Conn, selectedSchemas []string) ([]*sqlcapture.DiscoveryInfo, error) {
	logrus.Debug("listing all tables in the database")
	var tables []*sqlcapture.DiscoveryInfo
	var rows, err = conn.Query(ctx, queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName string
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
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
			BaseTable:   true, // PostgreSQL discovery queries only ever list 'BASE TABLE' entities
			OmitBinding: omitBinding,
		})
	}
	return tables, rows.Err()
}

const queryDiscoverColumns = `
  SELECT nc.nspname as table_schema,
         c.relname as table_name,
		 a.attnum as ordinal_position,
		 a.attname as column_name,
		 NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable,
		 COALESCE(bt.typname, t.typname) AS udt_name,
		 t.typtype::text AS typtype
	FROM pg_catalog.pg_attribute a
	JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
	JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
	JOIN pg_catalog.pg_namespace nc ON c.relnamespace = nc.oid
	LEFT JOIN (pg_catalog.pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid)
	       ON t.typtype = 'd'::"char" AND t.typbasetype = bt.oid
    WHERE NOT pg_is_other_temp_schema(nc.oid)
	  AND a.attnum > 0
	  AND NOT a.attisdropped
	  AND (c.relkind = ANY (ARRAY['r'::"char", 'v'::"char", 'f'::"char", 'p'::"char"]))
	ORDER BY nc.nspname, c.relname, a.attnum;`

func getColumns(ctx context.Context, conn *pgx.Conn) ([]sqlcapture.ColumnInfo, error) {
	logrus.Debug("listing all columns in the database")
	var columns []sqlcapture.ColumnInfo
	var rows, err = conn.Query(ctx, queryDiscoverColumns)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var col sqlcapture.ColumnInfo
		var typtype string
		if err := rows.Scan(&col.TableSchema, &col.TableName, &col.Index, &col.Name, &col.IsNullable, &col.DataType, &typtype); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		// Special cases for user-defined types where we must resolve the columnSchema directly.
		switch typtype {
		case "c": // composite
			// TODO(whb): We don't currently do any kind of special handling for composite
			// (tuple) types and just output whatever we get from from pgx's GenericText
			// decoder. The generated text for these isn't very usable and we may be able to
			// improve this by discovering composite types, building decoders for them, and
			// registering the decoders with the pgx connection. pgx v5 has new utility methods
			// specifically for doing this.
		case "e": // enum
			// Enum values are always strings corresponding to an enum label.
			col.DataType = columnSchema{jsonType: "string"}
		case "r", "m": // range, multirange
			// Capture ranges in their text form to retain inclusive (like `[`) & exclusive
			// (like `(`) bounds information. For example, the text form of a range representing
			// "integers greater than or equal to 1 but less than 5" is '[1,5)'
			col.DataType = columnSchema{jsonType: "string"}
		}

		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// Query copied from pgjdbc's method PgDatabaseMetaData.getPrimaryKeys() with
// the always-NULL `TABLE_CAT` column omitted.
//
// See: https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/jdbc/PgDatabaseMetaData.java#L2134
const queryDiscoverPrimaryKeys = `
  SELECT result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ
  FROM (
    SELECT n.nspname AS TABLE_SCHEM,
      ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,
      (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,
      information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM
    FROM pg_catalog.pg_class ct
      JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
      JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
      JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
      JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
    WHERE i.indisprimary
  ) result
  WHERE result.A_ATTNUM = (result.KEYS).x
  ORDER BY result.table_name, result.pk_name, result.key_seq;
`

// getPrimaryKeys queries the database to produce a map from table names to
// primary keys. Table names are fully qualified as "<schema>.<name>", and
// primary keys are represented as a list of column names, in the order that
// they form the table's primary key.
func getPrimaryKeys(ctx context.Context, conn *pgx.Conn) (map[string][]string, error) {
	logrus.Debug("listing all primary-key columns in the database")
	var keys = make(map[string][]string)
	var rows, err = conn.Query(ctx, queryDiscoverPrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName, columnName string
		var columnIndex int
		if err := rows.Scan(&tableSchema, &tableName, &columnName, &columnIndex); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		keys[streamID] = append(keys[streamID], columnName)
		if columnIndex != len(keys[streamID]) {
			return nil, fmt.Errorf("primary key column %q of table %q appears out of order (expected index %d, in context %q)", columnName, streamID, columnIndex, keys[streamID])
		}
	}
	return keys, rows.Err()
}

const queryDiscoverSecondaryIndices = `
SELECT r.table_schema, r.table_name, r.index_name, (r.index_keys).n, r.column_name
FROM (
  SELECT tn.nspname AS table_schema,
         tc.relname AS table_name,
	     ic.relname AS index_name,
	     information_schema._pg_expandarray(ix.indkey) AS index_keys,
		 a.attnum AS column_number,
		 a.attname AS column_name
  FROM pg_catalog.pg_index ix
       JOIN pg_catalog.pg_class ic ON (ic.oid = ix.indexrelid)
	   JOIN pg_catalog.pg_class tc ON (tc.oid = ix.indrelid)
	   JOIN pg_catalog.pg_namespace tn ON (tn.oid = tc.relnamespace)
	   JOIN pg_catalog.pg_attribute a ON (a.attrelid = tc.oid)
  WHERE ix.indisunique AND ix.indexprs IS NULL AND tc.relkind = 'r'
    AND NOT ix.indisprimary
  ORDER BY tc.relname, ic.relname
) r
WHERE r.column_number = (r.index_keys).x
ORDER BY r.table_schema, r.table_name, r.index_name, (r.index_keys).n
`

func getSecondaryIndexes(ctx context.Context, conn *pgx.Conn) (map[string]map[string][]string, error) {
	logrus.Debug("listing secondary indexes")
	// Run the 'list secondary indexes' query and aggregate results into
	// a `map[StreamID]map[IndexName][]ColumnName`
	var streamIndexColumns = make(map[string]map[string][]string)
	var rows, err = conn.Query(ctx, queryDiscoverSecondaryIndices)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName, indexName, columnName string
		var keySequence int
		if err := rows.Scan(&tableSchema, &tableName, &indexName, &keySequence, &columnName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
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

	return streamIndexColumns, rows.Err()
}

const queryColumnDescriptions = `
    SELECT * FROM (
		SELECT
		    isc.table_schema,
			isc.table_name,
			isc.column_name,
			pg_catalog.col_description(format('"%s"."%s"',isc.table_schema,isc.table_name)::regclass::oid,isc.ordinal_position) description
		FROM information_schema.columns isc
	) as descriptions WHERE description != '';
`

type columnDescription struct {
	TableSchema string
	TableName   string
	ColumnName  string
	Description string
}

func getColumnDescriptions(ctx context.Context, conn *pgx.Conn) ([]columnDescription, error) {
	var descriptions []columnDescription
	var rows, err = conn.Query(ctx, queryColumnDescriptions)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var desc columnDescription
		if err := rows.Scan(&desc.TableSchema, &desc.TableName, &desc.ColumnName, &desc.Description); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		descriptions = append(descriptions, desc)
	}
	return descriptions, rows.Err()
}
