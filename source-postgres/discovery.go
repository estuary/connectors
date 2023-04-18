package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *postgresDatabase) DiscoverTables(ctx context.Context) (map[string]*sqlcapture.DiscoveryInfo, error) {
	// Get lists of all columns and primary keys in the database
	var columns, err = getColumns(ctx, db.conn)
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
	for _, column := range columns {
		var streamID = sqlcapture.JoinStreamID(column.TableSchema, column.TableName)
		var info, ok = tableMap[streamID]
		if !ok {
			info = &sqlcapture.DiscoveryInfo{Schema: column.TableSchema, Name: column.TableName}
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
		for _, columns := range indexColumns {
			// Test that for each column the value is non-nullable
			if columnsNonNullable(info.Columns, columns) {
				logrus.WithFields(logrus.Fields{
					"table": streamID,
					"index": columns,
				}).Trace("using unique secondary index as primary key")
				info.PrimaryKey = columns
				break
			} else {
				logrus.WithFields(logrus.Fields{
					"table": streamID,
					"index": columns,
				}).Trace("cannot use secondary index because some of its columns are nullable")
			}
		}
	}
	return tableMap, nil
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
	// If the column type looks like `_foo` then it's an array of elements of type `foo`.
	var columnType, ok = column.DataType.(string)
	if !ok {
		return nil, fmt.Errorf("unhandled PostgreSQL type %q", columnType)
	}
	var arrayColumn = false
	if strings.HasPrefix(columnType, "_") {
		columnType = strings.TrimPrefix(columnType, "_")
		arrayColumn = true
	}

	// Translate the basic value/element type into a JSON Schema type
	colSchema, ok := postgresTypeToJSON[columnType]
	if !ok {
		return nil, fmt.Errorf("unhandled PostgreSQL type %q", columnType)
	}
	colSchema.nullable = column.IsNullable
	var jsonType = colSchema.toType()

	// If the column is an array, wrap the element type in a multidimensional
	// array structure.
	if arrayColumn {
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
						Items: jsonType,
					},
				},
			},
			Required: []string{"dimensions", "elements"},
		}
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

// translateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodeable output format. As a concrete example, the
// PostgreSQL `cidr` type becomes a `*net.IPNet`, but the default JSON
// marshalling of a `net.IPNet` isn't a great fit and we'd prefer to use
// the `String()` method to get the usual "192.168.100.0/24" notation.
func translateRecordField(column *sqlcapture.ColumnInfo, val interface{}) (interface{}, error) {
	switch x := val.(type) {
	case *net.IPNet:
		return x.String(), nil
	case net.HardwareAddr:
		return x.String(), nil
	case [16]uint8: // UUIDs
		var s = new(strings.Builder)
		for i := range x {
			if i == 4 || i == 6 || i == 8 || i == 10 {
				s.WriteString("-")
			}
			fmt.Fprintf(s, "%02x", x[i])
		}
		return s.String(), nil
	case pgtype.Bytea:
		return x.Bytes, nil
	case pgtype.Float4:
		return x.Float, nil
	case pgtype.Float8:
		return x.Float, nil
	case pgtype.BPCharArray, pgtype.BoolArray, pgtype.ByteaArray, pgtype.CIDRArray,
		pgtype.DateArray, pgtype.EnumArray, pgtype.Float4Array, pgtype.Float8Array,
		pgtype.HstoreArray, pgtype.InetArray, pgtype.Int2Array, pgtype.Int4Array,
		pgtype.Int8Array, pgtype.JSONBArray, pgtype.MacaddrArray, pgtype.NumericArray,
		pgtype.TextArray, pgtype.TimestampArray, pgtype.TimestamptzArray, pgtype.TsrangeArray,
		pgtype.TstzrangeArray, pgtype.UUIDArray, pgtype.UntypedTextArray, pgtype.VarcharArray:
		// TODO(wgd): If PostgreSQL value translation starts using the provided column
		// information, this will need to be plumbed through the array translation
		// logic so that the same behavior can apply to individual array elements.
		return translateArray(nil, x)
	}
	if _, ok := val.(json.Marshaler); ok {
		return val, nil
	}
	if enc, ok := val.(pgtype.TextEncoder); ok {
		var bs, err = enc.EncodeText(nil, nil)
		return string(bs), err
	}
	return val, nil
}

func translateArray(column *sqlcapture.ColumnInfo, x interface{}) (interface{}, error) {
	// Use reflection to extract the 'elements' field
	var array = reflect.ValueOf(x)
	if array.Kind() != reflect.Struct {
		return nil, fmt.Errorf("array translation expected struct, got %v", array.Kind())
	}

	var elements = array.FieldByName("Elements")
	if elements.Kind() != reflect.Slice {
		return nil, fmt.Errorf("array translation expected Elements slice, got %v", elements.Kind())
	}
	var vals = make([]interface{}, elements.Len())
	for idx := 0; idx < len(vals); idx++ {
		var element = elements.Index(idx)
		var translated, err = translateRecordField(column, element.Interface())
		if err != nil {
			return nil, fmt.Errorf("error translating array element %d: %w", idx, err)
		}
		vals[idx] = translated
	}

	var dimensions, ok = array.FieldByName("Dimensions").Interface().([]pgtype.ArrayDimension)
	if !ok {
		return nil, fmt.Errorf("array translation error: expected Dimensions to have type []ArrayDimension")
	}
	var dims = make([]int, len(dimensions))
	for idx := 0; idx < len(dims); idx++ {
		dims[idx] = int(dimensions[idx].Length)
	}

	return map[string]interface{}{
		"dimensions": dims,
		"elements":   vals,
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

const queryDiscoverColumns = `
  SELECT
		c.table_schema,
		c.table_name,
		c.ordinal_position,
		c.column_name,
		c.is_nullable::boolean,
		c.udt_name
  FROM information_schema.columns c
  JOIN information_schema.tables t ON (c.table_schema = t.table_schema AND c.table_name = t.table_name)
  WHERE
		c.table_schema != 'pg_catalog' AND
		c.table_schema != 'information_schema' AND
		c.table_schema != 'pg_internal' AND
		c.table_schema != 'catalog_history' AND
		c.table_schema != 'cron' AND
		t.table_type = 'BASE TABLE'
  ORDER BY
		c.table_schema,
		c.table_name,
		c.ordinal_position
	;`

func getColumns(ctx context.Context, conn *pgx.Conn) ([]sqlcapture.ColumnInfo, error) {
	logrus.Debug("listing all tables/columns in the database")
	var columns []sqlcapture.ColumnInfo
	var sc sqlcapture.ColumnInfo
	var _, err = conn.QueryFunc(ctx, queryDiscoverColumns, nil,
		[]interface{}{&sc.TableSchema, &sc.TableName, &sc.Index, &sc.Name, &sc.IsNullable, &sc.DataType},
		func(r pgx.QueryFuncRow) error {
			columns = append(columns, sc)
			return nil
		})
	return columns, err
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
	var tableSchema, tableName, columnName string
	var columnIndex int
	var _, err = conn.QueryFunc(ctx, queryDiscoverPrimaryKeys, nil,
		[]interface{}{&tableSchema, &tableName, &columnName, &columnIndex},
		func(r pgx.QueryFuncRow) error {
			var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
			keys[streamID] = append(keys[streamID], columnName)
			if columnIndex != len(keys[streamID]) {
				return fmt.Errorf("primary key column %q of table %q appears out of order (expected index %d, in context %q)", columnName, streamID, columnIndex, keys[streamID])
			}
			return nil
		})
	return keys, err
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
	var tableSchema, tableName, indexName, columnName string
	var keySequence int
	var _, err = conn.QueryFunc(ctx, queryDiscoverSecondaryIndices, nil,
		[]interface{}{&tableSchema, &tableName, &indexName, &keySequence, &columnName},
		func(r pgx.QueryFuncRow) error {
			var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
			var indexColumns = streamIndexColumns[streamID]
			if indexColumns == nil {
				indexColumns = make(map[string][]string)
				streamIndexColumns[streamID] = indexColumns
			}
			indexColumns[indexName] = append(indexColumns[indexName], columnName)
			if len(indexColumns[indexName]) != keySequence {
				return fmt.Errorf("internal error: secondary index key ordering failure: index %q on stream %q: column %q appears out of order", indexName, streamID, columnName)
			}
			return nil
		})
	return streamIndexColumns, err
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
	var desc columnDescription
	var _, err = conn.QueryFunc(ctx, queryColumnDescriptions, nil,
		[]interface{}{&desc.TableSchema, &desc.TableName, &desc.ColumnName, &desc.Description},
		func(r pgx.QueryFuncRow) error {
			descriptions = append(descriptions, desc)
			return nil
		})
	return descriptions, err
}
