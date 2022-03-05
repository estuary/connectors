package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"

	"github.com/alecthomas/jsonschema"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

// DiscoverTables queries the database for information about tables available for capture.
func (db *postgresDatabase) DiscoverTables(ctx context.Context) (map[string]sqlcapture.TableInfo, error) {
	// Get lists of all columns and primary keys in the database
	var columns, err = getColumns(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database columns: %w", err)
	}
	primaryKeys, err := getPrimaryKeys(ctx, db.conn)
	if err != nil {
		return nil, fmt.Errorf("unable to list database primary keys: %w", err)
	}

	// Aggregate column and primary key information into TableInfo structs
	// using a map from fully-qualified "<schema>.<name>" table names to
	// the corresponding TableInfo.
	var tableMap = make(map[string]sqlcapture.TableInfo)
	for _, column := range columns {
		var id = column.TableSchema + "." + column.TableName
		var info, ok = tableMap[id]
		if !ok {
			info = sqlcapture.TableInfo{Schema: column.TableSchema, Name: column.TableName}
		}
		if info.Columns == nil {
			info.Columns = make(map[string]sqlcapture.ColumnInfo)
		}
		info.Columns[column.Name] = column
		tableMap[id] = info
	}
	for id, key := range primaryKeys {
		// The `getColumns()` query implements the "exclude system schemas" logic,
		// so here we ignore primary key information for tables we don't care about.
		var info, ok = tableMap[id]
		if !ok {
			continue
		}
		logrus.WithFields(logrus.Fields{"table": id, "key": key}).Debug("queried primary key")
		info.PrimaryKey = key
		tableMap[id] = info
	}
	return tableMap, nil
}

// TranslateDBToJSONType returns JSON schema information about the provided database column type.
func (db *postgresDatabase) TranslateDBToJSONType(column sqlcapture.ColumnInfo) (*jsonschema.Type, error) {
	// If the column type looks like `_foo` then it's an array of elements of type `foo`.
	var columnType = column.DataType
	var arrayColumn = false
	if strings.HasPrefix(columnType, "_") {
		columnType = strings.TrimPrefix(columnType, "_")
		arrayColumn = true
	}

	// Translate the basic value/element type into a JSON Schema type
	var colSchema, ok = postgresTypeToJSON[columnType]
	if !ok {
		return nil, fmt.Errorf("unhandled PostgreSQL type %q", columnType)
	}
	colSchema.nullable = column.IsNullable
	var jsonType = colSchema.toType()

	// If the column is an array, wrap the element type in a multidimensional
	// array structure.
	if arrayColumn {
		jsonType = &jsonschema.Type{
			Type: "object",
			Extras: map[string]interface{}{
				"properties": map[string]*jsonschema.Type{
					"dimensions": {
						Type:  "array",
						Items: &jsonschema.Type{Type: "integer"},
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

// translateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodeable output format. As a concrete example, the
// PostgreSQL `cidr` type becomes a `*net.IPNet`, but the default JSON
// marshalling of a `net.IPNet` isn't a great fit and we'd prefer to use
// the `String()` method to get the usual "192.168.100.0/24" notation.
func (db *postgresDatabase) TranslateRecordField(column *sqlcapture.ColumnInfo, val interface{}) (interface{}, error) {
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
		return db.translateArray(nil, x)
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

func (db *postgresDatabase) translateArray(column *sqlcapture.ColumnInfo, x interface{}) (interface{}, error) {
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
		var translated, err = db.TranslateRecordField(column, element.Interface())
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
	type_           string
}

func (s columnSchema) toType() *jsonschema.Type {
	var out = &jsonschema.Type{
		Format: s.format,
		Extras: make(map[string]interface{}),
	}

	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding // New in 2019-09.
	}

	if s.type_ == "" {
		// No type constraint.
	} else if s.nullable {
		out.Extras["type"] = []string{s.type_, "null"} // Use variadic form.
	} else {
		out.Type = s.type_
	}
	return out
}

var postgresTypeToJSON = map[string]columnSchema{
	"bool": {type_: "boolean"},

	"int2": {type_: "integer"},
	"int4": {type_: "integer"},
	"int8": {type_: "integer"},

	"numeric": {type_: "string"},
	"float4":  {type_: "number"},
	"float8":  {type_: "number"},

	"varchar": {type_: "string"},
	"bpchar":  {type_: "string"},
	"text":    {type_: "string"},
	"bytea":   {type_: "string", contentEncoding: "base64"},
	"xml":     {type_: "string"},
	"bit":     {type_: "string"},
	"varbit":  {type_: "string"},

	"json":     {},
	"jsonb":    {},
	"jsonpath": {type_: "string"},

	// Domain-Specific Types
	"date":        {type_: "string", format: "date-time"},
	"timestamp":   {type_: "string", format: "date-time"},
	"timestamptz": {type_: "string", format: "date-time"},
	"time":        {type_: "integer"},
	"timetz":      {type_: "string", format: "time"},
	"interval":    {type_: "string"},
	"money":       {type_: "string"},
	"point":       {type_: "string"},
	"line":        {type_: "string"},
	"lseg":        {type_: "string"},
	"box":         {type_: "string"},
	"path":        {type_: "string"},
	"polygon":     {type_: "string"},
	"circle":      {type_: "string"},
	"inet":        {type_: "string"},
	"cidr":        {type_: "string"},
	"macaddr":     {type_: "string"},
	"macaddr8":    {type_: "string"},
	"tsvector":    {type_: "string"},
	"tsquery":     {type_: "string"},
	"uuid":        {type_: "string", format: "uuid"},
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
		t.table_type = 'BASE TABLE'
  ORDER BY
		c.table_schema,
		c.table_name,
		c.ordinal_position
	;`

const queryColumnDescription = `SELECT pg_catalog.col_description($1::regclass::oid, $2) AS description;`

func getColumns(ctx context.Context, conn *pgx.Conn) ([]sqlcapture.ColumnInfo, error) {
	var columns []sqlcapture.ColumnInfo
	var sc sqlcapture.ColumnInfo
	var _, err = conn.QueryFunc(ctx, queryDiscoverColumns, nil,
		[]interface{}{&sc.TableSchema, &sc.TableName, &sc.Index, &sc.Name, &sc.IsNullable, &sc.DataType},
		func(r pgx.QueryFuncRow) error {
			columns = append(columns, sc)
			return nil
		})
	for idx := range columns {
		var col = &columns[idx]
		// Ignore errors when trying to get column descriptions, because failures are non-fatal.
		conn.QueryFunc(ctx, queryColumnDescription,
			[]interface{}{col.TableSchema + "." + col.TableName, col.Index},
			[]interface{}{&col.Description},
			func(r pgx.QueryFuncRow) error { return nil })
	}
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
	var keys = make(map[string][]string)
	var tableSchema, tableName, columnName string
	var columnIndex int
	var _, err = conn.QueryFunc(ctx, queryDiscoverPrimaryKeys, nil,
		[]interface{}{&tableSchema, &tableName, &columnName, &columnIndex},
		func(r pgx.QueryFuncRow) error {
			var id = fmt.Sprintf("%s.%s", tableSchema, tableName)
			keys[id] = append(keys[id], columnName)
			if columnIndex != len(keys[id]) {
				return fmt.Errorf("primary key column %q appears out of order (expected index %d, in context %q)", columnName, columnIndex, keys[id])
			}
			return nil
		})
	return keys, err
}
