package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/protocols/airbyte"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func DiscoverCatalog(ctx context.Context, config Config) (*airbyte.Catalog, error) {
	conn, err := pgx.Connect(ctx, config.ConnectionURI)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to database")
	}
	defer conn.Close(ctx)

	tables, err := GetDatabaseTables(ctx, conn)
	if err != nil {
		return nil, err
	}

	catalog := new(airbyte.Catalog)
	for _, table := range tables {
		logrus.WithFields(logrus.Fields{
			"namespace":  table.TableSchema,
			"table":      table.TableName,
			"primaryKey": table.PrimaryKey,
		}).Info("discovered table")

		// TODO(wgd): Maybe generate the schema in a less hackish fashion
		schema := `{"type":"object","properties":{`
		for idx, column := range table.Columns {
			if idx > 0 {
				schema += ","
			}
			jsonType, ok := postgresTypeToJSON[column.DataType]
			if !ok {
				return nil, errors.Errorf("cannot translate PostgreSQL column type %q to JSON schema", column.DataType)
			}
			schema += fmt.Sprintf("%q:%s", column.ColumnName, jsonType)
		}
		schema += `}}`

		logrus.WithFields(logrus.Fields{
			"namespace": table.TableSchema,
			"table":     table.TableName,
			"columns":   table.Columns,
			"schema":    schema,
		}).Debug("translated table schema")

		var sourceDefinedPrimaryKey [][]string
		for _, colName := range table.PrimaryKey {
			sourceDefinedPrimaryKey = append(sourceDefinedPrimaryKey, []string{colName})
		}

		catalog.Streams = append(catalog.Streams, airbyte.Stream{
			Name:                    table.TableName,
			Namespace:               table.TableSchema,
			JSONSchema:              json.RawMessage(schema),
			SupportedSyncModes:      airbyte.AllSyncModes,
			SourceDefinedCursor:     true,
			SourceDefinedPrimaryKey: sourceDefinedPrimaryKey,
		})
	}
	return catalog, err
}

// TODO(wgd): Revisit whether the PostgreSQL Schema -> JSON Spec translation for
// column types can be done automatically, or at least flesh out the set of types
// handled in this map.
var postgresTypeToJSON = map[string]string{
	"int4":    `{"type":"number"}`,
	"float4":  `{"type":"number"}`,
	"varchar": `{"type":"string"}`,
	"text":    `{"type":"string"}`,
}

type TableInfo struct {
	TableSchema string
	TableName   string
	Columns     []ColumnInfo
	PrimaryKey  []string
}

type ColumnInfo struct {
	TableSchema string
	TableName   string
	ColumnName  string
	ColumnIndex int
	IsNullable  bool
	DataType    string
}

// GetDatabaseTables queries the database to produce a list of all tables
// (with the exception of some internal system schemas) with information
// about their column types and primary key.
func GetDatabaseTables(ctx context.Context, conn *pgx.Conn) ([]TableInfo, error) {
	// Get lists of all columns and primary keys in the database
	columns, err := getColumns(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list database columns")
	}
	primaryKeys, err := GetPrimaryKeys(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list database primary keys")
	}

	// Aggregate column and primary key information into TableInfo structs
	// using a map from fully-qualified "<schema>.<name>" table names to
	// the corresponding TableInfo.
	tableMap := make(map[string]*TableInfo)
	for _, column := range columns {
		id := column.TableSchema + "." + column.TableName
		if _, ok := tableMap[id]; !ok {
			tableMap[id] = &TableInfo{TableSchema: column.TableSchema, TableName: column.TableName}
		}
		tableMap[id].Columns = append(tableMap[id].Columns, column)
	}
	for id, key := range primaryKeys {
		// The `getColumns()` query implements the "exclude system schemas" logic,
		// so here we ignore primary key information for tables we don't care about.
		if _, ok := tableMap[id]; !ok {
			continue
		}
		logrus.WithField("table", id).WithField("key", key).Debug("queried primary key")
		tableMap[id].PrimaryKey = key
	}

	// Now that aggregation is complete, discard map keys and return
	// just the list of TableInfo structs.
	var tables []TableInfo
	for _, info := range tableMap {
		tables = append(tables, *info)
	}
	return tables, nil
}

const COLUMNS_QUERY = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable::boolean, udt_name
  FROM information_schema.columns
  WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema'
        AND table_schema != 'pg_internal' AND table_schema != 'catalog_history'
  ORDER BY table_schema, table_name, ordinal_position;`

func getColumns(ctx context.Context, conn *pgx.Conn) ([]ColumnInfo, error) {
	var columns []ColumnInfo
	var sc ColumnInfo
	_, err := conn.QueryFunc(ctx, COLUMNS_QUERY, nil,
		[]interface{}{&sc.TableSchema, &sc.TableName, &sc.ColumnIndex, &sc.ColumnName, &sc.IsNullable, &sc.DataType},
		func(r pgx.QueryFuncRow) error {
			columns = append(columns, sc)
			return nil
		})
	return columns, err
}

// Query copied from pgjdbc's method PgDatabaseMetaData.getPrimaryKeys() with
// the always-NULL `TABLE_CAT` column omitted.
const PRIMARY_KEYS_QUERY = `
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
    WHERE true
     AND i.indisprimary
  ) result
  WHERE result.A_ATTNUM = (result.KEYS).x
  ORDER BY result.table_name, result.pk_name, result.key_seq;
`

// GetPrimaryKeys queries the database to produce a map from table names to
// primary keys. Table names are fully qualified as "<schema>.<name>", and
// primary keys are represented as a list of column names, in the order that
// they form the table's primary key.
func GetPrimaryKeys(ctx context.Context, conn *pgx.Conn) (map[string][]string, error) {
	keys := make(map[string][]string)
	var tableSchema, tableName, columnName string
	var columnIndex int
	_, err := conn.QueryFunc(ctx, PRIMARY_KEYS_QUERY, nil,
		[]interface{}{&tableSchema, &tableName, &columnName, &columnIndex},
		func(r pgx.QueryFuncRow) error {
			id := fmt.Sprintf("%s.%s", tableSchema, tableName)
			keys[id] = append(keys[id], columnName)
			if columnIndex != len(keys[id]) {
				return errors.Errorf("primary key column %q appears out of order (expected index %d, in context %q)", columnName, columnIndex, keys[id])
			}
			return nil
		})
	return keys, err
}
