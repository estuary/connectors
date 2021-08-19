package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

var spec = airbyte.Spec{
	SupportsIncremental:           true,                            // TODO(wgd): Verify that this is true once implemented
	SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes, // TODO(wgd): Verify that this is true once implemented
	ConnectionSpecification:       json.RawMessage(configSchema),
}

type Config struct {
	ConnectionURI string `json:"connectionURI"`
}

func (c *Config) Validate() error { return nil }

const configSchema = `{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title":   "Postgres Source Spec",
	"type":    "object",
	"properties": {
		"connectionURI": {
			"type":        "string",
			"title":       "Database Connection URI",
			"description": "Connection parameters, as a libpq-compatible connection string",
			"default":     "postgres://flow:flow@localhost:5432/flow"
		}
	},
	"required": [ "connectionURI" ]
}`

func doCheck(args airbyte.CheckCmd) error {
	result := &airbyte.ConnectionStatus{Status: airbyte.StatusSucceeded}
	if _, err := discoverCatalog(args.ConfigFile); err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func doDiscover(args airbyte.DiscoverCmd) error {
	catalog, err := discoverCatalog(args.ConfigFile)
	if err != nil {
		return err
	}
	log.Printf("Discover completed with %d streams", len(catalog.Streams))
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:    airbyte.MessageTypeCatalog,
		Catalog: catalog,
	})
}

type DBTable struct {
	TableSchema string
	TableName   string
	Columns     []DBColumn
	PrimaryKeys []DBPrimaryKey
}

func discoverCatalog(configFile airbyte.ConfigFile) (*airbyte.Catalog, error) {
	var config Config
	if err := configFile.Parse(&config); err != nil {
		return nil, err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, config.ConnectionURI)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to database")
	}
	defer conn.Close(ctx)

	// Get lists of all columns and primary keys in the database
	columns, err := getColumns(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list database columns")
	}
	primaryKeys, err := getPrimaryKeys(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list database primary keys")
	}

	// Aggregate column and primary key information into DBTable structs using
	// (TableSchema, TableName) tuples combined into a string ID.
	tables := make(map[string]*DBTable)
	for _, column := range columns {
		id := column.TableSchema + ":" + column.TableName
		if _, ok := tables[id]; !ok {
			tables[id] = &DBTable{TableSchema: column.TableSchema, TableName: column.TableName}
		}
		tables[id].Columns = append(tables[id].Columns, column)
	}
	for _, key := range primaryKeys {
		id := key.TableSchema + ":" + key.TableName
		if _, ok := tables[id]; !ok {
			// We only need primary key info for tables which getColumns() returned,
			// so if the table doesn't exist at this point then skip it.
			continue
		}
		tables[id].PrimaryKeys = append(tables[id].PrimaryKeys, key)
	}

	catalog := new(airbyte.Catalog)
	for _, table := range tables {
		log.Printf("Found table (%v, %v)", table.TableSchema, table.TableName)
		log.Printf("  Columns: %v", table.Columns)
		log.Printf("  Primary Keys: %v", table.PrimaryKeys)

		// TODO: Maybe generate the schema in a less hackish fashion
		rowSchema := `{"type":"object","properties":{`
		for idx, column := range table.Columns {
			if idx > 0 {
				rowSchema += ","
			}
			rowSchema += fmt.Sprintf("%q:%s", column.ColumnName, postgresTypeToJSON[column.DataType])
		}
		rowSchema += `}}`
		log.Printf("  Schema: %v", rowSchema)

		var primaryKeys []string
		for _, pk := range table.PrimaryKeys {
			primaryKeys = append(primaryKeys, pk.ColumnName)
		}

		catalog.Streams = append(catalog.Streams, airbyte.Stream{
			Name:                    table.TableName,
			Namespace:               table.TableSchema,
			JSONSchema:              json.RawMessage(rowSchema),
			SupportedSyncModes:      airbyte.AllSyncModes,
			SourceDefinedCursor:     true,
			SourceDefinedPrimaryKey: [][]string{primaryKeys},
		})
	}
	return catalog, err
}

// Airbyte's Postgres Source Discovery:
// {"type":"CATALOG",
//  "catalog":{
// 	 "streams":[
// 		 {"name":"surnames",
// 		  "json_schema":{"type":"object","properties":{"name":{"type":"string"},"id":{"type":"number"},"_ab_cdc_lsn":{"type":"number"},"_ab_cdc_updated_at":{"type":"string"},"_ab_cdc_deleted_at":{"type":"string"}}},
// 		  "supported_sync_modes":["full_refresh","incremental"],
// 		  "source_defined_cursor":true,
// 		  "default_cursor_field":[],
// 		  "source_defined_primary_key":[["id"]],
// 		  "namespace":"public"},
// 		 {"name":"babynames",
// 		  "json_schema":{"type":"object","properties":{"name":{"type":"string"},"id":{"type":"number"},"_ab_cdc_lsn":{"type":"number"},"_ab_cdc_updated_at":{"type":"string"},"_ab_cdc_deleted_at":{"type":"string"}}},
// 		  "supported_sync_modes":["full_refresh","incremental"],
// 		  "source_defined_cursor":true,
// 		  "default_cursor_field":[],
// 		  "source_defined_primary_key":[["id"]],
// 		  "namespace":"public"}]}}

// Our Postgres Discovery:
// {"type":"CATALOG",
//  "catalog":{
//    "streams":[
//      {"name":"babynames",
//       "json_schema":{"type":"object","properties":{"id":{"type":"number"},"name":{"type":"string"}}},
//       "supported_sync_modes":["incremental","full_refresh"],
//       "source_defined_cursor":true,
//       "source_defined_primary_key":[["id"]],
//       "namespace":"public"},
//      {"name":"surnames",
//       "json_schema":{"type":"object","properties":{"id":{"type":"number"},"name":{"type":"string"}}},
//       "supported_sync_modes":["incremental","full_refresh"],
//       "source_defined_cursor":true,
//       "source_defined_primary_key":[["id"]],
//       "namespace":"public"}]}}

// So pretty similar, except that the Airbyte source adds some metadata fields
// and also has `default_cursor_field`.

type DBColumn struct {
	TableSchema string
	TableName   string
	ColumnName  string
	ColumnIndex int
	IsNullable  bool
	DataType    string
}

const COLUMNS_QUERY = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable::boolean, udt_name
  FROM information_schema.columns
  WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema'
        AND table_schema != 'pg_internal' AND table_schema != 'catalog_history'
  ORDER BY table_schema, table_name, ordinal_position;`

func getColumns(ctx context.Context, conn *pgx.Conn) ([]DBColumn, error) {
	var columns []DBColumn
	var sc DBColumn
	_, err := conn.QueryFunc(ctx, COLUMNS_QUERY, nil,
		[]interface{}{&sc.TableSchema, &sc.TableName, &sc.ColumnIndex, &sc.ColumnName, &sc.IsNullable, &sc.DataType},
		func(r pgx.QueryFuncRow) error {
			columns = append(columns, sc)
			return nil
		})
	return columns, err
}

var postgresTypeToJSON = map[string]string{
	"int4":    `{"type":"number"}`,
	"varchar": `{"type":"string"}`,
}

type DBPrimaryKey struct {
	TableSchema string
	TableName   string
	ColumnName  string
	ColumnIndex int
	PKName      string
}

// Query copied from pgjdbc's method PgDatabaseMetaData.getPrimaryKeys() with
// the always-NULL `TABLE_CAT` column omitted.
const PRIMARY_KEYS_QUERY = `
  SELECT result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME
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

func getPrimaryKeys(ctx context.Context, conn *pgx.Conn) ([]DBPrimaryKey, error) {
	var keys []DBPrimaryKey
	var sk DBPrimaryKey
	_, err := conn.QueryFunc(ctx, PRIMARY_KEYS_QUERY, nil,
		[]interface{}{&sk.TableSchema, &sk.TableName, &sk.ColumnName, &sk.ColumnIndex, &sk.PKName},
		func(r pgx.QueryFuncRow) error {
			keys = append(keys, sk)
			return nil
		})
	return keys, err
}

func doRead(args airbyte.ReadCmd) error {
	// TODO: Implement
	return nil
}
