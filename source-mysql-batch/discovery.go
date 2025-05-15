package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

// Discover enumerates tables and views from `information_schema.tables` and generates
// placeholder capture queries for those tables.
func (drv *BatchSQLDriver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	var db, err = drv.Connect(ctx, &cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tables, err := discoverTables(ctx, db, cfg.Advanced.DiscoverSchemas)
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %w", err)
	}
	keys, err := discoverPrimaryKeys(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("error listing primary keys: %w", err)
	}

	// We rebuild this map even though `discoverPrimaryKeys` already built and
	// discarded it, in order to keep the `discoverTables` / `discoverPrimaryKeys`
	// interface as stupidly simple as possible, which ought to make it just that
	// little bit easier to do this generically for multiple databases.
	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for _, key := range keys {
		keysByTable[key.Schema+"."+key.Table] = key
	}

	var bindings []*pc.Response_Discovered_Binding
	for _, table := range tables {
		// Exclude tables in "system schemas" such as information_schema or pg_catalog.
		if slices.Contains(drv.ExcludedSystemSchemas, table.Schema) {
			continue
		}

		var tableID = table.Schema + "." + table.Name

		var recommendedName = recommendedCatalogName(table.Schema, table.Name)
		var res, err = drv.GenerateResource(&cfg, recommendedName, table.Schema, table.Name, table.Type)
		if err != nil {
			log.WithFields(log.Fields{
				"reason": err,
				"table":  tableID,
				"type":   table.Type,
			}).Warn("unable to generate resource spec")
			continue
		}
		resourceConfigJSON, err := json.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("error serializing resource spec: %w", err)
		}

		// Start with a minimal schema and a fallback collection key, which will be
		// replaced with more useful versions if we have sufficient information.
		collectionSchema, err := generateCollectionSchema(&cfg, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("error generating minimal collection schema: %w", err)
		}
		var collectionKey = fallbackKey
		if !cfg.Advanced.parsedFeatureFlags["keyless_row_id"] {
			collectionKey = fallbackKeyOld
		}

		if tableKey, ok := keysByTable[tableID]; ok {
			if generatedSchema, err := generateCollectionSchema(&cfg, tableKey.Columns, tableKey.ColumnTypes); err == nil {
				collectionSchema = generatedSchema
				collectionKey = nil
				for _, colName := range tableKey.Columns {
					collectionKey = append(collectionKey, primaryKeyToCollectionKey(colName))
				}
			} else {
				log.WithFields(log.Fields{"table": tableID, "err": err}).Warn("unable to generate collection schema")
			}
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    recommendedName,
			ResourceConfigJson: resourceConfigJSON,
			DocumentSchemaJson: collectionSchema,
			Key:                collectionKey,
			ResourcePath:       []string{res.Name},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

// primaryKeyToCollectionKey converts a database primary key column name into a Flow collection key
// JSON pointer with escaping for '~' and '/' applied per RFC6901.
func primaryKeyToCollectionKey(key string) string {
	// Any encoded '~' must be escaped first to prevent a second escape on escaped '/' values as
	// '~1'.
	key = strings.ReplaceAll(key, "~", "~0")
	key = strings.ReplaceAll(key, "/", "~1")
	return "/" + key
}

const queryDiscoverTables = `SELECT table_schema, table_name, table_type FROM information_schema.tables;`

type discoveredTable struct {
	Schema string
	Name   string
	Type   string // Usually 'BASE TABLE' or 'VIEW'
}

func discoverTables(ctx context.Context, db *client.Conn, discoverSchemas []string) ([]*discoveredTable, error) {
	var results, err = db.Execute(queryDiscoverTables)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}
	defer results.Close()

	var tables []*discoveredTable
	for _, row := range results.Values {
		var tableSchema = string(row[0].AsString())
		var tableName = string(row[1].AsString())
		var tableType = string(row[2].AsString())

		if len(discoverSchemas) > 0 && !slices.Contains(discoverSchemas, tableSchema) {
			log.WithFields(log.Fields{"schema": tableSchema, "table": tableName}).Debug("ignoring table")
			continue
		}
		tables = append(tables, &discoveredTable{
			Schema: tableSchema,
			Name:   tableName,
			Type:   tableType,
		})
	}
	return tables, nil
}

// Joining on the 6-tuple {CONSTRAINT,TABLE}_{CATALOG,SCHEMA,NAME} is probably
// overkill but shouldn't hurt, and helps to make absolutely sure that we're
// matching up the constraint type with the column names/positions correctly.
const queryDiscoverPrimaryKeys = `
SELECT kcu.table_schema, kcu.table_name, kcu.column_name, kcu.ordinal_position, col.data_type
  FROM information_schema.key_column_usage kcu
  JOIN information_schema.table_constraints tcs
    ON  tcs.constraint_catalog = kcu.constraint_catalog
    AND tcs.constraint_schema = kcu.constraint_schema
    AND tcs.constraint_name = kcu.constraint_name
    AND tcs.table_schema = kcu.table_schema
    AND tcs.table_name = kcu.table_name
  JOIN information_schema.columns col
    ON  col.table_schema = kcu.table_schema
	AND col.table_name = kcu.table_name
	AND col.column_name = kcu.column_name
  WHERE tcs.constraint_type = 'PRIMARY KEY'
  ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;
`

type discoveredPrimaryKey struct {
	Schema      string
	Table       string
	Columns     []string
	ColumnTypes map[string]*jsonschema.Schema
}

func discoverPrimaryKeys(ctx context.Context, db *client.Conn) ([]*discoveredPrimaryKey, error) {
	var results, err = db.Execute(queryDiscoverPrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("error discovering primary keys: %w", err)
	}
	defer results.Close()

	var keysByTable = make(map[string]*discoveredPrimaryKey)
	for _, row := range results.Values {
		var tableSchema = string(row[0].AsString())
		var tableName = string(row[1].AsString())
		var columnName = string(row[2].AsString())
		var ordinalPosition = int(row[3].AsInt64())
		var dataType = string(row[4].AsString())

		var tableID = tableSchema + "." + tableName
		var keyInfo = keysByTable[tableID]
		if keyInfo == nil {
			keyInfo = &discoveredPrimaryKey{
				Schema:      tableSchema,
				Table:       tableName,
				ColumnTypes: make(map[string]*jsonschema.Schema),
			}
			keysByTable[tableID] = keyInfo
		}
		keyInfo.Columns = append(keyInfo.Columns, columnName)
		if jsonSchema, ok := databaseTypeToJSON[dataType]; ok {
			keyInfo.ColumnTypes[columnName] = jsonSchema
		} else {
			// Assume unknown types are strings, because it's almost always a string.
			// (Or it's an object, but those can't be Flow collection keys anyway)
			keyInfo.ColumnTypes[columnName] = &jsonschema.Schema{Type: "string"}
		}
		if ordinalPosition != len(keyInfo.Columns) {
			return nil, fmt.Errorf("primary key column %q (of table %q) appears out of order", columnName, tableID)
		}
	}

	var keys []*discoveredPrimaryKey
	for _, key := range keysByTable {
		keys = append(keys, key)
	}
	return keys, nil
}

var databaseTypeToJSON = map[string]*jsonschema.Schema{
	"int":      {Type: "integer"},
	"bigint":   {Type: "integer"},
	"smallint": {Type: "integer"},
	"tinyint":  {Type: "integer"},
}

var catalogNameSanitizerRe = regexp.MustCompile(`(?i)[^a-z0-9\-_.]`)

func recommendedCatalogName(schema, table string) string {
	var catalogName string
	// Omit 'default schema' names for Postgres and SQL Server. There is
	// no default schema for MySQL databases.
	if schema == "public" || schema == "dbo" {
		catalogName = table
	} else {
		catalogName = schema + "_" + table
	}
	return catalogNameSanitizerRe.ReplaceAllString(strings.ToLower(catalogName), "_")
}
